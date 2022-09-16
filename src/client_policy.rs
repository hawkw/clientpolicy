use crate::{
    core, index,
    k8s::{self, policy::HttpRoute},
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{anyhow, ensure, Context, Error};
pub use client_policy::HttpFailureClassification;
use client_policy_k8s_api::{
    client_policy::{self, HttpClientPolicy},
    client_policy_binding::ClientPolicyBinding,
    NamespacedTargetRef,
};
use k8s_openapi::api;
use kube::ResourceExt;
use std::{collections::hash_map::Entry, convert::TryFrom, fmt, time::Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct Spec {
    pub name: String,
    pub target: Target,
    pub failure_classification: HttpFailureClassification,
    pub filters: Vec<Filter>,
}

/// Binds a set of client policies to client pods.
#[derive(Clone, Debug, PartialEq)]
pub struct Binding {
    pub policies: Vec<PolicyRef>,
    pub client_pod_selector: k8s::labels::Selector,
}

/// Binds a set of client policies to client pods.
#[derive(Clone, Debug, PartialEq)]
pub struct Bound {
    pub policies: Vec<Spec>,
    pub client_pod_selector: k8s::labels::Selector,
}

/// The set of client policies and bindings on a `Service` or `HttpRoute`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct PolicySet {
    pub policies: HashMap<PolicyRef, Spec>,
    pub bindings: HashMap<PolicyRef, Bound>,
}

#[derive(Hash, Clone, Debug, PartialEq, Eq)]
pub struct PolicyRef {
    pub name: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Target {
    /// This policy's parent ref is a `Service`.
    Service {
        name: String,
        namespace: String,
        port: String,
    },

    /// This policy's parent ref is an `HTTPRoute`.
    HttpRoute { name: String, namespace: String },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TargetRef<'a> {
    /// This policy's parent ref is a `Service`.
    Service {
        name: &'a str,
        namespace: &'a str,
        port: &'a str,
    },

    /// This policy's parent ref is an `HTTPRoute`.
    HttpRoute { name: &'a str, namespace: &'a str },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Filter {
    Timeout(Duration),
}

// === impl ClientPolicy ===

impl Spec {
    pub fn target_ns(&self) -> &str {
        match self.target {
            Target::Service { ref namespace, .. } => namespace,
            Target::HttpRoute { ref namespace, .. } => namespace,
        }
    }

    pub fn selects(&self, target_ref: TargetRef<'_>) -> bool {
        match (target_ref, &self.target) {
            (
                TargetRef::Service {
                    name: n,
                    namespace: ns,
                    port: p,
                },
                Target::Service {
                    ref name,
                    ref namespace,
                    ref port,
                },
            ) => name == n && namespace == ns && port == p,
            (
                TargetRef::HttpRoute {
                    name: n,
                    namespace: ns,
                },
                Target::HttpRoute {
                    ref name,
                    ref namespace,
                },
            ) => name == n && namespace == ns,
            _ => false,
        }
    }
}

impl TryFrom<HttpClientPolicy> for Spec {
    type Error = Error;

    fn try_from(policy: HttpClientPolicy) -> Result<Self, Self::Error> {
        let ns = policy
            .namespace()
            .ok_or_else(|| anyhow!("HttpClientPolicy must be namespaced"))?;
        let name = policy.name_unchecked();
        let target = Target::try_from_target_ref(ns, policy.spec.target_ref)?;
        // XXX(eliza): it would be good to validate that these are real http
        // status ranges, but "meh".
        let failure_classification = policy.spec.failure_classification;
        let filters = policy
            .spec
            .filters
            .into_iter()
            .map(Filter::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            name,
            target,
            failure_classification,
            filters,
        })
    }
}

// === impl Binding ===

impl TryFrom<ClientPolicyBinding> for Binding {
    type Error = Error;

    fn try_from(binding: ClientPolicyBinding) -> Result<Self, Self::Error> {
        let ns = binding
            .namespace()
            .ok_or_else(|| anyhow!("ClientPolicyBinding must be namespaced"))?;
        let name = binding.name_unchecked();
        let policy_refs = binding.spec.policy_refs.into_iter().map(|policy_ref| {
            ensure!(policy_ref.targets_kind::<HttpClientPolicy>(), "ClientPolicyBinding {name} included a policy ref {policy_ref:?} that does not target an HTTPClientPolicy!");
            Ok(PolicyRef {
                name: policy_ref.name,
                namespace: policy_ref.namespace.unwrap_or_else(|| ns.clone()),
            })
        }).collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            policies: policy_refs,
            client_pod_selector: binding.spec.pod_selector,
        })
    }
}

// === impl Target ===

impl Target {
    fn try_from_target_ref(ns: String, target: NamespacedTargetRef) -> Result<Self, Error> {
        match target {
            t if t.targets_kind::<api::core::v1::Service>() => {
                let port = t.port.ok_or_else(|| anyhow!("a ClientPolicy targeting a Service must include the target port in its targetRef"))?;
                Ok(Target::Service {
                    name: t.name,
                    namespace: t.namespace.unwrap_or(ns),
                    port,
                })
            }
            t if t.targets_kind::<HttpRoute>() => Ok(Target::HttpRoute {
                name: t.name,
                namespace: t.namespace.unwrap_or(ns),
            }),
            _ => Err(anyhow!(
                "a ClientPolicy must target either a Service or an HTTPRoute"
            )),
        }
    }
}

// === impl Filter ===

impl TryFrom<client_policy::HttpClientPolicyFilter> for Filter {
    type Error = Error;

    fn try_from(filter: client_policy::HttpClientPolicyFilter) -> Result<Self, Self::Error> {
        match filter {
            client_policy::HttpClientPolicyFilter::Timeout { timeout } => {
                humantime::parse_duration(&timeout)
                    .map(Filter::Timeout)
                    .with_context(|| format!("invalid timeout duration '{timeout}'"))
            }
        }
    }
}

// === impl PolicySet ===

impl PolicySet {
    pub fn reindex(&mut self, target: TargetRef<'_>, index: &index::ClientPolicyNsIndex) -> bool {
        let mut changed = false;
        let mut unmatched_policies = self.policies.keys().cloned().collect::<HashSet<_>>();
        for (policy, spec) in index.policies_for(target) {
            let _span = tracing::debug_span!("policy", policy.ns = %policy.namespace, message = %policy.name).entered();
            unmatched_policies.remove(&policy);
            match self.policies.entry(policy) {
                Entry::Occupied(mut entry) => {
                    if entry.get() != spec {
                        tracing::debug!("updated policy");
                        entry.insert(spec.clone());
                        changed = true;
                    }
                }
                Entry::Vacant(entry) => {
                    tracing::debug!("added policy");
                    entry.insert(spec.clone());
                    changed = true;
                }
            }
        }
        for policy in unmatched_policies {
            tracing::debug!(policy.ns = %policy.namespace, %policy.name, "removed policy");
            self.policies.remove(&policy);
            changed = true;
        }

        let mut unmatched_bindings = self.bindings.keys().cloned().collect::<HashSet<_>>();
        for (binding, spec) in index.bindings_for(&self.policies) {
            let _span = tracing::debug_span!("binding", binding.ns = %binding.namespace, message = %binding.name).entered();
            unmatched_bindings.remove(&binding);
            match self.bindings.entry(binding) {
                Entry::Occupied(mut entry) => {
                    if entry.get() != &spec {
                        tracing::debug!("updated policy binding");
                        entry.insert(spec);
                        changed = true;
                    }
                }
                Entry::Vacant(entry) => {
                    tracing::debug!("added policy binding");
                    entry.insert(spec);
                    changed = true;
                }
            }
        }

        for binding in unmatched_bindings {
            tracing::debug!(binding.ns = %binding.namespace, %binding.name, "removed policy binding");
            self.bindings.remove(&binding);
            changed = true;
        }

        if !changed {
            tracing::debug!("no changes");
        }

        changed
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::Service {
                name,
                namespace,
                port,
            } => write!(f, "Service {namespace}/{name}:{port}"),
            Target::HttpRoute { name, namespace } => write!(f, "HTTPRoute {namespace}/{name}"),
        }
    }
}
