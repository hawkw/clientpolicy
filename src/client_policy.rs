use crate::{
    core,
    k8s::{
        self,
        policy::{HttpRoute, NamespacedTargetRef},
    },
};
use ahash::AHashMap;
use anyhow::{anyhow, ensure, Context, Error};
pub use client_policy::HttpFailureClassification;
use client_policy_k8s_api::{
    client_policy::{self, HttpClientPolicy},
    client_policy_binding::ClientPolicyBinding,
};
use k8s_openapi::api;
use kube::ResourceExt;
use std::{convert::TryFrom, time::Duration};

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

#[derive(Hash, Clone, Debug, PartialEq, Eq)]
pub struct PolicyRef {
    pub name: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Target {
    /// This policy's parent ref is a `Service`.
    Service { name: String, namespace: String },

    /// This policy's parent ref is an `HTTPRoute`.
    HttpRoute { name: String, namespace: String },
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

    pub fn selects_service(&self, ns: &str, svc: &str) -> bool {
        match self.target {
            Target::Service {
                ref namespace,
                ref name,
            } => ns == namespace && svc == name,
            Target::HttpRoute { .. } => false,
        }
    }

    pub fn selects_route(&self, ns: &str, route: &core::InboundHttpRouteRef) -> bool {
        match (&self.target, route) {
            (
                Target::HttpRoute {
                    ref namespace,
                    ref name,
                },
                core::InboundHttpRouteRef::Linkerd(route),
            ) => ns == namespace && route == name,
            // TODO(eliza): should we allow a ClientPolicy to target a default route?
            (_, _) => false,
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
            ensure!(policy_ref.targets_kind::<HttpClientPolicy>(), "ClientPolicyBinding {name} included a policy ref that does not target an HTTPClientPolicy!");
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
            t if t.targets_kind::<api::core::v1::Service>() => Ok(Target::Service {
                name: t.name,
                namespace: t.namespace.unwrap_or(ns),
            }),
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
