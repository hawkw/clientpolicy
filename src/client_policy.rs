use crate::{
    core,
    k8s::policy::{self, HttpRoute, NamespacedTargetRef},
};
use anyhow::{anyhow, Context, Error};
use client_policy_k8s_api::client_policy as k8s;
pub use k8s::FailureClassification;
use kube::ResourceExt;
use std::{convert::TryFrom, time::Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct Spec {
    pub name: String,
    pub target: Target,
    pub failure_classification: FailureClassification,
    pub filters: Vec<Filter>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Target {
    /// This policy's parent ref is a `Server`.
    Server { name: String, namespace: String },

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
            Target::Server { ref namespace, .. } => namespace,
            Target::HttpRoute { ref namespace, .. } => namespace,
        }
    }

    pub fn selects_server(&self, ns: &str, srv: &str) -> bool {
        match self.target {
            Target::Server {
                ref namespace,
                ref name,
            } => ns == namespace && srv == name,
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

impl TryFrom<k8s::ClientPolicy> for Spec {
    type Error = Error;

    fn try_from(policy: k8s::ClientPolicy) -> Result<Self, Self::Error> {
        let ns = policy
            .namespace()
            .ok_or_else(|| anyhow!("ClientPolicy must be namespaced"))?;
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

// === impl Target ===

impl Target {
    fn try_from_target_ref(ns: String, target: NamespacedTargetRef) -> Result<Self, Error> {
        match target {
            t if t.targets_kind::<policy::Server>() => Ok(Target::Server {
                name: t.name,
                namespace: t.namespace.unwrap_or(ns),
            }),
            t if t.targets_kind::<HttpRoute>() => Ok(Target::HttpRoute {
                name: t.name,
                namespace: t.namespace.unwrap_or(ns),
            }),
            _ => Err(anyhow!(
                "a ClientPolicy must target either a Server or an HTTPRoute"
            )),
        }
    }
}

// === impl Filter ===

impl TryFrom<k8s::ClientPolicyFilter> for Filter {
    type Error = Error;

    fn try_from(filter: k8s::ClientPolicyFilter) -> Result<Self, Self::Error> {
        match filter {
            k8s::ClientPolicyFilter::Timeout { timeout } => humantime::parse_duration(&timeout)
                .map(Filter::Timeout)
                .with_context(|| format!("invalid timeout duration '{timeout}'")),
        }
    }
}
