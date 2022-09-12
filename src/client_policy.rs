use crate::k8s::policy::{self, HttpRoute, NamespacedTargetRef, Server};
use anyhow::{Context, Error};
use client_policy_k8s_api::client_policy as k8s;
pub use k8s::FailureClassification;
use std::{convert::TryFrom, time::Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct ClientPolicy {
    pub target: Target,
    pub failure_classification: FailureClassification,
    pub filters: Vec<Filter>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Target {
    /// This policy's parent ref is a `Server`.
    Server {
        name: String,
        namespace: Option<String>,
    },

    /// This policy's parent ref is an `HTTPRoute`.
    HttpRoute {
        name: String,
        namespace: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Filter {
    Timeout(Duration),
}

// === impl ClientPolicy ===

impl TryFrom<k8s::ClientPolicy> for ClientPolicy {
    type Error = Error;

    fn try_from(policy: k8s::ClientPolicy) -> Result<Self, Self::Error> {
        let target = Target::try_from(policy.spec.target_ref)?;
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
            target,
            failure_classification,
            filters,
        })
    }
}

// === impl Target ===

impl TryFrom<NamespacedTargetRef> for Target {
    type Error = Error;

    fn try_from(target_ref: NamespacedTargetRef) -> Result<Self, Self::Error> {
        match target_ref {
            t if t.targets_kind::<Server>() => Ok(Target::Server {
                name: t.name,
                namespace: t.namespace,
            }),
            t if t.targets_kind::<HttpRoute>() => Ok(Target::HttpRoute {
                name: t.name,
                namespace: t.namespace,
            }),
            _ => Err(anyhow::anyhow!(
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
