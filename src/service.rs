use crate::{
    client_policy::{self, PolicyRef},
    core::{self, ProxyProtocol},
    index, k8s, pod,
    route::OutboundHttpRoute,
    ClusterInfo,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{anyhow, bail, Context, Result};
use k8s_openapi::api::core::v1::Service;
use kube::ResourceExt;
use std::{
    collections::hash_map::Entry,
    fmt,
    net::{IpAddr, SocketAddr},
    num::NonZeroU16,
    sync::Arc,
};

/// Like `linkerd_policy_controller_core::InboundServer`, but...outbound.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboundService {
    pub reference: OutboundServiceRef,
    pub fqdn: Option<Arc<str>>,
    // pub protocol: ProxyProtocol,
    pub cluster_addrs: Vec<SocketAddr>,
    pub ports: HashMap<String, ServicePort>,
    pub http_routes: HashMap<core::InboundHttpRouteRef, OutboundHttpRoute>,
    pub client_policies: HashMap<PolicyRef, client_policy::Spec>,
    pub client_bindings: HashMap<PolicyRef, client_policy::Bound>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OutboundServiceRef {
    Service { name: String, ns: String },
    Default,
}

// #[derive(Debug, Clone, PartialEq)]
// pub struct Spec {
//     pub fqdn: Arc<str>,
//     pub labels: k8s::Labels,
//     pub pod_selector: k8s::labels::Selector,
//     pub ports: HashMap<String, ServicePort>,
// }

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ServicePort {
    pub number: NonZeroU16,
    pub opaque: bool,
}

const OPAQUE_PORTS_ANNOTATION: &str = "config.linkerd.io/opaque-ports";

impl OutboundService {
    pub fn from_resource(config: &ClusterInfo, ns: String, svc: Service) -> Result<Self> {
        let name = svc.name_unchecked();
        let spec = svc.spec.ok_or_else(|| anyhow!("service has no spec"))?;
        // let pod_selector = spec
        //     .selector
        //     .ok_or_else(|| {
        //         anyhow!("service does not select any pods, not a valid target for client policy")
        //     })?
        //     .into_iter()
        //     .collect();
        let opaque_ports = svc
            .metadata
            .annotations
            .and_then(|annotations| {
                annotations.get(OPAQUE_PORTS_ANNOTATION).map(|spec| {
                    parse_portset(spec).unwrap_or_else(|error| {
                        tracing::info!(%spec, %error, "Invalid ports list");
                        Default::default()
                    })
                })
            })
            .unwrap_or_default();
        let ports = spec
            .ports
            .ok_or_else(|| anyhow!("service does not have any ports"))?
            .into_iter()
            .map(|port| {
                let name = port.name.unwrap_or_else(|| "default".to_string());
                let port = u16::try_from(port.port)
                    .map_err(anyhow::Error::from)
                    .and_then(|port| {
                        let number = NonZeroU16::new(port)
                            .ok_or_else(|| anyhow!("0 is not a valid port!"))?;
                        let opaque = opaque_ports.contains(&number);
                        Ok(ServicePort { number, opaque })
                    })
                    .with_context(|| format!("invalid port {name}"))?;
                Ok((name, port))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let cluster_addrs = {
            let cluster_ip = spec
                .cluster_ip
                .ok_or_else(|| anyhow!("client policy not supported for headless services"))?
                .parse::<IpAddr>()?;
            ports
                .values()
                .map(|port| SocketAddr::new(cluster_ip, port.number.into()))
                .collect()
        };

        let fqdn = format!("{name}.{ns}.svc.{}", config.cluster_domain).into();
        // let labels = svc.metadata.labels.into();

        Ok(OutboundService {
            reference: OutboundServiceRef::Service { name, ns },
            fqdn: Some(fqdn),
            ports,
            cluster_addrs,
            http_routes: Default::default(),
            client_policies: Default::default(),
            client_bindings: Default::default(),
        })
    }

    pub fn reindex_policies(&mut self, policies: &index::ClientPolicyNsIndex) -> bool {
        let _span = tracing::info_span!(
            "reindex_policies",
            service = %self.reference.name()
        )
        .entered();
        let mut changed = false;

        let mut unmatched_policies = self.client_policies.keys().cloned().collect::<HashSet<_>>();
        for (policy, spec) in policies.policies_for(&self.reference) {
            let _span = tracing::debug_span!("policy", policy.ns = %policy.namespace, message = %policy.name).entered();
            unmatched_policies.remove(&policy);
            match self.client_policies.entry(policy) {
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
            self.client_policies.remove(&policy);
            changed = true;
        }

        let mut unmatched_bindings = self.client_bindings.keys().cloned().collect::<HashSet<_>>();
        for (binding, spec) in policies.bindings_for(&self.client_policies) {
            let _span = tracing::debug_span!("binding", binding.ns = %binding.namespace, message = %binding.name).entered();
            unmatched_bindings.remove(&binding);
            match self.client_bindings.entry(binding) {
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
            self.client_bindings.remove(&binding);
            changed = true;
        }

        if !changed {
            tracing::debug!("no changes");
        }

        changed
    }
}

/// NOTE: this is copied from the policy-controller's pod module, where it's private.
fn parse_portset(s: &str) -> Result<pod::PortSet> {
    let mut ports = pod::PortSet::default();

    for spec in s.split(',') {
        match spec.split_once('-') {
            None => {
                if !spec.trim().is_empty() {
                    let port = spec.trim().parse().context("parsing port")?;
                    ports.insert(port);
                }
            }
            Some((floor, ceil)) => {
                let floor = floor.trim().parse::<NonZeroU16>().context("parsing port")?;
                let ceil = ceil.trim().parse::<NonZeroU16>().context("parsing port")?;
                if floor > ceil {
                    bail!("Port range must be increasing");
                }
                ports.extend(
                    (u16::from(floor)..=u16::from(ceil)).map(|p| NonZeroU16::try_from(p).unwrap()),
                );
            }
        }
    }

    Ok(ports)
}

impl OutboundServiceRef {
    pub fn namespace(&self) -> &str {
        match self {
            Self::Service { ns, .. } => ns.as_ref(),
            Self::Default => "n/a",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Service { name, .. } => name.as_ref(),
            Self::Default => "default",
        }
    }
}

impl fmt::Display for OutboundServiceRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Service { name, ns } => write!(f, "{}/{}", ns, name),
            Self::Default => write!(f, "default"),
        }
    }
}
