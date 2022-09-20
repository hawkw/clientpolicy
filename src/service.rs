use crate::{
    client_policy::{self, PolicySet},
    core::{self},
    index, pod,
    route::{self, OutboundHttpRoute},
};
use ahash::AHashMap as HashMap;
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
    pub(crate) port_names: pod::PortMap<String>,
    pub(crate) opaque_ports: pod::PortSet,
    pub http_routes: HashMap<core::InboundHttpRouteRef, OutboundHttpRoute>,
    // client policies by port name
    pub client_policies: HashMap<String, PolicySet>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboundServiceRef {
    Service { name: String, ns: String },
    Default,
}

const OPAQUE_PORTS_ANNOTATION: &str = "config.linkerd.io/opaque-ports";

impl OutboundService {
    pub fn from_resource(index: &mut index::LockedIndex, ns: String, svc: Service) -> Result<Self> {
        let name = svc.name_unchecked();
        let spec = svc.spec.ok_or_else(|| anyhow!("service has no spec"))?;
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
        let port_names = spec
            .ports
            .ok_or_else(|| anyhow!("service does not have any ports"))?
            .into_iter()
            .map(|port| {
                let name = port.name.unwrap_or_else(|| "default".to_string());
                let port = u16::try_from(port.port)
                    .map_err(anyhow::Error::from)
                    .and_then(|port| {
                        NonZeroU16::new(port).ok_or_else(|| anyhow!("0 is not a valid port!"))
                    })
                    .with_context(|| format!("invalid port {name}"))?;
                Ok((port, name))
            })
            .collect::<Result<pod::PortMap<_>>>()?;
        let cluster_addrs = {
            let cluster_ip = spec
                .cluster_ip
                .ok_or_else(|| anyhow!("client policy not supported for headless services"))?
                .parse::<IpAddr>()?;
            port_names
                .keys()
                .map(|&port| SocketAddr::new(cluster_ip, port.into()))
                .collect()
        };

        let fqdn = format!("{name}.{ns}.svc.{}", index.cluster_info.cluster_domain).into();
        let mut svc = OutboundService {
            reference: OutboundServiceRef::Service {
                name,
                ns: ns.clone(),
            },
            fqdn: Some(fqdn),
            port_names,
            opaque_ports,
            cluster_addrs,
            http_routes: Default::default(),
            client_policies: Default::default(),
        };
        svc.reindex_routes(&index.ns_or_default(ns).policies.http_routes);
        svc.reindex_policies(&index.client_policies);
        Ok(svc)
    }

    pub fn policies_for_port(&self, port: NonZeroU16) -> Option<&PolicySet> {
        let port_name = self.port_names.get(&port)?;
        self.client_policies.get(port_name)
    }

    pub fn reindex_policies(&mut self, index: &index::ClientPolicyNsIndex) -> bool {
        let namespace = self.reference.namespace();
        let _span = tracing::info_span!(
            "reindex_policies",
            message = %self.reference.name()
        )
        .entered();

        let mut changed = false;
        for (port, policies) in self.client_policies.iter_mut() {
            let _span = tracing::debug_span!("port", message = %port).entered();
            let target = client_policy::TargetRef::Service {
                name: self.reference.name(),
                namespace,
                port,
            };
            changed |= policies.reindex(target, index);
        }

        for (route_ref, route) in self.http_routes.iter_mut() {
            let name = match route_ref {
                core::InboundHttpRouteRef::Linkerd(ref name) => name.as_ref(),
                core::InboundHttpRouteRef::Default(name) => *name,
            };
            let _span = tracing::debug_span!("route", message = %name).entered();
            changed |= route.client_policies.reindex(
                client_policy::TargetRef::HttpRoute { namespace, name },
                index,
            );
        }

        tracing::info!(changed, "reindexed service policies");
        changed
    }

    pub fn reindex_routes<'r>(
        &mut self,
        routes: impl IntoIterator<Item = (&'r String, &'r route::OutboundRouteBinding)> + 'r,
    ) -> bool {
        let name = self.reference.name();
        let _span = tracing::info_span!("reindex_routes", message = %name).entered();

        let mut changed = false;
        for (route_name, route) in routes {
            let _span = tracing::debug_span!("route", message = %route_name).entered();
            if route.selects_service(name) {
                tracing::debug!("route selects service");
                match self
                    .http_routes
                    .entry(core::InboundHttpRouteRef::Linkerd(route_name.clone()))
                {
                    Entry::Occupied(mut entry) => {
                        if entry.get() == &route.route {
                            tracing::debug!("route unchanged");
                        } else {
                            tracing::debug!("route updated");
                            entry.insert(route.route.clone());
                            changed = true;
                        }
                    }
                    Entry::Vacant(entry) => {
                        tracing::debug!("route added");
                        entry.insert(route.route.clone());
                        changed = true;
                    }
                }
            } else {
                tracing::debug!("route does not select service");
            }
        }

        tracing::info!(changed, "reindexed service HTTPRoutes");
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
