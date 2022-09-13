use crate::{
    client_policy,
    core::{self, ProxyProtocol},
    k8s, pod,
    route::OutboundHttpRoute,
    ClusterInfo,
};
use ahash::AHashMap;
use anyhow::{anyhow, bail, Context, Result};
use k8s_openapi::api::core::v1::Service;
use kube::ResourceExt;
use std::{collections::HashMap, num::NonZeroU16, sync::Arc};

/// Like `linkerd_policy_controller_core::InboundServer`, but...outbound.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboundService {
    pub reference: OutboundServiceRef,
    pub fqdn: Option<Arc<str>>,

    pub protocol: ProxyProtocol,
    pub http_routes: HashMap<core::InboundHttpRouteRef, OutboundHttpRoute>,
    pub client_policy: Option<client_policy::Spec>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OutboundServiceRef {
    Service(String),
    Default,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Spec {
    pub fqdn: Arc<str>,
    pub labels: k8s::Labels,
    pub pod_selector: k8s::labels::Selector,
    pub ports: AHashMap<String, ServicePort>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ServicePort {
    pub number: NonZeroU16,
    pub opaque: bool,
}

const OPAQUE_PORTS_ANNOTATION: &str = "config.linkerd.io/opaque-ports";

impl Spec {
    pub fn from_resource(config: &ClusterInfo, ns: &String, svc: Service) -> Result<Self> {
        let name = svc.name_unchecked();
        let spec = svc.spec.ok_or_else(|| anyhow!("service has no spec"))?;
        let pod_selector = spec
            .selector
            .ok_or_else(|| {
                anyhow!("service does not select any pods, not a valid target for client policy")
            })?
            .into_iter()
            .collect();
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
            .collect::<Result<AHashMap<_, _>>>()?;
        let fqdn = format!("{name}.{ns}.svc.{}", config.cluster_domain).into();
        let labels = svc.metadata.labels.into();

        Ok(Self {
            fqdn,
            labels,
            pod_selector,
            ports,
        })
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
    pub fn as_str(&self) -> &str {
        match self {
            Self::Service(name) => name.as_ref(),
            Self::Default => "default",
        }
    }
}
