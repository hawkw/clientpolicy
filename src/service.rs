use crate::{k8s, ClusterInfo};
use ahash::AHashMap;
use anyhow::{anyhow, Context, Result};
use k8s_openapi::api::core::v1::Service;
use kube::ResourceExt;
use std::{num::NonZeroU16, sync::Arc};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Spec {
    pub fqdn: Arc<str>,
    pub labels: k8s::Labels,
    pub pod_selector: k8s::labels::Selector,
    pub ports: AHashMap<String, NonZeroU16>,
}

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
        let ports = spec
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
