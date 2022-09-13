pub(crate) use linkerd_policy_controller_core as core;
pub(crate) use linkerd_policy_controller_k8s_api as k8s;

use crate::index::Index;
use anyhow::Result;
use clap::Parser;
use client_policy_k8s_api::{
    client_policy::ClientPolicy, client_policy_binding::ClientPolicyBinding,
};
use std::net::SocketAddr;
use std::time::Duration;

mod client_policy;
mod defaults;
pub mod index;
mod pod;
mod route;
mod server;
use ipnet::IpNet;

#[derive(Clone, Debug)]
pub struct ClusterInfo {
    /// The cluster-wide default protocol detection timeout.
    pub default_detect_timeout: Duration,

    /// The networks that probes are expected to be from.
    pub probe_networks: Vec<IpNet>,

    pub default_policy: defaults::DefaultPolicy,
}

#[derive(Parser)]
#[clap(name = "client-policy", version)]
struct Args {
    /// The tracing filter used for logs
    #[clap(
        long,
        env = "LINKERD_CLIENT_POLICY_CONTROLLER_LOG",
        default_value = "info"
    )]
    log_level: kubert::LogFilter,

    /// The logging format
    #[clap(long, default_value = "plain")]
    log_format: kubert::LogFormat,

    #[clap(flatten)]
    client: kubert::ClientArgs,

    #[clap(flatten)]
    admin: kubert::AdminArgs,

    #[clap(long, default_value = "0.0.0.0:8091")]
    grpc_addr: SocketAddr,

    /// Network CIDRs of all expected probes.
    #[clap(long)]
    probe_networks: Option<IpNets>,

    #[clap(long, default_value = "all-unauthenticated")]
    default_policy: defaults::DefaultPolicy,

    /// Dump the current state of the index every `dump_interval_secs`, if
    /// enabled.
    #[clap(long)]
    dump_interval_secs: Option<u64>,
}
const DETECT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        log_level,
        log_format,
        client,
        admin,
        grpc_addr,
        probe_networks,
        dump_interval_secs,
        default_policy,
    } = Args::parse();

    let mut rt = kubert::Runtime::builder()
        .with_log(log_level, log_format)
        .with_admin(admin)
        .with_client(client)
        .build()
        .await?;

    let client = rt.client();

    let client_policies = kube::Api::<ClientPolicy>::all(client.clone())
        .list(&kube::api::ListParams::default())
        .await?;
    for client_policy in client_policies.items.into_iter() {
        tracing::info!(?client_policy, "Look at this cool policy!");
    }

    let client_policy_bindings = kube::Api::<ClientPolicyBinding>::all(client)
        .list(&kube::api::ListParams::default())
        .await?;
    for binding in client_policy_bindings.items.into_iter() {
        tracing::info!(?binding, "Look at this cool policy binding!");
    }

    let probe_networks = probe_networks.map(|IpNets(nets)| nets).unwrap_or_default();
    let index = Index::new(ClusterInfo {
        probe_networks,
        default_detect_timeout: DETECT_TIMEOUT,
        default_policy,
    });

    let indices = index.spawn_index_tasks(&mut rt);

    if let Some(secs) = dump_interval_secs {
        index.dump_index(Duration::from_secs(secs));
    }

    tracing::info!(%grpc_addr, "serving ClientPolicy gRPC API");

    // Run the Kubert indexers.
    rt.run().await?;
    indices.await?;

    Ok(())
}

#[derive(Debug)]
struct IpNets(Vec<IpNet>);

impl std::str::FromStr for IpNets {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        s.split(',')
            .map(|n| n.parse().map_err(Into::into))
            .collect::<Result<Vec<IpNet>>>()
            .map(Self)
    }
}
