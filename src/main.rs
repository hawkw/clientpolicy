use anyhow::Result;
use clap::Parser;
use client_policy_k8s_api::{
    client_policy::ClientPolicy, client_policy_binding::ClientPolicyBinding,
};
use std::net::SocketAddr;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        log_level,
        log_format,
        client,
        admin,
        grpc_addr,
    } = Args::parse();

    let rt = kubert::Runtime::builder()
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

    tracing::info!(%grpc_addr, "serving ClientPolicy gRPC API");

    Ok(())
}
