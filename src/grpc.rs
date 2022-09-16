use crate::index::Index;
use futures::prelude::*;
use linkerd2_proxy_api::{
    client_policy::{
        self as proto,
        client_policies_server::{ClientPolicies, ClientPoliciesServer},
    },
    destination,
};
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct Server {
    index: Index,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContextToken {
    ns: String,
    node_name: Option<String>,
    pod: String,
}

// === impl Server ===
type BoxWatchStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<proto::ClientPolicy, tonic::Status>> + Send + Sync>>;

impl Server {
    pub fn new(index: Index) -> ClientPoliciesServer<Self> {
        ClientPoliciesServer::new(Self { index })
    }
}

#[async_trait::async_trait]
impl ClientPolicies for Server {
    type GetClientPolicyStream = BoxWatchStream;
    async fn get_client_policy(
        &self,
        req: tonic::Request<destination::GetDestination>,
    ) -> Result<tonic::Response<BoxWatchStream>, tonic::Status> {
        let get = req.into_inner();
        let context = serde_json::from_str::<ContextToken>(&get.context_token).map_err(|err| {
            tonic::Status::invalid_argument(format!("invalid context token: {err}"))
        })?;
        let path = get.path;
        let addr = path.parse::<SocketAddr>().map_err(|err| {
            tonic::Status::invalid_argument(format!(
                "invalid path {path:?}, not a socket address: {err}"
            ))
        })?;
        let (svc_watch, pod_watch) = self
            .index
            .lookup(addr, &context.ns, &context.pod)
            .map_err(|err| tonic::Status::not_found(err.to_string()))?;
        Ok(tonic::Response::new(Box::pin(async_stream::stream! {
            loop {
                let update = {
                    let pod = pod_watch.borrow();
                    let svc = svc_watch.borrow();

                    // TODO: actually translate the service watch + pod metadata
                    // into a client policy (alex, wanna do this part?)
                    Err(tonic::Status::unimplemented("todo"))
                };

                yield update;

                // wait for one of our watches to change
                tokio::select! {
                    _ = svc_watch.changed() => {},
                    _ = pod_watch.changed() => {},
                }
            }
        })))
    }
}
