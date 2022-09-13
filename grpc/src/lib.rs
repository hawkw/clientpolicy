#![deny(warnings, rust_2018_idioms)]
#![forbid(unsafe_code)]

use futures::prelude::*;
use linkerd2_proxy_api::{
    client_policy::{self as proto, client_policies_server::ClientPolicies},
    destination,
};

#[derive(Clone, Debug)]
pub struct Server {}

// === impl Server ===
type BoxWatchStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<proto::ClientPolicy, tonic::Status>> + Send + Sync>>;

#[async_trait::async_trait]
impl ClientPolicies for Server {
    type GetClientPolicyStream = BoxWatchStream;
    async fn get_client_policy(
        &self,
        _req: tonic::Request<destination::GetDestination>,
    ) -> Result<tonic::Response<BoxWatchStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("todo"))
    }
}
