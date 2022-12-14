use crate::{
    client_policy::{Filter, PolicySet},
    index::Index,
    pod::Meta,
    route::OutboundHttpRoute,
    service::OutboundService,
};
use futures::prelude::*;
use linkerd2_proxy_api::{
    client_policy::{
        self as proto,
        client_policies_server::{ClientPolicies, ClientPoliciesServer},
        proxy_protocol::{Detect, Kind},
        ProxyProtocol,
    },
    destination, http_route, meta,
};
use linkerd_policy_controller_core::{
    http_route::{
        HeaderMatch, HostMatch, HttpRouteMatch, InboundHttpRouteRule, PathMatch, QueryParamMatch,
    },
    InboundHttpRouteRef,
};
use std::{net::SocketAddr, num::NonZeroU16};
use tracing_futures::Instrument;

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
        let (mut svc_watch, mut pod_watch) = self
            .index
            .lookup(addr, &context.ns, &context.pod)
            .map_err(|err| tonic::Status::not_found(err.to_string()))?;
        let port = NonZeroU16::new(addr.port())
            .ok_or_else(|| tonic::Status::invalid_argument("port must not be zero"))?;
        let stream = async_stream::stream! {
            tracing::debug!("started get_client_policy stream");
            loop {
                let update = {
                    let pod = pod_watch.borrow_and_update();
                    let svc = svc_watch.borrow_and_update();

                    // TODO(eliza): do the same for route policies...

                    to_client_policy(&svc, port, &pod)
                };

                yield update;

                // wait for one of our watches to change
                tokio::select! {
                    _ = svc_watch.changed() => {},
                    _ = pod_watch.changed() => {},
                }
            }
        };
        Ok(tonic::Response::new(Box::pin(stream.instrument(
            tracing::debug_span!("get_client_policy", %addr, ?context.pod, ?context.ns),
        ))))
    }
}

fn to_client_policy(
    svc: &OutboundService,
    port: NonZeroU16,
    pod: &Meta,
) -> Result<proto::ClientPolicy, tonic::Status> {
    let http_routes = svc
        .http_routes
        .iter()
        .map(|(route_ref, route)| to_http_route(route_ref, route, pod))
        .collect::<Result<_, _>>()?;

    // is there a service-level policy for this port that's
    // bound to the client pod?
    let filters = match svc.policies_for_port(port) {
        Some(policies) => to_filters(policies, pod)?,
        None => {
            tracing::debug!(port, "no client policies for this service target port");
            Vec::new()
        }
    };

    Ok(proto::ClientPolicy {
        fully_qualified_name: svc.fqdn.as_ref().map(|s| s.to_string()).unwrap_or_default(),
        endpoint: None,
        protocol: Some(ProxyProtocol {
            kind: Some(Kind::Detect(Detect {
                timeout: Default::default(),
                http_routes,
            })),
        }),
        filters,
    })
}

fn to_http_route(
    reference: &InboundHttpRouteRef,
    OutboundHttpRoute {
        hostnames,
        client_policies,
        rules,
        creation_timestamp: _,
    }: &OutboundHttpRoute,
    pod: &Meta,
) -> Result<proto::HttpRoute, tonic::Status> {
    let metadata = meta::Metadata {
        kind: Some(match reference {
            InboundHttpRouteRef::Default(name) => meta::metadata::Kind::Default(name.to_string()),
            InboundHttpRouteRef::Linkerd(name) => meta::metadata::Kind::Resource(meta::Resource {
                group: "policy.linkerd.io".to_string(),
                kind: "HTTPRoute".to_string(),
                name: name.to_string(),
            }),
        }),
    };

    let hosts = hostnames.iter().map(convert_host_match).collect();

    let rules = rules
        .iter()
        .map(|rule| convert_rule(client_policies, rule, pod))
        .collect::<Result<_, _>>()?;

    Ok(proto::HttpRoute {
        metadata: Some(metadata),
        hosts,
        rules,
    })
}

fn convert_host_match(h: &HostMatch) -> http_route::HostMatch {
    http_route::HostMatch {
        r#match: Some(match h {
            HostMatch::Exact(host) => http_route::host_match::Match::Exact(host.clone()),
            HostMatch::Suffix { reverse_labels } => {
                http_route::host_match::Match::Suffix(http_route::host_match::Suffix {
                    reverse_labels: reverse_labels.to_vec(),
                })
            }
        }),
    }
}

fn to_filters(policies: &PolicySet, pod: &Meta) -> Result<Vec<proto::Filter>, tonic::Status> {
    // TODO(eliza): also find route policies

    let filters = policies
        .policies_for(pod)
        .flat_map(|spec| spec.filters.iter())
        .map(convert_filter)
        .collect::<Result<_, _>>()?;
    Ok(filters)
}

fn convert_filter(filter: &Filter) -> Result<proto::Filter, tonic::Status> {
    match filter {
        Filter::Timeout(t) => {
            let t = (*t).try_into().map_err(|e| {
                tonic::Status::internal(format!("Failed to convert timeout duration: {}", e))
            })?;

            Ok(proto::Filter {
                filter: Some(proto::filter::Filter::Timeout(t)),
            })
        }
    }
}

fn convert_match(
    HttpRouteMatch {
        headers,
        path,
        query_params,
        method,
    }: &HttpRouteMatch,
) -> http_route::HttpRouteMatch {
    let headers = headers
        .iter()
        .map(|hm| match hm {
            HeaderMatch::Exact(name, value) => http_route::HeaderMatch {
                name: name.to_string(),
                value: Some(http_route::header_match::Value::Exact(
                    value.as_bytes().to_vec(),
                )),
            },
            HeaderMatch::Regex(name, re) => http_route::HeaderMatch {
                name: name.to_string(),
                value: Some(http_route::header_match::Value::Regex(re.to_string())),
            },
        })
        .collect();

    let path = path.as_ref().map(|path| http_route::PathMatch {
        kind: Some(match path {
            PathMatch::Exact(path) => http_route::path_match::Kind::Exact(path.clone()),
            PathMatch::Prefix(prefix) => http_route::path_match::Kind::Prefix(prefix.clone()),
            PathMatch::Regex(regex) => http_route::path_match::Kind::Regex(regex.to_string()),
        }),
    });

    let query_params = query_params
        .iter()
        .map(|qpm| match qpm {
            QueryParamMatch::Exact(name, value) => http_route::QueryParamMatch {
                name: name.clone(),
                value: Some(http_route::query_param_match::Value::Exact(value.clone())),
            },
            QueryParamMatch::Regex(name, re) => http_route::QueryParamMatch {
                name: name.clone(),
                value: Some(http_route::query_param_match::Value::Regex(re.to_string())),
            },
        })
        .collect();

    http_route::HttpRouteMatch {
        headers,
        path,
        query_params,
        method: method.clone().map(Into::into),
    }
}

fn convert_rule(
    client_policies: &PolicySet,
    InboundHttpRouteRule {
        matches,
        filters: _, // These are the inbound policy filters and we should ignore them.
    }: &InboundHttpRouteRule,
    pod: &Meta,
) -> Result<proto::http_route::Rule, tonic::Status> {
    Ok(proto::http_route::Rule {
        matches: matches.iter().map(convert_match).collect(),
        filters: to_filters(client_policies, pod)?,
    })
}
