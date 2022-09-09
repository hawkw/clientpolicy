pub use crate::core::http_route::*;
use chrono::{offset::Utc, DateTime};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundHttpRoute {
    pub hostnames: Vec<HostMatch>,
    // TODO(eliza): need a separate outbound rule type eventually...
    pub rules: Vec<InboundHttpRouteRule>,

    /// This is required for ordering returned `HttpRoute`s by their creation
    /// timestamp.
    pub creation_timestamp: Option<DateTime<Utc>>,
}

impl Default for OutboundHttpRoute {
    fn default() -> Self {
        Self {
            hostnames: Vec::new(),
            rules: Vec::new(),
            creation_timestamp: None,
        }
    }
}
