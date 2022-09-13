pub(crate) use crate::core::http_route::*;
use crate::{
    client_policy,
    k8s::{self, policy::httproute as policy},
};
use anyhow::{anyhow, Error, Result};
use chrono::{offset::Utc, DateTime};
use k8s_gateway_api as api;
use k8s_openapi::api::core::v1::Service;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OutboundHttpRoute {
    pub hostnames: Vec<HostMatch>,
    // TODO(eliza): need a separate outbound rule type eventually...
    pub rules: Vec<InboundHttpRouteRule>,
    pub client_policy: Option<client_policy::Spec>,

    /// This is required for ordering returned `HttpRoute`s by their creation
    /// timestamp.
    pub creation_timestamp: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OutboundRouteBinding {
    pub parents: Vec<OutboundParentRef>,
    pub route: OutboundHttpRoute,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboundParentRef {
    Service(String),
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum InvalidParentRef {
    #[error("HTTPRoute resource does not reference a Service resource")]
    DoesNotSelectService,

    #[error("HTTPRoute resource may not reference a parent Service in an other namespace")]
    ServiceInAnotherNamespace,

    #[error("HTTPRoute resource may not reference a parent by port")]
    SpecifiesPort,

    #[error("HTTPRoute resource may not reference a parent by section name")]
    SpecifiesSection,
}

impl TryFrom<policy::HttpRoute> for OutboundRouteBinding {
    type Error = Error;

    fn try_from(route: policy::HttpRoute) -> Result<Self, Self::Error> {
        let route_ns = route.metadata.namespace.as_deref();
        let creation_timestamp = route.metadata.creation_timestamp.map(|k8s::Time(t)| t);
        let parents = OutboundParentRef::collect_from(route_ns, route.spec.inner.parent_refs)?;
        let hostnames = route
            .spec
            .hostnames
            .into_iter()
            .flatten()
            .map(convert::host_match)
            .collect();

        let rules = route
            .spec
            .rules
            .into_iter()
            .flatten()
            .map(|policy::HttpRouteRule { matches, filters }| Self::try_rule(matches, filters))
            .collect::<Result<_>>()?;

        Ok(OutboundRouteBinding {
            parents,
            route: OutboundHttpRoute {
                hostnames,
                rules,
                creation_timestamp,
                client_policy: None,
            },
        })
    }
}

impl OutboundRouteBinding {
    #[inline]
    pub fn selects_service(&self, name: &str) -> bool {
        self.parents
            .iter()
            .any(|p| matches!(p, OutboundParentRef::Service(n) if n == name))
    }

    fn try_rule<F>(
        matches: Option<Vec<api::HttpRouteMatch>>,
        filters: Option<Vec<F>>,
    ) -> Result<InboundHttpRouteRule> {
        let matches = matches
            .into_iter()
            .flatten()
            .map(Self::try_match)
            .collect::<Result<_>>()?;

        // TODO(eliza): how will we figure out what route rules are applied on
        // the outbound side? Are we even gonna do that?
        // let filters = filters
        //     .into_iter()
        //     .flatten()
        //     .map(try_filter)
        //     .collect::<Result<_>>()?;

        Ok(InboundHttpRouteRule {
            matches,
            filters: Vec::new(),
        })
    }

    fn try_match(
        api::HttpRouteMatch {
            path,
            headers,
            query_params,
            method,
        }: api::HttpRouteMatch,
    ) -> Result<HttpRouteMatch> {
        let path = path.map(convert::path_match).transpose()?;

        let headers = headers
            .into_iter()
            .flatten()
            .map(convert::header_match)
            .collect::<Result<_>>()?;

        let query_params = query_params
            .into_iter()
            .flatten()
            .map(convert::query_param_match)
            .collect::<Result<_>>()?;

        let method = method.as_deref().map(Method::try_from).transpose()?;

        Ok(HttpRouteMatch {
            path,
            headers,
            query_params,
            method,
        })
    }
}

impl OutboundParentRef {
    fn collect_from(
        route_ns: Option<&str>,
        parent_refs: Option<Vec<api::ParentReference>>,
    ) -> Result<Vec<Self>, InvalidParentRef> {
        let parents = parent_refs
            .into_iter()
            .flatten()
            .filter_map(|parent_ref| Self::from_parent_ref(route_ns, parent_ref))
            .collect::<Result<Vec<_>, InvalidParentRef>>()?;

        // If there are no valid parents, then the route is invalid.
        if parents.is_empty() {
            return Err(InvalidParentRef::DoesNotSelectService);
        }

        Ok(parents)
    }

    fn from_parent_ref(
        route_ns: Option<&str>,
        parent_ref: api::ParentReference,
    ) -> Option<Result<Self, InvalidParentRef>> {
        // Skip parent refs that don't target a `Server` resource.
        if !policy::parent_ref_targets_kind::<Service>(&parent_ref) || parent_ref.name.is_empty() {
            return None;
        }

        let api::ParentReference {
            group: _,
            kind: _,
            namespace,
            name,
            section_name,
            port,
        } = parent_ref;

        if namespace.is_some() && namespace.as_deref() != route_ns {
            return Some(Err(InvalidParentRef::ServiceInAnotherNamespace));
        }
        if port.is_some() {
            return Some(Err(InvalidParentRef::SpecifiesPort));
        }
        if section_name.is_some() {
            return Some(Err(InvalidParentRef::SpecifiesSection));
        }

        Some(Ok(OutboundParentRef::Service(name)))
    }
}

pub mod convert {
    use super::*;

    pub fn path_match(path_match: api::HttpPathMatch) -> Result<PathMatch> {
        match path_match {
            api::HttpPathMatch::Exact { value } | api::HttpPathMatch::PathPrefix { value }
                if !value.starts_with('/') =>
            {
                Err(anyhow!("HttpPathMatch paths must be absolute (begin with `/`); {value:?} is not an absolute path"))
            }
            api::HttpPathMatch::Exact { value } => Ok(PathMatch::Exact(value)),
            api::HttpPathMatch::PathPrefix { value } => Ok(PathMatch::Prefix(value)),
            api::HttpPathMatch::RegularExpression { value } => value
                .parse()
                .map(PathMatch::Regex)
                .map_err(Into::into),
        }
    }

    pub fn host_match(hostname: api::Hostname) -> HostMatch {
        if hostname.starts_with("*.") {
            let mut reverse_labels = hostname
                .split('.')
                .skip(1)
                .map(|label| label.to_string())
                .collect::<Vec<String>>();
            reverse_labels.reverse();
            HostMatch::Suffix { reverse_labels }
        } else {
            HostMatch::Exact(hostname)
        }
    }

    pub fn header_match(header_match: api::HttpHeaderMatch) -> Result<HeaderMatch> {
        match header_match {
            api::HttpHeaderMatch::Exact { name, value } => {
                Ok(HeaderMatch::Exact(name.parse()?, value.parse()?))
            }
            api::HttpHeaderMatch::RegularExpression { name, value } => {
                Ok(HeaderMatch::Regex(name.parse()?, value.parse()?))
            }
        }
    }

    pub fn query_param_match(query_match: api::HttpQueryParamMatch) -> Result<QueryParamMatch> {
        match query_match {
            api::HttpQueryParamMatch::Exact { name, value } => {
                Ok(QueryParamMatch::Exact(name, value))
            }
            api::HttpQueryParamMatch::RegularExpression { name, value } => {
                Ok(QueryParamMatch::Exact(name, value.parse()?))
            }
        }
    }
}
