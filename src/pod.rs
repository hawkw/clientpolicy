use crate::k8s;
use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hasher},
    num::NonZeroU16,
};

/// Holds pod metadata
#[derive(Debug, PartialEq)]
pub struct Meta {
    /// The pod's labels. Used by `Server` pod selectors.
    pub labels: k8s::Labels,
}

// /// Per-pod settings, as configured by the pod's annotations.
// #[derive(Debug, Default, PartialEq)]
// pub(crate) struct Settings {
//     // pub require_id_ports: PortSet,
//     pub opaque_ports: PortSet,
//     pub default_policy: Option<DefaultPolicy>,
// }

/// A `HashSet` specialized for ports.
///
/// Because ports are `u16` values, this type avoids the overhead of actually
/// hashing ports.
pub(crate) type PortSet = HashSet<NonZeroU16, BuildHasherDefault<PortHasher>>;

/// A `HashMap` specialized for ports.
///
/// Because ports are `NonZeroU16` values, this type avoids the overhead of
/// actually hashing ports.
pub(crate) type PortMap<V> = HashMap<NonZeroU16, V, BuildHasherDefault<PortHasher>>;

/// A hasher for ports.
///
/// Because ports are single `NonZeroU16` values, we don't have to hash them; we can just use
/// the integer values as hashes directly.
///
/// Borrowed from the proxy.
#[derive(Debug, Default)]
pub(crate) struct PortHasher(u16);

// === impl Meta ===

impl Meta {
    pub(crate) fn from_metadata(meta: k8s::ObjectMeta) -> Self {
        Self {
            labels: meta.labels.into(),
        }
    }
}

// === impl PortHasher ===

impl Hasher for PortHasher {
    fn write(&mut self, _: &[u8]) {
        unreachable!("hashing a `u16` calls `write_u16`");
    }

    #[inline]
    fn write_u16(&mut self, port: u16) {
        self.0 = port;
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0 as u64
    }
}

// pub(crate) fn tcp_ports_by_name(spec: &k8s::PodSpec) -> AHashMap<String, PortSet> {
//     let mut ports = AHashMap::<String, PortSet>::default();
//     for (port, name) in spec
//         .containers
//         .iter()
//         .flat_map(|c| c.ports.iter().flatten())
//         .filter_map(named_tcp_port)
//     {
//         ports.entry(name.to_string()).or_default().insert(port);
//     }
//     ports
// }

// /// Gets the container probe ports for a Pod.
// ///
// /// The result is a mapping for each probe port exposed by a container in the
// /// Pod and the paths for which probes are expected.
// pub(crate) fn pod_http_probes(pod: &k8s::PodSpec) -> PortMap<BTreeSet<String>> {
//     let mut probes = PortMap::<BTreeSet<String>>::default();
//     for (port, path) in pod.containers.iter().flat_map(container_http_probe_paths) {
//         probes.entry(port).or_default().insert(path);
//     }
//     probes
// }

// fn container_http_probe_paths(
//     container: &k8s::Container,
// ) -> impl Iterator<Item = (NonZeroU16, String)> + '_ {
//     fn find_by_name(name: &str, ports: &[k8s::ContainerPort]) -> Option<NonZeroU16> {
//         for (p, n) in ports.iter().filter_map(named_tcp_port) {
//             if n.eq_ignore_ascii_case(name) {
//                 return Some(p);
//             }
//         }
//         None
//     }

//     fn get_port(port: &k8s::IntOrString, container: &k8s::Container) -> Option<NonZeroU16> {
//         match port {
//             k8s::IntOrString::Int(p) => u16::try_from(*p).ok()?.try_into().ok(),
//             k8s::IntOrString::String(n) => find_by_name(n, container.ports.as_ref()?),
//         }
//     }

//     (container.liveness_probe.iter())
//         .chain(container.readiness_probe.iter())
//         .chain(container.startup_probe.iter())
//         .filter_map(|p| {
//             let probe = p.http_get.as_ref()?;
//             let port = get_port(&probe.port, container)?;
//             let path = probe.path.clone().unwrap_or_else(|| "/".to_string());
//             Some((port, path))
//         })
// }

// fn named_tcp_port(port: &k8s::ContainerPort) -> Option<(NonZeroU16, &str)> {
//     if let Some(ref proto) = port.protocol {
//         if !proto.eq_ignore_ascii_case("TCP") {
//             return None;
//         }
//     }
//     let p = u16::try_from(port.container_port)
//         .and_then(NonZeroU16::try_from)
//         .ok()?;
//     let n = port.name.as_deref()?;
//     Some((p, n))
// }

// impl Settings {
//     /// Reads pod settings from the pod metadata including:
//     ///
//     /// - Opaque ports
//     /// - Ports that require identity
//     /// - The pod's default policy
//     pub(crate) fn from_metadata(meta: &k8s::ObjectMeta) -> Self {
//         let anns = match meta.annotations.as_ref() {
//             None => return Self::default(),
//             Some(anns) => anns,
//         };

//         let default_policy = default_policy(anns).unwrap_or_else(|error| {
//             tracing::warn!(%error, "invalid default policy annotation value");
//             None
//         });

//         let opaque_ports = ports_annotation(anns, "config.linkerd.io/opaque-ports");
//         // let require_id_ports = ports_annotation(
//         //     anns,
//         //     "config.linkerd.io/proxy-require-identity-inbound-ports",
//         // );

//         Self {
//             default_policy,
//             opaque_ports,
//             // require_id_ports,
//         }
//     }
// }

// /// Attempts to read a default policy override from an annotation map.
// fn default_policy(
//     ann: &std::collections::BTreeMap<String, String>,
// ) -> Result<Option<DefaultPolicy>> {
//     if let Some(v) = ann.get("config.linkerd.io/default-inbound-policy") {
//         let mode = v.parse()?;
//         return Ok(Some(mode));
//     }

//     Ok(None)
// }

// /// Reads `annotation` from the provided set of annotations, parsing it as a port set.  If the
// /// annotation is not set or is invalid, the empty set is returned.
// fn ports_annotation(
//     annotations: &std::collections::BTreeMap<String, String>,
//     annotation: &str,
// ) -> PortSet {
//     annotations
//         .get(annotation)
//         .map(|spec| {
//             parse_portset(spec).unwrap_or_else(|error| {
//                 tracing::info!(%spec, %error, %annotation, "Invalid ports list");
//                 Default::default()
//             })
//         })
//         .unwrap_or_default()
// }

// /// Read a comma-separated of ports or port ranges from the given string.
// fn parse_portset(s: &str) -> Result<PortSet> {
//     let mut ports = PortSet::default();

//     for spec in s.split(',') {
//         match spec.split_once('-') {
//             None => {
//                 if !spec.trim().is_empty() {
//                     let port = spec.trim().parse().context("parsing port")?;
//                     ports.insert(port);
//                 }
//             }
//             Some((floor, ceil)) => {
//                 let floor = floor.trim().parse::<NonZeroU16>().context("parsing port")?;
//                 let ceil = ceil.trim().parse::<NonZeroU16>().context("parsing port")?;
//                 if floor > ceil {
//                     bail!("Port range must be increasing");
//                 }
//                 ports.extend(
//                     (u16::from(floor)..=u16::from(ceil)).map(|p| NonZeroU16::try_from(p).unwrap()),
//                 );
//             }
//         }
//     }

//     Ok(ports)
// }
