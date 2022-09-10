use crate::{
    core,
    k8s::{self, ResourceExt},
    pod,
    route::OutboundHttpRoute,
    server::{OutboundServer, Server},
    ClusterInfo,
};
use ahash::{AHashMap, AHashSet};
use anyhow::Result;
use kubert::client::api::ListParams;
use parking_lot::RwLock;
use std::{
    collections::{
        hash_map::{Entry, HashMap},
        BTreeSet,
    },
    net::{IpAddr, SocketAddr},
    num::NonZeroU16,
    sync::Arc,
};
use tokio::{
    sync::watch,
    time::{Duration, Instant},
};
use tracing::Instrument;

#[derive(Debug)]
pub struct Index {
    index: Arc<RwLock<LockedIndex>>,
}

#[derive(Debug)]
struct LockedIndex {
    cluster_info: Arc<ClusterInfo>,
    namespaces: AHashMap<String, Namespace>,
    servers_by_addr: ServersByAddr,
    changed: Instant,
}

type ServersByAddr = HashMap<SocketAddr, watch::Receiver<OutboundServer>>;

#[derive(Debug)]
struct Namespace {
    pods: AHashMap<String, Pod>,
    policies: PolicyIndex,
}

#[derive(Debug)]
struct Pod {
    meta: pod::Meta,
    ip: IpAddr,
    port_names: AHashMap<String, pod::PortSet>,
    port_servers: pod::PortMap<PodPortServer>,
    probes: pod::PortMap<BTreeSet<String>>,
}

#[derive(Debug)]
struct PolicyIndex {
    cluster_info: Arc<ClusterInfo>,
    servers: HashMap<String, Server>,
}

struct NsUpdate<T> {
    added: Vec<(String, T)>,
    removed: AHashSet<String>,
}

/// Holds the state of a single port on a pod.
#[derive(Debug)]
struct PodPortServer {
    /// The name of the server resource that matches this port. Unset when no
    /// server resources match this pod/port (and, i.e., the default policy is
    /// used).
    name: Option<String>,

    /// A sender used to broadcast pod port server updates.
    tx: watch::Sender<OutboundServer>,

    /// A receiver that is updated when the pod's server is updated.
    rx: watch::Receiver<OutboundServer>,
}
const CONTROL_PLANE_NS_LABEL: &str = "linkerd.io/control-plane-ns";

impl Index {
    pub fn new(cluster: ClusterInfo) -> Self {
        Self {
            index: Arc::new(RwLock::new(LockedIndex {
                cluster_info: Arc::new(cluster),
                namespaces: AHashMap::new(),
                servers_by_addr: HashMap::new(),
                changed: Instant::now(),
            })),
        }
    }

    pub fn index_pods(&self, rt: &mut kubert::Runtime) -> tokio::task::JoinHandle<()> {
        let watch = rt.watch_all::<k8s::Pod>(ListParams::default().labels(CONTROL_PLANE_NS_LABEL));
        let index = kubert::index::namespaced(self.index.clone(), watch)
            .instrument(tracing::info_span!("index_pods"));
        let join = tokio::spawn(index);
        tracing::info!("started pod indexing");
        join
    }

    pub fn index_servers(&self, rt: &mut kubert::Runtime) -> tokio::task::JoinHandle<()> {
        let watch = rt.watch_all::<k8s::policy::Server>(ListParams::default());
        let index = kubert::index::namespaced(self.index.clone(), watch)
            .instrument(tracing::info_span!("index_servers"));
        let join = tokio::spawn(index);
        tracing::info!("started server indexing");
        join
    }

    pub fn dump_index(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        tracing::debug!(?every, "dumping index changes");
        let index = self.index.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(every);
            let mut last_changed = index.read().changed;
            loop {
                interval.tick().await;
                let index = index.read();
                if index.changed >= last_changed {
                    use comfy_table::{presets::UTF8_FULL, *};
                    let mut srvs_by_addr = Table::new();
                    srvs_by_addr
                        .load_preset(UTF8_FULL)
                        .set_content_arrangement(ContentArrangement::Dynamic)
                        .set_width(80)
                        .set_header(Row::from(vec!["ADDRESS", "SERVER", "KIND", "PROTOCOL"]));
                    for (addr, srv) in &index.servers_by_addr {
                        let srv = srv.borrow();
                        let (name, kind) = match srv.reference {
                            core::ServerRef::Server(ref name) => (name.as_str(), "server"),
                            core::ServerRef::Default(name) => (name, "default"),
                        };
                        srvs_by_addr.add_row(Row::from(vec![
                            Cell::new(&addr.to_string()),
                            Cell::new(name),
                            Cell::new(kind),
                            Cell::new(&format!("{:?}", srv.protocol)),
                        ]));
                    }
                    println!("{srvs_by_addr}");
                    last_changed = index.changed;
                }
            }
        })
    }
}

impl LockedIndex {
    fn ns_with_reindex(&mut self, namespace: String, f: impl FnOnce(&mut Namespace) -> bool) {
        if let Entry::Occupied(mut ns) = self.namespaces.entry(namespace) {
            if f(ns.get_mut()) {
                if ns.get().pods.is_empty() {
                    ns.remove();
                } else {
                    ns.get_mut().reindex_servers(&mut self.servers_by_addr);
                }
            }
        }
    }

    // fn ns_or_default(&mut self, namespace: String) -> &mut Namespace {
    //     self.namespaces
    //         .entry(namespace)
    //         .or_insert_with(|| Namespace::new(&self.cluster_info))
    // }

    fn ns_or_default_with_reindex(
        &mut self,
        namespace: String,
        f: impl FnOnce(&mut Namespace) -> bool,
    ) {
        let ns = self
            .namespaces
            .entry(namespace)
            .or_insert_with(|| Namespace::new(&self.cluster_info));
        if f(ns) {
            ns.reindex_servers(&mut self.servers_by_addr);
        }
    }
}

impl kubert::index::IndexNamespacedResource<k8s::Pod> for LockedIndex {
    fn apply(&mut self, pod: k8s::Pod) {
        let ns = pod.namespace().unwrap();
        let name = pod.name_unchecked();
        let _span = tracing::info_span!("apply", ns = %ns, %name).entered();

        let ns = self
            .namespaces
            .entry(ns)
            .or_insert_with(|| Namespace::new(&self.cluster_info));
        if let Err(error) = ns.update_pod(name, pod, &mut self.servers_by_addr) {
            tracing::error!(%error, "illegal pod update");
        } else {
            self.changed = Instant::now();
        }
    }

    #[tracing::instrument(name = "delete", fields(%ns, %name))]
    fn delete(&mut self, ns: String, name: String) {
        if let Entry::Occupied(mut ns) = self.namespaces.entry(ns) {
            if let Some(pod) = ns.get_mut().pods.remove(&name) {
                // if there was a pod, remove its ports from the index of
                // servers by IP.
                for addr in pod.addrs() {
                    self.servers_by_addr.remove(&addr);
                }

                // if there are no more pods in the ns, we can also delete the
                // ns.
                if ns.get().pods.is_empty() {
                    tracing::debug!("namespace has no more pods; removing it");
                    ns.remove();
                }
            }
            tracing::info!("pod deleted");

            self.changed = Instant::now();
        } else {
            tracing::debug!("tried to delete a pod in a namespace that does not exist!");
        }
    }
}

impl kubert::index::IndexNamespacedResource<k8s::policy::Server> for LockedIndex {
    fn apply(&mut self, srv: k8s::policy::Server) {
        let ns = srv.namespace().expect("server must be namespaced");
        let name = srv.name_unchecked();
        let _span = tracing::info_span!("apply", %ns, %name).entered();

        let server = Server::from_resource(srv, &self.cluster_info);
        self.ns_or_default_with_reindex(ns, |ns| ns.policies.update_server(name, server));
        self.changed = Instant::now();
    }

    #[tracing::instrument(name = "delete", skip(self), fields(%ns, %name))]
    fn delete(&mut self, ns: String, name: String) {
        self.ns_with_reindex(ns, |ns| ns.policies.servers.remove(&name).is_some());

        self.changed = Instant::now();
    }

    fn reset(
        &mut self,
        srvs: Vec<k8s::policy::Server>,
        deleted: AHashMap<String, AHashSet<String>>,
    ) {
        let _span = tracing::info_span!("reset").entered();

        // Aggregate all of the updates by namespace so that we only reindex
        // once per namespace.
        type Ns = NsUpdate<Server>;
        let mut updates_by_ns = AHashMap::<String, Ns>::default();
        for srv in srvs.into_iter() {
            let namespace = srv.namespace().expect("server must be namespaced");
            let name = srv.name_unchecked();
            let server = Server::from_resource(srv, &self.cluster_info);
            updates_by_ns
                .entry(namespace)
                .or_default()
                .added
                .push((name, server));
        }
        for (ns, names) in deleted.into_iter() {
            updates_by_ns.entry(ns).or_default().removed = names;
        }

        for (namespace, Ns { added, removed }) in updates_by_ns.into_iter() {
            if added.is_empty() {
                // If there are no live resources in the namespace, we do not
                // want to create a default namespace instance, we just want to
                // clear out all resources for the namespace (and then drop the
                // whole namespace, if necessary).
                self.ns_with_reindex(namespace, |ns| {
                    ns.policies.servers.clear();
                    true
                });
            } else {
                // Otherwise, we take greater care to reindex only when the
                // state actually changed. The vast majority of resets will see
                // no actual data change.
                self.ns_or_default_with_reindex(namespace, |ns| {
                    let mut changed = !removed.is_empty();
                    for name in removed.into_iter() {
                        ns.policies.servers.remove(&name);
                    }
                    for (name, server) in added.into_iter() {
                        changed = ns.policies.update_server(name, server) || changed;
                    }
                    changed
                });
            }
        }

        self.changed = Instant::now();
    }
}

impl Namespace {
    fn new(cluster: &Arc<ClusterInfo>) -> Self {
        Self {
            pods: AHashMap::default(),
            policies: PolicyIndex {
                cluster_info: cluster.clone(),
                servers: HashMap::default(),
            },
        }
    }

    fn reindex_servers(&mut self, servers_by_addr: &mut ServersByAddr) {
        for (_, pod) in &mut self.pods {
            pod.reindex_servers(&mut self.policies, servers_by_addr)
        }
    }

    fn update_pod(
        &mut self,
        name: String,
        pod: k8s::Pod,
        servers_by_addr: &mut ServersByAddr,
    ) -> Result<()> {
        let port_names = pod
            .spec
            .as_ref()
            .map(pod::tcp_ports_by_name)
            .unwrap_or_default();
        let probes = pod
            .spec
            .as_ref()
            .map(pod::pod_http_probes)
            .unwrap_or_default();
        let meta = pod::Meta::from_metadata(pod.metadata);
        let ip = pod
            .status
            .ok_or_else(|| anyhow::format_err!("pod has no status"))?
            .pod_ip
            .ok_or_else(|| anyhow::format_err!("pod has no IP"))?
            .parse::<IpAddr>()?;
        let pod = match self.pods.entry(name.clone()) {
            Entry::Vacant(entry) => Some(entry.insert(Pod {
                meta,
                port_names,
                port_servers: pod::PortMap::default(),
                probes,
                ip,
            })),

            Entry::Occupied(entry) => {
                let pod = entry.into_mut();

                // Pod labels and annotations may change at runtime, but the
                // port list may not
                if pod.port_names != port_names {
                    anyhow::bail!("pod {} port names must not change", name);
                }

                // If there aren't meaningful changes, then don't bother doing
                // any more work.
                if pod.meta == meta && ip == pod.ip {
                    tracing::debug!(pod = %name, "No changes");
                    None
                } else {
                    tracing::debug!(pod = %name, "Updating");
                    pod.meta = meta;
                    pod.ip = ip;
                    Some(pod)
                }
            }
        };

        match pod {
            Some(pod) => {
                tracing::info!("pod updated");
                pod.reindex_servers(&mut self.policies, servers_by_addr);
            }
            None => tracing::info!("pod added"),
        };

        Ok(())
    }
}

impl Pod {
    /// Determines the policies for ports on this pod.
    fn reindex_servers(&mut self, policies: &mut PolicyIndex, servers_by_addr: &mut ServersByAddr) {
        // Keep track of the ports that are already known in the pod so that, after applying server
        // matches, we can ensure remaining ports are set to the default policy.
        let mut unmatched_ports = self.port_servers.keys().copied().collect::<pod::PortSet>();

        // Keep track of which ports have been matched to servers to that we can detect when
        // multiple servers match a single port.
        //
        // We start with capacity for the known ports on the pod; but this can grow if servers
        // select additional ports.
        let mut matched_ports = pod::PortMap::with_capacity_and_hasher(
            unmatched_ports.len(),
            std::hash::BuildHasherDefault::<pod::PortHasher>::default(),
        );

        for (srvname, server) in policies.servers.iter() {
            if server.pod_selector.matches(&self.meta.labels) {
                for port in self.select_ports(&server.port_ref).into_iter() {
                    // If the port is already matched to a server, then log a warning and skip
                    // updating it so it doesn't flap between servers.
                    if let Some(prior) = matched_ports.get(&port) {
                        tracing::warn!(
                            port = %port,
                            server = %prior,
                            conflict = %srvname,
                            "Port already matched by another server; skipping"
                        );
                        continue;
                    }

                    let addr = SocketAddr::new(self.ip, port.into());
                    let probe_paths = self
                        .probes
                        .get(&port)
                        .into_iter()
                        .flatten()
                        .map(|p| p.as_str());
                    let s = policies.outbound_server(srvname.clone(), server, probe_paths);
                    let rx = self.update_server(port, srvname, s);
                    match servers_by_addr.entry(addr) {
                        Entry::Occupied(entry) => assert!(rx.same_channel(entry.get())),
                        Entry::Vacant(entry) => {
                            entry.insert(rx);
                        }
                    }

                    matched_ports.insert(port, srvname.clone());
                    unmatched_ports.remove(&port);
                }
            }
        }

        // Reset all remaining ports to the default policy.
        for port in unmatched_ports.into_iter() {
            let addr = SocketAddr::new(self.ip, port.into());
            let rx = self.set_default_server(port, &policies.cluster_info);
            match servers_by_addr.entry(addr) {
                Entry::Occupied(entry) => assert!(rx.same_channel(entry.get())),
                Entry::Vacant(entry) => {
                    entry.insert(rx);
                }
            }
        }
    }

    /// Enumerates ports.
    ///
    /// A named port may refer to an arbitrary number of port numbers.
    fn select_ports(&mut self, port_ref: &k8s::policy::server::Port) -> Vec<NonZeroU16> {
        use k8s::policy::server::Port;
        match port_ref {
            Port::Number(p) => Some(*p).into_iter().collect(),
            Port::Name(name) => self
                .port_names
                .get(name)
                .into_iter()
                .flatten()
                .cloned()
                .collect(),
        }
    }

    /// Returns an iterator over all the `SocketAddr`s of this pod's ports.
    fn addrs(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        let ports = self.port_servers.keys().chain(self.probes.keys());
        ports.map(|&port| SocketAddr::new(self.ip, port.into()))
    }

    /// Updates a pod-port to use the given named server.
    ///
    /// The name is used explicity (and not derived from the `server` itself) to
    /// ensure that we're not handling a default server.
    fn update_server(
        &mut self,
        port: NonZeroU16,
        name: &str,
        server: OutboundServer,
    ) -> watch::Receiver<OutboundServer> {
        let rx = match self.port_servers.entry(port) {
            Entry::Vacant(entry) => {
                tracing::trace!(port = %port, server = %name, "Creating server");
                let (tx, rx) = watch::channel(server);
                entry
                    .insert(PodPortServer {
                        name: Some(name.to_string()),
                        tx,
                        rx,
                    })
                    .rx
                    .clone()
            }

            Entry::Occupied(mut entry) => {
                let ps = entry.get_mut();

                // Avoid sending redundant updates.
                if ps.name.as_deref() == Some(name) && *ps.rx.borrow() == server {
                    tracing::trace!(port = %port, server = %name, "Skipped redundant server update");
                    tracing::trace!(?server);
                    return ps.rx.clone();
                }

                // If the port's server previously matched a different server,
                // this can either mean that multiple servers currently match
                // the pod:port, or that we're in the middle of an update. We
                // make the opportunistic choice to assume the cluster is
                // configured coherently so we take the update. The admission
                // controller should prevent conflicts.
                tracing::trace!(port = %port, server = %name, "Updating server");
                ps.name = Some(name.to_string());
                ps.tx.send(server).expect("a receiver is held by the index");
                ps.rx.clone()
            }
        };

        tracing::debug!(port = %port, server = %name, "Updated server");
        rx
    }

    /// Updates a pod-port to use the given named server.
    fn set_default_server(
        &mut self,
        port: NonZeroU16,
        config: &ClusterInfo,
    ) -> watch::Receiver<OutboundServer> {
        let server = Self::default_outbound_server(
            port,
            &self.meta.settings,
            self.probes
                .get(&port)
                .into_iter()
                .flatten()
                .map(|p| p.as_str()),
            config,
        );
        match self.port_servers.entry(port) {
            Entry::Vacant(entry) => {
                tracing::debug!(%port, "Creating default server");
                let (tx, rx) = watch::channel(server);
                let rx2 = rx.clone();
                entry.insert(PodPortServer { name: None, tx, rx });
                rx2
            }

            Entry::Occupied(mut entry) => {
                let ps = entry.get_mut();

                // Avoid sending redundant updates.
                if *ps.rx.borrow() == server {
                    tracing::trace!(%port, "Default server already set");
                    return ps.rx.clone();
                }

                tracing::debug!(%port, "Setting default server");
                ps.name = None;
                ps.tx.send(server).expect("a receiver is held by the index");
                ps.rx.clone()
            }
        }
    }

    fn default_outbound_server<'p>(
        port: NonZeroU16,
        settings: &pod::Settings,
        probe_paths: impl Iterator<Item = &'p str>,
        config: &ClusterInfo,
    ) -> OutboundServer {
        let protocol = if settings.opaque_ports.contains(&port) {
            core::ProxyProtocol::Opaque
        } else {
            core::ProxyProtocol::Detect {
                timeout: config.default_detect_timeout,
            }
        };
        let policy = settings.default_policy.unwrap_or(config.default_policy);

        let http_routes = config.default_outbound_http_routes(probe_paths);

        OutboundServer {
            reference: core::ServerRef::Default(policy.as_str()),
            protocol,
            http_routes,
        }
    }
}

impl PolicyIndex {
    fn update_server(&mut self, name: String, server: Server) -> bool {
        match self.servers.entry(name.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(server);
            }
            Entry::Occupied(entry) => {
                let srv = entry.into_mut();
                if *srv == server {
                    tracing::debug!(server = %name, "no changes");
                    return false;
                }
                tracing::debug!(server = %name, "updating");
                *srv = server;
            }
        }
        true
    }

    fn outbound_server<'p>(
        &self,
        name: String,
        server: &Server,
        probe_paths: impl Iterator<Item = &'p str>,
    ) -> OutboundServer {
        tracing::trace!(%name, ?server, "Creating outbound server");
        let http_routes = self.http_routes(&name, probe_paths);

        OutboundServer {
            reference: core::ServerRef::Server(name),
            protocol: server.protocol.clone(),
            http_routes,
        }
    }

    fn http_routes<'p>(
        &self,
        server_name: &str,
        probe_paths: impl Iterator<Item = &'p str>,
    ) -> HashMap<core::InboundHttpRouteRef, OutboundHttpRoute> {
        // TODO(eliza): actually index routes...
        // let routes = self
        //     .http_routes
        //     .iter()
        //     .filter(|(_, route)| route.selects_server(server_name))
        //     .map(|(name, route)| {
        //         let mut route = route.route.clone();
        //         route.authorizations = self.route_client_authzs(name, authentications);
        //         (InboundHttpRouteRef::Linkerd(name.clone()), route)
        //     })
        //     .collect::<HashMap<_, _>>();
        // if !routes.is_empty() {
        //     return routes;
        // }
        self.cluster_info.default_outbound_http_routes(probe_paths)
    }
}

// === imp NsUpdate ===

impl<T> Default for NsUpdate<T> {
    fn default() -> Self {
        Self {
            added: vec![],
            removed: Default::default(),
        }
    }
}

impl ClusterInfo {
    fn default_outbound_http_routes<'p>(
        &self,
        probe_paths: impl Iterator<Item = &'p str>,
    ) -> HashMap<core::InboundHttpRouteRef, OutboundHttpRoute> {
        use crate::route::*;
        let mut routes = HashMap::with_capacity(2);

        // If no routes are defined for the server, use a default route that
        // matches all requests. Default authorizations are instrumented on
        // the server.
        routes.insert(
            core::InboundHttpRouteRef::Default("default"),
            OutboundHttpRoute::default(),
        );

        // If there are no probe networks, there are no probe routes to
        // authorize.
        if self.probe_networks.is_empty() {
            return routes;
        }

        // Generate an `Exact` path match for each probe path defined on the
        // pod.
        let matches: Vec<HttpRouteMatch> = probe_paths
            .map(|path| HttpRouteMatch {
                path: Some(PathMatch::Exact(path.to_string())),
                headers: vec![],
                query_params: vec![],
                method: Some(Method::GET),
            })
            .collect();

        // If there are no matches, then are no probe routes to authorize.
        if matches.is_empty() {
            return routes;
        }

        let probe_route = OutboundHttpRoute {
            hostnames: Vec::new(),
            rules: vec![InboundHttpRouteRule {
                matches,
                filters: Vec::new(),
            }],
            creation_timestamp: None,
        };
        routes.insert(core::InboundHttpRouteRef::Default("probe"), probe_route);

        routes
    }
}