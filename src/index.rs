use crate::{
    client_policy, core,
    k8s::{self, ResourceExt},
    pod,
    route::{OutboundHttpRoute, OutboundRouteBinding},
    service::{self, OutboundService, OutboundServiceRef},
    ClusterInfo,
};
use ahash::{AHashMap as HashMap, AHashSet};
use anyhow::{anyhow, Result};
use client_policy_k8s_api::{
    client_policy::HttpClientPolicy, client_policy_binding::ClientPolicyBinding,
};
use futures::TryFutureExt;
use k8s_openapi::api::core::v1::Service;
use kubert::client::api::ListParams;
use parking_lot::RwLock;
use std::{
    collections::{hash_map::Entry, BTreeSet},
    future::Future,
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
    namespaces: HashMap<String, Namespace>,
    client_policies: ClientPolicyNsIndex,
    services_by_addr: ServicesByAddr,
    changed: Instant,
}

type ServicesByAddr = HashMap<SocketAddr, watch::Receiver<OutboundService>>;

/// Holds all `ClientPolicy` indices by-namespace.
///
/// This is separate from `NamespaceIndex` because client policies may reference
/// target resources across namespaces.
#[derive(Debug, Default)]
pub struct ClientPolicyNsIndex {
    by_ns: HashMap<String, ClientPolicyIndex>,
}

#[derive(Debug, Default)]
struct ClientPolicyIndex {
    policies: HashMap<String, client_policy::Spec>,
    bindings: HashMap<String, client_policy::Binding>,
}

#[derive(Debug)]
struct Namespace {
    name: Arc<String>,
    pods: HashMap<String, Pod>,
    policies: PolicyIndex,
}

#[derive(Debug)]
struct Pod {
    meta: pod::Meta,
    ip: IpAddr,
    port_names: HashMap<String, pod::PortSet>,
    // port_servers: pod::PortMap<PodPortServer>,
    probes: pod::PortMap<BTreeSet<String>>,
}

#[derive(Debug)]
struct PolicyIndex {
    namespace: Arc<String>,
    cluster_info: Arc<ClusterInfo>,
    services: HashMap<String, watch::Sender<OutboundService>>,
    http_routes: HashMap<String, OutboundRouteBinding>,
}

struct NsUpdate<T> {
    added: Vec<(String, T)>,
    removed: AHashSet<String>,
}
const CONTROL_PLANE_NS_LABEL: &str = "linkerd.io/control-plane-ns";

impl Index {
    pub fn new(cluster: ClusterInfo) -> Self {
        Self {
            index: Arc::new(RwLock::new(LockedIndex {
                cluster_info: Arc::new(cluster),
                namespaces: HashMap::new(),
                services_by_addr: HashMap::new(),
                changed: Instant::now(),
                client_policies: ClientPolicyNsIndex::default(),
            })),
        }
    }

    /// Looks up the [`OutboundService`] for a given [`SocketAddr`], returning a
    /// `watch` on that address' outbound policy.
    ///
    /// This is the main entrypoint to the index that's used by the gRPC API
    /// server.
    pub fn addr_server_rx(&self, addr: SocketAddr) -> Result<watch::Receiver<OutboundService>> {
        self.index
            .read()
            .services_by_addr
            .get(&addr)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no server found for address {}", addr))
    }

    pub fn spawn_index_tasks(&self, rt: &mut kubert::Runtime) -> impl Future<Output = Result<()>> {
        // let pods = self.index_pods(rt);
        let services = self.index_resource::<Service>(rt);
        let routes = self.index_resource::<k8s::policy::HttpRoute>(rt);
        let policies = self.index_resource::<HttpClientPolicy>(rt);
        let bindings = self.index_resource::<ClientPolicyBinding>(rt);
        async move {
            tokio::try_join! {
                // pods,
                services, routes, policies, bindings,
            }
            .map(|_| ())
        }
    }

    // fn index_pods(&self, rt: &mut kubert::Runtime) -> impl Future<Output = Result<()>> {
    //     let watch = rt.watch_all::<k8s::Pod>(ListParams::default().labels(CONTROL_PLANE_NS_LABEL));
    //     let index = kubert::index::namespaced(self.index.clone(), watch)
    //         .instrument(tracing::info_span!("index", kind = %"Pod"));

    //     let join = tokio::spawn(index);
    //     tracing::info!("started Pod indexing");
    //     join.map_err(|err| anyhow!("index task for Pods failed: {err}"))
    // }

    fn index_resource<R>(&self, rt: &mut kubert::Runtime) -> impl Future<Output = Result<()>>
    where
        R: kube::Resource + serde::de::DeserializeOwned + Clone + std::fmt::Debug + Send + 'static,
        R::DynamicType: Default,
        LockedIndex: kubert::index::IndexNamespacedResource<R>,
    {
        let kind = std::any::type_name::<R>().split("::").last().unwrap();
        let watch = rt.watch_all::<R>(ListParams::default());
        let index = kubert::index::namespaced(self.index.clone(), watch)
            .instrument(tracing::info_span!("index", %kind));
        let join = tokio::spawn(index);
        tracing::info!("started {kind} indexing");
        join.map_err(move |err| anyhow!("index task for {kind}s failed: {err}"))
    }

    pub fn dump_index(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        tracing::debug!(?every, "dumping index changes");
        let index = self.index.clone();

        fn route_name(route: &core::InboundHttpRouteRef) -> &str {
            match route {
                core::InboundHttpRouteRef::Default(name) => name,
                core::InboundHttpRouteRef::Linkerd(name) => name.as_str(),
            }
        }

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(every);
            let mut last_changed = index.read().changed;
            loop {
                interval.tick().await;
                let index = index.read();
                if index.changed >= last_changed {
                    use comfy_table::{presets::UTF8_FULL, *};
                    let mut svcs_by_addr = Table::new();
                    svcs_by_addr
                        .load_preset(UTF8_FULL)
                        .set_content_arrangement(ContentArrangement::Dynamic)
                        .set_header(Row::from(vec![
                            "ADDRESS",
                            "SERVICE",
                            "FQDN",
                            "POLICY BINDINGS",
                        ]));
                    for (addr, svc) in &index.services_by_addr {
                        let svc = svc.borrow();
                        // let route_list = svc
                        //     .http_routes
                        //     .keys()
                        //     .map(route_name)
                        //     .collect::<Vec<_>>()
                        //     .join("\n");
                        let bindings = svc
                            .client_bindings
                            .iter()
                            .map(|(&client_policy::PolicyRef { ref name, .. }, bound)| {
                                use std::fmt::Write;
                                let mut policies = String::new();
                                for policy in bound.policies.iter() {
                                    write!(&mut policies, "\n - {policy:?}");
                                }
                                format!("{name}: {:?}\n{policies}", bound.client_pod_selector)
                            })
                            .collect::<Vec<_>>()
                            .join("\n");

                        svcs_by_addr.add_row(Row::from(vec![
                            Cell::new(&addr.to_string()),
                            Cell::new(svc.reference.to_string()),
                            Cell::new(format!("{:?}", svc.fqdn)),
                            Cell::new(bindings),
                        ]));
                    }

                    let mut policies = Table::new();
                    policies
                        .load_preset(UTF8_FULL)
                        .set_content_arrangement(ContentArrangement::Dynamic)
                        .set_header(Row::from(vec![
                            "NAMESPACE",
                            "POLICY",
                            "FAILURE CLASSIFICATION",
                            "FILTERS",
                        ]));
                    let policies_by_ns =
                        index
                            .client_policies
                            .by_ns
                            .iter()
                            .flat_map(|(ns_name, ns)| {
                                ns.policies
                                    .iter()
                                    .map(move |(name, policy)| (ns_name, name, policy))
                            });
                    for (ns, name, policy) in policies_by_ns {
                        policies.add_row(Row::from(vec![
                            Cell::new(&ns),
                            Cell::new(name),
                            Cell::new(&format!("{:?}", policy.failure_classification)),
                            Cell::new(format!("{:?}", policy.filters)),
                        ]));
                    }
                    println!("{svcs_by_addr}\n{policies}");
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
                    ns.get_mut().reindex_policies(&self.client_policies);
                }

                self.changed = Instant::now();
            }
        }
    }

    fn ns_or_default(&mut self, namespace: String) -> &mut Namespace {
        self.namespaces
            .entry(namespace.clone())
            .or_insert_with(|| Namespace::new(namespace, &self.cluster_info))
    }

    fn ns_or_default_with_reindex(
        &mut self,
        namespace: String,
        f: impl FnOnce(&mut Namespace) -> bool,
    ) {
        let ns = self
            .namespaces
            .entry(namespace.clone())
            .or_insert_with(|| Namespace::new(namespace, &self.cluster_info));
        if f(ns) {
            ns.reindex_policies(&self.client_policies);

            self.changed = Instant::now();
        }
    }

    fn reindex_all(&mut self) {
        tracing::debug!("Reindexing all namespaces");
        for ns in self.namespaces.values_mut() {
            ns.reindex_policies(&self.client_policies);
        }

        self.changed = Instant::now();
    }
}

// impl kubert::index::IndexNamespacedResource<k8s::Pod> for LockedIndex {
//     fn apply(&mut self, pod: k8s::Pod) {
//         let ns = pod.namespace().unwrap();
//         let name = pod.name_unchecked();
//         let _span = tracing::info_span!("apply", ns = %ns, %name).entered();

//         let ns = self
//             .namespaces
//             .entry(ns.clone())
//             .or_insert_with(|| Namespace::new(ns, &self.cluster_info));
//         if let Err(error) =
//             ns.update_pod(name, pod, &self.client_policies, &mut self.services_by_addr)
//         {
//             tracing::error!(%error, "illegal pod update");
//         }
//     }

//     #[tracing::instrument(name = "delete", fields(%ns, %name))]
//     fn delete(&mut self, ns: String, name: String) {
//         if let Entry::Occupied(mut ns) = self.namespaces.entry(ns) {
//             if let Some(pod) = ns.get_mut().pods.remove(&name) {
//                 // if there was a pod, remove its ports from the index of
//                 // servers by IP.
//                 for addr in pod.addrs() {
//                     self.services_by_addr.remove(&addr);
//                 }

//                 // if there are no more pods in the ns, we can also delete the
//                 // ns.
//                 if ns.get().pods.is_empty() {
//                     tracing::debug!("namespace has no more pods; removing it");
//                     ns.remove();
//                 }
//             }
//             tracing::info!("pod deleted");

//             self.changed = Instant::now();
//         } else {
//             tracing::debug!("tried to delete a pod in a namespace that does not exist!");
//         }
//     }
// }

impl kubert::index::IndexNamespacedResource<Service> for LockedIndex {
    fn apply(&mut self, svc: Service) {
        let ns = svc.namespace().expect("service must be namespaced");
        let name = svc.name_unchecked();
        let _span = tracing::info_span!("apply", %ns, %name).entered();
        let svc = match OutboundService::from_resource(&self.cluster_info, ns.clone(), svc) {
            Ok(svc) => svc,
            Err(error) => {
                tracing::warn!(%error, "invalid Service");
                return;
            }
        };
        if let Some(rx) = self.ns_or_default(ns).policies.update_service(name, svc) {
            for &addr in &rx.borrow().cluster_addrs {
                match self.services_by_addr.entry(addr) {
                    Entry::Vacant(entry) => {
                        entry.insert(rx.clone());
                    }
                    Entry::Occupied(entry) => {
                        assert!(entry.get().same_channel(&rx));
                    }
                }
            }
        }
    }

    #[tracing::instrument(name = "delete", skip(self), fields(%ns, %name))]
    fn delete(&mut self, ns: String, name: String) {
        match self.namespaces.entry(ns) {
            Entry::Vacant(_) => {
                tracing::debug!("tried to delete services in a namespace that does not exist!");
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().policies.services.remove(&name);
                tracing::info!("deleted service");
                if entry.get().is_empty() {
                    tracing::info!("namespace empty");
                    entry.remove();
                }
            }
        }
    }

    fn reset(&mut self, svcs: Vec<Service>, deleted: HashMap<String, AHashSet<String>>) {
        let _span = tracing::info_span!("reset").entered();

        for svc in svcs.into_iter() {
            let ns = svc.namespace().expect("service must be namespaced");
            let name = svc.name_unchecked();
            let _span = tracing::info_span!("apply", %ns, %name).entered();
            let svc = match OutboundService::from_resource(&self.cluster_info, ns.clone(), svc) {
                Ok(svc) => svc,
                Err(error) => {
                    tracing::warn!(%error, "invalid Service");
                    continue;
                }
            };
            if let Some(rx) = self.ns_or_default(ns).policies.update_service(name, svc) {
                for &addr in &rx.borrow().cluster_addrs {
                    match self.services_by_addr.entry(addr) {
                        Entry::Vacant(entry) => {
                            entry.insert(rx.clone());
                        }
                        Entry::Occupied(entry) => {
                            assert!(entry.get().same_channel(&rx));
                        }
                    }
                }
            }
        }
        for (ns, names) in deleted.into_iter() {
            let _span = tracing::info_span!("delete", %ns).entered();
            match self.namespaces.entry(ns) {
                Entry::Vacant(_) => {
                    tracing::debug!(
                        ?names,
                        "tried to delete services in a namespace that does not exist!"
                    );
                }
                Entry::Occupied(mut entry) => {
                    for service in names {
                        tracing::info!(
                            %service, "deleted",
                        );
                        entry.get_mut().policies.services.remove(&service);
                    }
                    if entry.get().is_empty() {
                        tracing::info!("namespace empty");
                        entry.remove();
                    }
                }
            }
        }
    }
}

impl kubert::index::IndexNamespacedResource<k8s::policy::HttpRoute> for LockedIndex {
    fn apply(&mut self, route: k8s::policy::HttpRoute) {
        let ns = route.namespace().expect("HttpRoute must have a namespace");
        let name = route.name_unchecked();
        let _span = tracing::info_span!("apply", %ns, %name).entered();

        let route_binding = match route.try_into() {
            Ok(binding) => binding,
            Err(error) => {
                tracing::info!(%ns, %name, %error, "Ignoring HTTPRoute");
                return;
            }
        };

        self.ns_or_default_with_reindex(ns, |ns| ns.policies.update_http_route(name, route_binding))
    }

    fn reset(
        &mut self,
        routes: Vec<k8s::policy::HttpRoute>,
        deleted: kubert::index::NamespacedRemoved,
    ) {
        let _span = tracing::info_span!("reset").entered();

        // Aggregate all of the updates by namespace so that we only reindex
        // once per namespace.
        type Ns = NsUpdate<OutboundRouteBinding>;
        let mut updates_by_ns = HashMap::<String, Ns>::default();
        for route in routes.into_iter() {
            let namespace = route.namespace().expect("HttpRoute must be namespaced");
            let name = route.name_unchecked();
            let route_binding = match route.try_into() {
                Ok(binding) => binding,
                Err(error) => {
                    tracing::info!(ns = %namespace, %name, %error, "Ignoring HTTPRoute");
                    continue;
                }
            };
            updates_by_ns
                .entry(namespace)
                .or_default()
                .added
                .push((name, route_binding));
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
                    ns.policies.http_routes.clear();
                    true
                });
            } else {
                // Otherwise, we take greater care to reindex only when the
                // state actually changed. The vast majority of resets will see
                // no actual data change.
                self.ns_or_default_with_reindex(namespace, |ns| {
                    let mut changed = !removed.is_empty();
                    for name in removed.into_iter() {
                        ns.policies.http_routes.remove(&name);
                    }
                    for (name, route_binding) in added.into_iter() {
                        changed = ns.policies.update_http_route(name, route_binding) || changed;
                    }
                    changed
                });
            }
        }
    }

    fn delete(&mut self, ns: String, name: String) {
        let _span = tracing::info_span!("delete", %ns, %name).entered();
        self.ns_with_reindex(ns, |ns| ns.policies.http_routes.remove(&name).is_some())
    }
}

impl kubert::index::IndexNamespacedResource<HttpClientPolicy> for LockedIndex {
    fn apply(&mut self, policy: HttpClientPolicy) {
        let ns = policy
            .namespace()
            .expect("HttpClientPolicy must have a namespace");
        let name = policy.name_unchecked();
        let _span = tracing::info_span!("apply", %ns, %name).entered();
        let spec = match client_policy::Spec::try_from(policy) {
            Ok(spec) => spec,
            Err(error) => {
                tracing::warn!(%error, "invalid HttpClientPolicy");
                return;
            }
        };

        if self.client_policies.update_policy(ns, name, spec) {
            self.reindex_all()
        }
    }

    fn reset(
        &mut self,
        policies: Vec<HttpClientPolicy>,
        deleted: kubert::index::NamespacedRemoved,
    ) {
        let _span = tracing::info_span!("reset").entered();
        let mut changed = false;
        for policy in policies.into_iter() {
            let ns = policy
                .namespace()
                .expect("HttpClientPolicy must have a namespace");
            let name = policy.name_unchecked();
            let spec = match client_policy::Spec::try_from(policy) {
                Ok(spec) => spec,
                Err(error) => {
                    tracing::warn!(%ns, %name, %error, "invalid HttpClientPolicy");
                    continue;
                }
            };
            changed |= self.client_policies.update_policy(ns, name, spec);
        }
        for (ns_name, names) in deleted.into_iter() {
            let _span = tracing::debug_span!("delete", ns = %ns_name).entered();
            if let Entry::Occupied(mut ns) = self.client_policies.by_ns.entry(ns_name) {
                for name in names.into_iter() {
                    ns.get_mut().policies.remove(&name);
                    tracing::debug!(%name, "deleting HttpClientPolicy");
                }
                if ns.get().is_empty() {
                    tracing::debug!("namespace has no HttpClientPolicies, removing it");
                    ns.remove();
                }
            }
        }

        if changed {
            self.reindex_all()
        }
    }

    #[tracing::instrument(skip(self), fields(%ns, %name))]
    fn delete(&mut self, ns: String, name: String) {
        if let Entry::Occupied(mut ns) = self.client_policies.by_ns.entry(ns) {
            tracing::debug!("deleting HttpClientPolicy");
            ns.get_mut().policies.remove(&name);
            if ns.get().is_empty() {
                tracing::debug!("namespace has no client policies, deleting it");
                ns.remove();
            }
            self.reindex_all();
        } else {
            tracing::warn!("namespace already deleted!");
        }
    }
}

impl kubert::index::IndexNamespacedResource<ClientPolicyBinding> for LockedIndex {
    fn apply(&mut self, binding: ClientPolicyBinding) {
        let ns = binding
            .namespace()
            .expect("ClientPolicyBinding must have a namespace");
        let name = binding.name_unchecked();
        let _span = tracing::info_span!("apply", %ns, %name).entered();
        let binding = match client_policy::Binding::try_from(binding) {
            Ok(spec) => spec,
            Err(error) => {
                tracing::warn!(%error, "invalid ClientPolicyBinding");
                return;
            }
        };

        if self.client_policies.update_binding(ns, name, binding) {
            self.reindex_all()
        }
    }

    fn reset(
        &mut self,
        policies: Vec<ClientPolicyBinding>,
        deleted: kubert::index::NamespacedRemoved,
    ) {
        let _span = tracing::info_span!("reset").entered();
        let mut changed = false;
        for policy in policies.into_iter() {
            let ns = policy
                .namespace()
                .expect("ClientPolicyBinding must have a namespace");
            let name = policy.name_unchecked();
            let spec = match client_policy::Binding::try_from(policy) {
                Ok(spec) => spec,
                Err(error) => {
                    tracing::warn!(%ns, %name, %error, "invalid ClientPolicyBinding");
                    continue;
                }
            };
            changed |= self.client_policies.update_binding(ns, name, spec);
        }
        for (ns_name, names) in deleted.into_iter() {
            let _span = tracing::debug_span!("delete", ns = %ns_name).entered();
            if let Entry::Occupied(mut ns) = self.client_policies.by_ns.entry(ns_name) {
                for name in names.into_iter() {
                    ns.get_mut().policies.remove(&name);
                    tracing::debug!(%name, "deleting ClientPolicyBinding");
                }
                if ns.get().is_empty() {
                    tracing::debug!("namespace has no client policies, removing it");
                    ns.remove();
                }
            }
        }

        if changed {
            self.reindex_all()
        }
    }

    #[tracing::instrument(skip(self), fields(%ns, %name))]
    fn delete(&mut self, ns: String, name: String) {
        if let Entry::Occupied(mut ns) = self.client_policies.by_ns.entry(ns) {
            tracing::debug!("deleting HttpClientPolicy");
            ns.get_mut().policies.remove(&name);
            if ns.get().is_empty() {
                tracing::debug!("namespace has no client policies, deleting it");
                ns.remove();
            }
            self.reindex_all();
        } else {
            tracing::warn!("namespace already deleted!");
        }
    }
}

impl Namespace {
    fn new(name: String, cluster: &Arc<ClusterInfo>) -> Self {
        let name = Arc::new(name);
        Self {
            name: name.clone(),
            pods: HashMap::default(),
            policies: PolicyIndex {
                namespace: name,
                cluster_info: cluster.clone(),
                services: HashMap::default(),
                http_routes: HashMap::default(),
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.pods.is_empty() && self.policies.is_empty()
    }

    fn reindex_policies(&mut self, client_policies: &ClientPolicyNsIndex) {
        let _span = tracing::info_span!("reindex_policies", ns = %self.name).entered();
        for (name, svc) in &mut self.policies.services {
            svc.send_if_modified(|svc| {
                if svc.reindex_policies(client_policies) {
                    tracing::info!(service = %name, "reindexed policies for");
                    true
                } else {
                    false
                }
            });
        }
    }

    // fn update_pod(
    //     &mut self,
    //     name: String,
    //     pod: k8s::Pod,
    //     client_policies: &ClientPolicyNsIndex,
    //     services_by_addr: &mut ServicesByAddr,
    // ) -> Result<()> {
    //     let port_names = pod
    //         .spec
    //         .as_ref()
    //         .map(pod::tcp_ports_by_name)
    //         .unwrap_or_default();
    //     let probes = pod
    //         .spec
    //         .as_ref()
    //         .map(pod::pod_http_probes)
    //         .unwrap_or_default();
    //     let meta = pod::Meta::from_metadata(pod.metadata);
    //     let ip = pod
    //         .status
    //         .ok_or_else(|| anyhow::format_err!("pod has no status"))?
    //         .pod_ip
    //         .ok_or_else(|| anyhow::format_err!("pod has no IP"))?
    //         .parse::<IpAddr>()?;
    //     let pod = match self.pods.entry(name.clone()) {
    //         Entry::Vacant(entry) => {
    //             let mut pod = Pod {
    //                 meta,
    //                 port_names: Default::default(),
    //                 port_servers: pod::PortMap::default(),
    //                 probes,
    //                 ip,
    //             };
    //             // XXX(eliza): here is where we populate the `services_by_addr`
    //             // map with *all* the pod's default servers. this is different
    //             // from what the policy-controller does: when a pod has only the
    //             // default servers, the watch sender isn't added to the index
    //             // until someone actually looks it up. that would be more
    //             // efficient, but...nobody's actually doing lookups right now,
    //             // so we populate the map eagerly so that we can generate a
    //             // nice-looking table.
    //             //
    //             // if we end up using this code to serve real lookups, we should
    //             // probably create these default servers lazily instead.
    //             for &port in port_names.values().flatten() {
    //                 let server = pod.default_outbound_server(port, &self.policies.cluster_info);
    //                 let rx = pod.set_default_server(port, server);
    //                 services_by_addr.insert(SocketAddr::new(ip, port.into()), rx);
    //             }
    //             pod.port_names = port_names;
    //             Some(entry.insert(pod))
    //         }

    //         Entry::Occupied(entry) => {
    //             let pod = entry.into_mut();

    //             // Pod labels and annotations may change at runtime, but the
    //             // port list may not
    //             if pod.port_names != port_names {
    //                 anyhow::bail!("pod {} port names must not change", name);
    //             }

    //             // If there aren't meaningful changes, then don't bother doing
    //             // any more work.
    //             if pod.meta == meta && ip == pod.ip {
    //                 tracing::debug!(pod = %name, "No changes");
    //                 None
    //             } else {
    //                 tracing::debug!(pod = %name, "Updating");
    //                 pod.meta = meta;
    //                 pod.ip = ip;
    //                 Some(pod)
    //             }
    //         }
    //     };

    //     match pod {
    //         Some(pod) => {
    //             tracing::info!("pod updated");
    //             pod.reindex_services(services_by_addr, client_policies, &mut self.policies)
    //         }
    //         None => {} // no update
    //     };

    //     Ok(())
    // }
}

// impl Pod {
//     /// Determines the policies for ports on this pod.
//     fn reindex_services(
//         &mut self,
//         services_by_addr: &mut ServicesByAddr,
//         client_policies: &ClientPolicyNsIndex,
//         ns_policies: &mut PolicyIndex,
//     ) {
//         // Keep track of the ports that are already known in the pod so that, after applying server
//         // matches, we can ensure remaining ports are set to the default policy.
//         let mut unmatched_ports = self.port_servers.keys().copied().collect::<pod::PortSet>();

//         // Keep track of which ports have been matched to servers to that we can detect when
//         // multiple servers match a single port.
//         //
//         // We start with capacity for the known ports on the pod; but this can grow if servers
//         // select additional ports.
//         let mut matched_ports = pod::PortMap::with_capacity_and_hasher(
//             unmatched_ports.len(),
//             std::hash::BuildHasherDefault::<pod::PortHasher>::default(),
//         );

//         for (service_name, service) in ns_policies.services.iter() {
//             if service.pod_selector.matches(&self.meta.labels) {
//                 for (portname, &port) in &service.ports {
//                     // If the port is already matched to a server, then log a warning and skip
//                     // updating it so it doesn't flap between servers.
//                     if let Some(prior) = matched_ports.get(&port.number) {
//                         tracing::warn!(
//                             port.name = %portname,
//                             port.number,
//                             port.opaque,
//                             server = %prior,
//                             conflict = %service_name,
//                             "Port already matched by another server; skipping"
//                         );
//                         continue;
//                     }

//                     let addr = SocketAddr::new(self.ip, port.number.into());
//                     let s = ns_policies.outbound_server(
//                         port,
//                         self,
//                         service_name,
//                         service,
//                         client_policies,
//                     );
//                     let rx = self.update_server(port.number, service_name, s);
//                     match services_by_addr.entry(addr) {
//                         Entry::Occupied(entry) => assert!(rx.same_channel(entry.get())),
//                         Entry::Vacant(entry) => {
//                             entry.insert(rx);
//                         }
//                     }

//                     matched_ports.insert(port.number, service_name.clone());
//                     unmatched_ports.remove(&port.number);
//                 }
//             }
//         }

//         // Handle ports that don't have a service.
//         for port in unmatched_ports.into_iter() {
//             let addr = SocketAddr::new(self.ip, port.into());
//             let server = self.default_outbound_server(port, &ns_policies.cluster_info);
//             let rx = self.set_default_server(port, server);
//             match services_by_addr.entry(addr) {
//                 Entry::Occupied(entry) => assert!(rx.same_channel(entry.get())),
//                 Entry::Vacant(entry) => {
//                     entry.insert(rx);
//                 }
//             }
//         }
//     }

//     /// Returns an iterator over all the `SocketAddr`s of this pod's ports.
//     fn addrs(&self) -> impl Iterator<Item = SocketAddr> + '_ {
//         let ports = self.port_servers.keys().chain(self.probes.keys());
//         ports.map(|&port| SocketAddr::new(self.ip, port.into()))
//     }

//     /// Updates a pod-port to use the given named server.
//     ///
//     /// The name is used explicity (and not derived from the `server` itself) to
//     /// ensure that we're not handling a default server.
//     fn update_server(
//         &mut self,
//         port: NonZeroU16,
//         name: &str,
//         server: OutboundService,
//     ) -> watch::Receiver<OutboundService> {
//         let rx = match self.port_servers.entry(port) {
//             Entry::Vacant(entry) => {
//                 tracing::trace!(port = %port, server = %name, "Creating server");
//                 let (tx, rx) = watch::channel(server);
//                 entry
//                     .insert(PodPortServer {
//                         name: Some(name.to_string()),
//                         tx,
//                         rx,
//                     })
//                     .rx
//                     .clone()
//             }

//             Entry::Occupied(mut entry) => {
//                 let ps = entry.get_mut();

//                 // Avoid sending redundant updates.
//                 if ps.name.as_deref() == Some(name) && *ps.rx.borrow() == server {
//                     tracing::trace!(port = %port, server = %name, "Skipped redundant server update");
//                     tracing::trace!(?server);
//                     return ps.rx.clone();
//                 }

//                 // If the port's server previously matched a different server,
//                 // this can either mean that multiple servers currently match
//                 // the pod:port, or that we're in the middle of an update. We
//                 // make the opportunistic choice to assume the cluster is
//                 // configured coherently so we take the update. The admission
//                 // controller should prevent conflicts.
//                 tracing::trace!(port = %port, server = %name, "Updating server");
//                 ps.name = Some(name.to_string());
//                 ps.tx.send(server).expect("a receiver is held by the index");
//                 ps.rx.clone()
//             }
//         };

//         tracing::debug!(port = %port, server = %name, "Updated server");
//         rx
//     }

//     /// Updates a pod-port to use the given named server.
//     fn set_default_server(
//         &mut self,
//         port: NonZeroU16,
//         server: OutboundService,
//     ) -> watch::Receiver<OutboundService> {
//         match self.port_servers.entry(port) {
//             Entry::Vacant(entry) => {
//                 tracing::debug!(%port, "Creating default server");
//                 let (tx, rx) = watch::channel(server);
//                 let rx2 = rx.clone();
//                 entry.insert(PodPortServer { name: None, tx, rx });
//                 rx2
//             }

//             Entry::Occupied(mut entry) => {
//                 let ps = entry.get_mut();

//                 // Avoid sending redundant updates.
//                 if *ps.rx.borrow() == server {
//                     tracing::trace!(%port, "Default server already set");
//                     return ps.rx.clone();
//                 }

//                 tracing::debug!(%port, "Setting default server");
//                 ps.name = None;
//                 ps.tx.send(server).expect("a receiver is held by the index");
//                 ps.rx.clone()
//             }
//         }
//     }

//     fn probe_paths(&self, port: NonZeroU16) -> impl Iterator<Item = &str> + '_ {
//         self.probes
//             .get(&port)
//             .into_iter()
//             .flatten()
//             .map(|p| p.as_str())
//     }
//     fn default_outbound_server(&self, port: NonZeroU16, config: &ClusterInfo) -> OutboundService {
//         let protocol = if self.meta.settings.opaque_ports.contains(&port) {
//             core::ProxyProtocol::Opaque
//         } else {
//             core::ProxyProtocol::Detect {
//                 timeout: config.default_detect_timeout,
//             }
//         };

//         let http_routes = config.default_outbound_http_routes(self.probe_paths(port));

//         OutboundService {
//             reference: OutboundServiceRef::Default,
//             fqdn: None,
//             protocol,
//             http_routes,
//             client_policy: None,
//         }
//     }
// }

impl PolicyIndex {
    fn is_empty(&self) -> bool {
        self.services.is_empty() && self.http_routes.is_empty()
    }

    fn update_service(
        &mut self,
        name: String,
        service: OutboundService,
    ) -> Option<watch::Receiver<OutboundService>> {
        match self.services.entry(name) {
            Entry::Vacant(entry) => {
                tracing::debug!(?service, "adding to index");
                let (tx, rx) = watch::channel(service);
                entry.insert(tx);
                Some(rx)
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                if *entry.borrow() == service {
                    tracing::debug!(?service, "no changes");
                    return None;
                }

                tracing::debug!(?service, "updating");
                let rx = entry.subscribe();
                entry.send(service);
                Some(rx)
            }
        }
    }

    fn update_http_route(&mut self, name: String, route: OutboundRouteBinding) -> bool {
        match self.http_routes.entry(name) {
            Entry::Vacant(entry) => {
                tracing::debug!(?route, "adding to index");
                entry.insert(route);
            }
            Entry::Occupied(mut entry) => {
                if *entry.get() == route {
                    tracing::debug!(?route, "no changes");
                    return false;
                }

                tracing::debug!(?route, "updating");
                entry.insert(route);
            }
        }

        true
    }

    // fn update_service(&mut self, name: String, svc: service::Spec) -> bool {
    //     match self.services.entry(name.clone()) {
    //         Entry::Vacant(entry) => {
    //             entry.insert(svc);
    //         }
    //         Entry::Occupied(entry) => {
    //             let current = entry.into_mut();
    //             if *current == svc {
    //                 tracing::debug!(service = %name, "no changes");
    //                 return false;
    //             }
    //             tracing::debug!(service = %name, "updating");
    //             *current = svc;
    //         }
    //     }
    //     true
    // }

    // fn outbound_server(
    //     &self,
    //     port: service::ServicePort,
    //     pod: &Pod,
    //     service_name: &str,
    //     service: &service::Spec,
    //     client_policies: &ClientPolicyNsIndex,
    // ) -> OutboundService {
    //     let opaque = port.opaque || pod.meta.settings.opaque_ports.contains(&port.number);
    //     tracing::debug!(
    //         service = %service_name,
    //         port.number,
    //         port.opaque = opaque,
    //         "Creating outbound server"
    //     );

    //     let mut http_routes = self.http_routes(service_name, pod.probe_paths(port.number));
    //     let client_policy = self.client_policies(service_name, client_policies, &mut http_routes);

    //     let protocol = if opaque {
    //         core::ProxyProtocol::Opaque
    //     } else {
    //         core::ProxyProtocol::Detect {
    //             timeout: self.cluster_info.default_detect_timeout,
    //         }
    //     };

    //     OutboundService {
    //         reference: OutboundServiceRef::Service(service_name.to_string()),
    //         fqdn: Some(service.fqdn.clone()),
    //         protocol,
    //         http_routes,
    //         client_policies,
    //     }
    // }

    // fn http_routes<'p>(
    //     &self,
    //     service_name: &str,
    //     probe_paths: impl Iterator<Item = &'p str>,
    // ) -> HashMap<core::InboundHttpRouteRef, OutboundHttpRoute> {
    //     let routes = self
    //         .http_routes
    //         .iter()
    //         .filter(|(_, route)| route.selects_service(service_name))
    //         .map(|(name, route)| {
    //             let route = route.route.clone();
    //             (core::InboundHttpRouteRef::Linkerd(name.clone()), route)
    //         })
    //         .collect::<HashMap<_, _>>();
    //     if !routes.is_empty() {
    //         return routes;
    //     }
    //     self.cluster_info.default_outbound_http_routes(probe_paths)
    // }

    // fn client_policies(
    //     &self,
    //     service_name: &str,
    //     client_policies: &ClientPolicyNsIndex,
    //     http_routes: &mut HashMap<core::InboundHttpRouteRef, OutboundHttpRoute>,
    // ) -> Option<client_policy::Spec> {
    //     let mut server_policy = None;
    //     let potential_policies = client_policies
    //         .all_policies()
    //         .filter(|(_, policy)| policy.target_ns() == *self.namespace);
    //     let _span = tracing::debug_span!("client_policies", ns = %self.namespace.as_ref(), service = %service_name).entered();
    //     for (name, policy) in potential_policies {
    //         let _span = tracing::debug_span!("policy", message = %name).entered();
    //         if policy.selects_service(self.namespace.as_ref(), service_name) {
    //             tracing::debug!("policy selects service");
    //             // TODO(eliza): how to handle multiple ClientPolicies selecting
    //             // the same service?
    //             assert_eq!(
    //                 server_policy, None,
    //                 "we already found a policy that selects this service!"
    //             );
    //             server_policy = Some(policy.clone());
    //         } else {
    //             tracing::debug!("policy does not select service");
    //         }

    //         for (reference, route) in http_routes.iter_mut() {
    //             let _span = tracing::debug_span!("route", ?reference).entered();
    //             if policy.selects_route(self.namespace.as_ref(), reference) {
    //                 tracing::debug!("policy selects route on service");

    //                 // TODO(eliza): how to handle multiple ClientPolicies selecting
    //                 // the same server?
    //                 assert_eq!(
    //                     server_policy, None,
    //                     "we already found a policy that selects this route!"
    //                 );
    //                 route.client_policy = Some(policy.clone());
    //             } else {
    //                 tracing::debug!("policy does not select this route");
    //             }
    //         }
    //     }
    //     server_policy
    // }
}

// === impl ClientPolicyNsIndex ===

impl ClientPolicyNsIndex {
    /// Returns an iterator over all ClientPolicies in the index.
    fn all_policies(&self) -> impl Iterator<Item = (&String, &client_policy::Spec)> + '_ {
        self.by_ns.values().flat_map(|ns| ns.policies.iter())
    }

    pub fn policies_for<'a>(
        &'a self,
        service: &'a OutboundServiceRef,
    ) -> impl Iterator<Item = (client_policy::PolicyRef, &'a client_policy::Spec)> + 'a {
        let service = match service {
            OutboundServiceRef::Service { ns, name } => Some((ns, name)),
            OutboundServiceRef::Default => None,
        };
        service.into_iter().flat_map(|(ns, svc)| {
            self.by_ns.iter().flat_map(|(policy_ns, index)| {
                index.policies.iter().filter_map(|(name, policy)| {
                    if policy.selects_service(ns, svc) {
                        Some((
                            client_policy::PolicyRef {
                                name: name.clone(),
                                namespace: policy_ns.clone(),
                            },
                            policy,
                        ))
                    } else {
                        None
                    }
                })
            })
        })
    }

    pub fn bindings_for<'a>(
        &'a self,
        svc_policies: &'a HashMap<client_policy::PolicyRef, client_policy::Spec>,
    ) -> impl Iterator<Item = (client_policy::PolicyRef, client_policy::Bound)> + 'a {
        self.by_ns.iter().flat_map(|(policy_ns, index)| {
            index.bindings.iter().filter_map(|(name, binding)| {
                let policies = binding
                    .policies
                    .iter()
                    .filter_map(|policy| svc_policies.get(&policy).cloned())
                    .collect::<Vec<_>>();

                // no targets in this binding exist on this service.
                if policies.is_empty() {
                    return None;
                }

                Some((
                    client_policy::PolicyRef {
                        name: name.clone(),
                        namespace: policy_ns.clone(),
                    },
                    client_policy::Bound {
                        client_pod_selector: binding.client_pod_selector.clone(),
                        policies,
                    },
                ))
            })
        })
    }

    /// Returns an iterator over all ClientPolicyBindings in the index.
    pub fn all_bindings(&self) -> impl Iterator<Item = (&String, &client_policy::Binding)> + '_ {
        self.by_ns.values().flat_map(|ns| ns.bindings.iter())
    }

    pub fn policy(
        &self,
        client_policy::PolicyRef { namespace, name }: &client_policy::PolicyRef,
    ) -> Option<&client_policy::Spec> {
        self.by_ns.get(namespace)?.policies.get(name)
    }

    fn update_policy(&mut self, ns: String, name: String, policy: client_policy::Spec) -> bool {
        match self.by_ns.entry(ns).or_default().policies.entry(name) {
            Entry::Vacant(entry) => {
                tracing::debug!(?policy, "adding to index");
                entry.insert(policy);
            }
            Entry::Occupied(mut entry) => {
                if *entry.get() == policy {
                    tracing::debug!(?policy, "no changes");
                    return false;
                }
                tracing::debug!(?policy, "updating");
                entry.insert(policy);
            }
        }

        true
    }

    fn update_binding(
        &mut self,
        ns: String,
        name: String,
        binding: client_policy::Binding,
    ) -> bool {
        match self.by_ns.entry(ns).or_default().bindings.entry(name) {
            Entry::Vacant(entry) => {
                tracing::debug!(?binding, "adding to index");
                entry.insert(binding);
            }
            Entry::Occupied(mut entry) => {
                if *entry.get() == binding {
                    tracing::debug!(?binding, "no changes");
                    return false;
                }
                tracing::debug!(?binding, "updating");
                entry.insert(binding);
            }
        }

        true
    }
}

impl ClientPolicyIndex {
    fn is_empty(&self) -> bool {
        self.policies.is_empty() && self.bindings.is_empty()
    }
}

// === impl NsUpdate ===

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
            client_policy: None,
        };
        routes.insert(core::InboundHttpRouteRef::Default("probe"), probe_route);

        routes
    }
}
