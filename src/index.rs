use crate::{
    client_policy, core,
    k8s::{self, ResourceExt},
    pod,
    route::OutboundRouteBinding,
    service::OutboundService,
    ClusterInfo,
};
use ahash::{AHashMap as HashMap, AHashSet};
use anyhow::{anyhow, Result};
use client_policy_k8s_api::{
    client_policy::HttpClientPolicy, client_policy_binding::ClientPolicyBinding,
};
use futures::TryFutureExt;
use k8s_openapi::{
    api::core::v1::{Pod, Service},
    Metadata,
};
use kubert::client::api::ListParams;
use parking_lot::RwLock;
use std::{
    collections::hash_map::Entry, future::Future, net::SocketAddr, num::NonZeroU16, sync::Arc,
};
use tokio::sync::{watch, Notify};
use tracing::Instrument;

#[derive(Debug, Clone)]
pub struct Index {
    index: Arc<RwLock<LockedIndex>>,
}

#[derive(Debug)]
pub struct LockedIndex {
    pub cluster_info: Arc<ClusterInfo>,
    namespaces: HashMap<String, Namespace>,
    pub client_policies: ClientPolicyNsIndex,
    services_by_addr: ServicesByAddr,
    changed: Arc<Notify>,
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
pub struct Namespace {
    name: Arc<String>,
    pods: HashMap<String, watch::Sender<pod::Meta>>,
    pub policies: PolicyIndex,
}

#[derive(Debug)]
pub struct PolicyIndex {
    // namespace: Arc<String>,
    // cluster_info: Arc<ClusterInfo>,
    services: HashMap<String, watch::Sender<OutboundService>>,
    pub http_routes: HashMap<String, OutboundRouteBinding>,
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
                changed: Arc::new(Notify::new()),
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

    pub fn lookup(
        &self,
        addr: SocketAddr,
        ns: &str,
        workload: &str,
    ) -> Result<(watch::Receiver<OutboundService>, watch::Receiver<pod::Meta>)> {
        let index = self.index.read();
        let pod = index
            .namespaces
            .get(ns)
            .ok_or_else(|| anyhow!("no namespace found for {ns}/{workload}"))?
            .pods
            .get(workload)
            .map(|pod| pod.subscribe())
            .ok_or_else(|| anyhow!("no pod {ns}/{workload}"))?;
        let svc = index
            .services_by_addr
            .get(&addr)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no server found for address {addr}"))?;

        Ok((svc, pod))
    }

    pub fn spawn_index_tasks(&self, rt: &mut kubert::Runtime) -> impl Future<Output = Result<()>> {
        let pods = self.index_pods(rt);
        let services = self.index_resource::<Service>(rt);
        let routes = self.index_resource::<k8s::policy::HttpRoute>(rt);
        let policies = self.index_resource::<HttpClientPolicy>(rt);
        let bindings = self.index_resource::<ClientPolicyBinding>(rt);
        async move {
            tokio::try_join! {
                pods,
                services, routes, policies, bindings,
            }
            .map(|_| ())
        }
    }

    fn index_pods(&self, rt: &mut kubert::Runtime) -> impl Future<Output = Result<()>> {
        let watch = rt.watch_all::<k8s::Pod>(ListParams::default().labels(CONTROL_PLANE_NS_LABEL));
        let index = kubert::index::namespaced(self.index.clone(), watch)
            .instrument(tracing::info_span!("index", kind = %"Pod"));

        let join = tokio::spawn(index);
        tracing::info!("started Pod indexing");
        join.map_err(|err| anyhow!("index task for Pods failed: {err}"))
    }

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

    pub fn dump_index(&self) -> tokio::task::JoinHandle<()> {
        tracing::debug!("dumping index changes");
        let index = self.index.clone();
        let changed = index.read().changed.clone();

        fn route_name(route: &core::InboundHttpRouteRef) -> &str {
            match route {
                core::InboundHttpRouteRef::Default(name) => name,
                core::InboundHttpRouteRef::Linkerd(name) => name.as_str(),
            }
        }

        tokio::spawn(async move {
            loop {
                {
                    let index = index.read();
                    use comfy_table::{presets::ASCII_MARKDOWN, *};
                    let mut svcs_by_addr = Table::new();
                    svcs_by_addr
                        .load_preset(ASCII_MARKDOWN)
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
                        let rt_policy_bindings =
                            svc.http_routes.iter().flat_map(|(name, route)| {
                                route.client_policies.bindings.iter().map(
                                    |(binding_ref, binding)| {
                                        (route_name(name), binding_ref, binding)
                                    },
                                )
                            });
                        let port = NonZeroU16::new(addr.port()).expect("port must be nonzero");
                        let port_name = svc.port_names.get(&port);
                        let svc_policy_bindings = port_name
                            .and_then(|port_name| svc.client_policies.get(port_name))
                            .into_iter()
                            .flat_map(|policies| policies.bindings.iter())
                            .map(|(binding_ref, binding)| ("svc", binding_ref, binding));
                        let bindings = svc_policy_bindings
                            .chain(rt_policy_bindings)
                            .map(
                                |(kind, &client_policy::PolicyRef { ref name, .. }, bound)| {
                                    use std::fmt::Write;
                                    let mut policies = String::new();
                                    for policy in bound.policies.iter() {
                                        write!(&mut policies, "\n- policy: `{policy:?}`").unwrap();
                                    }
                                    format!(
                                        "{kind}/{name}:\n- selector: `{:?}`{policies}",
                                        bound.client_pod_selector
                                    )
                                },
                            )
                            .collect::<Vec<_>>()
                            .join("\n");

                        svcs_by_addr.add_row(Row::from(vec![
                            Cell::new(&addr.to_string()),
                            Cell::new(svc.reference.to_string()),
                            Cell::new(svc.fqdn.as_ref().map(|s| s.as_ref()).unwrap_or("n/a")),
                            Cell::new(bindings),
                        ]));
                    }

                    let mut policies = Table::new();
                    policies
                        .load_preset(ASCII_MARKDOWN)
                        .set_content_arrangement(ContentArrangement::Dynamic)
                        .set_header(Row::from(vec![
                            "NAMESPACE",
                            "POLICY",
                            "TARGET",
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
                            Cell::new(policy.target.to_string()),
                            Cell::new(&format!("`{:?}`", policy.failure_classification)),
                            Cell::new(format!("`{:?}`", policy.filters)),
                        ]));
                    }

                    let mut svcs = Table::new();
                    svcs.load_preset(ASCII_MARKDOWN)
                        .set_content_arrangement(ContentArrangement::Dynamic)
                        .set_header(Row::from(vec![
                            "SERVICE",
                            "PORT NAME",
                            "PORT NUMBER",
                            "OPAQUE",
                            "POLICY BINDINGS",
                        ]));
                    let services_by_ns = index.namespaces.iter().flat_map(|(ns_name, ns)| {
                        ns.policies
                            .services
                            .iter()
                            .map(move |(name, svc)| (ns_name, name, svc))
                    });
                    for (ns_name, svc_name, svc) in services_by_ns {
                        let svc = svc.borrow();
                        let svcname = format!("{ns_name}/{svc_name}");
                        for (port_num, port_name) in svc.port_names.iter() {
                            let policies = svc.client_policies.get(port_name);
                            svcs.add_row(Row::from(vec![
                                Cell::new(&svcname),
                                Cell::new(&port_name),
                                Cell::new(format!("{port_num}")),
                                Cell::new(svc.opaque_ports.contains(port_num)),
                                Cell::new(format!("{policies:?}")),
                            ]));
                        }
                    }
                    println!("\n{svcs_by_addr}\n\n{policies}\n\n{svcs}\n");
                }
                changed.notified().await;
            }
        })
    }
}

impl LockedIndex {
    fn ns_with_reindex(&mut self, namespace: String, f: impl FnOnce(&mut Namespace) -> bool) {
        if let Entry::Occupied(mut ns) = self.namespaces.entry(namespace) {
            if f(ns.get_mut()) {
                if ns.get().is_empty() {
                    ns.remove();
                } else {
                    ns.get_mut().reindex(&self.client_policies);
                }
                self.changed.notify_one();
            }
        }
    }

    pub fn ns_or_default(&mut self, namespace: String) -> &mut Namespace {
        self.namespaces
            .entry(namespace.clone())
            .or_insert_with(|| Namespace::new(namespace))
    }

    fn ns_or_default_with_reindex(
        &mut self,
        namespace: String,
        f: impl FnOnce(&mut Namespace) -> bool,
    ) {
        let ns = self
            .namespaces
            .entry(namespace.clone())
            .or_insert_with(|| Namespace::new(namespace));
        if f(ns) {
            ns.reindex(&self.client_policies);
            self.changed.notify_one();
        }
    }

    fn reindex_all(&mut self) {
        tracing::debug!("Reindexing all namespaces");
        for ns in self.namespaces.values_mut() {
            ns.reindex(&self.client_policies);
        }

        self.changed.notify_one();
    }
}

impl kubert::index::IndexNamespacedResource<Pod> for LockedIndex {
    fn apply(&mut self, pod: Pod) {
        let ns = pod.namespace().unwrap();
        let name = pod.name_unchecked();
        let _span = tracing::info_span!("apply", ns = %ns, %name).entered();
        let pod = pod::Meta::from_metadata(pod.metadata);
        match self.ns_or_default(ns).pods.entry(name) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().send_if_modified(|current| {
                    if *current != pod {
                        *current = pod;
                        tracing::info!("pod updated");
                        true
                    } else {
                        tracing::info!("no changes");
                        false
                    }
                });
            }
            Entry::Vacant(entry) => {
                let (pod, _) = watch::channel(pod);
                entry.insert(pod);
                tracing::info!("pod created");
            }
        }
    }

    #[tracing::instrument(name = "delete", fields(%ns, %name))]
    fn delete(&mut self, ns: String, name: String) {
        if let Entry::Occupied(mut ns) = self.namespaces.entry(ns) {
            if ns.get_mut().pods.remove(&name).is_some() {
                // if there are no more pods in the ns, we can also delete the
                // ns.
                if ns.get().pods.is_empty() {
                    tracing::debug!("namespace has no more pods; removing it");
                    ns.remove();
                }
            }
            tracing::info!("pod deleted")
        } else {
            tracing::debug!("tried to delete a pod in a namespace that does not exist!");
        }
    }
}

impl kubert::index::IndexNamespacedResource<Service> for LockedIndex {
    fn apply(&mut self, svc: Service) {
        let ns = svc.namespace().expect("service must be namespaced");
        let name = svc.name_unchecked();
        let _span = tracing::info_span!("apply", %ns, %name).entered();
        let svc = match OutboundService::from_resource(self, ns.clone(), svc) {
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
            self.changed.notify_one();
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

                self.changed.notify_one();
            }
        }
    }

    fn reset(&mut self, svcs: Vec<Service>, deleted: HashMap<String, AHashSet<String>>) {
        let _span = tracing::info_span!("reset").entered();
        let mut changed = false;
        for svc in svcs.into_iter() {
            let ns = svc.namespace().expect("service must be namespaced");
            let name = svc.name_unchecked();
            let _span = tracing::info_span!("apply", %ns, %name).entered();
            let svc = match OutboundService::from_resource(self, ns.clone(), svc) {
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
                changed = true;
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
                    changed = true;
                }
            }
        }

        if changed {
            self.changed.notify_one();
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
    fn new(name: String) -> Self {
        let name = Arc::new(name);
        Self {
            name,
            pods: HashMap::default(),
            policies: PolicyIndex {
                services: HashMap::default(),
                http_routes: HashMap::default(),
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.pods.is_empty() && self.policies.is_empty()
    }

    fn reindex(&mut self, client_policies: &ClientPolicyNsIndex) -> bool {
        let _span = tracing::info_span!("reindex", ns = %self.name).entered();
        let mut changed = false;
        for (name, svc) in &mut self.policies.services {
            let _span = tracing::info_span!("service", message = %name).entered();
            svc.send_if_modified(|svc| {
                let routes_changed = svc.reindex_routes(&self.policies.http_routes);
                let policies_changed = svc.reindex_policies(client_policies);
                if routes_changed || policies_changed {
                    tracing::info!("reindexed service");
                    changed = true;
                    true
                } else {
                    false
                }
            });
        }
        changed
    }

    fn update_pod(&mut self, name: String, pod: pod::Meta) {
        match self.pods.entry(name.clone()) {
            Entry::Vacant(entry) => {
                let (tx, _) = watch::channel(pod);
                entry.insert(tx);
                tracing::debug!("created pod");
            }

            Entry::Occupied(entry) => {
                let entry = entry.into_mut();

                // Pod labels and annotations may change at runtime
                entry.send_if_modified(|current| {
                    if *current == pod {
                        tracing::debug!("no changes");
                        false
                    } else {
                        *current = pod;
                        tracing::debug!("pod updated");
                        true
                    }
                });
            }
        }
    }
}

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
                entry
                    .send(service)
                    .expect("we just created an rx, this cannot fail");
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
}

// === impl ClientPolicyNsIndex ===

impl ClientPolicyNsIndex {
    pub fn policies_for<'a>(
        &'a self,
        target: client_policy::TargetRef<'a>,
    ) -> impl Iterator<Item = (client_policy::PolicyRef, &'a client_policy::Spec)> + 'a {
        self.by_ns.iter().flat_map(move |(policy_ns, index)| {
            index.policies.iter().filter_map(move |(name, policy)| {
                if policy.selects(target) {
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
                    .filter_map(|policy| svc_policies.get(policy).cloned())
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
