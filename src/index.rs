use kubert::client::api::ListParams;
use linkerd_policy_controller_k8s_api::{self as k8s, ResourceExt};
use parking_lot::RwLock;
use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;
use tracing::Instrument;

#[derive(Default)]
pub struct Index {
    index: Arc<RwLock<LockedIndex>>,
}

#[derive(Default, Debug)]
struct LockedIndex {
    namespaces: HashMap<String, Namespace>,
}

#[derive(Debug, Default)]
struct Namespace {
    pods: HashMap<String, k8s::Pod>,
}

const CONTROL_PLANE_NS_LABEL: &str = "linkerd.io/control-plane-ns";

impl Index {
    pub fn index_pods(&self, rt: &mut kubert::Runtime) -> tokio::task::JoinHandle<()> {
        let watch = rt.watch_all::<k8s::Pod>(ListParams::default().labels(CONTROL_PLANE_NS_LABEL));
        let index = kubert::index::namespaced(self.index.clone(), watch)
            .instrument(tracing::info_span!("index_pods"));
        let join = tokio::spawn(index);
        tracing::info!("started pod indexing");
        join
    }
}

impl kubert::index::IndexNamespacedResource<k8s::Pod> for LockedIndex {
    fn apply(&mut self, pod: k8s::Pod) {
        let ns = pod.namespace().unwrap();
        let name = pod.name_unchecked();
        let _span = tracing::info_span!("apply", ns = %ns, %name).entered();

        let ns = self.namespaces.entry(ns).or_default();
        ns.pods.insert(name, pod);
        tracing::info!("applied pod");
    }

    #[tracing::instrument(name = "delete", fields(%ns, %name))]
    fn delete(&mut self, ns: String, name: String) {
        if let Entry::Occupied(mut ns) = self.namespaces.entry(ns) {
            // Once the pod is removed, there's nothing else to update. Any open
            // watches will complete.  No other parts of the index need to be
            // updated.
            if ns.get_mut().pods.remove(&name).is_some() && ns.get().pods.is_empty() {
                tracing::debug!("namespace has no more pods; removing it");
                ns.remove();
            }
            tracing::info!("pod deleted");
        } else {
            tracing::debug!("tried to delete a pod in a namespace that does not exist!");
        }
    }
}
