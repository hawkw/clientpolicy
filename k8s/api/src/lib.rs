pub mod client_policy;
pub mod client_policy_binding;

// modified from the upstream `linkerd-policy-controller-k8s-api` version of
// `NamespacedTargetRef`, in order to allow an optional port.
#[derive(
    Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub struct NamespacedTargetRef {
    pub group: Option<String>,
    pub kind: String,
    pub name: String,
    pub namespace: Option<String>,
    pub port: Option<String>,
}

impl NamespacedTargetRef {
    pub fn from_resource<T>(resource: &T) -> Self
    where
        T: kube::Resource,
        T::DynamicType: Default,
    {
        let (group, kind, name) = group_kind_name(resource);
        let namespace = resource.meta().namespace.clone();
        Self {
            group,
            kind,
            name,
            namespace,
            port: None,
        }
    }

    /// Returns the target ref kind, qualified by its group, if necessary.
    pub fn canonical_kind(&self) -> String {
        canonical_kind(self.group.as_deref(), &self.kind)
    }

    /// Checks whether the target references the given resource type
    pub fn targets_kind<T>(&self) -> bool
    where
        T: kube::Resource,
        T::DynamicType: Default,
    {
        targets_kind::<T>(self.group.as_deref(), &self.kind)
    }

    /// Checks whether the target references the given namespaced resource
    pub fn targets<T>(&self, resource: &T, local_ns: &str) -> bool
    where
        T: kube::Resource,
        T::DynamicType: Default,
    {
        if !self.targets_kind::<T>() {
            return false;
        }

        // If the resource specifies a namespace other than the target or the
        // default namespace, that's a deal-breaker.
        let tns = self.namespace.as_deref().unwrap_or(local_ns);
        match resource.meta().namespace.as_deref() {
            Some(rns) if rns.eq_ignore_ascii_case(tns) => {}
            _ => return false,
        };

        match resource.meta().name.as_deref() {
            None => return false,
            Some(rname) => {
                if !self.name.eq_ignore_ascii_case(rname) {
                    return false;
                }
            }
        }

        true
    }
}

fn canonical_kind(group: Option<&str>, kind: &str) -> String {
    if let Some(group) = group {
        format!("{}.{}", kind, group)
    } else {
        kind.to_string()
    }
}

fn group_kind_name<T>(resource: &T) -> (Option<String>, String, String)
where
    T: kube::Resource,
    T::DynamicType: Default,
{
    let dt = Default::default();

    let group = match T::group(&dt) {
        g if (*g).is_empty() => None,
        g => Some(g.to_string()),
    };

    let kind = T::kind(&dt).to_string();

    let name = resource
        .meta()
        .name
        .clone()
        .expect("resource must have a name");

    (group, kind, name)
}

fn targets_kind<T>(group: Option<&str>, kind: &str) -> bool
where
    T: kube::Resource,
    T::DynamicType: Default,
{
    let dt = Default::default();

    let mut t_group = &*T::group(&dt);
    if t_group.is_empty() {
        t_group = "core";
    }

    group.unwrap_or("core").eq_ignore_ascii_case(t_group)
        && kind.eq_ignore_ascii_case(&*T::kind(&dt))
}
