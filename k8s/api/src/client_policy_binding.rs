use linkerd_policy_controller_k8s_api::{labels, policy::NamespacedTargetRef};

#[derive(
    Clone, Debug, kube::CustomResource, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[kube(
    group = "policy.linkerd.io",
    version = "v1alpha1",
    kind = "ClientPolicyBinding",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct ClientPolicyBindingSpec {
    pub policy_refs: Vec<NamespacedTargetRef>,
    pub pod_selector: labels::Selector,
}
