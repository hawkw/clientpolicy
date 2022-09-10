use linkerd_policy_controller_k8s_api::policy::NamespacedTargetRef;

#[derive(
    Clone, Debug, kube::CustomResource, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[kube(
    group = "policy.linkerd.io",
    version = "v1alpha1",
    kind = "ClientPolicy",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct ClientPolicySpec {
    pub target_ref: NamespacedTargetRef,
    pub failure_classification: FailureClassification,
    pub filters: Vec<ClientPolicyFilter>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailureClassification {
    pub statuses: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum ClientPolicyFilter {
    // TODO: Add more filter varients
    Timeout { timeout: String },
}
