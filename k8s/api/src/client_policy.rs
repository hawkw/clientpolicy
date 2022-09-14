use linkerd_policy_controller_k8s_api::policy::NamespacedTargetRef;

#[derive(
    Clone, Debug, kube::CustomResource, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[kube(
    group = "policy.linkerd.io",
    version = "v1alpha1",
    kind = "HttpClientPolicy",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct HttpClientPolicySpec {
    pub target_ref: NamespacedTargetRef,
    pub failure_classification: HttpFailureClassification,
    pub filters: Vec<HttpClientPolicyFilter>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HttpFailureClassification {
    pub statuses: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum HttpClientPolicyFilter {
    // TODO: Add more filter varients
    Timeout { timeout: String },
}
