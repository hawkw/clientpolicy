use anyhow::{anyhow, Error, Result};

/// Indicates the default behavior to apply when no Server is found for a port.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum DefaultPolicy {
    Allow {
        /// Indicates that, by default, all traffic must be authenticated.
        authenticated_only: bool,

        /// Indicates that all traffic must, by default, be from an IP address within the cluster.
        cluster_only: bool,
    },

    /// Indicates that all traffic is denied unless explicitly permitted by an authorization policy.
    Deny,
}

// === impl DefaultPolicy ===

impl std::str::FromStr for DefaultPolicy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all-authenticated" => Ok(Self::Allow {
                authenticated_only: true,
                cluster_only: false,
            }),
            "all-unauthenticated" => Ok(Self::Allow {
                authenticated_only: false,
                cluster_only: false,
            }),
            "cluster-authenticated" => Ok(Self::Allow {
                authenticated_only: true,
                cluster_only: true,
            }),
            "cluster-unauthenticated" => Ok(Self::Allow {
                authenticated_only: false,
                cluster_only: true,
            }),
            "deny" => Ok(Self::Deny),
            s => Err(anyhow!("invalid mode: {:?}", s)),
        }
    }
}

impl DefaultPolicy {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Allow {
                authenticated_only: true,
                cluster_only: false,
            } => "all-authenticated",
            Self::Allow {
                authenticated_only: false,
                cluster_only: false,
            } => "all-unauthenticated",
            Self::Allow {
                authenticated_only: true,
                cluster_only: true,
            } => "cluster-authenticated",
            Self::Allow {
                authenticated_only: false,
                cluster_only: true,
            } => "cluster-unauthenticated",
            Self::Deny => "deny",
        }
    }
}
