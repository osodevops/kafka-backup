//! Concrete token providers for `OauthBearerPlugin` and Schema Registry
//! clients. Each is wrapped in a shared
//! [`TokenCache`](crate::oauth::cache::TokenCache); none caches by itself.
//!
//! | Module | YAML `type` | Source |
//! |---|---|---|
//! | [`oidc`] | `oidc_client_credentials` | RFC 6749 §4.4 (Entra ID, Okta, Keycloak, ...) |
//! | [`azure_imds`] | `azure_managed_identity` | Azure Instance Metadata Service |
//! | [`azure_workload`] | `azure_workload_identity` | Entra client-assertion exchange (AKS) |
//! | [`file`] | `file` | a token file kept fresh by something else |
//! | [`static_token`] | `static` | a fixed token (tests only) |

pub mod azure_imds;
pub mod azure_workload;
pub mod file;
pub mod oidc;
pub mod static_token;
