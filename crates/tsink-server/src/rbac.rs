use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use reqwest::blocking::Client;
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{self, RsaPublicKeyComponents, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const RBAC_AUTH_VERIFIED_HEADER: &str = "x-tsink-rbac-verified";
pub const RBAC_AUTH_PRINCIPAL_ID_HEADER: &str = "x-tsink-auth-principal-id";
pub const RBAC_AUTH_ROLE_HEADER: &str = "x-tsink-auth-role";
pub const RBAC_AUTH_METHOD_HEADER: &str = "x-tsink-auth-method";
pub const RBAC_AUTH_PROVIDER_HEADER: &str = "x-tsink-auth-provider";
pub const RBAC_AUTH_SUBJECT_HEADER: &str = "x-tsink-auth-subject";

const RBAC_AUDIT_CAPACITY: usize = 256;
const OIDC_CLOCK_SKEW_SECONDS: u64 = 60;
const SERVICE_ACCOUNT_TOKEN_BYTES: usize = 32;
const OIDC_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RbacAction {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RbacResourceKind {
    Tenant,
    Admin,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RbacResource {
    pub kind: RbacResourceKind,
    pub name: String,
}

impl RbacResource {
    pub fn tenant(name: impl Into<String>) -> Self {
        Self {
            kind: RbacResourceKind::Tenant,
            name: name.into(),
        }
    }

    pub fn admin(name: impl Into<String>) -> Self {
        Self {
            kind: RbacResourceKind::Admin,
            name: name.into(),
        }
    }

    pub fn system(name: impl Into<String>) -> Self {
        Self {
            kind: RbacResourceKind::System,
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RbacPermission {
    pub action: RbacAction,
    pub resource: RbacResource,
}

impl RbacPermission {
    pub fn new(action: RbacAction, resource: RbacResource) -> Self {
        Self { action, resource }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedPrincipal {
    pub principal_id: String,
    pub role: String,
    pub auth_method: String,
    pub provider: Option<String>,
    pub subject: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthorizationIdentity {
    principal_id: Option<String>,
    auth_method: Option<String>,
    provider: Option<String>,
    subject: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationError {
    status: u16,
    code: &'static str,
    detail: Option<String>,
    identity: Option<Box<AuthorizationIdentity>>,
}

impl AuthorizationError {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    fn unauthorized(code: &'static str) -> Self {
        Self {
            status: 401,
            code,
            detail: None,
            identity: None,
        }
    }

    fn forbidden(code: &'static str) -> Self {
        Self {
            status: 403,
            code,
            detail: None,
            identity: None,
        }
    }

    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    fn with_identity(
        mut self,
        principal_id: Option<String>,
        auth_method: Option<String>,
        provider: Option<String>,
        subject: Option<String>,
    ) -> Self {
        self.identity = if principal_id.is_none()
            && auth_method.is_none()
            && provider.is_none()
            && subject.is_none()
        {
            None
        } else {
            Some(Box::new(AuthorizationIdentity {
                principal_id,
                auth_method,
                provider,
                subject,
            }))
        };
        self
    }

    fn principal_id(&self) -> Option<&String> {
        self.identity
            .as_ref()
            .and_then(|identity| identity.principal_id.as_ref())
    }

    fn auth_method(&self) -> Option<&String> {
        self.identity
            .as_ref()
            .and_then(|identity| identity.auth_method.as_ref())
    }

    fn provider(&self) -> Option<&String> {
        self.identity
            .as_ref()
            .and_then(|identity| identity.provider.as_ref())
    }

    fn subject(&self) -> Option<&String> {
        self.identity
            .as_ref()
            .and_then(|identity| identity.subject.as_ref())
    }

    fn detail(&self) -> Option<&String> {
        self.detail.as_ref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RbacConfigFile {
    #[serde(default)]
    roles: BTreeMap<String, RoleDefinition>,
    #[serde(default)]
    principals: Vec<PrincipalDefinition>,
    #[serde(default)]
    service_accounts: Vec<ServiceAccountDefinition>,
    #[serde(default)]
    oidc_providers: Vec<OidcProviderDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RoleDefinition {
    #[serde(default)]
    grants: Vec<GrantDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantDefinition {
    action: RbacAction,
    resource: RbacResource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrincipalDefinition {
    id: String,
    token: String,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    bindings: Vec<BindingDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceAccountDefinition {
    id: String,
    token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    created_unix_ms: u64,
    #[serde(default)]
    updated_unix_ms: u64,
    #[serde(default)]
    last_rotated_unix_ms: u64,
    #[serde(default)]
    bindings: Vec<BindingDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BindingDefinition {
    role: String,
    #[serde(default)]
    scopes: Vec<RbacResource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OidcProviderDefinition {
    name: String,
    issuer: String,
    #[serde(default)]
    audiences: Vec<String>,
    #[serde(default)]
    username_claim: Option<String>,
    #[serde(default)]
    jwks_url: Option<String>,
    #[serde(default)]
    jwks: Vec<OidcJwkDefinition>,
    #[serde(default)]
    claim_mappings: Vec<OidcClaimMappingDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OidcClaimMappingDefinition {
    claim: String,
    value: String,
    #[serde(default)]
    bindings: Vec<BindingDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OidcJwkDefinition {
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    alg: Option<String>,
    kty: String,
    #[serde(default, rename = "use")]
    use_: Option<String>,
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
    #[serde(default)]
    x: Option<String>,
    #[serde(default)]
    y: Option<String>,
    #[serde(default)]
    crv: Option<String>,
    #[serde(default)]
    k: Option<String>,
}

#[derive(Debug, Clone)]
struct RoleRuntime {
    grants: Vec<GrantRuntime>,
}

#[derive(Debug, Clone)]
struct GrantRuntime {
    action: RbacAction,
    resource: RbacResource,
}

#[derive(Debug, Clone)]
struct PrincipalRuntime {
    disabled: bool,
    bindings: Vec<BindingRuntime>,
}

#[derive(Debug, Clone)]
struct ServiceAccountRuntime {
    description: Option<String>,
    disabled: bool,
    created_unix_ms: u64,
    updated_unix_ms: u64,
    last_rotated_unix_ms: u64,
    bindings: Vec<BindingRuntime>,
}

#[derive(Debug, Clone)]
struct BindingRuntime {
    role: String,
    scopes: Vec<RbacResource>,
}

#[derive(Debug, Clone)]
struct OidcProviderRuntime {
    name: String,
    issuer: String,
    audiences: Vec<String>,
    username_claim: Option<String>,
    jwks_url: Option<String>,
    keys: Vec<OidcKeyRuntime>,
    claim_mappings: Vec<OidcClaimMappingRuntime>,
}

#[derive(Debug, Clone)]
struct OidcClaimMappingRuntime {
    claim: String,
    value: String,
    bindings: Vec<BindingRuntime>,
}

#[derive(Debug, Clone)]
enum OidcKeyRuntime {
    Rsa {
        kid: Option<String>,
        alg: Option<String>,
        n: Vec<u8>,
        e: Vec<u8>,
    },
    EcP256 {
        kid: Option<String>,
        alg: Option<String>,
        x: Vec<u8>,
        y: Vec<u8>,
    },
    Oct {
        kid: Option<String>,
        alg: Option<String>,
        secret: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
enum TokenIdentity {
    Principal(String),
    ServiceAccount(String),
}

#[derive(Debug, Clone)]
struct RbacRuntimeState {
    source_path: Option<PathBuf>,
    last_loaded_unix_ms: u64,
    config: RbacConfigFile,
    roles: BTreeMap<String, RoleRuntime>,
    principals: BTreeMap<String, PrincipalRuntime>,
    service_accounts: BTreeMap<String, ServiceAccountRuntime>,
    token_index: BTreeMap<String, TokenIdentity>,
    oidc_providers: BTreeMap<String, OidcProviderRuntime>,
}

#[derive(Debug)]
pub struct RbacRegistry {
    state: RwLock<RbacRuntimeState>,
    audit: Mutex<VecDeque<RbacAuditEntry>>,
    audit_seq: AtomicU64,
}

#[derive(Default)]
struct RbacAuditEntryInput {
    event: String,
    outcome: String,
    principal_id: Option<String>,
    role: Option<String>,
    action: Option<RbacAction>,
    resource: Option<RbacResource>,
    code: String,
    auth_method: Option<String>,
    provider: Option<String>,
    subject: Option<String>,
    detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacStateSnapshot {
    pub enabled: bool,
    pub source_path: Option<String>,
    pub last_loaded_unix_ms: u64,
    pub roles: Vec<RbacRoleSnapshot>,
    pub principals: Vec<RbacPrincipalSnapshot>,
    pub service_accounts: Vec<RbacServiceAccountSnapshot>,
    pub oidc_providers: Vec<RbacOidcProviderSnapshot>,
    pub audit_entries: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacRoleSnapshot {
    pub name: String,
    pub grants: Vec<RbacGrantSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacGrantSnapshot {
    pub action: RbacAction,
    pub resource: RbacResource,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacPrincipalSnapshot {
    pub id: String,
    pub disabled: bool,
    pub bindings: Vec<RbacBindingSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RbacBindingSnapshot {
    pub role: String,
    #[serde(default)]
    pub scopes: Vec<RbacResource>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacServiceAccountSnapshot {
    pub id: String,
    pub description: Option<String>,
    pub disabled: bool,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
    pub last_rotated_unix_ms: u64,
    pub bindings: Vec<RbacBindingSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacOidcProviderSnapshot {
    pub name: String,
    pub issuer: String,
    pub audiences: Vec<String>,
    pub username_claim: Option<String>,
    pub jwks_url: Option<String>,
    pub key_ids: Vec<String>,
    pub claim_mappings: Vec<RbacOidcClaimMappingSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacOidcClaimMappingSnapshot {
    pub claim: String,
    pub value: String,
    pub bindings: Vec<RbacBindingSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacAuditEntry {
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub event: String,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<RbacAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<RbacResource>,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceAccountSpec {
    pub id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub bindings: Vec<RbacBindingSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountCredential {
    pub service_account: RbacServiceAccountSnapshot,
    pub token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JwtHeader {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JwkSetDocument {
    #[serde(default)]
    keys: Vec<OidcJwkDefinition>,
}

impl RbacRegistry {
    pub fn load_from_path(path: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|err| format!("failed to read RBAC config {}: {err}", path.display()))?;
        Self::from_json_source(&raw, Some(path.to_path_buf()))
    }

    #[cfg(test)]
    pub fn from_json_str(raw: &str) -> Result<Self, String> {
        Self::from_json_source(raw, None)
    }

    fn from_json_source(raw: &str, source_path: Option<PathBuf>) -> Result<Self, String> {
        let file: RbacConfigFile =
            serde_json::from_str(raw).map_err(|err| format!("invalid RBAC config JSON: {err}"))?;
        let state = build_runtime_state(file, source_path)?;
        let registry = Self {
            state: RwLock::new(state),
            audit: Mutex::new(VecDeque::with_capacity(RBAC_AUDIT_CAPACITY)),
            audit_seq: AtomicU64::new(0),
        };
        registry.push_audit_entry(RbacAuditEntryInput {
            event: "reload".to_string(),
            outcome: "success".to_string(),
            code: "rbac_loaded".to_string(),
            ..Default::default()
        });
        Ok(registry)
    }

    pub fn reload(&self) -> Result<RbacStateSnapshot, String> {
        let source_path = {
            let state = self
                .state
                .read()
                .expect("RBAC registry state lock should not be poisoned");
            state
                .source_path
                .clone()
                .ok_or_else(|| "RBAC registry does not have a backing config path".to_string())?
        };
        let raw = fs::read_to_string(&source_path).map_err(|err| {
            format!(
                "failed to read RBAC config {}: {err}",
                source_path.display()
            )
        })?;
        let file: RbacConfigFile =
            serde_json::from_str(&raw).map_err(|err| format!("invalid RBAC config JSON: {err}"))?;
        let state = build_runtime_state(file, Some(source_path.clone()))?;
        {
            let mut current = self
                .state
                .write()
                .expect("RBAC registry state lock should not be poisoned");
            *current = state;
        }
        self.push_audit_entry(RbacAuditEntryInput {
            event: "reload".to_string(),
            outcome: "success".to_string(),
            code: "rbac_reloaded".to_string(),
            detail: Some(source_path.display().to_string()),
            ..Default::default()
        });
        Ok(self.state_snapshot())
    }

    pub fn authorize(
        &self,
        token: Option<&str>,
        permission: &RbacPermission,
    ) -> Result<AuthorizedPrincipal, AuthorizationError> {
        let Some(token) = token else {
            let err = AuthorizationError::unauthorized("auth_token_missing");
            self.record_deny(permission, &err);
            return Err(err);
        };

        let decision = {
            let state = self
                .state
                .read()
                .expect("RBAC registry state lock should not be poisoned");
            if let Some(identity) = state.token_index.get(token) {
                authorize_token_identity(&state, identity, permission)
            } else if looks_like_jwt(token) {
                authorize_oidc_token(&state, token, permission)
            } else {
                Err(AuthorizationError::unauthorized("auth_token_invalid"))
            }
        };

        match &decision {
            Ok(authorized) => self.record_allow(permission, authorized),
            Err(err) => self.record_deny(permission, err),
        }
        decision
    }

    pub fn create_service_account(
        &self,
        spec: ServiceAccountSpec,
    ) -> Result<ServiceAccountCredential, String> {
        let id = spec.id.trim().to_string();
        let token = self.generate_unique_service_account_token()?;
        let now = now_unix_ms();
        let result = self.mutate_config(|config| {
            validate_identifier(&id, "service account id")?;
            if config.principals.iter().any(|principal| principal.id == id) {
                return Err(format!(
                    "service account id '{}' conflicts with an existing principal",
                    id
                ));
            }
            if config
                .service_accounts
                .iter()
                .any(|service_account| service_account.id == id)
            {
                return Err(format!("duplicate service account id '{}'", id));
            }
            config.service_accounts.push(ServiceAccountDefinition {
                id: id.clone(),
                token: token.clone(),
                description: normalize_optional_text(spec.description.clone()),
                disabled: spec.disabled,
                created_unix_ms: now,
                updated_unix_ms: now,
                last_rotated_unix_ms: now,
                bindings: binding_snapshots_to_definitions(&spec.bindings),
            });
            Ok(())
        });
        match result {
            Ok(()) => {
                let service_account = self
                    .service_account_snapshot(&id)
                    .ok_or_else(|| format!("service account '{}' was not persisted", id))?;
                self.push_audit_entry(RbacAuditEntryInput {
                    event: "service_account_create".to_string(),
                    outcome: "success".to_string(),
                    principal_id: Some(id.clone()),
                    code: "service_account_created".to_string(),
                    auth_method: Some("service_account".to_string()),
                    ..Default::default()
                });
                Ok(ServiceAccountCredential {
                    service_account,
                    token,
                })
            }
            Err(err) => {
                self.push_audit_entry(RbacAuditEntryInput {
                    event: "service_account_create".to_string(),
                    outcome: "error".to_string(),
                    principal_id: Some(id),
                    code: "service_account_create_failed".to_string(),
                    auth_method: Some("service_account".to_string()),
                    detail: Some(err.clone()),
                    ..Default::default()
                });
                Err(err)
            }
        }
    }

    pub fn update_service_account(
        &self,
        spec: ServiceAccountSpec,
    ) -> Result<RbacServiceAccountSnapshot, String> {
        let id = spec.id.trim().to_string();
        let now = now_unix_ms();
        let result = self.mutate_config(|config| {
            let Some(service_account) = config
                .service_accounts
                .iter_mut()
                .find(|service_account| service_account.id == id)
            else {
                return Err(format!("unknown service account '{}'", id));
            };
            service_account.description = normalize_optional_text(spec.description.clone());
            service_account.disabled = spec.disabled;
            service_account.updated_unix_ms = now;
            service_account.bindings = binding_snapshots_to_definitions(&spec.bindings);
            Ok(())
        });
        match result {
            Ok(()) => {
                let snapshot = self
                    .service_account_snapshot(&id)
                    .ok_or_else(|| format!("service account '{}' was not persisted", id))?;
                self.push_audit_entry(RbacAuditEntryInput {
                    event: "service_account_update".to_string(),
                    outcome: "success".to_string(),
                    principal_id: Some(id),
                    code: "service_account_updated".to_string(),
                    auth_method: Some("service_account".to_string()),
                    ..Default::default()
                });
                Ok(snapshot)
            }
            Err(err) => {
                self.push_audit_entry(RbacAuditEntryInput {
                    event: "service_account_update".to_string(),
                    outcome: "error".to_string(),
                    principal_id: Some(id),
                    code: "service_account_update_failed".to_string(),
                    auth_method: Some("service_account".to_string()),
                    detail: Some(err.clone()),
                    ..Default::default()
                });
                Err(err)
            }
        }
    }

    pub fn rotate_service_account(&self, id: &str) -> Result<ServiceAccountCredential, String> {
        let id = id.trim().to_string();
        validate_identifier(&id, "service account id")?;
        let token = self.generate_unique_service_account_token()?;
        let now = now_unix_ms();
        let result = self.mutate_config(|config| {
            let Some(service_account) = config
                .service_accounts
                .iter_mut()
                .find(|service_account| service_account.id == id)
            else {
                return Err(format!("unknown service account '{}'", id));
            };
            service_account.token = token.clone();
            service_account.updated_unix_ms = now;
            service_account.last_rotated_unix_ms = now;
            Ok(())
        });
        match result {
            Ok(()) => {
                let service_account = self
                    .service_account_snapshot(&id)
                    .ok_or_else(|| format!("service account '{}' was not persisted", id))?;
                self.push_audit_entry(RbacAuditEntryInput {
                    event: "service_account_rotate".to_string(),
                    outcome: "success".to_string(),
                    principal_id: Some(id.clone()),
                    code: "service_account_rotated".to_string(),
                    auth_method: Some("service_account".to_string()),
                    ..Default::default()
                });
                Ok(ServiceAccountCredential {
                    service_account,
                    token,
                })
            }
            Err(err) => {
                self.push_audit_entry(RbacAuditEntryInput {
                    event: "service_account_rotate".to_string(),
                    outcome: "error".to_string(),
                    principal_id: Some(id),
                    code: "service_account_rotate_failed".to_string(),
                    auth_method: Some("service_account".to_string()),
                    detail: Some(err.clone()),
                    ..Default::default()
                });
                Err(err)
            }
        }
    }

    pub fn set_service_account_disabled(
        &self,
        id: &str,
        disabled: bool,
    ) -> Result<RbacServiceAccountSnapshot, String> {
        let id = id.trim().to_string();
        validate_identifier(&id, "service account id")?;
        let now = now_unix_ms();
        let result = self.mutate_config(|config| {
            let Some(service_account) = config
                .service_accounts
                .iter_mut()
                .find(|service_account| service_account.id == id)
            else {
                return Err(format!("unknown service account '{}'", id));
            };
            service_account.disabled = disabled;
            service_account.updated_unix_ms = now;
            Ok(())
        });
        match result {
            Ok(()) => {
                let snapshot = self
                    .service_account_snapshot(&id)
                    .ok_or_else(|| format!("service account '{}' was not persisted", id))?;
                self.push_audit_entry(RbacAuditEntryInput {
                    event: if disabled {
                        "service_account_disable".to_string()
                    } else {
                        "service_account_enable".to_string()
                    },
                    outcome: "success".to_string(),
                    principal_id: Some(id),
                    code: if disabled {
                        "service_account_disabled".to_string()
                    } else {
                        "service_account_enabled".to_string()
                    },
                    auth_method: Some("service_account".to_string()),
                    ..Default::default()
                });
                Ok(snapshot)
            }
            Err(err) => {
                self.push_audit_entry(RbacAuditEntryInput {
                    event: if disabled {
                        "service_account_disable".to_string()
                    } else {
                        "service_account_enable".to_string()
                    },
                    outcome: "error".to_string(),
                    principal_id: Some(id),
                    code: if disabled {
                        "service_account_disable_failed".to_string()
                    } else {
                        "service_account_enable_failed".to_string()
                    },
                    auth_method: Some("service_account".to_string()),
                    detail: Some(err.clone()),
                    ..Default::default()
                });
                Err(err)
            }
        }
    }

    pub fn state_snapshot(&self) -> RbacStateSnapshot {
        let state = self
            .state
            .read()
            .expect("RBAC registry state lock should not be poisoned");
        let roles = state
            .roles
            .iter()
            .map(|(name, role)| RbacRoleSnapshot {
                name: name.clone(),
                grants: role
                    .grants
                    .iter()
                    .map(|grant| RbacGrantSnapshot {
                        action: grant.action,
                        resource: grant.resource.clone(),
                    })
                    .collect(),
            })
            .collect();
        let principals = state
            .principals
            .iter()
            .map(|(id, principal)| RbacPrincipalSnapshot {
                id: id.clone(),
                disabled: principal.disabled,
                bindings: bindings_to_snapshots(&principal.bindings),
            })
            .collect();
        let service_accounts = state
            .service_accounts
            .iter()
            .map(|(id, service_account)| RbacServiceAccountSnapshot {
                id: id.clone(),
                description: service_account.description.clone(),
                disabled: service_account.disabled,
                created_unix_ms: service_account.created_unix_ms,
                updated_unix_ms: service_account.updated_unix_ms,
                last_rotated_unix_ms: service_account.last_rotated_unix_ms,
                bindings: bindings_to_snapshots(&service_account.bindings),
            })
            .collect();
        let oidc_providers = state
            .oidc_providers
            .values()
            .map(|provider| RbacOidcProviderSnapshot {
                name: provider.name.clone(),
                issuer: provider.issuer.clone(),
                audiences: provider.audiences.clone(),
                username_claim: provider.username_claim.clone(),
                jwks_url: provider.jwks_url.clone(),
                key_ids: provider
                    .keys
                    .iter()
                    .map(OidcKeyRuntime::descriptor)
                    .collect(),
                claim_mappings: provider
                    .claim_mappings
                    .iter()
                    .map(|mapping| RbacOidcClaimMappingSnapshot {
                        claim: mapping.claim.clone(),
                        value: mapping.value.clone(),
                        bindings: bindings_to_snapshots(&mapping.bindings),
                    })
                    .collect(),
            })
            .collect();
        let audit_entries = self
            .audit
            .lock()
            .expect("RBAC audit lock should not be poisoned")
            .len();
        RbacStateSnapshot {
            enabled: true,
            source_path: state
                .source_path
                .as_ref()
                .map(|path| path.display().to_string()),
            last_loaded_unix_ms: state.last_loaded_unix_ms,
            roles,
            principals,
            service_accounts,
            oidc_providers,
            audit_entries,
        }
    }

    pub fn audit_snapshot(&self, limit: usize) -> Vec<RbacAuditEntry> {
        let audit = self
            .audit
            .lock()
            .expect("RBAC audit lock should not be poisoned");
        let count = if limit == 0 {
            audit.len()
        } else {
            limit.min(audit.len())
        };
        audit.iter().rev().take(count).cloned().collect()
    }

    fn mutate_config<F>(&self, mutator: F) -> Result<(), String>
    where
        F: FnOnce(&mut RbacConfigFile) -> Result<(), String>,
    {
        let (source_path, mut config) = {
            let state = self
                .state
                .read()
                .expect("RBAC registry state lock should not be poisoned");
            (state.source_path.clone(), state.config.clone())
        };
        mutator(&mut config)?;
        let state = build_runtime_state(config.clone(), source_path.clone())?;
        if let Some(path) = source_path.as_ref() {
            let raw = serde_json::to_vec_pretty(&config)
                .map_err(|err| format!("failed to encode RBAC config: {err}"))?;
            fs::write(path, raw)
                .map_err(|err| format!("failed to write RBAC config {}: {err}", path.display()))?;
        }
        let mut current = self
            .state
            .write()
            .expect("RBAC registry state lock should not be poisoned");
        *current = state;
        Ok(())
    }

    fn service_account_snapshot(&self, id: &str) -> Option<RbacServiceAccountSnapshot> {
        let state = self
            .state
            .read()
            .expect("RBAC registry state lock should not be poisoned");
        let service_account = state.service_accounts.get(id)?;
        Some(RbacServiceAccountSnapshot {
            id: id.to_string(),
            description: service_account.description.clone(),
            disabled: service_account.disabled,
            created_unix_ms: service_account.created_unix_ms,
            updated_unix_ms: service_account.updated_unix_ms,
            last_rotated_unix_ms: service_account.last_rotated_unix_ms,
            bindings: bindings_to_snapshots(&service_account.bindings),
        })
    }

    fn generate_unique_service_account_token(&self) -> Result<String, String> {
        let random = SystemRandom::new();
        for _ in 0..32 {
            let mut bytes = [0_u8; SERVICE_ACCOUNT_TOKEN_BYTES];
            random
                .fill(&mut bytes)
                .map_err(|_| "failed to generate service account token".to_string())?;
            let token = format!("tsa_{}", URL_SAFE_NO_PAD.encode(bytes));
            let exists = {
                let state = self
                    .state
                    .read()
                    .expect("RBAC registry state lock should not be poisoned");
                state.token_index.contains_key(&token)
            };
            if !exists {
                return Ok(token);
            }
        }
        Err("failed to generate a unique service account token".to_string())
    }

    fn record_allow(&self, permission: &RbacPermission, authorized: &AuthorizedPrincipal) {
        self.push_audit_entry(RbacAuditEntryInput {
            event: "decision".to_string(),
            outcome: "allow".to_string(),
            principal_id: Some(authorized.principal_id.clone()),
            role: Some(authorized.role.clone()),
            action: Some(permission.action),
            resource: Some(permission.resource.clone()),
            code: "authorized".to_string(),
            auth_method: Some(authorized.auth_method.clone()),
            provider: authorized.provider.clone(),
            subject: authorized.subject.clone(),
            detail: None,
        });
    }

    fn record_deny(&self, permission: &RbacPermission, err: &AuthorizationError) {
        self.push_audit_entry(RbacAuditEntryInput {
            event: "decision".to_string(),
            outcome: "deny".to_string(),
            principal_id: err.principal_id().cloned(),
            role: None,
            action: Some(permission.action),
            resource: Some(permission.resource.clone()),
            code: err.code().to_string(),
            auth_method: err.auth_method().cloned(),
            provider: err.provider().cloned(),
            subject: err.subject().cloned(),
            detail: err.detail().cloned(),
        });
    }

    fn push_audit_entry(&self, input: RbacAuditEntryInput) {
        let sequence = self.audit_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let entry = RbacAuditEntry {
            sequence,
            timestamp_unix_ms: now_unix_ms(),
            event: input.event,
            outcome: input.outcome,
            principal_id: input.principal_id,
            role: input.role,
            action: input.action,
            resource: input.resource,
            code: input.code,
            auth_method: input.auth_method,
            provider: input.provider,
            subject: input.subject,
            detail: input.detail,
        };
        let mut audit = self
            .audit
            .lock()
            .expect("RBAC audit lock should not be poisoned");
        if audit.len() == RBAC_AUDIT_CAPACITY {
            audit.pop_front();
        }
        audit.push_back(entry);
    }
}

fn build_runtime_state(
    file: RbacConfigFile,
    source_path: Option<PathBuf>,
) -> Result<RbacRuntimeState, String> {
    let mut roles = BTreeMap::new();
    for (role_name, role) in &file.roles {
        validate_identifier(role_name, "RBAC role name")?;
        let mut grants = Vec::with_capacity(role.grants.len());
        for grant in &role.grants {
            validate_resource(&grant.resource)?;
            grants.push(GrantRuntime {
                action: grant.action,
                resource: grant.resource.clone(),
            });
        }
        roles.insert(role_name.clone(), RoleRuntime { grants });
    }

    let mut principals = BTreeMap::new();
    let mut service_accounts = BTreeMap::new();
    let mut token_index = BTreeMap::new();

    for principal in &file.principals {
        validate_identifier(&principal.id, "RBAC principal id")?;
        let token = principal.token.trim();
        if token.is_empty() {
            return Err(format!(
                "RBAC principal '{}' token must not be empty",
                principal.id
            ));
        }
        if principals.contains_key(&principal.id) {
            return Err(format!("duplicate RBAC principal id '{}'", principal.id));
        }
        if service_accounts.contains_key(&principal.id) {
            return Err(format!(
                "RBAC principal '{}' conflicts with a service account id",
                principal.id
            ));
        }
        if token_index.contains_key(token) {
            return Err("duplicate RBAC token values are not allowed".to_string());
        }
        principals.insert(
            principal.id.clone(),
            PrincipalRuntime {
                disabled: principal.disabled,
                bindings: build_bindings(&principal.bindings, &roles, &principal.id)?,
            },
        );
        token_index.insert(
            token.to_string(),
            TokenIdentity::Principal(principal.id.clone()),
        );
    }

    for service_account in &file.service_accounts {
        validate_identifier(&service_account.id, "service account id")?;
        let token = service_account.token.trim();
        if token.is_empty() {
            return Err(format!(
                "service account '{}' token must not be empty",
                service_account.id
            ));
        }
        if principals.contains_key(&service_account.id) {
            return Err(format!(
                "service account '{}' conflicts with an existing principal id",
                service_account.id
            ));
        }
        if service_accounts.contains_key(&service_account.id) {
            return Err(format!(
                "duplicate service account id '{}'",
                service_account.id
            ));
        }
        if token_index.contains_key(token) {
            return Err("duplicate RBAC token values are not allowed".to_string());
        }
        service_accounts.insert(
            service_account.id.clone(),
            ServiceAccountRuntime {
                description: normalize_optional_text(service_account.description.clone()),
                disabled: service_account.disabled,
                created_unix_ms: service_account.created_unix_ms,
                updated_unix_ms: service_account.updated_unix_ms,
                last_rotated_unix_ms: service_account.last_rotated_unix_ms,
                bindings: build_bindings(&service_account.bindings, &roles, &service_account.id)?,
            },
        );
        token_index.insert(
            token.to_string(),
            TokenIdentity::ServiceAccount(service_account.id.clone()),
        );
    }

    let mut oidc_providers = BTreeMap::new();
    for provider in &file.oidc_providers {
        validate_identifier(&provider.name, "OIDC provider name")?;
        if oidc_providers.contains_key(&provider.name) {
            return Err(format!("duplicate OIDC provider name '{}'", provider.name));
        }
        let issuer = provider.issuer.trim();
        if issuer.is_empty() {
            return Err(format!(
                "OIDC provider '{}' issuer must not be empty",
                provider.name
            ));
        }
        let audiences = provider
            .audiences
            .iter()
            .map(|audience| audience.trim().to_string())
            .collect::<Vec<_>>();
        if audiences.iter().any(|audience| audience.is_empty()) {
            return Err(format!(
                "OIDC provider '{}' audiences must not contain empty values",
                provider.name
            ));
        }
        let username_claim = normalize_optional_text(provider.username_claim.clone());
        let claim_mappings = provider
            .claim_mappings
            .iter()
            .map(|mapping| {
                validate_identifier(&mapping.claim, "OIDC claim mapping claim")?;
                let value = mapping.value.trim();
                if value.is_empty() {
                    return Err(format!(
                        "OIDC provider '{}' claim mappings must not have an empty value",
                        provider.name
                    ));
                }
                Ok(OidcClaimMappingRuntime {
                    claim: mapping.claim.clone(),
                    value: value.to_string(),
                    bindings: build_bindings(&mapping.bindings, &roles, &provider.name)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let keys = load_oidc_keys(provider)?;
        if keys.is_empty() {
            return Err(format!(
                "OIDC provider '{}' must define at least one signing key",
                provider.name
            ));
        }
        oidc_providers.insert(
            provider.name.clone(),
            OidcProviderRuntime {
                name: provider.name.clone(),
                issuer: issuer.to_string(),
                audiences,
                username_claim,
                jwks_url: normalize_optional_text(provider.jwks_url.clone()),
                keys,
                claim_mappings,
            },
        );
    }

    Ok(RbacRuntimeState {
        source_path,
        last_loaded_unix_ms: now_unix_ms(),
        config: file,
        roles,
        principals,
        service_accounts,
        token_index,
        oidc_providers,
    })
}

fn build_bindings(
    bindings: &[BindingDefinition],
    roles: &BTreeMap<String, RoleRuntime>,
    owner: &str,
) -> Result<Vec<BindingRuntime>, String> {
    let mut runtime = Vec::with_capacity(bindings.len());
    for binding in bindings {
        if !roles.contains_key(&binding.role) {
            return Err(format!(
                "RBAC principal '{}' references unknown role '{}'",
                owner, binding.role
            ));
        }
        for scope in &binding.scopes {
            validate_resource(scope)?;
        }
        runtime.push(BindingRuntime {
            role: binding.role.clone(),
            scopes: binding.scopes.clone(),
        });
    }
    Ok(runtime)
}

fn load_oidc_keys(provider: &OidcProviderDefinition) -> Result<Vec<OidcKeyRuntime>, String> {
    let mut keys = Vec::new();
    for key in &provider.jwks {
        keys.push(parse_oidc_jwk(key).map_err(|err| {
            format!(
                "OIDC provider '{}' has an invalid JWK: {err}",
                provider.name
            )
        })?);
    }
    if let Some(jwks_url) = provider
        .jwks_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        for key in fetch_remote_jwks(jwks_url)? {
            keys.push(parse_oidc_jwk(&key).map_err(|err| {
                format!(
                    "OIDC provider '{}' JWKS {} contains an invalid key: {err}",
                    provider.name, jwks_url
                )
            })?);
        }
    }
    Ok(keys)
}

fn fetch_remote_jwks(jwks_url: &str) -> Result<Vec<OidcJwkDefinition>, String> {
    let client = Client::builder()
        .timeout(OIDC_HTTP_TIMEOUT)
        .build()
        .map_err(|err| format!("failed to build OIDC JWKS client: {err}"))?;
    let response = client
        .get(jwks_url)
        .send()
        .map_err(|err| format!("failed to fetch OIDC JWKS {}: {err}", jwks_url))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|err| format!("failed to read OIDC JWKS {}: {err}", jwks_url))?;
    if !status.is_success() {
        return Err(format!("OIDC JWKS {} returned HTTP {}", jwks_url, status));
    }
    if let Ok(document) = serde_json::from_str::<JwkSetDocument>(&body) {
        return Ok(document.keys);
    }
    serde_json::from_str::<Vec<OidcJwkDefinition>>(&body)
        .map_err(|err| format!("invalid OIDC JWKS document {}: {err}", jwks_url))
}

fn parse_oidc_jwk(key: &OidcJwkDefinition) -> Result<OidcKeyRuntime, String> {
    if let Some(use_) = key.use_.as_deref() {
        if !use_.eq_ignore_ascii_case("sig") {
            return Err(format!(
                "unsupported JWK use '{}'; only 'sig' is supported",
                use_
            ));
        }
    }
    match key.kty.as_str() {
        "RSA" => {
            let n = base64url_decode(
                key.n
                    .as_deref()
                    .ok_or_else(|| "RSA JWK is missing modulus 'n'".to_string())?,
            )?;
            let e = base64url_decode(
                key.e
                    .as_deref()
                    .ok_or_else(|| "RSA JWK is missing exponent 'e'".to_string())?,
            )?;
            Ok(OidcKeyRuntime::Rsa {
                kid: key.kid.clone(),
                alg: key.alg.clone(),
                n,
                e,
            })
        }
        "EC" => {
            let crv = key.crv.as_deref().unwrap_or_default();
            if crv != "P-256" {
                return Err(format!(
                    "unsupported EC curve '{}'; only P-256 is supported",
                    crv
                ));
            }
            let x = base64url_decode(
                key.x
                    .as_deref()
                    .ok_or_else(|| "EC JWK is missing coordinate 'x'".to_string())?,
            )?;
            let y = base64url_decode(
                key.y
                    .as_deref()
                    .ok_or_else(|| "EC JWK is missing coordinate 'y'".to_string())?,
            )?;
            Ok(OidcKeyRuntime::EcP256 {
                kid: key.kid.clone(),
                alg: key.alg.clone(),
                x,
                y,
            })
        }
        "oct" => {
            let secret = base64url_decode(
                key.k
                    .as_deref()
                    .ok_or_else(|| "oct JWK is missing secret 'k'".to_string())?,
            )?;
            Ok(OidcKeyRuntime::Oct {
                kid: key.kid.clone(),
                alg: key.alg.clone(),
                secret,
            })
        }
        other => Err(format!("unsupported JWK key type '{}'", other)),
    }
}

fn authorize_token_identity(
    state: &RbacRuntimeState,
    identity: &TokenIdentity,
    permission: &RbacPermission,
) -> Result<AuthorizedPrincipal, AuthorizationError> {
    match identity {
        TokenIdentity::Principal(id) => {
            let principal = state
                .principals
                .get(id)
                .expect("principal token index should point to an existing principal");
            if principal.disabled {
                return Err(AuthorizationError::forbidden("auth_principal_disabled")
                    .with_identity(Some(id.clone()), Some("token".to_string()), None, None));
            }
            authorize_bindings(
                &state.roles,
                &principal.bindings,
                id.clone(),
                "token",
                None,
                None,
                None,
                permission,
            )
        }
        TokenIdentity::ServiceAccount(id) => {
            let service_account = state
                .service_accounts
                .get(id)
                .expect("service account token index should point to an existing account");
            if service_account.disabled {
                return Err(AuthorizationError::forbidden("auth_principal_disabled")
                    .with_identity(
                        Some(id.clone()),
                        Some("service_account".to_string()),
                        None,
                        None,
                    ));
            }
            authorize_bindings(
                &state.roles,
                &service_account.bindings,
                id.clone(),
                "service_account",
                None,
                Some(id.clone()),
                Some(id.clone()),
                permission,
            )
        }
    }
}

fn authorize_oidc_token(
    state: &RbacRuntimeState,
    token: &str,
    permission: &RbacPermission,
) -> Result<AuthorizedPrincipal, AuthorizationError> {
    let (encoded_header, encoded_claims, encoded_signature) = token
        .split_once('.')
        .and_then(|(head, rest)| {
            rest.split_once('.')
                .map(|(claims, sig)| (head, claims, sig))
        })
        .ok_or_else(|| AuthorizationError::unauthorized("auth_token_invalid"))?;
    let signed_data = format!("{encoded_header}.{encoded_claims}");

    let header_bytes = base64url_decode(encoded_header)
        .map_err(|err| AuthorizationError::unauthorized("auth_token_invalid").with_detail(err))?;
    let claims_bytes = base64url_decode(encoded_claims)
        .map_err(|err| AuthorizationError::unauthorized("auth_token_invalid").with_detail(err))?;
    let signature = base64url_decode(encoded_signature)
        .map_err(|err| AuthorizationError::unauthorized("auth_token_invalid").with_detail(err))?;

    let header: JwtHeader = serde_json::from_slice(&header_bytes).map_err(|err| {
        AuthorizationError::unauthorized("auth_token_invalid")
            .with_detail(format!("invalid JWT header: {err}"))
    })?;
    let claims: JsonValue = serde_json::from_slice(&claims_bytes).map_err(|err| {
        AuthorizationError::unauthorized("auth_token_invalid")
            .with_detail(format!("invalid JWT claims: {err}"))
    })?;

    let issuer = claim_string(&claims, "iss")
        .ok_or_else(|| AuthorizationError::unauthorized("auth_oidc_issuer_missing"))?;
    let provider = state
        .oidc_providers
        .values()
        .find(|provider| provider.issuer == issuer)
        .ok_or_else(|| {
            AuthorizationError::unauthorized("auth_oidc_provider_not_configured")
                .with_detail(format!("issuer '{}' is not configured", issuer))
        })?;
    verify_oidc_signature(provider, &header, signed_data.as_bytes(), &signature).map_err(
        |err| {
            err.with_identity(
                None,
                Some("oidc".to_string()),
                Some(provider.name.clone()),
                None,
            )
        },
    )?;
    validate_oidc_claims(provider, &claims).map_err(|err| {
        err.with_identity(
            None,
            Some("oidc".to_string()),
            Some(provider.name.clone()),
            None,
        )
    })?;

    let subject = claim_string(&claims, "sub")
        .ok_or_else(|| AuthorizationError::unauthorized("auth_oidc_subject_missing"))?;
    let principal_id = format!("oidc:{}:{}", provider.name, subject);
    let display_name = provider
        .username_claim
        .as_deref()
        .and_then(|claim| claim_values(&claims, claim).into_iter().next());

    let matched_bindings = provider
        .claim_mappings
        .iter()
        .filter(|mapping| {
            claim_values(&claims, &mapping.claim)
                .iter()
                .any(|value| pattern_matches(&mapping.value, value))
        })
        .flat_map(|mapping| mapping.bindings.iter().cloned())
        .collect::<Vec<_>>();

    authorize_bindings(
        &state.roles,
        &matched_bindings,
        principal_id,
        "oidc",
        Some(provider.name.clone()),
        Some(subject),
        display_name,
        permission,
    )
}

fn verify_oidc_signature(
    provider: &OidcProviderRuntime,
    header: &JwtHeader,
    signed_data: &[u8],
    signature: &[u8],
) -> Result<(), AuthorizationError> {
    let mut attempted = false;
    for key in &provider.keys {
        if !key.matches_kid(header.kid.as_deref()) {
            continue;
        }
        if !key.matches_alg(&header.alg) {
            continue;
        }
        attempted = true;
        let result = match key {
            OidcKeyRuntime::Rsa { n, e, .. } if header.alg == "RS256" => RsaPublicKeyComponents {
                n: n.as_slice(),
                e: e.as_slice(),
            }
            .verify(
                &signature::RSA_PKCS1_2048_8192_SHA256,
                signed_data,
                signature,
            ),
            OidcKeyRuntime::EcP256 { x, y, .. } if header.alg == "ES256" => {
                let mut public_key = Vec::with_capacity(1 + x.len() + y.len());
                public_key.push(0x04);
                public_key.extend_from_slice(x);
                public_key.extend_from_slice(y);
                UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, public_key)
                    .verify(signed_data, signature)
            }
            OidcKeyRuntime::Oct { secret, .. } if header.alg == "HS256" => hmac::verify(
                &hmac::Key::new(hmac::HMAC_SHA256, secret),
                signed_data,
                signature,
            ),
            _ => continue,
        };
        match result {
            Ok(()) => return Ok(()),
            Err(_) => {
                return Err(
                    AuthorizationError::unauthorized("auth_oidc_signature_invalid").with_detail(
                        format!(
                            "signature verification failed for provider '{}'",
                            provider.name
                        ),
                    ),
                )
            }
        }
    }
    if attempted {
        Err(AuthorizationError::unauthorized(
            "auth_oidc_signature_invalid",
        ))
    } else {
        Err(
            AuthorizationError::unauthorized("auth_oidc_signing_key_missing").with_detail(format!(
                "no signing key matched alg='{}' kid='{}'",
                header.alg,
                header.kid.as_deref().unwrap_or("")
            )),
        )
    }
}

fn validate_oidc_claims(
    provider: &OidcProviderRuntime,
    claims: &JsonValue,
) -> Result<(), AuthorizationError> {
    let Some(exp) = claims.get("exp").and_then(JsonValue::as_u64) else {
        return Err(AuthorizationError::unauthorized("auth_oidc_exp_missing"));
    };
    let now = now_unix_seconds();
    if exp.saturating_add(OIDC_CLOCK_SKEW_SECONDS) <= now {
        return Err(AuthorizationError::unauthorized("auth_oidc_token_expired"));
    }
    if let Some(nbf) = claims.get("nbf").and_then(JsonValue::as_u64) {
        if now.saturating_add(OIDC_CLOCK_SKEW_SECONDS) < nbf {
            return Err(AuthorizationError::unauthorized(
                "auth_oidc_token_not_yet_valid",
            ));
        }
    }
    if let Some(iat) = claims.get("iat").and_then(JsonValue::as_u64) {
        if now.saturating_add(OIDC_CLOCK_SKEW_SECONDS) < iat {
            return Err(AuthorizationError::unauthorized(
                "auth_oidc_token_not_yet_valid",
            ));
        }
    }
    if !provider.audiences.is_empty() {
        let audiences = claim_values(claims, "aud");
        if !audiences
            .iter()
            .any(|audience| provider.audiences.iter().any(|allowed| allowed == audience))
        {
            return Err(AuthorizationError::unauthorized(
                "auth_oidc_audience_invalid",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn authorize_bindings(
    roles: &BTreeMap<String, RoleRuntime>,
    bindings: &[BindingRuntime],
    principal_id: String,
    auth_method: &str,
    provider: Option<String>,
    subject: Option<String>,
    display_name: Option<String>,
    permission: &RbacPermission,
) -> Result<AuthorizedPrincipal, AuthorizationError> {
    let matched_role = bindings.iter().find_map(|binding| {
        let role = roles.get(&binding.role)?;
        let scope_allows = binding.scopes.is_empty()
            || binding
                .scopes
                .iter()
                .any(|scope| resource_matches(scope, &permission.resource));
        if !scope_allows {
            return None;
        }
        role.grants.iter().find_map(|grant| {
            if grant.action == permission.action
                && resource_matches(&grant.resource, &permission.resource)
            {
                Some(binding.role.clone())
            } else {
                None
            }
        })
    });

    match matched_role {
        Some(role) => Ok(AuthorizedPrincipal {
            principal_id,
            role,
            auth_method: auth_method.to_string(),
            provider,
            subject,
            display_name,
        }),
        None => Err(
            AuthorizationError::forbidden("auth_scope_denied").with_identity(
                Some(principal_id),
                Some(auth_method.to_string()),
                provider,
                subject,
            ),
        ),
    }
}

fn binding_snapshots_to_definitions(bindings: &[RbacBindingSnapshot]) -> Vec<BindingDefinition> {
    bindings
        .iter()
        .map(|binding| BindingDefinition {
            role: binding.role.clone(),
            scopes: binding.scopes.clone(),
        })
        .collect()
}

fn bindings_to_snapshots(bindings: &[BindingRuntime]) -> Vec<RbacBindingSnapshot> {
    bindings
        .iter()
        .map(|binding| RbacBindingSnapshot {
            role: binding.role.clone(),
            scopes: binding.scopes.clone(),
        })
        .collect()
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed.to_string())
    })
}

fn validate_identifier(value: &str, label: &str) -> Result<(), String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{label} must not contain control characters"));
    }
    Ok(())
}

fn validate_resource(resource: &RbacResource) -> Result<(), String> {
    let name = resource.name.trim();
    if name.is_empty() {
        return Err("RBAC resource name must not be empty".to_string());
    }
    if name.chars().any(char::is_control) {
        return Err("RBAC resource name must not contain control characters".to_string());
    }
    Ok(())
}

fn resource_matches(pattern: &RbacResource, requested: &RbacResource) -> bool {
    if pattern.kind != requested.kind {
        return false;
    }
    pattern_matches(&pattern.name, &requested.name)
}

fn pattern_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

fn base64url_decode(value: &str) -> Result<Vec<u8>, String> {
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|err| format!("invalid base64url value: {err}"))
}

fn looks_like_jwt(value: &str) -> bool {
    value
        .as_bytes()
        .iter()
        .filter(|byte| **byte == b'.')
        .count()
        == 2
}

fn claim_string(claims: &JsonValue, name: &str) -> Option<String> {
    claims
        .get(name)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn claim_values(claims: &JsonValue, name: &str) -> Vec<String> {
    match claims.get(name) {
        Some(JsonValue::String(value)) => {
            let value = value.trim();
            if value.is_empty() {
                Vec::new()
            } else {
                vec![value.to_string()]
            }
        }
        Some(JsonValue::Array(values)) => values
            .iter()
            .filter_map(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

impl OidcKeyRuntime {
    fn descriptor(&self) -> String {
        match self {
            Self::Rsa { kid, .. } | Self::EcP256 { kid, .. } | Self::Oct { kid, .. } => {
                kid.clone().unwrap_or_else(|| "<unnamed>".to_string())
            }
        }
    }

    fn matches_kid(&self, requested: Option<&str>) -> bool {
        match requested {
            Some(requested) => match self {
                Self::Rsa { kid, .. } | Self::EcP256 { kid, .. } | Self::Oct { kid, .. } => {
                    kid.as_deref() == Some(requested)
                }
            },
            None => true,
        }
    }

    fn matches_alg(&self, requested: &str) -> bool {
        match self {
            Self::Rsa { alg, .. } | Self::EcP256 { alg, .. } | Self::Oct { alg, .. } => {
                alg.as_deref().is_none_or(|alg| alg == requested)
            }
        }
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::hmac;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn scoped_bindings_limit_tenant_access() {
        let registry = RbacRegistry::from_json_str(
            r#"{
                "roles": {
                    "tenant-reader": {
                        "grants": [
                            {
                                "action": "read",
                                "resource": { "kind": "tenant", "name": "*" }
                            }
                        ]
                    }
                },
                "principals": [
                    {
                        "id": "reader-a",
                        "token": "reader-a-token",
                        "bindings": [
                            {
                                "role": "tenant-reader",
                                "scopes": [
                                    { "kind": "tenant", "name": "tenant-a" }
                                ]
                            }
                        ]
                    }
                ]
            }"#,
        )
        .expect("RBAC config should parse");

        let allowed = registry.authorize(
            Some("reader-a-token"),
            &RbacPermission::new(RbacAction::Read, RbacResource::tenant("tenant-a")),
        );
        assert_eq!(
            allowed.expect("tenant-a should be allowed"),
            AuthorizedPrincipal {
                principal_id: "reader-a".to_string(),
                role: "tenant-reader".to_string(),
                auth_method: "token".to_string(),
                provider: None,
                subject: None,
                display_name: None,
            }
        );

        let denied = registry
            .authorize(
                Some("reader-a-token"),
                &RbacPermission::new(RbacAction::Read, RbacResource::tenant("tenant-b")),
            )
            .expect_err("tenant-b should be denied");
        assert_eq!(denied.status(), 403);
        assert_eq!(denied.code(), "auth_scope_denied");
    }

    #[test]
    fn reload_replaces_state_and_records_audit() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let config_path = temp_dir.path().join("rbac.json");
        fs::write(
            &config_path,
            r#"{
                "roles": {
                    "rbac-reader": {
                        "grants": [
                            {
                                "action": "read",
                                "resource": { "kind": "admin", "name": "rbac" }
                            }
                        ]
                    }
                },
                "principals": [
                    {
                        "id": "reader",
                        "token": "reader-token",
                        "bindings": [{ "role": "rbac-reader" }]
                    }
                ]
            }"#,
        )
        .expect("RBAC config should be written");

        let registry =
            RbacRegistry::load_from_path(&config_path).expect("RBAC registry should load");
        registry
            .authorize(
                Some("reader-token"),
                &RbacPermission::new(RbacAction::Read, RbacResource::admin("rbac")),
            )
            .expect("initial reader should be allowed");

        fs::write(
            &config_path,
            r#"{
                "roles": {
                    "rbac-writer": {
                        "grants": [
                            {
                                "action": "write",
                                "resource": { "kind": "admin", "name": "rbac" }
                            }
                        ]
                    }
                },
                "principals": [
                    {
                        "id": "writer",
                        "token": "writer-token",
                        "bindings": [{ "role": "rbac-writer" }]
                    }
                ]
            }"#,
        )
        .expect("RBAC config should be overwritten");

        let snapshot = registry.reload().expect("reload should succeed");
        assert_eq!(snapshot.roles.len(), 1);
        assert_eq!(snapshot.principals.len(), 1);
        assert_eq!(snapshot.principals[0].id, "writer");

        let old = registry
            .authorize(
                Some("reader-token"),
                &RbacPermission::new(RbacAction::Read, RbacResource::admin("rbac")),
            )
            .expect_err("old token should no longer work");
        assert_eq!(old.code(), "auth_token_invalid");

        registry
            .authorize(
                Some("writer-token"),
                &RbacPermission::new(RbacAction::Write, RbacResource::admin("rbac")),
            )
            .expect("new writer should be allowed");

        let audit = registry.audit_snapshot(10);
        assert!(audit.iter().any(|entry| entry.code == "rbac_reloaded"));
    }

    #[test]
    fn oidc_claim_mappings_authorize_hs256_tokens() {
        let secret = b"super-secret-for-tests";
        let secret_b64 = URL_SAFE_NO_PAD.encode(secret);
        let registry = RbacRegistry::from_json_str(&format!(
            r#"{{
                    "roles": {{
                        "tenant-reader": {{
                            "grants": [
                                {{
                                    "action": "read",
                                    "resource": {{ "kind": "tenant", "name": "*" }}
                                }}
                            ]
                        }}
                    }},
                    "oidcProviders": [
                        {{
                            "name": "corp",
                            "issuer": "https://issuer.example",
                            "audiences": ["tsink"],
                            "usernameClaim": "email",
                            "jwks": [
                                {{
                                    "kid": "shared",
                                    "alg": "HS256",
                                    "kty": "oct",
                                    "k": "{secret_b64}"
                                }}
                            ],
                            "claimMappings": [
                                {{
                                    "claim": "groups",
                                    "value": "metrics-*",
                                    "bindings": [
                                        {{
                                            "role": "tenant-reader",
                                            "scopes": [
                                                {{ "kind": "tenant", "name": "team-a" }}
                                            ]
                                        }}
                                    ]
                                }}
                            ]
                        }}
                    ]
                }}"#
        ))
        .expect("RBAC config should parse");

        let token = encode_hs256_jwt(
            &json!({ "alg": "HS256", "kid": "shared" }),
            &json!({
                "iss": "https://issuer.example",
                "sub": "user-123",
                "aud": "tsink",
                "email": "user@example.com",
                "groups": ["metrics-readers"],
                "exp": now_unix_seconds() + 3600
            }),
            secret,
        );

        let authorized = registry
            .authorize(
                Some(&token),
                &RbacPermission::new(RbacAction::Read, RbacResource::tenant("team-a")),
            )
            .expect("OIDC token should authorize");
        assert_eq!(authorized.principal_id, "oidc:corp:user-123");
        assert_eq!(authorized.auth_method, "oidc");
        assert_eq!(authorized.provider.as_deref(), Some("corp"));
        assert_eq!(authorized.subject.as_deref(), Some("user-123"));
        assert_eq!(authorized.display_name.as_deref(), Some("user@example.com"));

        let denied = registry
            .authorize(
                Some(&token),
                &RbacPermission::new(RbacAction::Read, RbacResource::tenant("team-b")),
            )
            .expect_err("team-b should be denied");
        assert_eq!(denied.code(), "auth_scope_denied");
    }

    #[test]
    fn service_account_lifecycle_persists_and_audits() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let config_path = temp_dir.path().join("rbac.json");
        fs::write(
            &config_path,
            r#"{
                "roles": {
                    "tenant-reader": {
                        "grants": [
                            {
                                "action": "read",
                                "resource": { "kind": "tenant", "name": "*" }
                            }
                        ]
                    }
                }
            }"#,
        )
        .expect("RBAC config should be written");

        let registry =
            RbacRegistry::load_from_path(&config_path).expect("RBAC registry should load");
        let created = registry
            .create_service_account(ServiceAccountSpec {
                id: "ci-bot".to_string(),
                description: Some("CI writer".to_string()),
                disabled: false,
                bindings: vec![RbacBindingSnapshot {
                    role: "tenant-reader".to_string(),
                    scopes: vec![RbacResource::tenant("team-a")],
                }],
            })
            .expect("service account should create");
        assert_eq!(created.service_account.id, "ci-bot");

        registry
            .authorize(
                Some(&created.token),
                &RbacPermission::new(RbacAction::Read, RbacResource::tenant("team-a")),
            )
            .expect("created service account should authorize");

        let rotated = registry
            .rotate_service_account("ci-bot")
            .expect("service account should rotate");
        assert_ne!(created.token, rotated.token);
        let old = registry
            .authorize(
                Some(&created.token),
                &RbacPermission::new(RbacAction::Read, RbacResource::tenant("team-a")),
            )
            .expect_err("old token should stop authorizing");
        assert_eq!(old.code(), "auth_token_invalid");

        registry
            .set_service_account_disabled("ci-bot", true)
            .expect("service account should disable");
        let disabled = registry
            .authorize(
                Some(&rotated.token),
                &RbacPermission::new(RbacAction::Read, RbacResource::tenant("team-a")),
            )
            .expect_err("disabled service account should be blocked");
        assert_eq!(disabled.code(), "auth_principal_disabled");

        let persisted: JsonValue = serde_json::from_str(
            &fs::read_to_string(&config_path).expect("config should be readable"),
        )
        .expect("config should remain valid JSON");
        assert_eq!(
            persisted["serviceAccounts"][0]["id"].as_str(),
            Some("ci-bot")
        );

        let audit = registry.audit_snapshot(20);
        assert!(audit
            .iter()
            .any(|entry| entry.code == "service_account_created"));
        assert!(audit
            .iter()
            .any(|entry| entry.code == "service_account_rotated"));
        assert!(audit
            .iter()
            .any(|entry| entry.code == "service_account_disabled"));
    }

    fn encode_hs256_jwt(header: &JsonValue, claims: &JsonValue, secret: &[u8]) -> String {
        let header =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(header).expect("header should encode"));
        let claims =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("claims should encode"));
        let message = format!("{header}.{claims}");
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, secret),
            message.as_bytes(),
        );
        format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature.as_ref()))
    }
}
