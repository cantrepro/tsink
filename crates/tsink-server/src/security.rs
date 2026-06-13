use crate::cluster::rpc::{InternalApiConfig, RpcClient};
use crate::rbac::RbacRegistry;
use crate::server::ServerConfig;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_rustls::TlsAcceptor;

const GENERATED_TOKEN_BYTES: usize = 32;
const DEFAULT_ROTATION_OVERLAP_SECS: u64 = 300;
const SECURITY_AUDIT_CAPACITY: usize = 128;
static SECRET_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SecretRotationTarget {
    PublicAuthToken,
    AdminAuthToken,
    ClusterInternalAuthToken,
    ListenerTls,
    ClusterInternalMtls,
}

impl SecretRotationTarget {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PublicAuthToken => "public_auth_token",
            Self::AdminAuthToken => "admin_auth_token",
            Self::ClusterInternalAuthToken => "cluster_internal_auth_token",
            Self::ListenerTls => "listener_tls",
            Self::ClusterInternalMtls => "cluster_internal_mtls",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::PublicAuthToken => "public auth token",
            Self::AdminAuthToken => "admin auth token",
            Self::ClusterInternalAuthToken => "cluster internal auth token",
            Self::ListenerTls => "listener TLS",
            Self::ClusterInternalMtls => "cluster internal mTLS",
        }
    }
}

impl fmt::Display for SecretRotationTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum SecretRotationMode {
    Reload,
    #[default]
    Rotate,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaterialSourceSnapshot {
    pub path: Option<String>,
    pub source_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub rotate_supported: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenSecretSnapshot {
    pub target: SecretRotationTarget,
    pub restart_safe: bool,
    pub reloadable: bool,
    pub rotatable: bool,
    pub generation: u64,
    pub last_loaded_unix_ms: u64,
    pub last_rotated_unix_ms: u64,
    pub last_success_unix_ms: u64,
    pub last_failure_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_credential_expires_unix_ms: Option<u64>,
    pub accepts_previous_credential: bool,
    pub reloads_total: u64,
    pub rotations_total: u64,
    pub failures_total: u64,
    pub source: MaterialSourceSnapshot,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsSecretSnapshot {
    pub target: SecretRotationTarget,
    pub restart_safe: bool,
    pub reloadable: bool,
    pub rotatable: bool,
    pub generation: u64,
    pub last_loaded_unix_ms: u64,
    pub last_rotated_unix_ms: u64,
    pub last_success_unix_ms: u64,
    pub last_failure_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub reloads_total: u64,
    pub rotations_total: u64,
    pub failures_total: u64,
    pub cert: MaterialSourceSnapshot,
    pub key: MaterialSourceSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ca: Option<MaterialSourceSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum SecurityTargetSnapshot {
    Token(TokenSecretSnapshot),
    Tls(TlsSecretSnapshot),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityAuditEntry {
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub target: SecretRotationTarget,
    pub operation: String,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountRotationSummary {
    pub total: usize,
    pub disabled: usize,
    pub last_rotated_unix_ms: u64,
    pub audit_entries: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityStateSnapshot {
    pub targets: Vec<SecurityTargetSnapshot>,
    pub audit_entries: Vec<SecurityAuditEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_accounts: Option<ServiceAccountRotationSummary>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityRotateResult {
    pub target: SecretRotationTarget,
    pub mode: SecretRotationMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issued_credential: Option<String>,
    pub state: SecurityTargetSnapshot,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecMaterialManifest {
    kind: String,
    #[serde(default)]
    provider: Option<String>,
    command: Vec<String>,
    #[serde(default)]
    rotate_command: Vec<String>,
}

#[derive(Debug, Clone)]
enum MaterialResolverKind {
    File,
    Exec {
        provider: Option<String>,
        command: Vec<String>,
        rotate_command: Vec<String>,
    },
}

#[derive(Debug, Clone)]
struct MaterialResolver {
    path: PathBuf,
    kind: MaterialResolverKind,
}

impl MaterialResolver {
    fn snapshot(&self) -> MaterialSourceSnapshot {
        match &self.kind {
            MaterialResolverKind::File => MaterialSourceSnapshot {
                path: Some(self.path.display().to_string()),
                source_kind: "file".to_string(),
                provider: None,
                rotate_supported: false,
            },
            MaterialResolverKind::Exec {
                provider,
                rotate_command,
                ..
            } => MaterialSourceSnapshot {
                path: Some(self.path.display().to_string()),
                source_kind: "exec".to_string(),
                provider: provider.clone(),
                rotate_supported: !rotate_command.is_empty(),
            },
        }
    }

    fn load_bytes(&self, usage: &str) -> Result<Vec<u8>, String> {
        match &self.kind {
            MaterialResolverKind::File => fs::read(&self.path)
                .map_err(|err| format!("failed to read {usage} {}: {err}", self.path.display())),
            MaterialResolverKind::Exec {
                provider, command, ..
            } => execute_material_command(&self.path, provider.as_deref(), command, usage, "load"),
        }
    }

    fn rotate(&self, usage: &str) -> Result<(), String> {
        match &self.kind {
            MaterialResolverKind::File => Err(format!(
                "{usage} {} is file-backed and must be updated on disk before reloading",
                self.path.display()
            )),
            MaterialResolverKind::Exec {
                provider,
                rotate_command,
                ..
            } => {
                if rotate_command.is_empty() {
                    return Err(format!(
                        "{usage} {} does not define a rotateCommand hook",
                        self.path.display()
                    ));
                }
                let _ = execute_material_command(
                    &self.path,
                    provider.as_deref(),
                    rotate_command,
                    usage,
                    "rotate",
                )?;
                Ok(())
            }
        }
    }

    fn is_rotatable(&self) -> bool {
        matches!(
            self.kind,
            MaterialResolverKind::Exec {
                ref rotate_command,
                ..
            } if !rotate_command.is_empty()
        )
    }
}

#[derive(Debug)]
struct PreviousCredential {
    value: String,
    expires_unix_ms: u64,
}

#[derive(Debug)]
struct TokenSecretState {
    current: String,
    previous: Option<PreviousCredential>,
    source: MaterialSourceSnapshot,
    generation: u64,
    last_loaded_unix_ms: u64,
    last_rotated_unix_ms: u64,
    last_success_unix_ms: u64,
    last_failure_unix_ms: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone)]
enum TokenSecretSource {
    Inline,
    Path(PathBuf),
}

pub struct ManagedStringSecret {
    target: SecretRotationTarget,
    source: TokenSecretSource,
    restart_safe: bool,
    allow_generated_rotation: bool,
    state: RwLock<TokenSecretState>,
    reloads_total: AtomicU64,
    rotations_total: AtomicU64,
    failures_total: AtomicU64,
}

impl fmt::Debug for ManagedStringSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedStringSecret")
            .field("target", &self.target)
            .field("source", &self.source)
            .field("restart_safe", &self.restart_safe)
            .field("allow_generated_rotation", &self.allow_generated_rotation)
            .finish_non_exhaustive()
    }
}

impl ManagedStringSecret {
    pub fn inline(
        target: SecretRotationTarget,
        value: impl Into<String>,
        restart_safe: bool,
    ) -> Arc<Self> {
        let value = value.into();
        let now = unix_time_ms();
        Arc::new(Self {
            target,
            source: TokenSecretSource::Inline,
            restart_safe,
            allow_generated_rotation: false,
            state: RwLock::new(TokenSecretState {
                current: value,
                previous: None,
                source: MaterialSourceSnapshot {
                    path: None,
                    source_kind: "inline".to_string(),
                    provider: None,
                    rotate_supported: false,
                },
                generation: 1,
                last_loaded_unix_ms: now,
                last_rotated_unix_ms: 0,
                last_success_unix_ms: now,
                last_failure_unix_ms: 0,
                last_error: None,
            }),
            reloads_total: AtomicU64::new(0),
            rotations_total: AtomicU64::new(0),
            failures_total: AtomicU64::new(0),
        })
    }

    pub fn from_path(
        target: SecretRotationTarget,
        path: PathBuf,
        restart_safe: bool,
        allow_generated_rotation: bool,
    ) -> Result<Arc<Self>, String> {
        let (value, source) = load_text_secret_from_source(&path, target.display_name())?;
        let now = unix_time_ms();
        Ok(Arc::new(Self {
            target,
            source: TokenSecretSource::Path(path),
            restart_safe,
            allow_generated_rotation,
            state: RwLock::new(TokenSecretState {
                current: value,
                previous: None,
                source,
                generation: 1,
                last_loaded_unix_ms: now,
                last_rotated_unix_ms: 0,
                last_success_unix_ms: now,
                last_failure_unix_ms: 0,
                last_error: None,
            }),
            reloads_total: AtomicU64::new(0),
            rotations_total: AtomicU64::new(0),
            failures_total: AtomicU64::new(0),
        }))
    }

    pub fn current(&self) -> String {
        self.state
            .read()
            .expect("secret read lock should not be poisoned")
            .current
            .clone()
    }

    pub fn matches(&self, provided: Option<&str>) -> bool {
        let Some(provided) = provided else {
            return false;
        };
        let now = unix_time_ms();
        let state = self
            .state
            .read()
            .expect("secret read lock should not be poisoned");
        if state.current == provided {
            return true;
        }
        state
            .previous
            .as_ref()
            .is_some_and(|previous| previous.expires_unix_ms > now && previous.value == provided)
    }

    pub fn snapshot(&self) -> TokenSecretSnapshot {
        let now = unix_time_ms();
        let state = self
            .state
            .read()
            .expect("secret read lock should not be poisoned");
        let previous_credential_expires_unix_ms = state.previous.as_ref().and_then(|previous| {
            (previous.expires_unix_ms > now).then_some(previous.expires_unix_ms)
        });
        TokenSecretSnapshot {
            target: self.target,
            restart_safe: self.restart_safe,
            reloadable: matches!(self.source, TokenSecretSource::Path(_)),
            rotatable: match self.source {
                TokenSecretSource::Inline => false,
                TokenSecretSource::Path(ref path) => material_resolver_for_path(path)
                    .map(|resolver| resolver.is_rotatable() || self.allow_generated_rotation)
                    .unwrap_or(self.allow_generated_rotation),
            },
            generation: state.generation,
            last_loaded_unix_ms: state.last_loaded_unix_ms,
            last_rotated_unix_ms: state.last_rotated_unix_ms,
            last_success_unix_ms: state.last_success_unix_ms,
            last_failure_unix_ms: state.last_failure_unix_ms,
            last_error: state.last_error.clone(),
            previous_credential_expires_unix_ms,
            accepts_previous_credential: previous_credential_expires_unix_ms.is_some(),
            reloads_total: self.reloads_total.load(Ordering::Relaxed),
            rotations_total: self.rotations_total.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),
            source: state.source.clone(),
        }
    }

    pub fn reload(&self, overlap_secs: Option<u64>) -> Result<(), String> {
        self.reloads_total.fetch_add(1, Ordering::Relaxed);
        self.load_current_from_source(overlap_secs.unwrap_or(DEFAULT_ROTATION_OVERLAP_SECS), false)
    }

    pub fn rotate(
        &self,
        new_value: Option<String>,
        overlap_secs: Option<u64>,
    ) -> Result<Option<String>, String> {
        let overlap_secs = overlap_secs.unwrap_or(DEFAULT_ROTATION_OVERLAP_SECS);
        let path = match &self.source {
            TokenSecretSource::Inline => {
                return self.record_failure(format!(
                    "{} is configured inline and cannot be rotated without a restart",
                    self.target.display_name()
                ));
            }
            TokenSecretSource::Path(path) => path.clone(),
        };

        let resolver = material_resolver_for_path(&path)?;
        let issued_credential = match resolver.kind {
            MaterialResolverKind::File => {
                let next_value = if let Some(value) = new_value {
                    sanitize_provided_token(&value)?
                } else if self.allow_generated_rotation {
                    generate_token()?
                } else {
                    return self.record_failure(format!(
                        "{} rotation requires an explicit newValue or an external rotateCommand hook",
                        self.target.display_name()
                    ));
                };
                write_string_atomically(&path, &next_value)?;
                Some(next_value)
            }
            MaterialResolverKind::Exec { .. } => {
                if new_value.is_some() {
                    return self.record_failure(format!(
                        "{} is externally managed and does not accept newValue writes",
                        self.target.display_name()
                    ));
                }
                resolver.rotate(self.target.display_name())?;
                None
            }
        };

        self.rotations_total.fetch_add(1, Ordering::Relaxed);
        self.load_current_from_source(overlap_secs, true)?;
        Ok(issued_credential)
    }

    fn load_current_from_source(&self, overlap_secs: u64, is_rotation: bool) -> Result<(), String> {
        let (new_value, source) = match &self.source {
            TokenSecretSource::Inline => {
                return self.record_failure(format!(
                    "{} is configured inline and cannot be reloaded",
                    self.target.display_name()
                ));
            }
            TokenSecretSource::Path(path) => {
                load_text_secret_from_source(path, self.target.display_name())?
            }
        };
        let now = unix_time_ms();
        let mut state = self
            .state
            .write()
            .expect("secret write lock should not be poisoned");
        if state.current != new_value {
            let previous = std::mem::replace(&mut state.current, new_value);
            if overlap_secs > 0 && !previous.is_empty() {
                state.previous = Some(PreviousCredential {
                    value: previous,
                    expires_unix_ms: now.saturating_add(overlap_secs.saturating_mul(1_000)),
                });
            } else {
                state.previous = None;
            }
            state.generation = state.generation.saturating_add(1);
        } else if state
            .previous
            .as_ref()
            .is_some_and(|previous| previous.expires_unix_ms <= now)
        {
            state.previous = None;
        }
        state.source = source;
        state.last_loaded_unix_ms = now;
        state.last_success_unix_ms = now;
        if is_rotation {
            state.last_rotated_unix_ms = now;
        }
        state.last_error = None;
        Ok(())
    }

    fn record_failure<T>(&self, err: String) -> Result<T, String> {
        self.failures_total.fetch_add(1, Ordering::Relaxed);
        let now = unix_time_ms();
        let mut state = self
            .state
            .write()
            .expect("secret write lock should not be poisoned");
        state.last_failure_unix_ms = now;
        state.last_error = Some(err.clone());
        Err(err)
    }
}

#[derive(Debug)]
struct TlsMaterialState {
    generation: u64,
    last_loaded_unix_ms: u64,
    last_rotated_unix_ms: u64,
    last_success_unix_ms: u64,
    last_failure_unix_ms: u64,
    last_error: Option<String>,
    cert: MaterialSourceSnapshot,
    key: MaterialSourceSnapshot,
    client_ca: Option<MaterialSourceSnapshot>,
}

pub struct ManagedTlsBundle {
    target: SecretRotationTarget,
    cert_path: PathBuf,
    key_path: PathBuf,
    client_ca_path: Option<PathBuf>,
    restart_safe: bool,
    state: RwLock<TlsMaterialState>,
    reloads_total: AtomicU64,
    rotations_total: AtomicU64,
    failures_total: AtomicU64,
}

impl fmt::Debug for ManagedTlsBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedTlsBundle")
            .field("target", &self.target)
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .field("client_ca_path", &self.client_ca_path)
            .field("restart_safe", &self.restart_safe)
            .finish_non_exhaustive()
    }
}

impl ManagedTlsBundle {
    pub fn new(
        target: SecretRotationTarget,
        cert_path: PathBuf,
        key_path: PathBuf,
        client_ca_path: Option<PathBuf>,
        restart_safe: bool,
    ) -> Result<Arc<Self>, String> {
        let cert_resolver = material_resolver_for_path(&cert_path)?;
        let key_resolver = material_resolver_for_path(&key_path)?;
        let client_ca = client_ca_path
            .as_deref()
            .map(material_resolver_for_path)
            .transpose()?;
        let now = unix_time_ms();
        Ok(Arc::new(Self {
            target,
            cert_path,
            key_path,
            client_ca_path,
            restart_safe,
            state: RwLock::new(TlsMaterialState {
                generation: 0,
                last_loaded_unix_ms: 0,
                last_rotated_unix_ms: 0,
                last_success_unix_ms: now,
                last_failure_unix_ms: 0,
                last_error: None,
                cert: cert_resolver.snapshot(),
                key: key_resolver.snapshot(),
                client_ca: client_ca.as_ref().map(MaterialResolver::snapshot),
            }),
            reloads_total: AtomicU64::new(0),
            rotations_total: AtomicU64::new(0),
            failures_total: AtomicU64::new(0),
        }))
    }

    pub fn snapshot(&self) -> TlsSecretSnapshot {
        let state = self
            .state
            .read()
            .expect("TLS write lock should not be poisoned");
        let cert_rotatable = material_resolver_for_path(&self.cert_path)
            .map(|resolver| resolver.is_rotatable())
            .unwrap_or(false);
        let key_rotatable = material_resolver_for_path(&self.key_path)
            .map(|resolver| resolver.is_rotatable())
            .unwrap_or(false);
        let ca_rotatable = self
            .client_ca_path
            .as_deref()
            .map(material_resolver_for_path)
            .transpose()
            .map(|resolver| resolver.is_some_and(|resolver| resolver.is_rotatable()))
            .unwrap_or(false);
        TlsSecretSnapshot {
            target: self.target,
            restart_safe: self.restart_safe,
            reloadable: true,
            rotatable: cert_rotatable || key_rotatable || ca_rotatable,
            generation: state.generation,
            last_loaded_unix_ms: state.last_loaded_unix_ms,
            last_rotated_unix_ms: state.last_rotated_unix_ms,
            last_success_unix_ms: state.last_success_unix_ms,
            last_failure_unix_ms: state.last_failure_unix_ms,
            last_error: state.last_error.clone(),
            reloads_total: self.reloads_total.load(Ordering::Relaxed),
            rotations_total: self.rotations_total.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),
            cert: state.cert.clone(),
            key: state.key.clone(),
            client_ca: state.client_ca.clone(),
        }
    }

    pub fn reload_listener_acceptor(&self) -> Result<TlsAcceptor, String> {
        self.reloads_total.fetch_add(1, Ordering::Relaxed);
        let loaded = self.load_material(false)?;
        build_listener_tls_acceptor(&loaded.certs, loaded.key, loaded.client_ca_certs.as_deref())
    }

    pub fn reload(&self) -> Result<(), String> {
        self.reloads_total.fetch_add(1, Ordering::Relaxed);
        let _ = self.load_material(false)?;
        Ok(())
    }

    pub fn rotate_listener_acceptor(&self) -> Result<TlsAcceptor, String> {
        self.rotations_total.fetch_add(1, Ordering::Relaxed);
        self.execute_rotate_hooks()?;
        let loaded = self.load_material(true)?;
        build_listener_tls_acceptor(&loaded.certs, loaded.key, loaded.client_ca_certs.as_deref())
    }

    pub fn rotate(&self) -> Result<(), String> {
        self.rotations_total.fetch_add(1, Ordering::Relaxed);
        self.execute_rotate_hooks()?;
        let _ = self.load_material(true)?;
        Ok(())
    }

    fn execute_rotate_hooks(&self) -> Result<(), String> {
        let cert_resolver = material_resolver_for_path(&self.cert_path)?;
        let key_resolver = material_resolver_for_path(&self.key_path)?;
        let mut rotated = false;
        if cert_resolver.is_rotatable() {
            cert_resolver.rotate(self.target.display_name())?;
            rotated = true;
        }
        if key_resolver.is_rotatable() {
            key_resolver.rotate(self.target.display_name())?;
            rotated = true;
        }
        if let Some(path) = &self.client_ca_path {
            let resolver = material_resolver_for_path(path)?;
            if resolver.is_rotatable() {
                resolver.rotate(self.target.display_name())?;
                rotated = true;
            }
        }
        if !rotated {
            return self.record_failure(format!(
                "{} does not have any rotateCommand hooks configured",
                self.target.display_name()
            ));
        }
        Ok(())
    }

    fn load_material(&self, is_rotation: bool) -> Result<LoadedTlsMaterial, String> {
        let cert = load_pem_certs_from_source(&self.cert_path, "TLS certificate")?;
        let key = load_private_key_from_source(&self.key_path, "TLS private key")?;
        let client_ca = self
            .client_ca_path
            .as_deref()
            .map(|path| load_pem_certs_from_source(path, "TLS client CA bundle"))
            .transpose()?;
        let now = unix_time_ms();
        let mut state = self
            .state
            .write()
            .expect("TLS write lock should not be poisoned");
        state.generation = state.generation.saturating_add(1);
        state.last_loaded_unix_ms = now;
        state.last_success_unix_ms = now;
        if is_rotation {
            state.last_rotated_unix_ms = now;
        }
        state.last_error = None;
        state.cert = cert.1.clone();
        state.key = key.1.clone();
        state.client_ca = client_ca.as_ref().map(|(_, snapshot)| snapshot.clone());
        Ok(LoadedTlsMaterial {
            certs: cert.0,
            key: key.0,
            client_ca_certs: client_ca.map(|(certs, _)| certs),
        })
    }

    fn record_failure<T>(&self, err: String) -> Result<T, String> {
        self.failures_total.fetch_add(1, Ordering::Relaxed);
        let now = unix_time_ms();
        let mut state = self
            .state
            .write()
            .expect("TLS write lock should not be poisoned");
        state.last_failure_unix_ms = now;
        state.last_error = Some(err.clone());
        Err(err)
    }
}

struct LoadedTlsMaterial {
    certs: Vec<rustls::pki_types::CertificateDer<'static>>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
    client_ca_certs: Option<Vec<rustls::pki_types::CertificateDer<'static>>>,
}

pub struct SecurityManager {
    public_auth: Option<Arc<ManagedStringSecret>>,
    admin_auth: Option<Arc<ManagedStringSecret>>,
    cluster_internal_auth: Option<Arc<ManagedStringSecret>>,
    listener_tls: Option<Arc<ManagedTlsBundle>>,
    cluster_internal_mtls: Option<Arc<ManagedTlsBundle>>,
    listener_acceptor: RwLock<Option<TlsAcceptor>>,
    audit: Mutex<VecDeque<SecurityAuditEntry>>,
    audit_seq: AtomicU64,
}

impl fmt::Debug for SecurityManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecurityManager")
            .field(
                "public_auth",
                &self.public_auth.as_ref().map(|_| "configured"),
            )
            .field(
                "admin_auth",
                &self.admin_auth.as_ref().map(|_| "configured"),
            )
            .field(
                "cluster_internal_auth",
                &self.cluster_internal_auth.as_ref().map(|_| "configured"),
            )
            .field(
                "listener_tls",
                &self.listener_tls.as_ref().map(|_| "configured"),
            )
            .field(
                "cluster_internal_mtls",
                &self.cluster_internal_mtls.as_ref().map(|_| "configured"),
            )
            .finish_non_exhaustive()
    }
}

impl SecurityManager {
    pub fn from_config(config: &ServerConfig) -> Result<Arc<Self>, String> {
        let public_auth = build_server_token_secret(
            config.auth_token.as_deref(),
            config.auth_token_file.as_deref(),
            SecretRotationTarget::PublicAuthToken,
            true,
        )?;
        let admin_auth = build_server_token_secret(
            config.admin_auth_token.as_deref(),
            config.admin_auth_token_file.as_deref(),
            SecretRotationTarget::AdminAuthToken,
            true,
        )?;
        let cluster_internal_auth = build_server_token_secret(
            config.cluster.internal_auth_token.as_deref(),
            config.cluster.internal_auth_token_file.as_deref(),
            SecretRotationTarget::ClusterInternalAuthToken,
            false,
        )?;

        let internal_mtls = config.cluster.resolved_internal_mtls()?;
        let listener_tls = listener_tls_bundle(config, internal_mtls.as_ref())?;
        let cluster_internal_mtls = internal_mtls
            .as_ref()
            .map(|mtls| {
                ManagedTlsBundle::new(
                    SecretRotationTarget::ClusterInternalMtls,
                    mtls.cert.clone(),
                    mtls.key.clone(),
                    Some(mtls.ca_cert.clone()),
                    true,
                )
            })
            .transpose()?;

        let listener_acceptor = listener_tls
            .as_ref()
            .map(|bundle| bundle.reload_listener_acceptor())
            .transpose()?;

        Ok(Arc::new(Self {
            public_auth,
            admin_auth,
            cluster_internal_auth,
            listener_tls,
            cluster_internal_mtls,
            listener_acceptor: RwLock::new(listener_acceptor),
            audit: Mutex::new(VecDeque::with_capacity(SECURITY_AUDIT_CAPACITY)),
            audit_seq: AtomicU64::new(0),
        }))
    }

    pub fn attach_internal_api(&self, internal_api: &mut InternalApiConfig) {
        if let Some(secret) = &self.cluster_internal_auth {
            internal_api.set_auth_runtime(Arc::clone(secret));
        }
    }

    pub fn attach_rpc_client(&self, rpc_client: &mut RpcClient) {
        if let Some(secret) = &self.cluster_internal_auth {
            rpc_client.set_internal_auth_runtime(Arc::clone(secret));
        }
    }

    pub fn listener_acceptor(&self) -> Option<TlsAcceptor> {
        self.listener_acceptor
            .read()
            .expect("listener acceptor lock should not be poisoned")
            .clone()
    }

    pub fn public_auth_configured(&self) -> bool {
        self.public_auth.is_some()
    }

    pub fn public_auth_matches(&self, provided: Option<&str>) -> bool {
        self.public_auth
            .as_ref()
            .is_some_and(|secret| secret.matches(provided))
    }

    pub fn admin_auth_matches(&self, provided: Option<&str>) -> bool {
        self.admin_auth
            .as_ref()
            .is_some_and(|secret| secret.matches(provided))
    }

    pub fn admin_secret_configured(&self) -> bool {
        self.admin_auth.is_some()
    }

    pub fn state_snapshot(&self, rbac_registry: Option<&RbacRegistry>) -> SecurityStateSnapshot {
        let mut targets = Vec::new();
        if let Some(secret) = &self.public_auth {
            targets.push(SecurityTargetSnapshot::Token(secret.snapshot()));
        }
        if let Some(secret) = &self.admin_auth {
            targets.push(SecurityTargetSnapshot::Token(secret.snapshot()));
        }
        if let Some(secret) = &self.cluster_internal_auth {
            targets.push(SecurityTargetSnapshot::Token(secret.snapshot()));
        }
        if let Some(bundle) = &self.listener_tls {
            targets.push(SecurityTargetSnapshot::Tls(bundle.snapshot()));
        }
        if let Some(bundle) = &self.cluster_internal_mtls {
            targets.push(SecurityTargetSnapshot::Tls(bundle.snapshot()));
        }

        let audit_entries = self
            .audit
            .lock()
            .expect("security audit lock should not be poisoned")
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();

        let service_accounts = rbac_registry.map(service_account_rotation_summary);
        SecurityStateSnapshot {
            targets,
            audit_entries,
            service_accounts,
        }
    }

    pub fn rotate(
        &self,
        target: SecretRotationTarget,
        mode: SecretRotationMode,
        new_value: Option<String>,
        overlap_secs: Option<u64>,
        actor: Option<String>,
    ) -> Result<SecurityRotateResult, String> {
        let overlap_secs = overlap_secs.unwrap_or(DEFAULT_ROTATION_OVERLAP_SECS);
        let result = match target {
            SecretRotationTarget::PublicAuthToken => self.rotate_token_secret(
                self.public_auth.as_ref(),
                target,
                mode,
                new_value,
                overlap_secs,
            ),
            SecretRotationTarget::AdminAuthToken => self.rotate_token_secret(
                self.admin_auth.as_ref(),
                target,
                mode,
                new_value,
                overlap_secs,
            ),
            SecretRotationTarget::ClusterInternalAuthToken => self.rotate_token_secret(
                self.cluster_internal_auth.as_ref(),
                target,
                mode,
                new_value,
                overlap_secs,
            ),
            SecretRotationTarget::ListenerTls => {
                self.rotate_listener_tls(mode)
                    .map(|state| SecurityRotateResult {
                        target,
                        mode,
                        issued_credential: None,
                        state,
                    })
            }
            SecretRotationTarget::ClusterInternalMtls => self
                .rotate_cluster_internal_mtls(mode)
                .map(|state| SecurityRotateResult {
                    target,
                    mode,
                    issued_credential: None,
                    state,
                }),
        };

        match result {
            Ok(result) => {
                self.push_audit_entry(
                    target,
                    match mode {
                        SecretRotationMode::Reload => "reload",
                        SecretRotationMode::Rotate => "rotate",
                    }
                    .to_string(),
                    "success".to_string(),
                    actor,
                    None,
                );
                Ok(result)
            }
            Err(err) => {
                self.push_audit_entry(
                    target,
                    match mode {
                        SecretRotationMode::Reload => "reload",
                        SecretRotationMode::Rotate => "rotate",
                    }
                    .to_string(),
                    "failure".to_string(),
                    actor,
                    Some(err.clone()),
                );
                Err(err)
            }
        }
    }

    fn rotate_token_secret(
        &self,
        secret: Option<&Arc<ManagedStringSecret>>,
        target: SecretRotationTarget,
        mode: SecretRotationMode,
        new_value: Option<String>,
        overlap_secs: u64,
    ) -> Result<SecurityRotateResult, String> {
        let Some(secret) = secret else {
            return Err(format!("{} is not configured", target.display_name()));
        };
        let issued_credential = match mode {
            SecretRotationMode::Reload => {
                if new_value.is_some() {
                    return Err(format!(
                        "{} reload does not accept newValue",
                        target.display_name()
                    ));
                }
                secret.reload(Some(overlap_secs))?;
                None
            }
            SecretRotationMode::Rotate => secret.rotate(new_value, Some(overlap_secs))?,
        };
        Ok(SecurityRotateResult {
            target,
            mode,
            issued_credential,
            state: SecurityTargetSnapshot::Token(secret.snapshot()),
        })
    }

    fn rotate_listener_tls(
        &self,
        mode: SecretRotationMode,
    ) -> Result<SecurityTargetSnapshot, String> {
        let Some(bundle) = &self.listener_tls else {
            return Err("listener TLS is not configured".to_string());
        };
        let acceptor = match mode {
            SecretRotationMode::Reload => bundle.reload_listener_acceptor()?,
            SecretRotationMode::Rotate => bundle.rotate_listener_acceptor()?,
        };
        *self
            .listener_acceptor
            .write()
            .expect("listener acceptor lock should not be poisoned") = Some(acceptor);
        Ok(SecurityTargetSnapshot::Tls(bundle.snapshot()))
    }

    fn rotate_cluster_internal_mtls(
        &self,
        mode: SecretRotationMode,
    ) -> Result<SecurityTargetSnapshot, String> {
        let Some(bundle) = &self.cluster_internal_mtls else {
            return Err("cluster internal mTLS is not configured".to_string());
        };
        match mode {
            SecretRotationMode::Reload => bundle.reload()?,
            SecretRotationMode::Rotate => bundle.rotate()?,
        }

        if let Some(listener_bundle) = &self.listener_tls {
            let acceptor = listener_bundle.reload_listener_acceptor()?;
            *self
                .listener_acceptor
                .write()
                .expect("listener acceptor lock should not be poisoned") = Some(acceptor);
        }

        Ok(SecurityTargetSnapshot::Tls(bundle.snapshot()))
    }

    fn push_audit_entry(
        &self,
        target: SecretRotationTarget,
        operation: String,
        outcome: String,
        actor: Option<String>,
        detail: Option<String>,
    ) {
        let entry = SecurityAuditEntry {
            sequence: self.audit_seq.fetch_add(1, Ordering::Relaxed) + 1,
            timestamp_unix_ms: unix_time_ms(),
            target,
            operation,
            outcome,
            actor,
            detail,
        };
        let mut audit = self
            .audit
            .lock()
            .expect("security audit lock should not be poisoned");
        if audit.len() == SECURITY_AUDIT_CAPACITY {
            audit.pop_front();
        }
        audit.push_back(entry);
    }
}

fn build_server_token_secret(
    inline: Option<&str>,
    path: Option<&Path>,
    target: SecretRotationTarget,
    allow_generated_rotation: bool,
) -> Result<Option<Arc<ManagedStringSecret>>, String> {
    match (inline, path) {
        (Some(value), None) => Ok(Some(ManagedStringSecret::inline(
            target,
            value.to_string(),
            false,
        ))),
        (None, Some(path)) => Ok(Some(ManagedStringSecret::from_path(
            target,
            path.to_path_buf(),
            true,
            allow_generated_rotation,
        )?)),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(format!(
            "{} cannot be configured with both inline and file-backed sources",
            target.display_name()
        )),
    }
}

fn listener_tls_bundle(
    config: &ServerConfig,
    internal_mtls: Option<&crate::cluster::config::ClusterInternalMtlsResolved>,
) -> Result<Option<Arc<ManagedTlsBundle>>, String> {
    let Some((cert_path, key_path)) = config.resolved_listener_tls_paths()? else {
        return Ok(None);
    };
    let client_ca_path = internal_mtls.map(|mtls| mtls.ca_cert.clone());
    Ok(Some(ManagedTlsBundle::new(
        SecretRotationTarget::ListenerTls,
        cert_path,
        key_path,
        client_ca_path,
        true,
    )?))
}

pub fn load_text_secret_from_source(
    path: &Path,
    usage: &str,
) -> Result<(String, MaterialSourceSnapshot), String> {
    let resolver = material_resolver_for_path(path)?;
    let snapshot = resolver.snapshot();
    let bytes = resolver.load_bytes(usage)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| format!("{usage} {} must be valid UTF-8", path.display()))?;
    let token = text.trim();
    if token.is_empty() {
        return Err(format!(
            "{usage} {} resolved to an empty value",
            path.display()
        ));
    }
    Ok((token.to_string(), snapshot))
}

pub fn load_pem_certs_from_source(
    path: &Path,
    usage: &str,
) -> Result<
    (
        Vec<rustls::pki_types::CertificateDer<'static>>,
        MaterialSourceSnapshot,
    ),
    String,
> {
    let resolver = material_resolver_for_path(path)?;
    let snapshot = resolver.snapshot();
    let bytes = resolver.load_bytes(usage)?;
    let mut reader = BufReader::new(bytes.as_slice());
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("failed to parse {usage} from {}: {err}", path.display()))?;
    if certs.is_empty() {
        return Err(format!(
            "{usage} {} contains no certificates",
            path.display()
        ));
    }
    Ok((certs, snapshot))
}

pub fn load_private_key_from_source(
    path: &Path,
    usage: &str,
) -> Result<
    (
        rustls::pki_types::PrivateKeyDer<'static>,
        MaterialSourceSnapshot,
    ),
    String,
> {
    let resolver = material_resolver_for_path(path)?;
    let snapshot = resolver.snapshot();
    let bytes = resolver.load_bytes(usage)?;
    let mut reader = BufReader::new(bytes.as_slice());
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|err| format!("failed to parse {usage} from {}: {err}", path.display()))?
        .ok_or_else(|| format!("{usage} {} contains no private key", path.display()))?;
    Ok((key, snapshot))
}

pub fn load_cluster_internal_auth_token_from_config(
    config: &crate::cluster::config::ClusterConfig,
) -> Result<Option<String>, String> {
    match (
        config
            .internal_auth_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        config.internal_auth_token_file.as_deref(),
    ) {
        (Some(value), None) => Ok(Some(value.to_string())),
        (None, Some(path)) => load_text_secret_from_source(path, "cluster internal auth token")
            .map(|(value, _)| Some(value)),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(
            "--cluster-internal-auth-token and --cluster-internal-auth-token-file are mutually exclusive"
                .to_string(),
        ),
    }
}

fn material_resolver_for_path(path: &Path) -> Result<MaterialResolver, String> {
    let bytes = fs::read(path)
        .map_err(|err| format!("failed to read secret source {}: {err}", path.display()))?;
    if let Ok(text) = std::str::from_utf8(&bytes) {
        let trimmed = text.trim();
        if trimmed.starts_with('{') {
            let manifest =
                serde_json::from_str::<ExecMaterialManifest>(trimmed).map_err(|err| {
                    format!(
                        "failed to parse secret source manifest {}: {err}",
                        path.display()
                    )
                })?;
            if manifest.kind != "exec" {
                return Err(format!(
                    "secret source manifest {} has unsupported kind '{}'",
                    path.display(),
                    manifest.kind
                ));
            }
            if manifest.command.is_empty() {
                return Err(format!(
                    "secret source manifest {} must define a non-empty command",
                    path.display()
                ));
            }
            return Ok(MaterialResolver {
                path: path.to_path_buf(),
                kind: MaterialResolverKind::Exec {
                    provider: manifest.provider,
                    command: manifest.command,
                    rotate_command: manifest.rotate_command,
                },
            });
        }
    }
    Ok(MaterialResolver {
        path: path.to_path_buf(),
        kind: MaterialResolverKind::File,
    })
}

fn execute_material_command(
    manifest_path: &Path,
    provider: Option<&str>,
    command: &[String],
    usage: &str,
    action: &str,
) -> Result<Vec<u8>, String> {
    let program = command.first().ok_or_else(|| {
        format!(
            "secret source manifest {} has an empty {action} command",
            manifest_path.display()
        )
    })?;
    let output = Command::new(program)
        .args(&command[1..])
        .output()
        .map_err(|err| {
            format!(
                "failed to {action} {usage} via {}{}: {err}",
                provider
                    .map(|provider| format!("{provider} helper "))
                    .unwrap_or_default(),
                manifest_path.display()
            )
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "{} {} command failed with status {}{}",
            usage,
            manifest_path.display(),
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        ));
    }
    Ok(output.stdout)
}

fn write_string_atomically(path: &Path, value: &str) -> Result<(), String> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid secret file path {}", path.display()))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|err| {
        format!(
            "failed to create secret file directory {}: {err}",
            parent.display()
        )
    })?;

    let temp_path = reserve_secret_temp_path(parent, file_name)?;
    let write_result = write_secret_temp_file(&temp_path, value);
    if let Err(err) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }
    if let Err(err) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(format!(
            "failed to replace secret file {}: {err}",
            path.display()
        ));
    }
    sync_parent_dir(parent)?;
    Ok(())
}

fn reserve_secret_temp_path(parent: &Path, file_name: &str) -> Result<PathBuf, String> {
    let pid = std::process::id();
    for _ in 0..256 {
        let nonce = SECRET_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temp_path = parent.join(format!(
            ".{file_name}.tmp-{}-{pid}-{nonce:016x}",
            unix_time_ms()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        match options.open(&temp_path) {
            Ok(_) => return Ok(temp_path),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(format!(
                    "failed to reserve temporary secret file {}: {err}",
                    temp_path.display()
                ));
            }
        }
    }
    Err(format!(
        "failed to reserve unique temporary secret file for {}",
        parent.join(file_name).display()
    ))
}

fn write_secret_temp_file(temp_path: &Path, value: &str) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options.write(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(temp_path).map_err(|err| {
        format!(
            "failed to open temporary secret file {}: {err}",
            temp_path.display()
        )
    })?;
    file.write_all(value.as_bytes()).map_err(|err| {
        format!(
            "failed to write temporary secret file {}: {err}",
            temp_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        format!(
            "failed to fsync temporary secret file {}: {err}",
            temp_path.display()
        )
    })
}

fn sync_parent_dir(parent: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| {
                format!(
                    "failed to fsync secret file directory {}: {err}",
                    parent.display()
                )
            })?;
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
    }
    Ok(())
}

fn sanitize_provided_token(value: &str) -> Result<String, String> {
    let token = value.trim();
    if token.is_empty() {
        return Err("newValue must not be empty".to_string());
    }
    Ok(token.to_string())
}

fn generate_token() -> Result<String, String> {
    let mut bytes = [0_u8; GENERATED_TOKEN_BYTES];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "failed to generate a random token".to_string())?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn build_listener_tls_acceptor(
    certs: &[rustls::pki_types::CertificateDer<'static>],
    key: rustls::pki_types::PrivateKeyDer<'static>,
    client_ca_certs: Option<&[rustls::pki_types::CertificateDer<'static>]>,
) -> Result<TlsAcceptor, String> {
    ensure_rustls_crypto_provider();
    let config = if let Some(client_ca_certs) = client_ca_certs {
        let mut roots = rustls::RootCertStore::empty();
        for cert in client_ca_certs {
            roots.add(cert.clone()).map_err(|err| {
                format!("failed to add TLS client CA certificate to root store: {err}")
            })?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|err| format!("failed to build TLS client verifier: {err}"))?;
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs.to_vec(), key)
            .map_err(|err| format!("failed to build TLS acceptor: {err}"))?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs.to_vec(), key)
            .map_err(|err| format!("failed to build TLS acceptor: {err}"))?
    };
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn service_account_rotation_summary(registry: &RbacRegistry) -> ServiceAccountRotationSummary {
    let state = registry.state_snapshot();
    let disabled = state
        .service_accounts
        .iter()
        .filter(|account| account.disabled)
        .count();
    let last_rotated_unix_ms = state
        .service_accounts
        .iter()
        .map(|account| account.last_rotated_unix_ms)
        .max()
        .unwrap_or(0);
    ServiceAccountRotationSummary {
        total: state.service_accounts.len(),
        disabled,
        last_rotated_unix_ms,
        audit_entries: state.audit_entries,
    }
}

fn ensure_rustls_crypto_provider() {
    static RUSTLS_CRYPTO_PROVIDER: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    RUSTLS_CRYPTO_PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    const TEST_CERT: &str = include_str!("cluster/testdata/internal-mtls-server-cert.pem");
    const TEST_KEY: &str = include_str!("cluster/testdata/internal-mtls-server-key.pem");
    const TEST_CA: &str = include_str!("cluster/testdata/internal-mtls-ca-cert.pem");

    #[test]
    fn file_backed_secret_rotates_with_overlap_window() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let secret_path = temp_dir.path().join("public.token");
        fs::write(&secret_path, "token-a\n").expect("secret should write");
        let secret = ManagedStringSecret::from_path(
            SecretRotationTarget::PublicAuthToken,
            secret_path.clone(),
            true,
            true,
        )
        .expect("secret should open");
        assert!(secret.matches(Some("token-a")));

        let issued = secret
            .rotate(Some("token-b".to_string()), Some(60))
            .expect("rotation should succeed");
        assert_eq!(issued.as_deref(), Some("token-b"));
        assert!(secret.matches(Some("token-b")));
        assert!(secret.matches(Some("token-a")));
        assert_eq!(
            fs::read_to_string(secret_path).expect("secret file should read"),
            "token-b"
        );
    }

    #[cfg(unix)]
    #[test]
    fn write_string_atomically_restricts_secret_file_permissions() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let secret_path = temp_dir.path().join("public.token");
        fs::write(&secret_path, "token-a\n").expect("secret should write");
        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o644))
            .expect("permissions should update");

        write_string_atomically(&secret_path, "token-b").expect("secret should rotate");

        let mode = fs::metadata(&secret_path)
            .expect("secret metadata should read")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(
            fs::read_to_string(&secret_path).expect("secret should read"),
            "token-b"
        );
    }

    #[test]
    fn tls_bundle_reloads_manifest_aware_material() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let cert_path = temp_dir.path().join("listener.pem");
        let key_path = temp_dir.path().join("listener.key");
        let ca_path = temp_dir.path().join("listener-ca.pem");
        fs::write(&cert_path, TEST_CERT).expect("cert should write");
        fs::write(&key_path, TEST_KEY).expect("key should write");
        fs::write(&ca_path, TEST_CA).expect("ca should write");

        let bundle = ManagedTlsBundle::new(
            SecretRotationTarget::ListenerTls,
            cert_path,
            key_path,
            Some(ca_path),
            true,
        )
        .expect("bundle should build");
        let _acceptor = bundle
            .reload_listener_acceptor()
            .expect("TLS acceptor should build");
        let snapshot = bundle.snapshot();
        assert_eq!(snapshot.target, SecretRotationTarget::ListenerTls);
        assert!(snapshot.last_success_unix_ms > 0);
    }
}
