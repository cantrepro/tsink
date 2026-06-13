use crate::admission;
use crate::cluster::{
    self, audit::ClusterAuditLog, config::ClusterConfig, dedupe::DedupeWindowStore,
    outbox::HintedHandoffOutbox,
};
use crate::edge_sync;
use crate::exemplar_store::ExemplarStore;
use crate::handlers;
use crate::legacy_ingest::{self, StatsdAdapter};
use crate::managed_control_plane::ManagedControlPlane;
use crate::metadata_store::MetricMetadataStore;
use crate::rbac::RbacRegistry;
use crate::rules::RulesRuntime;
use crate::security::SecurityManager;
use crate::tenant;
use crate::usage::UsageAccounting;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tsink::promql::Engine;
use tsink::{Storage, StorageBuilder, StorageRuntimeMode, TimestampPrecision, WalSyncMode};

const MAX_CONNECTIONS: usize = 1024;
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(30);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(10);
const DEFAULT_STORAGE_RETENTION: Duration = Duration::from_secs(14 * 24 * 3600);
#[cfg(test)]
static RUSTLS_CRYPTO_PROVIDER: OnceLock<()> = OnceLock::new();

mod auth;
mod transport;

#[cfg(test)]
use self::auth::authorize_request_scope;
use self::transport::handle_connection;
#[cfg(test)]
use self::transport::{build_tls_acceptor, handle_http_loop};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen: String,
    pub statsd_listen: Option<String>,
    pub statsd_tenant_id: String,
    pub graphite_listen: Option<String>,
    pub graphite_tenant_id: String,
    pub data_path: Option<PathBuf>,
    pub object_store_path: Option<PathBuf>,
    pub wal_enabled: bool,
    pub timestamp_precision: TimestampPrecision,
    pub retention: Option<Duration>,
    pub hot_tier_retention: Option<Duration>,
    pub warm_tier_retention: Option<Duration>,
    pub storage_mode: StorageRuntimeMode,
    pub remote_segment_refresh_interval: Option<Duration>,
    pub mirror_hot_segments_to_object_store: bool,
    pub memory_limit: Option<usize>,
    pub cardinality_limit: Option<usize>,
    pub chunk_points: Option<usize>,
    pub max_writers: Option<usize>,
    pub wal_sync_mode: Option<WalSyncMode>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub auth_token: Option<String>,
    pub auth_token_file: Option<PathBuf>,
    pub admin_auth_token: Option<String>,
    pub admin_auth_token_file: Option<PathBuf>,
    pub tenant_config_path: Option<PathBuf>,
    pub rbac_config_path: Option<PathBuf>,
    pub admin_api_enabled: bool,
    pub admin_path_prefix: Option<PathBuf>,
    pub edge_sync_upstream_endpoint: Option<String>,
    pub edge_sync_auth_token: Option<String>,
    pub edge_sync_source_id: Option<String>,
    pub edge_sync_static_tenant: Option<String>,
    pub cluster: ClusterConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:9201".to_string(),
            statsd_listen: None,
            statsd_tenant_id: tenant::DEFAULT_TENANT_ID.to_string(),
            graphite_listen: None,
            graphite_tenant_id: tenant::DEFAULT_TENANT_ID.to_string(),
            data_path: None,
            object_store_path: None,
            wal_enabled: true,
            timestamp_precision: TimestampPrecision::Milliseconds,
            retention: None,
            hot_tier_retention: None,
            warm_tier_retention: None,
            storage_mode: StorageRuntimeMode::ReadWrite,
            remote_segment_refresh_interval: None,
            mirror_hot_segments_to_object_store: false,
            memory_limit: None,
            cardinality_limit: None,
            chunk_points: None,
            max_writers: None,
            wal_sync_mode: None,
            tls_cert: None,
            tls_key: None,
            auth_token: None,
            auth_token_file: None,
            admin_auth_token: None,
            admin_auth_token_file: None,
            tenant_config_path: None,
            rbac_config_path: None,
            admin_api_enabled: false,
            admin_path_prefix: None,
            edge_sync_upstream_endpoint: None,
            edge_sync_auth_token: None,
            edge_sync_source_id: None,
            edge_sync_static_tenant: None,
            cluster: ClusterConfig::default(),
        }
    }
}

impl ServerConfig {
    pub(crate) fn normalize_and_validate(mut self) -> Result<Self, String> {
        self.listen = normalize_required_string("--listen", self.listen)?;
        self.statsd_listen = normalize_optional_string("--statsd-listen", self.statsd_listen)?;
        self.statsd_tenant_id =
            normalize_required_string("--statsd-tenant", self.statsd_tenant_id)?;
        self.graphite_listen =
            normalize_optional_string("--graphite-listen", self.graphite_listen)?;
        self.graphite_tenant_id =
            normalize_required_string("--graphite-tenant", self.graphite_tenant_id)?;
        self.data_path = normalize_trimmed_optional_path("--data-path", self.data_path)?;
        self.object_store_path =
            normalize_trimmed_optional_path("--object-store-path", self.object_store_path)?;
        self.tls_cert = normalize_trimmed_optional_path("--tls-cert", self.tls_cert)?;
        self.tls_key = normalize_trimmed_optional_path("--tls-key", self.tls_key)?;
        self.auth_token = normalize_optional_string("--auth-token", self.auth_token)?;
        self.auth_token_file =
            normalize_trimmed_optional_path("--auth-token-file", self.auth_token_file)?;
        self.admin_auth_token =
            normalize_optional_string("--admin-auth-token", self.admin_auth_token)?;
        self.admin_auth_token_file =
            normalize_trimmed_optional_path("--admin-auth-token-file", self.admin_auth_token_file)?;
        self.tenant_config_path =
            normalize_trimmed_optional_path("--tenant-config", self.tenant_config_path)?;
        self.rbac_config_path =
            normalize_trimmed_optional_path("--rbac-config", self.rbac_config_path)?;
        self.admin_path_prefix =
            normalize_trimmed_optional_path("--admin-path-prefix", self.admin_path_prefix)?;
        self.edge_sync_upstream_endpoint =
            normalize_optional_string("--edge-sync-upstream", self.edge_sync_upstream_endpoint)?;
        self.edge_sync_auth_token =
            normalize_optional_string("--edge-sync-auth-token", self.edge_sync_auth_token)?;
        self.edge_sync_source_id =
            normalize_optional_string("--edge-sync-source-id", self.edge_sync_source_id)?;
        self.edge_sync_static_tenant =
            normalize_optional_string("--edge-sync-static-tenant", self.edge_sync_static_tenant)?;
        self.cluster = self.cluster.normalize()?;
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        self.validate_listener_config()?;
        self.validate_auth_config()?;
        self.validate_admin_config()?;
        self.validate_edge_sync_config()?;
        self.validate_storage_mode_config()?;
        let _ = self.resolved_listener_tls_paths()?;
        self.cluster.validate()?;
        Ok(())
    }

    pub(crate) fn resolved_listener_tls_paths(&self) -> Result<Option<(PathBuf, PathBuf)>, String> {
        let internal_mtls = self.cluster.resolved_internal_mtls()?;
        match (&self.tls_cert, &self.tls_key) {
            (Some(cert), Some(key)) => Ok(Some((cert.clone(), key.clone()))),
            (None, None) => Ok(internal_mtls.map(|mtls| (mtls.cert, mtls.key))),
            (Some(_), None) => Err("--tls-cert requires --tls-key".to_string()),
            (None, Some(_)) => Err("--tls-key requires --tls-cert".to_string()),
        }
    }

    fn validate_listener_config(&self) -> Result<(), String> {
        if self
            .statsd_listen
            .as_deref()
            .is_some_and(|listen| listen.trim().is_empty())
        {
            return Err("--statsd-listen must not be empty".to_string());
        }
        if self.statsd_tenant_id.trim().is_empty() {
            return Err("--statsd-tenant must not be empty".to_string());
        }
        if self
            .graphite_listen
            .as_deref()
            .is_some_and(|listen| listen.trim().is_empty())
        {
            return Err("--graphite-listen must not be empty".to_string());
        }
        if self.graphite_tenant_id.trim().is_empty() {
            return Err("--graphite-tenant must not be empty".to_string());
        }
        Ok(())
    }

    fn validate_auth_config(&self) -> Result<(), String> {
        if self.auth_token.is_some() && self.auth_token_file.is_some() {
            return Err("--auth-token and --auth-token-file are mutually exclusive".to_string());
        }
        if self.admin_auth_token.is_some() && self.admin_auth_token_file.is_some() {
            return Err(
                "--admin-auth-token and --admin-auth-token-file are mutually exclusive".to_string(),
            );
        }
        Ok(())
    }

    fn validate_admin_config(&self) -> Result<(), String> {
        if self.admin_api_enabled
            && !self.has_public_auth()
            && !self.has_admin_auth()
            && !self.has_rbac_auth()
        {
            return Err(
                "--enable-admin-api requires --admin-auth-token, --auth-token, or --rbac-config"
                    .to_string(),
            );
        }
        if self.admin_path_prefix.is_some() && !self.admin_api_enabled {
            return Err("--admin-path-prefix requires --enable-admin-api".to_string());
        }
        Ok(())
    }

    fn validate_edge_sync_config(&self) -> Result<(), String> {
        if self.edge_sync_auth_token.is_some() && self.cluster.enabled {
            return Err(
                "--edge-sync-auth-token cannot be used with --cluster-enabled=true".to_string(),
            );
        }
        if self.edge_sync_upstream_endpoint.is_some() && self.cluster.enabled {
            return Err(
                "--edge-sync-upstream cannot be used with --cluster-enabled=true".to_string(),
            );
        }
        if self.edge_sync_auth_token.is_some() && self.data_path.is_none() {
            return Err("--edge-sync-auth-token requires --data-path".to_string());
        }
        if self.edge_sync_upstream_endpoint.is_some() && self.edge_sync_auth_token.is_none() {
            return Err("--edge-sync-upstream requires --edge-sync-auth-token".to_string());
        }
        if self.edge_sync_upstream_endpoint.is_some()
            && self.storage_mode == StorageRuntimeMode::ComputeOnly
        {
            return Err(
                "--edge-sync-upstream cannot be used with --storage-mode=compute-only".to_string(),
            );
        }
        if self.edge_sync_static_tenant.is_some() && self.edge_sync_upstream_endpoint.is_none() {
            return Err("--edge-sync-static-tenant requires --edge-sync-upstream".to_string());
        }
        Ok(())
    }

    fn validate_storage_mode_config(&self) -> Result<(), String> {
        if self.storage_mode == StorageRuntimeMode::ComputeOnly {
            if self.object_store_path.is_none() {
                return Err("--storage-mode=compute-only requires --object-store-path".to_string());
            }
            if self.cluster.enabled
                && self.cluster.node_role != cluster::config::ClusterNodeRole::Query
            {
                return Err(
                    "--storage-mode=compute-only requires --cluster-node-role=query when cluster mode is enabled"
                        .to_string(),
                );
            }
        }
        if self.cluster.enabled
            && self.cluster.node_role == cluster::config::ClusterNodeRole::Query
            && self.storage_mode != StorageRuntimeMode::ComputeOnly
        {
            return Err(
                "--cluster-node-role=query requires --storage-mode=compute-only".to_string(),
            );
        }
        if self.mirror_hot_segments_to_object_store && self.object_store_path.is_none() {
            return Err(
                "--mirror-hot-segments-to-object-store requires --object-store-path".to_string(),
            );
        }
        if self.mirror_hot_segments_to_object_store
            && self.storage_mode == StorageRuntimeMode::ComputeOnly
        {
            return Err(
                "--mirror-hot-segments-to-object-store cannot be used with --storage-mode=compute-only"
                    .to_string(),
            );
        }
        Ok(())
    }

    fn has_public_auth(&self) -> bool {
        self.auth_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
            || self.auth_token_file.is_some()
    }

    fn has_admin_auth(&self) -> bool {
        self.admin_auth_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
            || self.admin_auth_token_file.is_some()
    }

    fn has_rbac_auth(&self) -> bool {
        self.rbac_config_path.is_some()
    }
}

fn normalize_required_string(flag: &str, value: String) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{flag} must not be empty"));
    }
    Ok(trimmed.to_string())
}

fn normalize_optional_string(flag: &str, value: Option<String>) -> Result<Option<String>, String> {
    value
        .map(|value| normalize_required_string(flag, value))
        .transpose()
}

fn normalize_trimmed_optional_path(
    flag: &str,
    value: Option<PathBuf>,
) -> Result<Option<PathBuf>, String> {
    value
        .map(|path| {
            let trimmed = path.to_string_lossy();
            let trimmed = trimmed.trim();
            if trimmed.is_empty() {
                return Err(format!("{flag} must not be empty"));
            }
            Ok(PathBuf::from(trimmed))
        })
        .transpose()
}

#[derive(Clone)]
struct ServerContext {
    storage: Arc<dyn Storage>,
    metadata_store: Arc<MetricMetadataStore>,
    exemplar_store: Arc<ExemplarStore>,
    rules_runtime: Arc<RulesRuntime>,
    engine: Arc<Engine>,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
    admin_api_enabled: bool,
    admin_path_prefix: Option<PathBuf>,
    security_manager: Arc<SecurityManager>,
    auth_token: Option<String>,
    admin_auth_token: Option<String>,
    internal_api: Option<Arc<cluster::rpc::InternalApiConfig>>,
    cluster_context: Option<Arc<cluster::ClusterRequestContext>>,
    tenant_registry: Option<Arc<tenant::TenantRegistry>>,
    rbac_registry: Option<Arc<RbacRegistry>>,
    edge_sync_context: Option<Arc<edge_sync::EdgeSyncRuntimeContext>>,
    usage_accounting: Arc<UsageAccounting>,
    managed_control_plane: Arc<ManagedControlPlane>,
}

impl ServerContext {
    fn handler_app_context(&self) -> handlers::AppContext<'_> {
        handlers::AppContext {
            storage: &self.storage,
            metadata_store: &self.metadata_store,
            exemplar_store: &self.exemplar_store,
            rules_runtime: Some(self.rules_runtime.as_ref()),
            engine: self.engine.as_ref(),
            cluster_context: self.cluster_context.as_deref(),
            tenant_registry: self.tenant_registry.as_deref(),
            rbac_registry: self.rbac_registry.as_deref(),
            edge_sync_context: self.edge_sync_context.as_deref(),
            security_manager: Some(self.security_manager.as_ref()),
            usage_accounting: Some(self.usage_accounting.as_ref()),
            managed_control_plane: Some(self.managed_control_plane.as_ref()),
        }
    }

    fn handler_request_context<'a>(
        &'a self,
        request: crate::http::HttpRequest,
    ) -> handlers::RequestContext<'a> {
        handlers::RequestContext {
            request,
            server_start: self.server_start,
            timestamp_precision: self.timestamp_precision,
            admin_api_enabled: self.admin_api_enabled,
            admin_path_prefix: self.admin_path_prefix.as_deref(),
            internal_api: self.internal_api.as_deref(),
        }
    }
}

struct AccessControlBootstrap {
    tenant_registry: Option<Arc<tenant::TenantRegistry>>,
    rbac_registry: Option<Arc<RbacRegistry>>,
}

struct ClusterBootstrap {
    cluster_context: Option<cluster::ClusterRequestContext>,
    internal_api: Option<cluster::rpc::InternalApiConfig>,
    edge_sync_context: Option<Arc<edge_sync::EdgeSyncRuntimeContext>>,
    auto_join_runtime: Option<cluster::membership::AutoJoinRuntime>,
}

impl ClusterBootstrap {
    fn clone_cluster_context(&self) -> Option<Arc<cluster::ClusterRequestContext>> {
        self.cluster_context
            .as_ref()
            .map(|context| Arc::new(context.clone()))
    }

    fn clone_internal_api(&self) -> Option<Arc<cluster::rpc::InternalApiConfig>> {
        self.internal_api
            .as_ref()
            .map(|internal_api| Arc::new(internal_api.clone()))
    }
}

struct SecurityBootstrap {
    security_manager: Arc<SecurityManager>,
}

struct StorageBootstrap {
    storage: Arc<dyn Storage>,
    metadata_store: Arc<MetricMetadataStore>,
    exemplar_store: Arc<ExemplarStore>,
    rules_runtime: Arc<RulesRuntime>,
    usage_accounting: Arc<UsageAccounting>,
    managed_control_plane: Arc<ManagedControlPlane>,
}

#[derive(Default)]
struct BackgroundWorkers {
    tasks: Vec<JoinHandle<()>>,
}

impl BackgroundWorkers {
    fn push(&mut self, task: JoinHandle<()>) {
        self.tasks.push(task);
    }

    async fn shutdown(self) {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            match task.await {
                Ok(()) => {}
                Err(err) if err.is_cancelled() => {}
                Err(err) => eprintln!("background worker task failed during shutdown: {err}"),
            }
        }
    }
}

struct ListenerBootstrap {
    listener: TcpListener,
    statsd_task: Option<JoinHandle<()>>,
    graphite_task: Option<JoinHandle<()>>,
}

struct ServerRuntime {
    listen_addr: String,
    app_context: ServerContext,
    listeners: ListenerBootstrap,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    storage: Arc<dyn Storage>,
    background_workers: BackgroundWorkers,
}

impl ServerRuntime {
    async fn bootstrap(config: ServerConfig) -> Result<Self, String> {
        let config = config.normalize_and_validate()?;
        let _ = admission::validate_public_write_admission_config()?;
        let _ = admission::validate_public_read_admission_config()?;
        admission::global_public_write_admission()?;
        admission::global_public_read_admission()?;

        let access_control = load_access_control(&config)?;
        let mut cluster = bootstrap_cluster_runtime(&config).await?;
        let security = build_security_runtime(&config, &mut cluster)?;
        let admin_path_prefix = resolve_admin_path_prefix(config.admin_path_prefix.as_deref())?;
        let storage = build_storage_runtime(&config, cluster.clone_cluster_context())?;
        let app_context = build_server_context(
            &config,
            admin_path_prefix,
            &access_control,
            &security,
            &storage,
            &cluster,
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let listeners = bind_listeners(&config, &app_context, &shutdown_rx).await?;
        let background_workers = start_background_workers(
            &cluster,
            Arc::clone(&storage.storage),
            &storage.rules_runtime,
        );

        Ok(Self {
            listen_addr: config.listen.clone(),
            app_context,
            listeners,
            shutdown_tx,
            shutdown_rx,
            storage: Arc::clone(&storage.storage),
            background_workers,
        })
    }

    async fn run(self) -> Result<(), String> {
        let ServerRuntime {
            listen_addr,
            app_context,
            listeners,
            shutdown_tx,
            shutdown_rx,
            storage,
            background_workers,
        } = self;
        let ListenerBootstrap {
            listener,
            statsd_task,
            graphite_task,
        } = listeners;
        let _shutdown_signal_task = start_shutdown_signal_task(shutdown_tx);
        let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));

        run_accept_loop(listener, app_context, shutdown_rx, semaphore, &listen_addr).await;
        await_listener_task(statsd_task).await;
        await_listener_task(graphite_task).await;
        background_workers.shutdown().await;
        close_storage_runtime(storage).await
    }
}

pub async fn run_server(config: ServerConfig) -> Result<(), String> {
    ServerRuntime::bootstrap(config).await?.run().await
}

fn load_access_control(config: &ServerConfig) -> Result<AccessControlBootstrap, String> {
    let tenant_registry = config
        .tenant_config_path
        .as_deref()
        .map(tenant::TenantRegistry::load_from_path)
        .transpose()?
        .map(Arc::new);
    let rbac_registry = config
        .rbac_config_path
        .as_deref()
        .map(RbacRegistry::load_from_path)
        .transpose()?
        .map(Arc::new);

    Ok(AccessControlBootstrap {
        tenant_registry,
        rbac_registry,
    })
}

async fn bootstrap_cluster_runtime(config: &ServerConfig) -> Result<ClusterBootstrap, String> {
    let mut cluster_context = None;
    let mut internal_api = None;
    let mut auto_join_runtime = None;

    if let Some(runtime) = cluster::ClusterRuntime::bootstrap(&config.cluster)? {
        let mut runtime = runtime;
        runtime.local_reads_serve_global_queries = config.cluster.enabled
            && config.cluster.node_role == cluster::config::ClusterNodeRole::Query
            && config.storage_mode == StorageRuntimeMode::ComputeOnly;
        let mut context = cluster::ClusterRequestContext::from_runtime(runtime)?;
        let control_state_store = Arc::new(build_cluster_control_store(config, &context.runtime)?);
        let bootstrap_state = cluster::control::ControlState::from_runtime(
            &context.runtime.membership,
            &context.runtime.ring,
        );
        let recovered_state = control_state_store.load_or_bootstrap(bootstrap_state)?;
        recovered_state
            .ensure_runtime_compatible(&context.runtime.membership, &context.runtime.ring)?;
        eprintln!(
            "cluster control-state store initialized at {} (schema v{})",
            control_state_store.path().display(),
            cluster::control::CONTROL_STATE_SCHEMA_VERSION
        );
        context.control_state_store = Some(Arc::clone(&control_state_store));

        let control_consensus = Arc::new(build_cluster_control_consensus(
            config,
            &context.runtime,
            Arc::clone(&control_state_store),
            recovered_state,
        )?);
        eprintln!(
            "cluster control-log consensus initialized at {}",
            control_consensus.log_path().display()
        );

        auto_join_runtime = cluster::membership::AutoJoinRuntime::from_membership(
            &context.runtime.membership,
            context.rpc_client.clone(),
        )?;
        let auto_join_satisfied = if let Some(runtime) = auto_join_runtime.as_ref() {
            runtime.attempt_once().await
        } else {
            false
        };
        if !auto_join_satisfied {
            if let Err(err) = control_consensus
                .ensure_leader_established(&context.rpc_client)
                .await
            {
                eprintln!("cluster control leader proposal did not commit at startup: {err}");
            }
        }
        context.control_consensus = Some(Arc::clone(&control_consensus));

        let audit_log = Arc::new(build_cluster_audit_log(config, &context.runtime)?);
        eprintln!(
            "cluster audit log initialized at {}",
            audit_log.path().display()
        );
        context.audit_log = Some(audit_log);

        let dedupe_store = Arc::new(build_cluster_dedupe_store(config, &context.runtime)?);
        eprintln!(
            "cluster dedupe marker store initialized at {}",
            dedupe_store.marker_path().display()
        );
        context.dedupe_store = Some(dedupe_store);

        let outbox = Arc::new(build_cluster_outbox_store(config, &context.runtime)?);
        eprintln!(
            "cluster hinted-handoff outbox initialized at {}",
            outbox.outbox_path().display()
        );
        context.write_router = context
            .write_router
            .clone()
            .with_outbox(Arc::clone(&outbox));
        context.outbox = Some(outbox);

        let digest_config = cluster::repair::DigestExchangeConfig::from_env()?;
        eprintln!(
            "cluster digest exchange configured (interval={}s, window={}s, max_shards_per_tick={}, max_bytes_per_tick={})",
            digest_config.interval.as_secs(),
            digest_config.window.as_secs(),
            digest_config.max_shards_per_tick,
            digest_config.max_bytes_per_tick
        );
        eprintln!(
            "cluster rebalance scheduler configured (interval={}s, max_rows_per_tick={}, max_shards_per_tick={})",
            digest_config.rebalance_interval.as_secs(),
            digest_config.rebalance_max_rows_per_tick,
            digest_config.rebalance_max_shards_per_tick
        );
        let digest_runtime = Arc::new(cluster::repair::DigestExchangeRuntime::new(
            context.runtime.membership.local_node_id.clone(),
            context.rpc_client.clone(),
            control_consensus,
            digest_config,
        ));
        context.digest_runtime = Some(digest_runtime);

        internal_api = Some(context.runtime.internal_api.clone());
        cluster_context = Some(context);
    }

    let standalone_edge_sync_accept = if cluster_context.is_none() {
        match (
            config.edge_sync_auth_token.as_deref(),
            config.data_path.as_deref(),
        ) {
            (Some(auth_token), Some(data_path)) => {
                let dedupe_config = edge_sync::edge_sync_accept_dedupe_config()?;
                let dedupe_store =
                    edge_sync::open_edge_sync_accept_dedupe_store(data_path, dedupe_config)?;
                Some((
                    Arc::new(edge_sync::edge_sync_accept_internal_api(auth_token)),
                    dedupe_store,
                    dedupe_config,
                ))
            }
            _ => None,
        }
    } else {
        None
    };
    let edge_sync_source = if let (Some(upstream_endpoint), Some(auth_token), Some(data_path)) = (
        config.edge_sync_upstream_endpoint.clone(),
        config.edge_sync_auth_token.clone(),
        config.data_path.as_deref(),
    ) {
        let tenant_mapping = if let Some(tenant_id) = config.edge_sync_static_tenant.clone() {
            edge_sync::EdgeSyncTenantMapping::static_tenant(tenant_id)
        } else {
            edge_sync::EdgeSyncTenantMapping::preserve()
        };
        let source_id = config
            .edge_sync_source_id
            .clone()
            .unwrap_or_else(|| config.listen.clone());
        Some(Arc::new(edge_sync::EdgeSyncSourceRuntime::open(
            data_path,
            edge_sync::EdgeSyncSourceBootstrap {
                source_id,
                upstream_endpoint,
                shared_auth_token: auth_token,
                tenant_mapping,
            },
        )?))
    } else {
        None
    };
    let edge_sync_context = if edge_sync_source.is_some() || standalone_edge_sync_accept.is_some() {
        Some(Arc::new(edge_sync::EdgeSyncRuntimeContext {
            source: edge_sync_source,
            accept_dedupe_store: standalone_edge_sync_accept
                .as_ref()
                .map(|(_, store, _)| Arc::clone(store)),
            accept_dedupe_config: standalone_edge_sync_accept
                .as_ref()
                .map(|(_, _, config)| *config),
        }))
    } else {
        None
    };
    let internal_api = internal_api.or_else(|| {
        standalone_edge_sync_accept
            .as_ref()
            .map(|(internal_api, _, _)| (**internal_api).clone())
    });

    Ok(ClusterBootstrap {
        cluster_context,
        internal_api,
        edge_sync_context,
        auto_join_runtime,
    })
}

fn build_security_runtime(
    config: &ServerConfig,
    cluster: &mut ClusterBootstrap,
) -> Result<SecurityBootstrap, String> {
    let security_manager = SecurityManager::from_config(config)?;
    if let Some(internal_api) = cluster.internal_api.as_mut() {
        security_manager.attach_internal_api(internal_api);
    }
    if let Some(context) = cluster.cluster_context.as_mut() {
        security_manager.attach_rpc_client(&mut context.rpc_client);
        if let Some(internal_api) = cluster.internal_api.as_ref() {
            context.runtime.internal_api = internal_api.clone();
        }
    }

    Ok(SecurityBootstrap { security_manager })
}

fn build_storage_runtime(
    config: &ServerConfig,
    cluster_context: Option<Arc<cluster::ClusterRequestContext>>,
) -> Result<StorageBootstrap, String> {
    let storage = build_storage(config).map_err(|err| format!("failed to build storage: {err}"))?;
    let metadata_store = Arc::new(
        MetricMetadataStore::open(config.data_path.as_deref())
            .map_err(|err| format!("failed to open metric metadata store: {err}"))?,
    );
    let exemplar_store = Arc::new(
        ExemplarStore::open(config.data_path.as_deref())
            .map_err(|err| format!("failed to open exemplar store: {err}"))?,
    );
    let usage_accounting = UsageAccounting::open(config.data_path.as_deref())
        .map_err(|err| format!("failed to open usage accounting store: {err}"))?;
    if let Err(err) = usage_accounting.reconcile_storage(&storage) {
        eprintln!("usage storage reconciliation at startup failed: {err}");
    }
    let managed_control_plane = Arc::new(
        ManagedControlPlane::open(config.data_path.as_deref())
            .map_err(|err| format!("failed to open managed control-plane store: {err}"))?,
    );
    let rules_runtime = RulesRuntime::open(
        config.data_path.as_deref(),
        Arc::clone(&storage),
        config.timestamp_precision,
        cluster_context,
        Some(Arc::clone(&usage_accounting)),
    )
    .map_err(|err| format!("failed to open rules runtime: {err}"))?;

    Ok(StorageBootstrap {
        storage,
        metadata_store,
        exemplar_store,
        rules_runtime,
        usage_accounting,
        managed_control_plane,
    })
}

fn build_server_context(
    config: &ServerConfig,
    admin_path_prefix: Option<PathBuf>,
    access_control: &AccessControlBootstrap,
    security: &SecurityBootstrap,
    storage: &StorageBootstrap,
    cluster: &ClusterBootstrap,
) -> ServerContext {
    ServerContext {
        storage: Arc::clone(&storage.storage),
        metadata_store: Arc::clone(&storage.metadata_store),
        exemplar_store: Arc::clone(&storage.exemplar_store),
        rules_runtime: Arc::clone(&storage.rules_runtime),
        engine: Arc::new(Engine::with_precision(
            Arc::clone(&storage.storage),
            config.timestamp_precision,
        )),
        server_start: Instant::now(),
        timestamp_precision: config.timestamp_precision,
        admin_api_enabled: config.admin_api_enabled,
        admin_path_prefix,
        security_manager: Arc::clone(&security.security_manager),
        auth_token: config.auth_token.clone(),
        admin_auth_token: config.admin_auth_token.clone(),
        internal_api: cluster.clone_internal_api(),
        cluster_context: cluster.clone_cluster_context(),
        tenant_registry: access_control.tenant_registry.clone(),
        rbac_registry: access_control.rbac_registry.clone(),
        edge_sync_context: cluster.edge_sync_context.clone(),
        usage_accounting: Arc::clone(&storage.usage_accounting),
        managed_control_plane: Arc::clone(&storage.managed_control_plane),
    }
}

fn start_background_workers(
    cluster: &ClusterBootstrap,
    storage: Arc<dyn Storage>,
    rules_runtime: &Arc<RulesRuntime>,
) -> BackgroundWorkers {
    let mut workers = BackgroundWorkers::default();

    if let Some(auto_join_runtime) = cluster.auto_join_runtime.as_ref() {
        workers.push(auto_join_runtime.start_worker());
    }
    if let Some(context) = cluster.cluster_context.as_ref() {
        if let Some(control_consensus) = context.control_consensus.as_ref() {
            workers.push(control_consensus.start_reconciler(context.rpc_client.clone()));
        }
        if let Some(dedupe_store) = context.dedupe_store.as_ref() {
            workers.push(dedupe_store.start_cleanup_worker());
        }
        if let Some(outbox) = context.outbox.as_ref() {
            workers.push(outbox.start_replay_worker(context.rpc_client.clone()));
            workers.push(outbox.start_cleanup_worker());
        }
        if let Some(digest_runtime) = context.digest_runtime.as_ref() {
            workers.push(digest_runtime.start_worker(Arc::clone(&storage)));
            workers.push(digest_runtime.start_rebalance_worker());
        }
    }
    if let Some(edge_sync_context) = cluster.edge_sync_context.as_ref() {
        if let Some(dedupe_store) = edge_sync_context.accept_dedupe_store.as_ref() {
            workers.push(dedupe_store.start_cleanup_worker());
        }
        if let Some(source) = edge_sync_context.source.as_ref() {
            workers.push(source.start_replay_worker());
            workers.push(source.start_cleanup_worker());
        }
    }

    workers.push(rules_runtime.start_worker());
    workers
}

async fn bind_listeners(
    config: &ServerConfig,
    app_context: &ServerContext,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<ListenerBootstrap, String> {
    let listener = TcpListener::bind(&config.listen)
        .await
        .map_err(|err| format!("bind {} failed: {err}", config.listen))?;
    let statsd_task = bind_statsd_listener(config, app_context, shutdown_rx).await?;
    let graphite_task = bind_graphite_listener(config, app_context, shutdown_rx).await?;

    Ok(ListenerBootstrap {
        listener,
        statsd_task,
        graphite_task,
    })
}

async fn bind_statsd_listener(
    config: &ServerConfig,
    app_context: &ServerContext,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<Option<JoinHandle<()>>, String> {
    legacy_ingest::set_listener_enabled(legacy_ingest::LegacyAdapterKind::Statsd, false);

    let Some(addr) = config.statsd_listen.clone() else {
        return Ok(None);
    };

    let socket = UdpSocket::bind(&addr)
        .await
        .map_err(|err| format!("bind statsd listener {addr} failed: {err}"))?;
    let bound = socket
        .local_addr()
        .map_err(|err| format!("statsd listener local_addr failed: {err}"))?;
    eprintln!(
        "statsd listener listening on {} for tenant '{}'",
        bound, config.statsd_tenant_id
    );
    legacy_ingest::set_listener_enabled(legacy_ingest::LegacyAdapterKind::Statsd, true);

    let statsd_tenant_id = config.statsd_tenant_id.clone();
    let app_context = app_context.clone();
    let mut shutdown_rx = shutdown_rx.clone();
    Ok(Some(tokio::spawn(async move {
        if let Err(err) =
            run_statsd_listener(socket, &app_context, &statsd_tenant_id, &mut shutdown_rx).await
        {
            eprintln!("statsd listener error: {err}");
        }
        legacy_ingest::set_listener_enabled(legacy_ingest::LegacyAdapterKind::Statsd, false);
    })))
}

async fn bind_graphite_listener(
    config: &ServerConfig,
    app_context: &ServerContext,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<Option<JoinHandle<()>>, String> {
    legacy_ingest::set_listener_enabled(legacy_ingest::LegacyAdapterKind::Graphite, false);

    let Some(addr) = config.graphite_listen.clone() else {
        return Ok(None);
    };

    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|err| format!("bind graphite listener {addr} failed: {err}"))?;
    let bound = listener
        .local_addr()
        .map_err(|err| format!("graphite listener local_addr failed: {err}"))?;
    eprintln!(
        "graphite listener listening on {} for tenant '{}'",
        bound, config.graphite_tenant_id
    );
    legacy_ingest::set_listener_enabled(legacy_ingest::LegacyAdapterKind::Graphite, true);

    let graphite_tenant_id = config.graphite_tenant_id.clone();
    let app_context = app_context.clone();
    let mut shutdown_rx = shutdown_rx.clone();
    Ok(Some(tokio::spawn(async move {
        if let Err(err) = run_graphite_listener(
            listener,
            &app_context,
            &graphite_tenant_id,
            &mut shutdown_rx,
        )
        .await
        {
            eprintln!("graphite listener error: {err}");
        }
        legacy_ingest::set_listener_enabled(legacy_ingest::LegacyAdapterKind::Graphite, false);
    })))
}

fn start_shutdown_signal_task(shutdown_tx: watch::Sender<bool>) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = wait_for_shutdown_signal().await {
            eprintln!("signal handler error: {err}");
        }
        eprintln!("shutting down...");
        let _ = shutdown_tx.send(true);
    })
}

async fn run_accept_loop(
    listener: TcpListener,
    app_context: ServerContext,
    shutdown_rx: watch::Receiver<bool>,
    semaphore: Arc<Semaphore>,
    listen_addr: &str,
) {
    println!("tsink server listening on {}", listen_addr);

    let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let shutdown_rx = shutdown_rx;

    loop {
        let permit = tokio::select! {
            permit = semaphore.clone().acquire_owned() => {
                match permit {
                    Ok(p) => p,
                    Err(_) => break,
                }
            }
            _ = shutdown_rx_changed(shutdown_rx.clone()) => {
                break;
            }
        };

        let stream = tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => stream,
                    Err(err) => {
                        eprintln!("accept error: {err}");
                        drop(permit);
                        continue;
                    }
                }
            }
            _ = shutdown_rx_changed(shutdown_rx.clone()) => {
                break;
            }
        };

        let app_context = app_context.clone();
        let active = Arc::clone(&active_connections);

        active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = handle_connection(stream, &app_context, None, &mut shutdown_rx).await;
            if let Err(err) = result {
                eprintln!("connection error: {err}");
            }
            active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    let drain_start = Instant::now();
    while active_connections.load(std::sync::atomic::Ordering::Relaxed) > 0 {
        if drain_start.elapsed() > SHUTDOWN_GRACE_PERIOD {
            eprintln!("shutdown grace period expired, closing with active connections");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn await_listener_task(task: Option<JoinHandle<()>>) {
    if let Some(task) = task {
        let _ = task.await;
    }
}

async fn close_storage_runtime(storage: Arc<dyn Storage>) -> Result<(), String> {
    match tokio::task::spawn_blocking(move || storage.close()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(format!("storage close failed: {err}")),
        Err(err) => Err(format!("storage close task failed: {err}")),
    }
}

#[cfg(test)]
fn ensure_rustls_crypto_provider() {
    RUSTLS_CRYPTO_PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(test)]
fn validate_runtime_config(config: &ServerConfig) -> Result<(), String> {
    config.clone().normalize_and_validate()?;
    let _ = admission::validate_public_write_admission_config()?;
    let _ = admission::validate_public_read_admission_config()?;
    Ok(())
}

fn resolve_admin_path_prefix(path: Option<&Path>) -> Result<Option<PathBuf>, String> {
    let Some(path) = path else {
        return Ok(None);
    };

    let canonical = std::fs::canonicalize(path).map_err(|err| {
        format!(
            "failed to resolve admin path prefix {}: {err}",
            path.display()
        )
    })?;
    if !canonical.is_dir() {
        return Err(format!(
            "admin path prefix must be a directory: {}",
            canonical.display()
        ));
    }
    Ok(Some(canonical))
}

async fn shutdown_rx_changed(mut rx: watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
        if *rx.borrow() {
            return;
        }
    }
}

async fn wait_for_shutdown_signal() -> Result<(), String> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).map_err(|e| format!("SIGTERM handler: {e}"))?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => Ok(()),
            _ = sigterm.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|e| format!("ctrl_c handler: {e}"))
    }
}

async fn run_statsd_listener(
    socket: UdpSocket,
    app_context: &ServerContext,
    tenant_id: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    let config = legacy_ingest::statsd_config();
    let adapter = StatsdAdapter::new();
    let mut buffer = vec![0_u8; config.max_packet_bytes.max(64)];

    loop {
        let received = tokio::select! {
            result = socket.recv_from(&mut buffer) => result.map_err(|err| format!("statsd recv failed: {err}"))?,
            _ = shutdown_rx.changed() => return Ok(()),
        };
        let packet = match std::str::from_utf8(&buffer[..received.0]) {
            Ok(packet) => packet,
            Err(_) => {
                legacy_ingest::record_request_rejected(legacy_ingest::LegacyAdapterKind::Statsd, 0);
                continue;
            }
        };
        let normalized = match adapter.normalize_packet(
            packet,
            tenant_id,
            current_timestamp_for_precision(app_context.timestamp_precision),
            config,
        ) {
            Ok(normalized) => normalized,
            Err(err) => {
                if legacy_ingest_error_is_throttled(&err) {
                    legacy_ingest::record_request_throttled(
                        legacy_ingest::LegacyAdapterKind::Statsd,
                        0,
                    );
                } else {
                    legacy_ingest::record_request_rejected(
                        legacy_ingest::LegacyAdapterKind::Statsd,
                        0,
                    );
                }
                continue;
            }
        };
        if normalized.request_units == 0 {
            continue;
        }
        match handlers::ingest_adapter_write_envelope(
            &app_context.storage,
            &app_context.metadata_store,
            &app_context.exemplar_store,
            app_context.cluster_context.as_deref(),
            app_context.tenant_registry.as_deref(),
            Some(app_context.managed_control_plane.as_ref()),
            app_context.edge_sync_context.as_deref(),
            Some(app_context.usage_accounting.as_ref()),
            tenant_id,
            "statsd",
            normalized.envelope,
        )
        .await
        {
            Ok(()) => legacy_ingest::record_request_accepted(
                legacy_ingest::LegacyAdapterKind::Statsd,
                normalized.sample_count,
            ),
            Err(err) => {
                if legacy_ingest_error_is_throttled(&err) {
                    legacy_ingest::record_request_throttled(
                        legacy_ingest::LegacyAdapterKind::Statsd,
                        normalized.sample_count,
                    );
                } else {
                    legacy_ingest::record_request_rejected(
                        legacy_ingest::LegacyAdapterKind::Statsd,
                        normalized.sample_count,
                    );
                }
            }
        }
    }
}

async fn run_graphite_listener(
    listener: TcpListener,
    app_context: &ServerContext,
    tenant_id: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    loop {
        let stream = tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result.map_err(|err| format!("graphite accept failed: {err}"))?;
                stream
            }
            _ = shutdown_rx.changed() => return Ok(()),
        };

        let app_context = app_context.clone();
        let tenant_id = tenant_id.to_string();
        let mut connection_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_graphite_connection(
                stream,
                &app_context,
                &tenant_id,
                &mut connection_shutdown,
            )
            .await
            {
                eprintln!("graphite connection error: {err}");
            }
        });
    }
}

async fn handle_graphite_connection(
    stream: tokio::net::TcpStream,
    app_context: &ServerContext,
    tenant_id: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    let config = legacy_ingest::graphite_config();
    let mut reader = tokio::io::BufReader::new(stream);
    let mut line = Vec::new();

    loop {
        line.clear();
        let bytes_read = tokio::select! {
            result = reader.read_until(b'\n', &mut line) => result.map_err(|err| format!("graphite read failed: {err}"))?,
            _ = shutdown_rx.changed() => return Ok(()),
        };
        if bytes_read == 0 {
            return Ok(());
        }
        if line.len() > config.max_line_bytes {
            legacy_ingest::record_request_throttled(legacy_ingest::LegacyAdapterKind::Graphite, 0);
            continue;
        }
        let raw_line = match std::str::from_utf8(&line) {
            Ok(line) => line,
            Err(_) => {
                legacy_ingest::record_request_rejected(
                    legacy_ingest::LegacyAdapterKind::Graphite,
                    0,
                );
                continue;
            }
        };
        let normalized = match legacy_ingest::normalize_graphite_plaintext_line(
            raw_line,
            tenant_id,
            app_context.timestamp_precision,
            current_timestamp_for_precision(app_context.timestamp_precision),
        ) {
            Ok(normalized) => normalized,
            Err(err) => {
                if legacy_ingest_error_is_throttled(&err) {
                    legacy_ingest::record_request_throttled(
                        legacy_ingest::LegacyAdapterKind::Graphite,
                        0,
                    );
                } else {
                    legacy_ingest::record_request_rejected(
                        legacy_ingest::LegacyAdapterKind::Graphite,
                        0,
                    );
                }
                continue;
            }
        };
        if normalized.request_units == 0 {
            continue;
        }
        match handlers::ingest_adapter_write_envelope(
            &app_context.storage,
            &app_context.metadata_store,
            &app_context.exemplar_store,
            app_context.cluster_context.as_deref(),
            app_context.tenant_registry.as_deref(),
            Some(app_context.managed_control_plane.as_ref()),
            app_context.edge_sync_context.as_deref(),
            Some(app_context.usage_accounting.as_ref()),
            tenant_id,
            "graphite",
            normalized.envelope,
        )
        .await
        {
            Ok(()) => legacy_ingest::record_request_accepted(
                legacy_ingest::LegacyAdapterKind::Graphite,
                normalized.sample_count,
            ),
            Err(err) => {
                if legacy_ingest_error_is_throttled(&err) {
                    legacy_ingest::record_request_throttled(
                        legacy_ingest::LegacyAdapterKind::Graphite,
                        normalized.sample_count,
                    );
                } else {
                    legacy_ingest::record_request_rejected(
                        legacy_ingest::LegacyAdapterKind::Graphite,
                        normalized.sample_count,
                    );
                }
            }
        }
    }
}

fn current_timestamp_for_precision(precision: TimestampPrecision) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    match precision {
        TimestampPrecision::Seconds => now.as_secs() as i64,
        TimestampPrecision::Milliseconds => now.as_millis() as i64,
        TimestampPrecision::Microseconds => now.as_micros() as i64,
        TimestampPrecision::Nanoseconds => now.as_nanos() as i64,
    }
}

fn legacy_ingest_error_is_throttled(err: &str) -> bool {
    err.contains("exceeds line limit")
        || err.contains("exceeds event limit")
        || err.contains("exceeds byte limit")
        || err.contains("limit exceeded")
}

fn build_storage(config: &ServerConfig) -> tsink::Result<Arc<dyn Storage>> {
    let mut builder = StorageBuilder::new()
        .with_wal_enabled(
            config.wal_enabled && config.storage_mode != StorageRuntimeMode::ComputeOnly,
        )
        .with_timestamp_precision(config.timestamp_precision);

    if let Some(path) = &config.object_store_path {
        builder = builder.with_object_store_path(path);
    }
    if let Some(retention) = config.hot_tier_retention.zip(config.warm_tier_retention) {
        builder = builder.with_tiered_retention_policy(retention.0, retention.1);
    } else if let Some(hot_retention) = config.hot_tier_retention {
        let warm_retention = config.retention.unwrap_or(DEFAULT_STORAGE_RETENTION);
        builder = builder.with_tiered_retention_policy(hot_retention, warm_retention);
    } else if let Some(warm_retention) = config.warm_tier_retention {
        let hot_retention =
            warm_retention.min(config.retention.unwrap_or(DEFAULT_STORAGE_RETENTION));
        builder = builder.with_tiered_retention_policy(hot_retention, warm_retention);
    }
    if config.storage_mode != StorageRuntimeMode::ReadWrite {
        builder = builder.with_runtime_mode(config.storage_mode);
    }
    if let Some(interval) = config.remote_segment_refresh_interval {
        builder = builder.with_remote_segment_refresh_interval(interval);
    }
    if config.mirror_hot_segments_to_object_store {
        builder = builder.with_mirror_hot_segments_to_object_store(true);
    }

    if let Some(path) = &config.data_path {
        if config.storage_mode == StorageRuntimeMode::ReadWrite {
            builder = builder.with_data_path(path);
        }
    }
    builder = builder.with_retention(config.retention.unwrap_or(DEFAULT_STORAGE_RETENTION));
    if let Some(limit) = config.memory_limit {
        builder = builder.with_memory_limit(limit);
    }
    if let Some(limit) = config.cardinality_limit {
        builder = builder.with_cardinality_limit(limit);
    }
    if let Some(points) = config.chunk_points {
        builder = builder.with_chunk_points(points);
    }
    if let Some(writers) = config.max_writers {
        builder = builder.with_max_writers(writers);
    }
    if let Some(mode) = config.wal_sync_mode {
        builder = builder.with_wal_sync_mode(mode);
    }
    if config.cluster.enabled {
        builder = builder.with_metadata_shard_count(config.cluster.shards);
    }

    builder.build()
}

fn build_cluster_dedupe_store(
    config: &ServerConfig,
    runtime: &cluster::ClusterRuntime,
) -> Result<DedupeWindowStore, String> {
    let dedupe_config = cluster::dedupe::DedupeConfig::from_env()?;
    let base_path = config
        .data_path
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("tsink-server"));
    let marker_path = base_path.join("cluster").join("dedupe").join(format!(
        "{}.markers.log",
        sanitize_path_component(&runtime.membership.local_node_id)
    ));
    DedupeWindowStore::open(marker_path, dedupe_config)
}

fn build_cluster_audit_log(
    config: &ServerConfig,
    runtime: &cluster::ClusterRuntime,
) -> Result<ClusterAuditLog, String> {
    let audit_config = cluster::audit::ClusterAuditConfig::from_env()?;
    let base_path = config
        .data_path
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("tsink-server"));
    let audit_path = base_path.join("cluster").join("audit").join(format!(
        "{}.audit.log",
        sanitize_path_component(&runtime.membership.local_node_id)
    ));
    ClusterAuditLog::open(audit_path, audit_config)
}

fn build_cluster_control_store(
    config: &ServerConfig,
    runtime: &cluster::ClusterRuntime,
) -> Result<cluster::control::ControlStateStore, String> {
    let base_path = config
        .data_path
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("tsink-server"));
    let control_state_path = base_path.join("cluster").join("control").join(format!(
        "{}.control-state.json",
        sanitize_path_component(&runtime.membership.local_node_id)
    ));
    cluster::control::ControlStateStore::open(control_state_path)
}

fn build_cluster_outbox_store(
    config: &ServerConfig,
    runtime: &cluster::ClusterRuntime,
) -> Result<HintedHandoffOutbox, String> {
    let outbox_config = cluster::outbox::OutboxConfig::from_env()?;
    let base_path = config
        .data_path
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("tsink-server"));
    let outbox_path = base_path.join("cluster").join("outbox").join(format!(
        "{}.outbox.log",
        sanitize_path_component(&runtime.membership.local_node_id)
    ));
    HintedHandoffOutbox::open(outbox_path, outbox_config)
}

fn build_cluster_control_consensus(
    config: &ServerConfig,
    runtime: &cluster::ClusterRuntime,
    control_state_store: Arc<cluster::control::ControlStateStore>,
    recovered_state: cluster::control::ControlState,
) -> Result<cluster::consensus::ControlConsensusRuntime, String> {
    let control_consensus_config = cluster::consensus::ControlConsensusConfig::from_env()?;
    let base_path = config
        .data_path
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("tsink-server"));
    let control_log_path = base_path.join("cluster").join("control").join(format!(
        "{}.control-log.json",
        sanitize_path_component(&runtime.membership.local_node_id)
    ));
    cluster::consensus::ControlConsensusRuntime::open(
        runtime.membership.clone(),
        control_state_store,
        recovered_state,
        control_log_path,
        control_consensus_config,
    )
}

fn sanitize_path_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "node".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prom_remote::{
        Label as PromLabel, LabelMatcher, MatcherType, Query as PromReadQuery,
        QueryResult as PromReadQueryResult, ReadRequest, ReadResponse, Sample as PromSample,
        TimeSeries, WriteRequest,
    };
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    use prost::Message;
    use ring::hmac;
    use serde_json::{json, Value as JsonValue};
    use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tsink::{DataPoint, Label, QueryOptions, StorageBuilder, TsinkError};

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(1)
            .build()
            .expect("storage should build")
    }

    struct CloseFailingStorage;

    impl Storage for CloseFailingStorage {
        fn insert_rows(&self, _rows: &[tsink::Row]) -> tsink::Result<()> {
            Ok(())
        }

        fn select(
            &self,
            _metric: &str,
            _labels: &[Label],
            _start: i64,
            _end: i64,
        ) -> tsink::Result<Vec<DataPoint>> {
            Ok(Vec::new())
        }

        fn select_with_options(
            &self,
            _metric: &str,
            _opts: QueryOptions,
        ) -> tsink::Result<Vec<DataPoint>> {
            Ok(Vec::new())
        }

        fn select_all(
            &self,
            _metric: &str,
            _start: i64,
            _end: i64,
        ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            Ok(Vec::new())
        }

        fn close(&self) -> tsink::Result<()> {
            Err(TsinkError::Other("close boom".to_string()))
        }
    }

    #[tokio::test]
    async fn close_storage_runtime_reports_close_errors() {
        let storage: Arc<dyn Storage> = Arc::new(CloseFailingStorage);
        let err = close_storage_runtime(storage)
            .await
            .expect_err("close failure should be surfaced");
        assert!(err.contains("close boom"));
    }

    #[tokio::test]
    async fn background_workers_shutdown_aborts_and_awaits_tasks() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        struct DropNotify(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for DropNotify {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }

        let task = tokio::spawn(async move {
            let _drop_notify = DropNotify(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("worker future should start");
        let mut workers = BackgroundWorkers::default();
        workers.push(task);

        workers.shutdown().await;
        dropped_rx
            .await
            .expect("aborted worker future should be dropped");
    }

    fn malformed_disabled_cluster_config() -> ClusterConfig {
        ClusterConfig {
            enabled: false,
            node_id: None,
            bind: None,
            seeds: vec![
                "not-an-endpoint".to_string(),
                "also-not-an-endpoint".to_string(),
            ],
            shards: 0,
            replication_factor: 0,
            ..ClusterConfig::default()
        }
    }

    fn build_http_request(
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Vec<u8> {
        let mut request = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n");
        for (name, value) in headers {
            request.push_str(name);
            request.push_str(": ");
            request.push_str(value);
            request.push_str("\r\n");
        }
        request.push_str("Connection: close\r\n");
        if !body.is_empty() {
            request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        request.push_str("\r\n");

        let mut bytes = request.into_bytes();
        bytes.extend_from_slice(body);
        bytes
    }

    fn response_body(response: &[u8]) -> &[u8] {
        response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|idx| &response[idx + 4..])
            .unwrap_or(&[])
    }

    fn response_header_value<'a>(response: &'a [u8], name: &str) -> Option<&'a str> {
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")?;
        let headers = std::str::from_utf8(&response[..header_end]).ok()?;
        for line in headers.lines().skip(1) {
            let Some((header_name, header_value)) = line.split_once(':') else {
                continue;
            };
            if header_name.trim().eq_ignore_ascii_case(name) {
                return Some(header_value.trim());
            }
        }
        None
    }

    fn encode_snappy_message<M: Message>(message: &M) -> Vec<u8> {
        let mut encoded = Vec::new();
        message
            .encode(&mut encoded)
            .expect("protobuf encoding should succeed");
        SnappyEncoder::new()
            .compress_vec(&encoded)
            .expect("snappy compression should succeed")
    }

    fn decode_snappy_message<M>(compressed: &[u8]) -> M
    where
        M: Message + Default,
    {
        let decoded = SnappyDecoder::new()
            .decompress_vec(compressed)
            .expect("snappy decompression should succeed");
        M::decode(decoded.as_slice()).expect("protobuf decoding should succeed")
    }

    async fn send_request_with_config(
        storage: Arc<dyn Storage>,
        config: &ServerConfig,
        raw_request: &[u8],
    ) -> Vec<u8> {
        let tenant_registry = config
            .tenant_config_path
            .as_deref()
            .map(tenant::TenantRegistry::load_from_path)
            .transpose()
            .expect("tenant registry should load")
            .map(Arc::new);
        let rbac_registry = config
            .rbac_config_path
            .as_deref()
            .map(RbacRegistry::load_from_path)
            .transpose()
            .expect("RBAC registry should load")
            .map(Arc::new);
        send_request_with_runtime(storage, config, tenant_registry, rbac_registry, raw_request)
            .await
    }

    async fn send_request_with_runtime(
        storage: Arc<dyn Storage>,
        config: &ServerConfig,
        tenant_registry: Option<Arc<tenant::TenantRegistry>>,
        rbac_registry: Option<Arc<RbacRegistry>>,
        raw_request: &[u8],
    ) -> Vec<u8> {
        validate_runtime_config(config).expect("config should be valid");
        let runtime = cluster::ClusterRuntime::bootstrap(&config.cluster)
            .expect("cluster bootstrap should not fail");
        assert!(
            runtime.is_none(),
            "cluster runtime must stay disabled in compatibility mode"
        );

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let auth_token_owned = config.auth_token.clone();
        let admin_auth_token_owned = config.admin_auth_token.clone();
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let timestamp_precision = config.timestamp_precision;
        let security_manager =
            SecurityManager::from_config(config).expect("security runtime should build");
        let rules_runtime =
            RulesRuntime::open(None, Arc::clone(&storage), timestamp_precision, None, None)
                .expect("rules runtime should open");
        let usage_accounting =
            UsageAccounting::open(None).expect("usage accounting should open in memory");
        let managed_control_plane = Arc::new(
            ManagedControlPlane::open(None).expect("managed control plane should open in memory"),
        );
        let app_context = ServerContext {
            storage: Arc::clone(&storage),
            metadata_store: Arc::clone(&metadata_store),
            exemplar_store: Arc::clone(&exemplar_store),
            rules_runtime: Arc::clone(&rules_runtime),
            engine: Arc::new(Engine::with_precision(
                Arc::clone(&storage),
                timestamp_precision,
            )),
            server_start: Instant::now(),
            timestamp_precision,
            admin_api_enabled: config.admin_api_enabled,
            admin_path_prefix: config.admin_path_prefix.clone(),
            security_manager,
            auth_token: auth_token_owned,
            admin_auth_token: admin_auth_token_owned,
            internal_api: None,
            cluster_context: None,
            tenant_registry,
            rbac_registry,
            edge_sync_context: None,
            usage_accounting,
            managed_control_plane,
        };

        let (client, server) = tokio::io::duplex(64 * 1024);
        let server_handle = tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(server);
            let mut shutdown_rx = shutdown_rx;
            let _ = handle_http_loop(
                &mut reader,
                &mut writer,
                &app_context,
                None,
                &mut shutdown_rx,
            )
            .await;
        });

        let (mut client_read, mut client_write) = tokio::io::split(client);
        client_write.write_all(raw_request).await.unwrap();
        drop(client_write);
        let mut response = Vec::new();
        client_read.read_to_end(&mut response).await.unwrap();

        let _ = server_handle.await;
        response
    }

    async fn send_request(raw_request: &[u8], auth_token: Option<&str>) -> Vec<u8> {
        let config = ServerConfig {
            auth_token: auth_token.map(ToString::to_string),
            ..ServerConfig::default()
        };
        send_request_with_config(make_storage(), &config, raw_request).await
    }

    async fn send_request_with_internal_api(
        storage: Arc<dyn Storage>,
        raw_request: &[u8],
        internal_api: cluster::rpc::InternalApiConfig,
    ) -> Vec<u8> {
        send_request_with_internal_api_and_auth(storage, raw_request, internal_api, None, None)
            .await
    }

    async fn send_request_with_internal_api_and_auth(
        storage: Arc<dyn Storage>,
        raw_request: &[u8],
        internal_api: cluster::rpc::InternalApiConfig,
        auth_token: Option<&str>,
        admin_auth_token: Option<&str>,
    ) -> Vec<u8> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let auth_token_owned = auth_token.map(ToString::to_string);
        let admin_auth_token_owned = admin_auth_token.map(ToString::to_string);
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let rules_runtime = RulesRuntime::open(
            None,
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("rules runtime should open");
        let usage_accounting =
            UsageAccounting::open(None).expect("usage accounting should open in memory");
        let managed_control_plane = Arc::new(
            ManagedControlPlane::open(None).expect("managed control plane should open in memory"),
        );
        let app_context = ServerContext {
            storage: Arc::clone(&storage),
            metadata_store: Arc::clone(&metadata_store),
            exemplar_store: Arc::clone(&exemplar_store),
            rules_runtime: Arc::clone(&rules_runtime),
            engine: Arc::new(Engine::with_precision(
                Arc::clone(&storage),
                TimestampPrecision::Milliseconds,
            )),
            server_start: Instant::now(),
            timestamp_precision: TimestampPrecision::Milliseconds,
            admin_api_enabled: false,
            admin_path_prefix: None,
            security_manager: SecurityManager::from_config(&ServerConfig::default())
                .expect("security runtime should build"),
            auth_token: auth_token_owned,
            admin_auth_token: admin_auth_token_owned,
            internal_api: Some(Arc::new(internal_api)),
            cluster_context: None,
            tenant_registry: None,
            rbac_registry: None,
            edge_sync_context: None,
            usage_accounting,
            managed_control_plane,
        };
        let (client, server) = tokio::io::duplex(64 * 1024);
        let server_handle = tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(server);
            let mut shutdown_rx = shutdown_rx;
            let _ = handle_http_loop(
                &mut reader,
                &mut writer,
                &app_context,
                None,
                &mut shutdown_rx,
            )
            .await;
        });

        let (mut client_read, mut client_write) = tokio::io::split(client);
        client_write.write_all(raw_request).await.unwrap();
        drop(client_write);
        let mut response = Vec::new();
        client_read.read_to_end(&mut response).await.unwrap();

        let _ = server_handle.await;
        response
    }

    fn write_test_rbac_config(path: &Path, team_scope: &str) {
        let config = json!({
            "roles": {
                "tenant-reader": {
                    "grants": [
                        {
                            "action": "read",
                            "resource": { "kind": "tenant", "name": "*" }
                        }
                    ]
                },
                "cluster-audit-reader": {
                    "grants": [
                        {
                            "action": "read",
                            "resource": { "kind": "admin", "name": "cluster.audit" }
                        }
                    ]
                },
                "cluster-membership-operator": {
                    "grants": [
                        {
                            "action": "write",
                            "resource": { "kind": "admin", "name": "cluster.membership" }
                        }
                    ]
                },
                "rbac-admin": {
                    "grants": [
                        {
                            "action": "read",
                            "resource": { "kind": "admin", "name": "rbac" }
                        },
                        {
                            "action": "write",
                            "resource": { "kind": "admin", "name": "rbac" }
                        }
                    ]
                }
            },
            "principals": [
                {
                    "id": "team-reader",
                    "token": "team-reader-token",
                    "bindings": [
                        {
                            "role": "tenant-reader",
                            "scopes": [
                                { "kind": "tenant", "name": team_scope }
                            ]
                        }
                    ]
                },
                {
                    "id": "cluster-audit",
                    "token": "cluster-audit-token",
                    "bindings": [{ "role": "cluster-audit-reader" }]
                },
                {
                    "id": "cluster-membership",
                    "token": "cluster-membership-token",
                    "bindings": [{ "role": "cluster-membership-operator" }]
                },
                {
                    "id": "rbac-admin",
                    "token": "rbac-admin-token",
                    "bindings": [{ "role": "rbac-admin" }]
                }
            ]
        });
        std::fs::write(
            path,
            serde_json::to_vec(&config).expect("RBAC config should encode"),
        )
        .expect("RBAC config should write");
    }

    fn write_test_oidc_rbac_config(path: &Path, team_scope: &str, shared_secret_b64: &str) {
        let config = json!({
            "roles": {
                "tenant-reader": {
                    "grants": [
                        {
                            "action": "read",
                            "resource": { "kind": "tenant", "name": "*" }
                        }
                    ]
                },
                "rbac-admin": {
                    "grants": [
                        {
                            "action": "read",
                            "resource": { "kind": "admin", "name": "rbac" }
                        },
                        {
                            "action": "write",
                            "resource": { "kind": "admin", "name": "rbac" }
                        }
                    ]
                }
            },
            "principals": [
                {
                    "id": "rbac-admin",
                    "token": "rbac-admin-token",
                    "bindings": [{ "role": "rbac-admin" }]
                }
            ],
            "oidcProviders": [
                {
                    "name": "corp",
                    "issuer": "https://issuer.example",
                    "audiences": ["tsink"],
                    "usernameClaim": "email",
                    "jwks": [
                        {
                            "kid": "shared",
                            "alg": "HS256",
                            "kty": "oct",
                            "k": shared_secret_b64
                        }
                    ],
                    "claimMappings": [
                        {
                            "claim": "groups",
                            "value": "metrics-readers",
                            "bindings": [
                                {
                                    "role": "tenant-reader",
                                    "scopes": [
                                        { "kind": "tenant", "name": team_scope }
                                    ]
                                }
                            ]
                        },
                        {
                            "claim": "groups",
                            "value": "auth-admins",
                            "bindings": [{ "role": "rbac-admin" }]
                        }
                    ]
                }
            ]
        });
        std::fs::write(
            path,
            serde_json::to_vec(&config).expect("OIDC RBAC config should encode"),
        )
        .expect("OIDC RBAC config should write");
    }

    fn encode_hs256_jwt(header: &JsonValue, claims: &JsonValue, secret: &[u8]) -> String {
        let encoded_header =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(header).expect("header should encode"));
        let encoded_claims =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("claims should encode"));
        let message = format!("{encoded_header}.{encoded_claims}");
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, secret),
            message.as_bytes(),
        );
        format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature.as_ref()))
    }

    fn extract_status_code(response: &[u8]) -> u16 {
        let status_line_end = response
            .windows(2)
            .position(|window| window == b"\r\n")
            .unwrap_or(response.len());
        let status_line = String::from_utf8_lossy(&response[..status_line_end]);
        status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    fn reserve_udp_addr() -> String {
        let socket =
            std::net::UdpSocket::bind("127.0.0.1:0").expect("ephemeral UDP port should bind");
        let addr = socket
            .local_addr()
            .expect("UDP socket should have local addr");
        drop(socket);
        addr.to_string()
    }

    fn reserve_tcp_addr() -> String {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral TCP port should bind");
        let addr = listener
            .local_addr()
            .expect("TCP listener should have local addr");
        drop(listener);
        addr.to_string()
    }

    async fn wait_for_points(
        storage: &Arc<dyn Storage>,
        metric: &str,
        labels: &[Label],
    ) -> Vec<tsink::DataPoint> {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let points = storage
                    .select(metric, labels, 0, i64::MAX)
                    .expect("select should not fail");
                if !points.is_empty() {
                    break points;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("point should arrive before timeout")
    }

    #[test]
    fn load_access_control_bootstraps_tenant_and_rbac_registries() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let tenant_config_path = temp_dir.path().join("tenant-policy.json");
        let rbac_config_path = temp_dir.path().join("rbac-policy.json");
        std::fs::write(
            &tenant_config_path,
            r#"{
                "tenants": {
                    "team-a": {
                        "auth": {
                            "tokens": [{ "token": "team-a-read", "scopes": ["read"] }]
                        }
                    }
                }
            }"#,
        )
        .expect("tenant config should be written");
        write_test_rbac_config(&rbac_config_path, "team-a");

        let access_control = load_access_control(&ServerConfig {
            tenant_config_path: Some(tenant_config_path),
            rbac_config_path: Some(rbac_config_path.clone()),
            ..ServerConfig::default()
        })
        .expect("access control should load");

        let tenant_registry = access_control
            .tenant_registry
            .expect("tenant registry should be present");
        assert!(tenant_registry.status_snapshot_for("team-a").is_ok());

        let rbac_registry = access_control
            .rbac_registry
            .expect("rbac registry should be present");
        let rbac_state = rbac_registry.state_snapshot();
        assert_eq!(
            rbac_state.source_path.as_deref(),
            Some(rbac_config_path.to_string_lossy().as_ref())
        );
        assert!(!rbac_state.roles.is_empty());
    }

    #[test]
    fn build_storage_runtime_initializes_durable_support_stores() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let data_path = temp_dir.path().join("data");
        let config = ServerConfig {
            data_path: Some(data_path),
            ..ServerConfig::default()
        };

        let storage = build_storage_runtime(&config, None).expect("storage runtime should build");

        assert!(storage.usage_accounting.ledger_status().durable);
        assert!(storage.managed_control_plane.status_snapshot().durable);
        assert_eq!(
            storage
                .metadata_store
                .query(tenant::DEFAULT_TENANT_ID, None, 10)
                .expect("metadata query should succeed")
                .len(),
            0
        );

        storage
            .storage
            .close()
            .expect("storage runtime should close cleanly");
    }

    #[test]
    fn build_server_context_threads_bootstrap_dependencies_into_handler_views() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let tenant_config_path = temp_dir.path().join("tenant-policy.json");
        let rbac_config_path = temp_dir.path().join("rbac-policy.json");
        let data_path = temp_dir.path().join("data");
        std::fs::write(
            &tenant_config_path,
            r#"{
                "tenants": {
                    "team-a": {
                        "auth": {
                            "tokens": [{ "token": "team-a-read", "scopes": ["read"] }]
                        }
                    }
                }
            }"#,
        )
        .expect("tenant config should be written");
        write_test_rbac_config(&rbac_config_path, "team-a");

        let config = ServerConfig {
            auth_token: Some("public-token".to_string()),
            admin_auth_token: Some("admin-token".to_string()),
            admin_api_enabled: true,
            tenant_config_path: Some(tenant_config_path),
            rbac_config_path: Some(rbac_config_path),
            data_path: Some(data_path),
            ..ServerConfig::default()
        };
        let access_control =
            load_access_control(&config).expect("access-control bootstrap should succeed");
        let mut cluster = ClusterBootstrap {
            cluster_context: None,
            internal_api: Some(cluster::rpc::InternalApiConfig::new(
                "cluster-token".to_string(),
                "test-version".to_string(),
                false,
                Vec::new(),
            )),
            edge_sync_context: None,
            auto_join_runtime: None,
        };
        let security =
            build_security_runtime(&config, &mut cluster).expect("security bootstrap should build");
        let storage =
            build_storage_runtime(&config, None).expect("storage bootstrap should succeed");
        let admin_path_prefix = Some(temp_dir.path().to_path_buf());

        let context = build_server_context(
            &config,
            admin_path_prefix.clone(),
            &access_control,
            &security,
            &storage,
            &cluster,
        );

        assert_eq!(context.auth_token.as_deref(), Some("public-token"));
        assert_eq!(context.admin_auth_token.as_deref(), Some("admin-token"));
        assert!(context.admin_api_enabled);

        let app_context = context.handler_app_context();
        assert!(Arc::ptr_eq(app_context.storage, &storage.storage));
        assert!(Arc::ptr_eq(
            app_context.metadata_store,
            &storage.metadata_store
        ));
        assert!(Arc::ptr_eq(
            app_context.exemplar_store,
            &storage.exemplar_store
        ));
        assert!(app_context.rules_runtime.is_some());
        assert!(app_context.security_manager.is_some());
        assert!(app_context.tenant_registry.is_some());
        assert!(app_context.rbac_registry.is_some());
        assert!(app_context.usage_accounting.is_some());
        assert!(app_context.managed_control_plane.is_some());

        let request_context = context.handler_request_context(crate::http::HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/rules/status".to_string(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        });
        assert!(request_context.admin_api_enabled);
        assert_eq!(
            request_context.admin_path_prefix,
            admin_path_prefix.as_deref()
        );
        assert_eq!(
            request_context
                .internal_api
                .expect("internal api should be threaded through")
                .auth_token,
            "cluster-token"
        );

        storage
            .storage
            .close()
            .expect("storage runtime should close cleanly");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn statsd_listener_ingests_packets_into_storage() {
        let storage = make_storage();
        let scoped = tenant::scoped_storage(Arc::clone(&storage), tenant::DEFAULT_TENANT_ID);
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let socket = UdpSocket::bind(reserve_udp_addr())
            .await
            .expect("statsd listener socket should bind");
        let listen_addr = socket
            .local_addr()
            .expect("statsd listener should expose local addr");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let usage_accounting =
            crate::usage::UsageAccounting::open(None).expect("usage store should open");
        let rules_runtime = RulesRuntime::open(
            None,
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("rules runtime should open");
        let listener_context = ServerContext {
            storage: Arc::clone(&storage),
            metadata_store: Arc::clone(&metadata_store),
            exemplar_store: Arc::clone(&exemplar_store),
            rules_runtime,
            engine: Arc::new(Engine::with_precision(
                Arc::clone(&storage),
                TimestampPrecision::Milliseconds,
            )),
            server_start: Instant::now(),
            timestamp_precision: TimestampPrecision::Milliseconds,
            admin_api_enabled: false,
            admin_path_prefix: None,
            security_manager: SecurityManager::from_config(&ServerConfig::default())
                .expect("security runtime should build"),
            auth_token: None,
            admin_auth_token: None,
            internal_api: None,
            cluster_context: None,
            tenant_registry: None,
            rbac_registry: None,
            edge_sync_context: None,
            usage_accounting,
            managed_control_plane: Arc::new(
                ManagedControlPlane::open(None)
                    .expect("managed control plane should open in memory"),
            ),
        };
        let listener_task = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            run_statsd_listener(
                socket,
                &listener_context,
                tenant::DEFAULT_TENANT_ID,
                &mut shutdown_rx,
            )
            .await
            .expect("statsd listener should run");
        });

        let client = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("statsd client socket should bind");
        client
            .send_to(b"api.requests:2|c|@0.5|#env:prod", listen_addr)
            .await
            .expect("statsd packet should send");

        let points = wait_for_points(&scoped, "api_requests", &[Label::new("env", "prod")]).await;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value_as_f64(), Some(4.0));

        let metadata = metadata_store
            .query(tenant::DEFAULT_TENANT_ID, Some("api_requests"), 10)
            .expect("metadata should be queryable");
        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata[0].metric_type,
            crate::prom_remote::MetricType::Counter as i32
        );

        let _ = shutdown_tx.send(true);
        let _ = listener_task.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn graphite_listener_ingests_plaintext_lines_into_storage() {
        let storage = make_storage();
        let scoped = tenant::scoped_storage(Arc::clone(&storage), tenant::DEFAULT_TENANT_ID);
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let listener = TcpListener::bind(reserve_tcp_addr())
            .await
            .expect("graphite listener should bind");
        let listen_addr = listener
            .local_addr()
            .expect("graphite listener should expose local addr");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let usage_accounting =
            crate::usage::UsageAccounting::open(None).expect("usage store should open");
        let rules_runtime = RulesRuntime::open(
            None,
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("rules runtime should open");
        let listener_context = ServerContext {
            storage: Arc::clone(&storage),
            metadata_store: Arc::clone(&metadata_store),
            exemplar_store: Arc::clone(&exemplar_store),
            rules_runtime,
            engine: Arc::new(Engine::with_precision(
                Arc::clone(&storage),
                TimestampPrecision::Milliseconds,
            )),
            server_start: Instant::now(),
            timestamp_precision: TimestampPrecision::Milliseconds,
            admin_api_enabled: false,
            admin_path_prefix: None,
            security_manager: SecurityManager::from_config(&ServerConfig::default())
                .expect("security runtime should build"),
            auth_token: None,
            admin_auth_token: None,
            internal_api: None,
            cluster_context: None,
            tenant_registry: None,
            rbac_registry: None,
            edge_sync_context: None,
            usage_accounting,
            managed_control_plane: Arc::new(
                ManagedControlPlane::open(None)
                    .expect("managed control plane should open in memory"),
            ),
        };
        let listener_task = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            run_graphite_listener(
                listener,
                &listener_context,
                tenant::DEFAULT_TENANT_ID,
                &mut shutdown_rx,
            )
            .await
            .expect("graphite listener should run");
        });

        let mut client = tokio::net::TcpStream::connect(listen_addr)
            .await
            .expect("graphite client should connect");
        client
            .write_all(b"servers.api.latency;env=prod 42.5 1700000000\n")
            .await
            .expect("graphite line should send");
        drop(client);

        let points =
            wait_for_points(&scoped, "servers_api_latency", &[Label::new("env", "prod")]).await;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value_as_f64(), Some(42.5));

        let metadata = metadata_store
            .query(tenant::DEFAULT_TENANT_ID, Some("servers_api_latency"), 10)
            .expect("metadata should be queryable");
        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata[0].metric_type,
            crate::prom_remote::MetricType::Gauge as i32
        );

        let _ = shutdown_tx.send(true);
        let _ = listener_task.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_token_required_when_configured() {
        let response = send_request(
            b"GET /api/v1/labels HTTP/1.1\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 401);
        let text = String::from_utf8_lossy(&response);
        assert!(text.contains("WWW-Authenticate: Bearer"));
        let response = send_request(
            b"GET /api/v1/labels HTTP/1.1\r\nAuthorization: Bearer secret-token\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 200);
        let response = send_request(
            b"GET /api/v1/labels HTTP/1.1\r\nAuthorization: Bearer wrong-token\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 401);
        assert_eq!(
            response_header_value(&response, "X-Tsink-Auth-Error-Code"),
            Some("auth_token_invalid")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_skipped_for_health_probes() {
        let response = send_request(
            b"GET /healthz HTTP/1.1\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 200);

        let response = send_request(
            b"GET /ready HTTP/1.1\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 200);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tenant_registry_keeps_legacy_public_token_as_fallback_only() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let tenant_config_path = temp_dir.path().join("tenant-policy.json");
        std::fs::write(
            &tenant_config_path,
            r#"{
                "tenants": {
                    "team-b": {
                        "auth": {
                            "tokens": [{ "token": "team-b-read", "scopes": ["read"] }]
                        }
                    }
                }
            }"#,
        )
        .expect("tenant config should be written");

        let config = ServerConfig {
            auth_token: Some("public-token".to_string()),
            tenant_config_path: Some(tenant_config_path),
            ..ServerConfig::default()
        };

        let default_unauthenticated = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request("GET", "/api/v1/labels", &[], &[]),
        )
        .await;
        assert_eq!(extract_status_code(&default_unauthenticated), 401);
        assert_eq!(
            response_header_value(&default_unauthenticated, "X-Tsink-Auth-Error-Code"),
            Some("auth_token_missing")
        );

        let default_authenticated = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[("Authorization", "Bearer public-token")],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&default_authenticated), 200);

        let tenant_with_public_token = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", "Bearer public-token"),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&tenant_with_public_token), 401);
        assert_eq!(
            response_header_value(&tenant_with_public_token, "X-Tsink-Auth-Error-Code"),
            Some("tenant_auth_token_invalid")
        );

        let tenant_with_tenant_token = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", "Bearer team-b-read"),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&tenant_with_tenant_token), 200);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plaintext_connection_cannot_spoof_verified_public_auth_headers() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let tenant_config_path = temp_dir.path().join("tenant-policy.json");
        std::fs::write(
            &tenant_config_path,
            r#"{
                "tenants": {
                    "team-b": {
                        "auth": {
                            "tokens": [{ "token": "team-b-read", "scopes": ["read"] }]
                        }
                    }
                }
            }"#,
        )
        .expect("tenant config should be written");

        let config = ServerConfig {
            auth_token: Some("public-token".to_string()),
            tenant_config_path: Some(tenant_config_path),
            ..ServerConfig::default()
        };

        let response = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    (tenant::PUBLIC_AUTH_REQUIRED_HEADER, "true"),
                    (tenant::PUBLIC_AUTH_VERIFIED_HEADER, "true"),
                ],
                &[],
            ),
        )
        .await;

        assert_eq!(extract_status_code(&response), 401);
        assert_eq!(
            response_header_value(&response, "X-Tsink-Auth-Error-Code"),
            Some("auth_token_missing")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_scoped_token_rejects_public_token_for_admin_endpoints() {
        let config = ServerConfig {
            auth_token: Some("public-token".to_string()),
            admin_auth_token: Some("admin-token".to_string()),
            admin_api_enabled: true,
            ..ServerConfig::default()
        };
        let payload = serde_json::to_vec(&json!({
            "nodeId": "node-b",
            "endpoint": "127.0.0.1:9302"
        }))
        .expect("json should encode");

        let with_public_token = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "POST",
                "/api/v1/admin/cluster/join",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer public-token"),
                ],
                &payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&with_public_token), 403);
        assert_eq!(
            response_header_value(&with_public_token, "X-Tsink-Auth-Error-Code"),
            Some("auth_scope_denied")
        );

        let with_admin_token = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "POST",
                "/api/v1/admin/cluster/join",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer admin-token"),
                ],
                &payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&with_admin_token), 503);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rbac_scopes_apply_to_public_and_cluster_admin_endpoints() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let rbac_config_path = temp_dir.path().join("rbac.json");
        write_test_rbac_config(&rbac_config_path, "team-a");

        let config = ServerConfig {
            admin_api_enabled: true,
            rbac_config_path: Some(rbac_config_path),
            ..ServerConfig::default()
        };
        let join_payload = serde_json::to_vec(&json!({
            "nodeId": "node-b",
            "endpoint": "127.0.0.1:9302"
        }))
        .expect("json should encode");

        let tenant_read = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", "Bearer team-reader-token"),
                    (tenant::TENANT_HEADER, "team-a"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&tenant_read), 200);

        let wrong_tenant = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", "Bearer team-reader-token"),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&wrong_tenant), 403);
        assert_eq!(
            response_header_value(&wrong_tenant, "X-Tsink-Auth-Error-Code"),
            Some("auth_scope_denied")
        );

        let audit_reader_denied = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "POST",
                "/api/v1/admin/cluster/join",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer cluster-audit-token"),
                ],
                &join_payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&audit_reader_denied), 403);
        assert_eq!(
            response_header_value(&audit_reader_denied, "X-Tsink-Auth-Error-Code"),
            Some("auth_scope_denied")
        );

        let membership_operator = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "POST",
                "/api/v1/admin/cluster/join",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer cluster-membership-token"),
                ],
                &join_payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&membership_operator), 503);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rbac_admin_surfaces_expose_state_reload_and_audit() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let rbac_config_path = temp_dir.path().join("rbac.json");
        write_test_rbac_config(&rbac_config_path, "team-a");

        let config = ServerConfig {
            admin_api_enabled: true,
            rbac_config_path: Some(rbac_config_path.clone()),
            ..ServerConfig::default()
        };
        let storage = make_storage();
        let rbac_registry = Arc::new(
            RbacRegistry::load_from_path(&rbac_config_path).expect("RBAC registry should load"),
        );

        let initial_team_a = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", "Bearer team-reader-token"),
                    (tenant::TENANT_HEADER, "team-a"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&initial_team_a), 200);

        let state_response = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/admin/rbac/state",
                &[("Authorization", "Bearer rbac-admin-token")],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&state_response), 200);
        let state_body: JsonValue =
            serde_json::from_slice(response_body(&state_response)).expect("state should be json");
        assert_eq!(state_body["data"]["enabled"], true);
        assert_eq!(
            state_body["data"]["principals"].as_array().map(Vec::len),
            Some(4)
        );

        write_test_rbac_config(&rbac_config_path, "team-b");
        let reload_response = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "POST",
                "/api/v1/admin/rbac/reload",
                &[("Authorization", "Bearer rbac-admin-token")],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&reload_response), 200);

        let team_a_after_reload = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", "Bearer team-reader-token"),
                    (tenant::TENANT_HEADER, "team-a"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&team_a_after_reload), 403);
        assert_eq!(
            response_header_value(&team_a_after_reload, "X-Tsink-Auth-Error-Code"),
            Some("auth_scope_denied")
        );

        let team_b_after_reload = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", "Bearer team-reader-token"),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&team_b_after_reload), 200);

        let audit_response = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/admin/rbac/audit?limit=20",
                &[("Authorization", "Bearer rbac-admin-token")],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&audit_response), 200);
        let audit_body: JsonValue =
            serde_json::from_slice(response_body(&audit_response)).expect("audit should be json");
        let entries = audit_body["data"]["entries"]
            .as_array()
            .expect("audit entries should be an array");
        assert!(entries
            .iter()
            .any(|entry| entry["code"].as_str() == Some("rbac_reloaded")));
        assert!(entries
            .iter()
            .any(|entry| entry["code"].as_str() == Some("auth_scope_denied")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oidc_tokens_authorize_public_and_admin_paths_through_rbac() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let rbac_config_path = temp_dir.path().join("rbac.json");
        let shared_secret = b"oidc-shared-secret-for-server-tests";
        write_test_oidc_rbac_config(
            &rbac_config_path,
            "team-a",
            &URL_SAFE_NO_PAD.encode(shared_secret),
        );

        let config = ServerConfig {
            admin_api_enabled: true,
            rbac_config_path: Some(rbac_config_path),
            ..ServerConfig::default()
        };

        let reader_token = encode_hs256_jwt(
            &json!({ "alg": "HS256", "kid": "shared" }),
            &json!({
                "iss": "https://issuer.example",
                "sub": "reader-1",
                "aud": "tsink",
                "email": "reader@example.com",
                "groups": ["metrics-readers"],
                "exp": 4_102_444_800_u64
            }),
            shared_secret,
        );
        let reader_response = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", &format!("Bearer {reader_token}")),
                    (tenant::TENANT_HEADER, "team-a"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&reader_response), 200);

        let reader_admin_denied = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/admin/rbac/state",
                &[("Authorization", &format!("Bearer {reader_token}"))],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&reader_admin_denied), 403);
        assert_eq!(
            response_header_value(&reader_admin_denied, "X-Tsink-Auth-Error-Code"),
            Some("auth_scope_denied")
        );

        let admin_token = encode_hs256_jwt(
            &json!({ "alg": "HS256", "kid": "shared" }),
            &json!({
                "iss": "https://issuer.example",
                "sub": "admin-1",
                "aud": "tsink",
                "email": "admin@example.com",
                "groups": ["auth-admins"],
                "exp": 4_102_444_800_u64
            }),
            shared_secret,
        );
        let admin_response = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/admin/rbac/state",
                &[("Authorization", &format!("Bearer {admin_token}"))],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&admin_response), 200);
        let admin_body: JsonValue = serde_json::from_slice(response_body(&admin_response))
            .expect("admin response should be json");
        assert_eq!(
            admin_body["data"]["oidcProviders"].as_array().map(Vec::len),
            Some(1)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn service_account_admin_apis_manage_scope_rotation_and_disablement() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let rbac_config_path = temp_dir.path().join("rbac.json");
        write_test_rbac_config(&rbac_config_path, "team-a");

        let config = ServerConfig {
            admin_api_enabled: true,
            rbac_config_path: Some(rbac_config_path.clone()),
            ..ServerConfig::default()
        };
        let storage = make_storage();
        let rbac_registry = Arc::new(
            RbacRegistry::load_from_path(&rbac_config_path).expect("RBAC registry should load"),
        );

        let create_payload = serde_json::to_vec(&json!({
            "id": "ops-bot",
            "description": "Operations bot",
            "bindings": [
                {
                    "role": "tenant-reader",
                    "scopes": [
                        { "kind": "tenant", "name": "team-a" }
                    ]
                }
            ]
        }))
        .expect("json should encode");
        let create_response = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "POST",
                "/api/v1/admin/rbac/service_accounts/create",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer rbac-admin-token"),
                ],
                &create_payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&create_response), 200);
        let create_body: JsonValue = serde_json::from_slice(response_body(&create_response))
            .expect("create response should be json");
        let created_token = create_body["data"]["token"]
            .as_str()
            .expect("service account token should be returned")
            .to_string();

        let team_a_allowed = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", &format!("Bearer {created_token}")),
                    (tenant::TENANT_HEADER, "team-a"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&team_a_allowed), 200);

        let team_b_denied = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", &format!("Bearer {created_token}")),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&team_b_denied), 403);

        let update_payload = serde_json::to_vec(&json!({
            "id": "ops-bot",
            "description": "Operations bot",
            "bindings": [
                {
                    "role": "tenant-reader",
                    "scopes": [
                        { "kind": "tenant", "name": "team-b" }
                    ]
                }
            ]
        }))
        .expect("json should encode");
        let update_response = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "POST",
                "/api/v1/admin/rbac/service_accounts/update",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer rbac-admin-token"),
                ],
                &update_payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&update_response), 200);

        let team_a_after_update = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", &format!("Bearer {created_token}")),
                    (tenant::TENANT_HEADER, "team-a"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&team_a_after_update), 403);

        let team_b_after_update = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", &format!("Bearer {created_token}")),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&team_b_after_update), 200);

        let rotate_payload =
            serde_json::to_vec(&json!({ "id": "ops-bot" })).expect("json should encode");
        let rotate_response = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "POST",
                "/api/v1/admin/rbac/service_accounts/rotate",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer rbac-admin-token"),
                ],
                &rotate_payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&rotate_response), 200);
        let rotate_body: JsonValue = serde_json::from_slice(response_body(&rotate_response))
            .expect("rotate response should be json");
        let rotated_token = rotate_body["data"]["token"]
            .as_str()
            .expect("rotated token should be returned")
            .to_string();
        assert_ne!(created_token, rotated_token);

        let old_token_denied = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", &format!("Bearer {created_token}")),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&old_token_denied), 401);

        let new_token_allowed = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", &format!("Bearer {rotated_token}")),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&new_token_allowed), 200);

        let disable_payload =
            serde_json::to_vec(&json!({ "id": "ops-bot" })).expect("json should encode");
        let disable_response = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "POST",
                "/api/v1/admin/rbac/service_accounts/disable",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer rbac-admin-token"),
                ],
                &disable_payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&disable_response), 200);

        let disabled_token_denied = send_request_with_runtime(
            Arc::clone(&storage),
            &config,
            None,
            Some(Arc::clone(&rbac_registry)),
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[
                    ("Authorization", &format!("Bearer {rotated_token}")),
                    (tenant::TENANT_HEADER, "team-b"),
                ],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&disabled_token_denied), 403);
        assert_eq!(
            response_header_value(&disabled_token_denied, "X-Tsink-Auth-Error-Code"),
            Some("auth_principal_disabled")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_endpoints_do_not_require_public_bearer_token() {
        let compatibility = cluster::rpc::CompatibilityProfile::default();
        let capabilities = compatibility.capabilities.join(",");
        let internal_api = cluster::rpc::InternalApiConfig::new(
            "cluster-test-token".to_string(),
            cluster::rpc::INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            false,
            vec!["node-a".to_string()],
        );
        let request = build_http_request(
            "POST",
            "/internal/v1/list_metrics",
            &[
                (cluster::rpc::INTERNAL_RPC_AUTH_HEADER, "cluster-test-token"),
                (
                    cluster::rpc::INTERNAL_RPC_VERSION_HEADER,
                    cluster::rpc::INTERNAL_RPC_PROTOCOL_VERSION,
                ),
                (
                    cluster::rpc::INTERNAL_RPC_CAPABILITIES_HEADER,
                    &capabilities,
                ),
                ("Content-Type", "application/json"),
            ],
            br#"{"ring_version":1}"#,
        );

        let response = send_request_with_internal_api_and_auth(
            make_storage(),
            &request,
            internal_api,
            Some("public-token"),
            Some("admin-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 200);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_bearer_token_cannot_authorize_internal_endpoint() {
        let internal_api = cluster::rpc::InternalApiConfig::new(
            "cluster-test-token".to_string(),
            cluster::rpc::INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            false,
            vec!["node-a".to_string()],
        );
        let request = build_http_request(
            "POST",
            "/internal/v1/list_metrics",
            &[
                ("Authorization", "Bearer public-token"),
                ("Content-Type", "application/json"),
            ],
            br#"{"ring_version":1}"#,
        );

        let response = send_request_with_internal_api_and_auth(
            make_storage(),
            &request,
            internal_api,
            Some("public-token"),
            Some("admin-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 401);
        let body: JsonValue =
            serde_json::from_slice(response_body(&response)).expect("response should be json");
        assert_eq!(body["code"], "internal_auth_failed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cluster_disabled_compatibility_matrix_matches_default_mode() {
        let default_config = ServerConfig::default();
        let compatibility_config = ServerConfig {
            cluster: malformed_disabled_cluster_config(),
            ..ServerConfig::default()
        };

        let default_storage = make_storage();
        let compatibility_storage = make_storage();

        let write_payload = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    PromLabel {
                        name: "__name__".to_string(),
                        value: "compat_metric".to_string(),
                    },
                    PromLabel {
                        name: "job".to_string(),
                        value: "alpha".to_string(),
                    },
                ],
                samples: vec![PromSample {
                    value: 7.0,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };

        let write_request = build_http_request(
            "POST",
            "/api/v1/write",
            &[
                ("Content-Type", "application/x-protobuf"),
                ("Content-Encoding", "snappy"),
            ],
            &encode_snappy_message(&write_payload),
        );
        let health_request = build_http_request("GET", "/healthz", &[], &[]);
        let ready_request = build_http_request("GET", "/ready", &[], &[]);
        let labels_request = build_http_request("GET", "/api/v1/labels", &[], &[]);
        let label_values_request = build_http_request("GET", "/api/v1/label/job/values", &[], &[]);
        let series_request =
            build_http_request("GET", "/api/v1/series?match[]=compat_metric", &[], &[]);
        let query_request = build_http_request(
            "GET",
            "/api/v1/query?query=compat_metric&time=1700000000000",
            &[],
            &[],
        );
        let remote_read_payload = ReadRequest {
            queries: vec![PromReadQuery {
                start_timestamp_ms: 1_700_000_000_000,
                end_timestamp_ms: 1_700_000_000_000,
                matchers: vec![
                    LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "__name__".to_string(),
                        value: "compat_metric".to_string(),
                    },
                    LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "job".to_string(),
                        value: "alpha".to_string(),
                    },
                ],
                hints: None,
            }],
            accepted_response_types: Vec::new(),
        };
        let remote_read_request = build_http_request(
            "POST",
            "/api/v1/read",
            &[
                ("Content-Type", "application/x-protobuf"),
                ("Content-Encoding", "snappy"),
            ],
            &encode_snappy_message(&remote_read_payload),
        );
        let status_request = build_http_request("GET", "/api/v1/status/tsdb", &[], &[]);

        let expected_labels_json = json!({
            "status": "success",
            "data": ["__name__", "job"]
        });
        let expected_label_values_json = json!({
            "status": "success",
            "data": ["alpha"]
        });
        let expected_series_json = json!({
            "status": "success",
            "data": [{
                "__name__": "compat_metric",
                "job": "alpha"
            }]
        });
        let expected_query_json = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{
                    "metric": {
                        "__name__": "compat_metric",
                        "job": "alpha"
                    },
                    "value": [1_700_000_000.0, "7"]
                }]
            }
        });
        let expected_remote_read = ReadResponse {
            results: vec![PromReadQueryResult {
                timeseries: vec![TimeSeries {
                    labels: vec![
                        PromLabel {
                            name: "__name__".to_string(),
                            value: "compat_metric".to_string(),
                        },
                        PromLabel {
                            name: "job".to_string(),
                            value: "alpha".to_string(),
                        },
                    ],
                    samples: vec![PromSample {
                        value: 7.0,
                        timestamp: 1_700_000_000_000,
                    }],
                    ..Default::default()
                }],
            }],
        };
        let expected_status_golden = json!({
            "status": "success",
            "data": {
                "seriesCount": 1
            }
        });

        let default_health = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &health_request,
        )
        .await;
        let compat_health = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &health_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_health), 200);
        assert_eq!(extract_status_code(&compat_health), 200);
        assert_eq!(response_body(&default_health), b"ok\n");
        assert_eq!(response_body(&compat_health), b"ok\n");

        let default_ready = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &ready_request,
        )
        .await;
        let compat_ready = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &ready_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_ready), 200);
        assert_eq!(extract_status_code(&compat_ready), 200);
        assert_eq!(response_body(&default_ready), b"ready\n");
        assert_eq!(response_body(&compat_ready), b"ready\n");

        let default_write = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &write_request,
        )
        .await;
        let compat_write = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &write_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_write), 200);
        assert_eq!(extract_status_code(&compat_write), 200);
        assert_eq!(response_body(&default_write), b"");
        assert_eq!(response_body(&compat_write), b"");

        let default_labels = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &labels_request,
        )
        .await;
        let compat_labels = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &labels_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_labels), 200);
        assert_eq!(extract_status_code(&compat_labels), 200);
        let default_labels_json: JsonValue =
            serde_json::from_slice(response_body(&default_labels)).expect("labels should be json");
        let compat_labels_json: JsonValue =
            serde_json::from_slice(response_body(&compat_labels)).expect("labels should be json");
        assert_eq!(default_labels_json, compat_labels_json);
        assert_eq!(default_labels_json, expected_labels_json);

        let default_label_values = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &label_values_request,
        )
        .await;
        let compat_label_values = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &label_values_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_label_values), 200);
        assert_eq!(extract_status_code(&compat_label_values), 200);
        let default_label_values_json: JsonValue =
            serde_json::from_slice(response_body(&default_label_values))
                .expect("label values should be json");
        let compat_label_values_json: JsonValue =
            serde_json::from_slice(response_body(&compat_label_values))
                .expect("label values should be json");
        assert_eq!(default_label_values_json, compat_label_values_json);
        assert_eq!(default_label_values_json, expected_label_values_json);

        let default_series = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &series_request,
        )
        .await;
        let compat_series = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &series_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_series), 200);
        assert_eq!(extract_status_code(&compat_series), 200);
        let default_series_json: JsonValue =
            serde_json::from_slice(response_body(&default_series)).expect("series should be json");
        let compat_series_json: JsonValue =
            serde_json::from_slice(response_body(&compat_series)).expect("series should be json");
        assert_eq!(default_series_json, compat_series_json);
        assert_eq!(default_series_json, expected_series_json);

        let default_query = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &query_request,
        )
        .await;
        let compat_query = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &query_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_query), 200);
        assert_eq!(extract_status_code(&compat_query), 200);
        let default_query_json: JsonValue =
            serde_json::from_slice(response_body(&default_query)).expect("query should be json");
        let compat_query_json: JsonValue =
            serde_json::from_slice(response_body(&compat_query)).expect("query should be json");
        assert_eq!(default_query_json, compat_query_json);
        assert_eq!(default_query_json, expected_query_json);

        let default_remote_read = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &remote_read_request,
        )
        .await;
        let compat_remote_read = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &remote_read_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_remote_read), 200);
        assert_eq!(extract_status_code(&compat_remote_read), 200);
        let default_remote_read_text = String::from_utf8_lossy(&default_remote_read);
        let compat_remote_read_text = String::from_utf8_lossy(&compat_remote_read);
        assert!(default_remote_read_text.contains("Content-Encoding: snappy"));
        assert!(compat_remote_read_text.contains("Content-Encoding: snappy"));
        let default_remote_read_payload: ReadResponse =
            decode_snappy_message(response_body(&default_remote_read));
        let compat_remote_read_payload: ReadResponse =
            decode_snappy_message(response_body(&compat_remote_read));
        assert_eq!(default_remote_read_payload, compat_remote_read_payload);
        assert_eq!(default_remote_read_payload, expected_remote_read);

        let default_status = send_request_with_config(
            Arc::clone(&default_storage),
            &default_config,
            &status_request,
        )
        .await;
        let compat_status = send_request_with_config(
            Arc::clone(&compatibility_storage),
            &compatibility_config,
            &status_request,
        )
        .await;
        assert_eq!(extract_status_code(&default_status), 200);
        assert_eq!(extract_status_code(&compat_status), 200);
        let default_status_json: JsonValue =
            serde_json::from_slice(response_body(&default_status)).expect("status should be json");
        let compat_status_json: JsonValue =
            serde_json::from_slice(response_body(&compat_status)).expect("status should be json");
        let default_status_golden = json!({
            "status": default_status_json["status"].clone(),
            "data": {
                "seriesCount": default_status_json["data"]["seriesCount"].clone()
            }
        });
        let compat_status_golden = json!({
            "status": compat_status_json["status"].clone(),
            "data": {
                "seriesCount": compat_status_json["data"]["seriesCount"].clone()
            }
        });
        assert_eq!(default_status_golden, compat_status_golden);
        assert_eq!(default_status_golden, expected_status_golden);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_behavior_is_unchanged_with_cluster_disabled() {
        let config = ServerConfig {
            auth_token: Some("secret-token".to_string()),
            cluster: malformed_disabled_cluster_config(),
            ..ServerConfig::default()
        };

        let unauthenticated = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request("GET", "/api/v1/labels", &[], &[]),
        )
        .await;
        assert_eq!(extract_status_code(&unauthenticated), 401);

        let authenticated = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "GET",
                "/api/v1/labels",
                &[("Authorization", "Bearer secret-token")],
                &[],
            ),
        )
        .await;
        assert_eq!(extract_status_code(&authenticated), 200);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unauthorized_admin_cluster_membership_mutation_is_denied() {
        let config = ServerConfig {
            auth_token: Some("secret-token".to_string()),
            admin_api_enabled: true,
            ..ServerConfig::default()
        };
        let payload = serde_json::to_vec(&json!({
            "nodeId": "node-b",
            "endpoint": "127.0.0.1:9302"
        }))
        .expect("json should encode");

        let unauthenticated = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "POST",
                "/api/v1/admin/cluster/join",
                &[("Content-Type", "application/json")],
                &payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&unauthenticated), 401);

        let authenticated = send_request_with_config(
            make_storage(),
            &config,
            &build_http_request(
                "POST",
                "/api/v1/admin/cluster/join",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer secret-token"),
                ],
                &payload,
            ),
        )
        .await;
        assert_eq!(extract_status_code(&authenticated), 503);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_api_requires_auth_token() {
        let config = ServerConfig {
            listen: "127.0.0.1:0".to_string(),
            admin_api_enabled: true,
            ..ServerConfig::default()
        };
        let err = run_server(config)
            .await
            .expect_err("admin API without auth token should fail");
        assert!(err.contains(
            "--enable-admin-api requires --admin-auth-token, --auth-token, or --rbac-config"
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_api_allows_admin_auth_token_without_public_auth_token() {
        let config = ServerConfig {
            listen: "127.0.0.1:0".to_string(),
            admin_auth_token: Some("admin-token".to_string()),
            admin_api_enabled: true,
            ..ServerConfig::default()
        };
        validate_runtime_config(&config).expect("admin token should satisfy admin auth");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_api_allows_rbac_config_without_legacy_tokens() {
        let config = ServerConfig {
            listen: "127.0.0.1:0".to_string(),
            admin_api_enabled: true,
            rbac_config_path: Some(PathBuf::from("/tmp/rbac.json")),
            ..ServerConfig::default()
        };
        validate_runtime_config(&config).expect("rbac config should satisfy admin auth");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn removed_admin_console_route_no_longer_bypasses_auth() {
        let config = ServerConfig {
            auth_token: Some("public-token".to_string()),
            admin_auth_token: Some("admin-token".to_string()),
            admin_api_enabled: true,
            ..ServerConfig::default()
        };

        let response = send_request_with_config(
            make_storage(),
            &config,
            b"GET /admin HTTP/1.1\r\nConnection: close\r\n\r\n",
        )
        .await;

        let response_text = std::str::from_utf8(&response).expect("response should decode");
        assert!(response_text.starts_with("HTTP/1.1 401"));
    }

    #[test]
    fn rotated_public_token_accepts_previous_credential_during_overlap() {
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let public_token_path = temp_dir.path().join("public.token");
        std::fs::write(&public_token_path, "public-old\n").expect("token file should write");
        let config = ServerConfig {
            auth_token_file: Some(public_token_path),
            ..ServerConfig::default()
        };
        let security_manager =
            SecurityManager::from_config(&config).expect("security manager should build");
        security_manager
            .rotate(
                crate::security::SecretRotationTarget::PublicAuthToken,
                crate::security::SecretRotationMode::Rotate,
                Some("public-new".to_string()),
                Some(60),
                Some("test".to_string()),
            )
            .expect("public token should rotate");

        let mut old_request = crate::http::HttpRequest {
            method: "GET".to_string(),
            path: "/metrics".to_string(),
            headers: std::collections::HashMap::from([(
                "authorization".to_string(),
                "Bearer public-old".to_string(),
            )]),
            body: Vec::new(),
        };
        authorize_request_scope(
            &mut old_request,
            Some(security_manager.as_ref()),
            None,
            None,
            false,
            None,
            None,
        )
        .expect("previous public token should remain valid during overlap");

        let mut new_request = crate::http::HttpRequest {
            method: "GET".to_string(),
            path: "/metrics".to_string(),
            headers: std::collections::HashMap::from([(
                "authorization".to_string(),
                "Bearer public-new".to_string(),
            )]),
            body: Vec::new(),
        };
        authorize_request_scope(
            &mut new_request,
            Some(security_manager.as_ref()),
            None,
            None,
            false,
            None,
            None,
        )
        .expect("new public token should be active immediately");
    }

    #[test]
    fn cluster_validation_is_skipped_when_disabled() {
        let config = ServerConfig {
            cluster: ClusterConfig {
                enabled: false,
                node_id: None,
                bind: None,
                seeds: vec!["bad-endpoint".to_string()],
                shards: 0,
                replication_factor: 0,
                ..ClusterConfig::default()
            },
            ..ServerConfig::default()
        };

        validate_runtime_config(&config).expect("cluster-disabled mode should not fail validation");
    }

    #[test]
    fn tls_validation_still_applies_when_cluster_is_disabled() {
        let mut config = ServerConfig {
            cluster: malformed_disabled_cluster_config(),
            ..ServerConfig::default()
        };
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let missing_cert = temp_dir.path().join("missing-cert.pem");
        let missing_key = temp_dir.path().join("missing-key.pem");

        config.tls_cert = Some(missing_cert.clone());
        let err = match build_tls_acceptor(&config) {
            Ok(_) => panic!("tls key should be required"),
            Err(err) => err,
        };
        assert!(err.contains("--tls-cert requires --tls-key"));

        config.tls_key = Some(missing_key);
        let err = match build_tls_acceptor(&config) {
            Ok(_) => panic!("missing cert path should be rejected"),
            Err(err) => err,
        };
        assert!(err.contains("failed to open TLS cert file"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plaintext_connection_cannot_spoof_verified_internal_mtls_node_id_header() {
        let compatibility = cluster::rpc::CompatibilityProfile::default();
        let capabilities = compatibility.capabilities.join(",");
        let internal_api = cluster::rpc::InternalApiConfig::new(
            "cluster-test-token".to_string(),
            cluster::rpc::INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            true,
            vec!["node-a".to_string()],
        );
        let request = build_http_request(
            "POST",
            "/internal/v1/list_metrics",
            &[
                (cluster::rpc::INTERNAL_RPC_AUTH_HEADER, "cluster-test-token"),
                (
                    cluster::rpc::INTERNAL_RPC_VERSION_HEADER,
                    cluster::rpc::INTERNAL_RPC_PROTOCOL_VERSION,
                ),
                (
                    cluster::rpc::INTERNAL_RPC_CAPABILITIES_HEADER,
                    &capabilities,
                ),
                (cluster::rpc::INTERNAL_RPC_NODE_ID_HEADER, "node-a"),
                (cluster::rpc::INTERNAL_RPC_VERIFIED_NODE_ID_HEADER, "node-a"),
                ("Content-Type", "application/json"),
            ],
            br#"{"ring_version":1}"#,
        );

        let response = send_request_with_internal_api(make_storage(), &request, internal_api).await;
        assert_eq!(extract_status_code(&response), 401);

        let body: JsonValue = serde_json::from_slice(response_body(&response))
            .expect("internal auth error should be json");
        assert_eq!(body["code"], "internal_mtls_auth_failed");
    }
}
