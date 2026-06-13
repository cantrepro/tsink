#![recursion_limit = "512"]

mod admission;
mod cluster;
mod edge_sync;
mod exemplar_store;
mod handlers;
mod http;
mod legacy_ingest;
mod managed_control_plane;
mod metadata_store;
mod otlp;
mod prom_remote;
mod prom_write;
mod rbac;
mod rules;
mod security;
mod server;
mod tenant;
mod usage;

use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::{error::ErrorKind, Parser};
use tsink::{StorageRuntimeMode, TimestampPrecision, WalSyncMode};

use crate::cluster::config::{
    ClusterConfig, ClusterInternalMtlsConfig, ClusterNodeRole, ClusterReadConsistency,
    ClusterReadPartialResponsePolicy, ClusterWriteConsistency, DEFAULT_CLUSTER_REPLICATION_FACTOR,
    DEFAULT_CLUSTER_SHARDS,
};
use crate::server::{run_server, ServerConfig};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    match parse_server_args_clap(env::args().skip(1)) {
        Ok(config) => run_server(config).await,
        Err(err) => match err.kind() {
            ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
                print!("{err}");
                Ok(())
            }
            _ => Err(err.to_string()),
        },
    }
}

const CLI_ENDPOINTS_HELP: &str = r#"Endpoints:
  GET  /healthz
  GET  /ready
  GET  /metrics
  GET/POST /api/v1/query            PromQL instant query
  GET/POST /api/v1/query_range      PromQL range query
  GET  /api/v1/series               Series metadata
  GET  /api/v1/labels               Label names
  GET  /api/v1/label/<name>/values  Label values
  POST /api/v1/write                Prometheus remote write
  POST /write                       Influx line protocol ingest
  POST /api/v2/write                Influx line protocol ingest (Influx v2-style path)
  POST /v1/metrics                  OTLP HTTP/protobuf metrics ingest
  POST /api/v1/read                 Prometheus remote read
  POST /api/v1/import/prometheus    Text exposition format ingestion
  GET  /api/v1/status/tsdb          TSDB stats
  POST /api/v1/admin/snapshot       Create atomic snapshot
  POST /api/v1/admin/restore        Restore snapshot to data path
  POST /api/v1/admin/rollups/apply  Replace persisted rollup policies
  POST /api/v1/admin/rollups/run    Run one synchronous rollup materialization pass
  GET  /api/v1/admin/rollups/status Rollup policy freshness and coverage
  POST /api/v1/admin/cluster/join|leave|recommission   Cluster membership mutations
  POST /api/v1/admin/cluster/handoff/begin|progress|complete   Handoff mutations
  GET  /api/v1/admin/cluster/handoff/status   Handoff progress and ETA
  POST /api/v1/admin/cluster/repair/pause|resume|cancel|run    Repair controls
  GET  /api/v1/admin/cluster/repair/status    Repair progress and ETA
  POST /api/v1/admin/cluster/rebalance/pause|resume|run   Rebalance controls
  GET  /api/v1/admin/cluster/rebalance/status Rebalance progress and ETA
  GET  /api/v1/admin/cluster/audit    Query cluster admin mutation audit log
  GET  /api/v1/admin/cluster/audit/export   Export filtered audit log as ndjson
  GET  /api/v1/admin/rbac/state      Inspect live RBAC roles, service accounts, OIDC mappings, and source path
  GET  /api/v1/admin/rbac/audit      Inspect recent RBAC decision and reload audit entries
  GET  /api/v1/admin/support_bundle  Download a bounded JSON support bundle for one tenant
  POST /api/v1/admin/rbac/reload     Reload RBAC config from disk
  POST /api/v1/admin/rbac/service_accounts/create   Create a scoped service account and return its token
  POST /api/v1/admin/rbac/service_accounts/update   Update service-account bindings, description, or disabled state
  POST /api/v1/admin/rbac/service_accounts/rotate   Rotate a service-account token and return the new token
  POST /api/v1/admin/rbac/service_accounts/disable  Disable a service account without deleting bindings
  POST /api/v1/admin/rbac/service_accounts/enable   Re-enable a disabled service account
  POST /api/v1/admin/cluster/control/snapshot  Snapshot control-plane state/log
  POST /api/v1/admin/cluster/control/restore   Restore control-plane state/log snapshot
  POST /api/v1/admin/cluster/snapshot  Orchestrate cluster-wide data+control snapshot
  POST /api/v1/admin/cluster/restore   Prepare cluster-wide restore into per-node data paths
  POST /api/v1/admin/delete_series  Delete series by match[] selector (optional start/end)"#;

#[derive(Debug, Parser)]
#[command(
    name = "tsink-server",
    bin_name = "tsink-server server",
    version,
    about = env!("CARGO_PKG_DESCRIPTION"),
    long_about = None,
    after_help = CLI_ENDPOINTS_HELP
)]
struct ServerCliArgs {
    #[arg(
        long,
        value_name = "ADDR",
        default_value_t = String::from("127.0.0.1:9201"),
        help = "Bind address"
    )]
    listen: String,
    #[arg(
        long,
        value_name = "ADDR",
        help = "Bind a UDP StatsD listener (disabled by default)"
    )]
    statsd_listen: Option<String>,
    #[arg(
        long = "statsd-tenant",
        value_name = "ID",
        default_value_t = tenant::DEFAULT_TENANT_ID.to_string(),
        help = "Tenant id for StatsD listener writes"
    )]
    statsd_tenant_id: String,
    #[arg(
        long,
        value_name = "ADDR",
        help = "Bind a TCP Graphite plaintext listener (disabled by default)"
    )]
    graphite_listen: Option<String>,
    #[arg(
        long = "graphite-tenant",
        value_name = "ID",
        default_value_t = tenant::DEFAULT_TENANT_ID.to_string(),
        help = "Tenant id for Graphite listener writes"
    )]
    graphite_tenant_id: String,
    #[arg(long, value_name = "PATH", help = "Persist tsink data under PATH")]
    data_path: Option<PathBuf>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Shared object-store root for hot/warm/cold segments"
    )]
    object_store_path: Option<PathBuf>,
    #[arg(
        long = "wal-enabled",
        action = clap::ArgAction::Set,
        value_name = "BOOL",
        default_value = "true",
        value_parser = parse_bool,
        help = "Enable WAL"
    )]
    wal_enabled: bool,
    #[arg(
        long,
        value_name = "PRECISION",
        default_value = "ms",
        value_parser = parse_timestamp_precision,
        help = "Timestamp precision (s, ms, us, ns)"
    )]
    timestamp_precision: TimestampPrecision,
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Data retention period (for example 14d or 720h)"
    )]
    retention: Option<Duration>,
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Hot-tier cutoff before warm offload"
    )]
    hot_tier_retention: Option<Duration>,
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Warm-tier cutoff before cold offload"
    )]
    warm_tier_retention: Option<Duration>,
    #[arg(
        long,
        value_name = "MODE",
        default_value = "read-write",
        value_parser = parse_storage_mode,
        help = "Storage runtime mode: read-write or compute-only"
    )]
    storage_mode: StorageRuntimeMode,
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_duration,
        help = "Metadata refresh TTL for compute-only nodes"
    )]
    remote_segment_refresh_interval: Option<Duration>,
    #[arg(
        long,
        action = clap::ArgAction::Set,
        value_name = "BOOL",
        default_value = "false",
        value_parser = parse_bool,
        help = "Copy hot persisted segments into object-store hot/"
    )]
    mirror_hot_segments_to_object_store: bool,
    #[arg(
        long,
        value_name = "BYTES",
        value_parser = parse_byte_size,
        help = "Memory budget (for example 1G or 1073741824)"
    )]
    memory_limit: Option<usize>,
    #[arg(long, value_name = "N", help = "Max unique series")]
    cardinality_limit: Option<usize>,
    #[arg(long, value_name = "N", help = "Target points per chunk")]
    chunk_points: Option<usize>,
    #[arg(long, value_name = "N", help = "Concurrent writer threads")]
    max_writers: Option<usize>,
    #[arg(
        long,
        value_name = "MODE",
        value_parser = parse_wal_sync_mode,
        help = "WAL acknowledgement policy (per-append default, periodic opt-in)"
    )]
    wal_sync_mode: Option<WalSyncMode>,
    #[arg(long, value_name = "PATH", help = "TLS certificate file (PEM)")]
    tls_cert: Option<PathBuf>,
    #[arg(long, value_name = "PATH", help = "TLS private key file (PEM)")]
    tls_key: Option<PathBuf>,
    #[arg(
        long,
        value_name = "TOKEN",
        help = "Require Bearer token on public requests"
    )]
    auth_token: Option<String>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Load the public Bearer token from a file or exec manifest"
    )]
    auth_token_file: Option<PathBuf>,
    #[arg(
        long,
        value_name = "TOKEN",
        help = "Admin-only Bearer token for /api/v1/admin/*"
    )]
    admin_auth_token: Option<String>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Load the admin Bearer token from a file or exec manifest"
    )]
    admin_auth_token_file: Option<PathBuf>,
    #[arg(
        long = "tenant-config",
        value_name = "PATH",
        help = "Tenant authz/quota/policy config (JSON)"
    )]
    tenant_config_path: Option<PathBuf>,
    #[arg(
        long = "rbac-config",
        value_name = "PATH",
        help = "RBAC roles, service accounts, and OIDC mappings (JSON)"
    )]
    rbac_config_path: Option<PathBuf>,
    #[arg(
        long = "enable-admin-api",
        action = clap::ArgAction::SetTrue,
        help = "Enable admin snapshot/restore and cluster admin endpoints"
    )]
    admin_api_enabled: bool,
    #[arg(
        long,
        value_name = "PATH",
        help = "Restrict admin file paths under PATH"
    )]
    admin_path_prefix: Option<PathBuf>,
    #[arg(
        long = "edge-sync-upstream",
        value_name = "HOST:PORT",
        help = "Replay locally accepted writes into an upstream tsink-server"
    )]
    edge_sync_upstream_endpoint: Option<String>,
    #[arg(
        long,
        value_name = "TOKEN",
        help = "Shared token for edge sync replay and accept-side internal ingest"
    )]
    edge_sync_auth_token: Option<String>,
    #[arg(
        long,
        value_name = "ID",
        help = "Stable source identifier for edge sync idempotency keys"
    )]
    edge_sync_source_id: Option<String>,
    #[arg(
        long,
        value_name = "ID",
        help = "Rewrite replayed rows into one upstream tenant instead of preserving local tenant labels"
    )]
    edge_sync_static_tenant: Option<String>,
    #[arg(
        long = "cluster-enabled",
        action = clap::ArgAction::Set,
        value_name = "BOOL",
        default_value = "false",
        value_parser = parse_bool,
        help = "Enable cluster mode"
    )]
    cluster_enabled: bool,
    #[arg(
        long = "cluster-node-id",
        value_name = "ID",
        help = "Stable node identifier in cluster mode"
    )]
    cluster_node_id: Option<String>,
    #[arg(
        long = "cluster-bind",
        value_name = "HOST:PORT",
        help = "Internal RPC bind/advertise endpoint"
    )]
    cluster_bind: Option<String>,
    #[arg(
        long = "cluster-node-role",
        value_name = "ROLE",
        default_value_t = ClusterNodeRole::Hybrid,
        value_parser = parse_cluster_node_role,
        help = "Cluster node role: storage, query, or hybrid"
    )]
    cluster_node_role: ClusterNodeRole,
    #[arg(
        long = "cluster-seeds",
        value_name = "HOST:PORT,...",
        value_delimiter = ',',
        help = "Seed peers for static membership bootstrap"
    )]
    cluster_seeds: Vec<String>,
    #[arg(
        long = "cluster-internal-auth-token",
        value_name = "TOKEN",
        help = "Shared secret for internal RPC when mTLS is disabled"
    )]
    cluster_internal_auth_token: Option<String>,
    #[arg(
        long = "cluster-internal-auth-token-file",
        value_name = "PATH",
        help = "Load the shared internal RPC token from a file or exec manifest"
    )]
    cluster_internal_auth_token_file: Option<PathBuf>,
    #[arg(
        long = "cluster-shards",
        value_name = "N",
        default_value_t = DEFAULT_CLUSTER_SHARDS,
        help = "Logical shard count"
    )]
    cluster_shards: u32,
    #[arg(
        long = "cluster-replication-factor",
        value_name = "N",
        default_value_t = DEFAULT_CLUSTER_REPLICATION_FACTOR,
        help = "Replicas per shard"
    )]
    cluster_replication_factor: u16,
    #[arg(
        long = "cluster-write-consistency",
        value_name = "MODE",
        default_value_t = ClusterWriteConsistency::Quorum,
        value_parser = parse_cluster_write_consistency,
        help = "Write consistency: one, quorum, or all"
    )]
    cluster_write_consistency: ClusterWriteConsistency,
    #[arg(
        long = "cluster-read-consistency",
        value_name = "MODE",
        default_value_t = ClusterReadConsistency::Eventual,
        value_parser = parse_cluster_read_consistency,
        help = "Read consistency: eventual, quorum, or strict"
    )]
    cluster_read_consistency: ClusterReadConsistency,
    #[arg(
        long = "cluster-read-partial-response",
        value_name = "MODE",
        default_value_t = ClusterReadPartialResponsePolicy::Allow,
        value_parser = parse_cluster_read_partial_response_policy,
        help = "Partial read policy: allow or deny"
    )]
    cluster_read_partial_response: ClusterReadPartialResponsePolicy,
    #[arg(
        long = "cluster-internal-mtls-enabled",
        action = clap::ArgAction::Set,
        value_name = "BOOL",
        default_value = "false",
        value_parser = parse_bool,
        help = "Enable mTLS for /internal/v1/* RPC"
    )]
    cluster_internal_mtls_enabled: bool,
    #[arg(
        long = "cluster-internal-mtls-ca-cert",
        value_name = "PATH",
        help = "PEM CA bundle for internal peer cert verification"
    )]
    cluster_internal_mtls_ca_cert: Option<PathBuf>,
    #[arg(
        long = "cluster-internal-mtls-cert",
        value_name = "PATH",
        help = "PEM client cert for outbound internal RPC"
    )]
    cluster_internal_mtls_cert: Option<PathBuf>,
    #[arg(
        long = "cluster-internal-mtls-key",
        value_name = "PATH",
        help = "PEM client key for outbound internal RPC"
    )]
    cluster_internal_mtls_key: Option<PathBuf>,
}

impl ServerCliArgs {
    fn into_config(self) -> Result<ServerConfig, String> {
        let config = ServerConfig {
            listen: self.listen,
            statsd_listen: self.statsd_listen,
            statsd_tenant_id: self.statsd_tenant_id,
            graphite_listen: self.graphite_listen,
            graphite_tenant_id: self.graphite_tenant_id,
            data_path: self.data_path,
            object_store_path: self.object_store_path,
            wal_enabled: self.wal_enabled,
            timestamp_precision: self.timestamp_precision,
            retention: self.retention,
            hot_tier_retention: self.hot_tier_retention,
            warm_tier_retention: self.warm_tier_retention,
            storage_mode: self.storage_mode,
            remote_segment_refresh_interval: self.remote_segment_refresh_interval,
            mirror_hot_segments_to_object_store: self.mirror_hot_segments_to_object_store,
            memory_limit: self.memory_limit,
            cardinality_limit: self.cardinality_limit,
            chunk_points: self.chunk_points,
            max_writers: self.max_writers,
            wal_sync_mode: self.wal_sync_mode,
            tls_cert: self.tls_cert,
            tls_key: self.tls_key,
            auth_token: self.auth_token,
            auth_token_file: self.auth_token_file,
            admin_auth_token: self.admin_auth_token,
            admin_auth_token_file: self.admin_auth_token_file,
            tenant_config_path: self.tenant_config_path,
            rbac_config_path: self.rbac_config_path,
            admin_api_enabled: self.admin_api_enabled,
            admin_path_prefix: self.admin_path_prefix,
            edge_sync_upstream_endpoint: self.edge_sync_upstream_endpoint,
            edge_sync_auth_token: self.edge_sync_auth_token,
            edge_sync_source_id: self.edge_sync_source_id,
            edge_sync_static_tenant: self.edge_sync_static_tenant,
            cluster: ClusterConfig {
                enabled: self.cluster_enabled,
                node_id: self.cluster_node_id,
                bind: self.cluster_bind,
                seeds: self.cluster_seeds,
                node_role: self.cluster_node_role,
                internal_auth_token: self.cluster_internal_auth_token,
                internal_auth_token_file: self.cluster_internal_auth_token_file,
                shards: self.cluster_shards,
                replication_factor: self.cluster_replication_factor,
                write_consistency: self.cluster_write_consistency,
                read_consistency: self.cluster_read_consistency,
                read_partial_response: self.cluster_read_partial_response,
                internal_mtls: ClusterInternalMtlsConfig {
                    enabled: self.cluster_internal_mtls_enabled,
                    ca_cert: self.cluster_internal_mtls_ca_cert,
                    cert: self.cluster_internal_mtls_cert,
                    key: self.cluster_internal_mtls_key,
                },
            },
        };

        config.normalize_and_validate()
    }
}

fn parse_server_args_clap<I>(args: I) -> Result<ServerConfig, clap::Error>
where
    I: IntoIterator<Item = String>,
{
    let normalized_args = normalize_server_args(args)
        .map_err(|message| clap::Error::raw(ErrorKind::InvalidSubcommand, message))?;
    let parsed = ServerCliArgs::try_parse_from(
        std::iter::once("tsink-server".to_string()).chain(normalized_args),
    )?;
    parsed
        .into_config()
        .map_err(|message| clap::Error::raw(ErrorKind::ValueValidation, message))
}

#[cfg(test)]
fn parse_server_args<I>(args: I) -> Result<ServerConfig, String>
where
    I: IntoIterator<Item = String>,
{
    parse_server_args_clap(args).map_err(|err| err.to_string())
}

fn normalize_server_args<I>(args: I) -> Result<Vec<String>, String>
where
    I: IntoIterator<Item = String>,
{
    let mut normalized: Vec<String> = args.into_iter().collect();

    if let Some(first) = normalized.first_mut() {
        match first.as_str() {
            "help" => *first = "--help".to_string(),
            value if !value.starts_with('-') => {
                return Err(format!(
                    "unknown mode '{value}'. Run with --help for usage."
                ));
            }
            _ => {}
        }
    }

    Ok(normalized)
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid boolean value '{value}'")),
    }
}

fn parse_timestamp_precision(value: &str) -> Result<TimestampPrecision, String> {
    match value.to_ascii_lowercase().as_str() {
        "seconds" | "s" => Ok(TimestampPrecision::Seconds),
        "milliseconds" | "ms" => Ok(TimestampPrecision::Milliseconds),
        "microseconds" | "us" => Ok(TimestampPrecision::Microseconds),
        "nanoseconds" | "ns" => Ok(TimestampPrecision::Nanoseconds),
        _ => Err(format!(
            "invalid timestamp precision '{value}', expected one of: s, ms, us, ns"
        )),
    }
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("empty duration value".to_string());
    }
    if let Ok(secs) = value.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }

    let (num_str, unit) = if let Some(stripped) = value.strip_suffix("ms") {
        (stripped, "ms")
    } else if value.len() > 1 {
        (&value[..value.len() - 1], &value[value.len() - 1..])
    } else {
        return Err(format!("invalid duration: '{value}'"));
    };

    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration: '{value}'"))?;

    let secs = match unit {
        "ms" => num / 1_000.0,
        "s" => num,
        "m" => num * 60.0,
        "h" => num * 3_600.0,
        "d" => num * 86_400.0,
        "w" => num * 604_800.0,
        "y" => num * 365.25 * 86_400.0,
        _ => return Err(format!("invalid duration unit: '{unit}'")),
    };

    if secs < 0.0 {
        return Err(format!("duration must be non-negative: '{value}'"));
    }

    if !secs.is_finite() {
        return Err(format!("invalid duration: '{value}'"));
    }

    Duration::try_from_secs_f64(secs).map_err(|_| format!("invalid duration: '{value}'"))
}

fn parse_byte_size(value: &str) -> Result<usize, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("empty byte size value".to_string());
    }
    if let Ok(bytes) = value.parse::<usize>() {
        return Ok(bytes);
    }

    let last = value.as_bytes()[value.len() - 1];
    let (num_str, multiplier) = match last {
        b'K' | b'k' => (&value[..value.len() - 1], 1_024_usize),
        b'M' | b'm' => (&value[..value.len() - 1], 1_024 * 1_024),
        b'G' | b'g' => (&value[..value.len() - 1], 1_024 * 1_024 * 1_024),
        b'T' | b't' => (&value[..value.len() - 1], 1_024 * 1_024 * 1_024 * 1_024),
        _ => return Err(format!("invalid byte size: '{value}'")),
    };

    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid byte size: '{value}'"))?;

    if !num.is_finite() {
        return Err(format!("invalid byte size: '{value}'"));
    }
    if num < 0.0 {
        return Err(format!("byte size must be non-negative: '{value}'"));
    }

    let bytes = num * multiplier as f64;
    if !bytes.is_finite() || bytes > usize::MAX as f64 {
        return Err(format!("invalid byte size: '{value}'"));
    }

    Ok(bytes as usize)
}

fn parse_storage_mode(value: &str) -> Result<StorageRuntimeMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "read-write" | "read_write" | "readwrite" | "full" => Ok(StorageRuntimeMode::ReadWrite),
        "compute-only" | "compute_only" | "computeonly" | "query" => {
            Ok(StorageRuntimeMode::ComputeOnly)
        }
        _ => Err(format!(
            "invalid storage mode '{value}', expected one of: read-write, compute-only"
        )),
    }
}

fn parse_wal_sync_mode(value: &str) -> Result<WalSyncMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "per-append" | "per_append" | "perappend" => Ok(WalSyncMode::PerAppend),
        "periodic" => Ok(WalSyncMode::Periodic(Duration::from_secs(1))),
        _ => Err(format!(
            "invalid WAL sync mode '{value}', expected 'per-append' or 'periodic'"
        )),
    }
}

fn parse_cluster_node_role(value: &str) -> Result<ClusterNodeRole, String> {
    ClusterNodeRole::from_str(value).map_err(|err| format!("--cluster-node-role {err}"))
}

fn parse_cluster_write_consistency(value: &str) -> Result<ClusterWriteConsistency, String> {
    ClusterWriteConsistency::from_str(value)
        .map_err(|err| format!("--cluster-write-consistency {err}"))
}

fn parse_cluster_read_consistency(value: &str) -> Result<ClusterReadConsistency, String> {
    ClusterReadConsistency::from_str(value)
        .map_err(|err| format!("--cluster-read-consistency {err}"))
}

fn parse_cluster_read_partial_response_policy(
    value: &str,
) -> Result<ClusterReadPartialResponsePolicy, String> {
    ClusterReadPartialResponsePolicy::from_str(value)
        .map_err(|err| format!("--cluster-read-partial-response {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_rejects_non_finite_values() {
        assert!(parse_duration("NaNs").is_err());
        assert!(parse_duration("infs").is_err());
    }

    #[test]
    fn parse_byte_size_rejects_non_finite_values() {
        assert!(parse_byte_size("NaNK").is_err());
        assert!(parse_byte_size("infM").is_err());
    }

    #[test]
    fn parse_byte_size_rejects_negative_values() {
        assert!(parse_byte_size("-1K").is_err());
    }

    #[test]
    fn shared_config_path_trims_server_and_cluster_values() {
        let args = vec![
            "--listen".to_string(),
            " 127.0.0.1:9201 ".to_string(),
            "--auth-token".to_string(),
            " public-secret ".to_string(),
            "--enable-admin-api".to_string(),
            "--admin-auth-token".to_string(),
            " admin-secret ".to_string(),
            "--data-path".to_string(),
            " /tmp/tsink-data ".to_string(),
            "--tls-cert".to_string(),
            " /tmp/server.pem ".to_string(),
            "--tls-key".to_string(),
            " /tmp/server.key ".to_string(),
            "--cluster-enabled".to_string(),
            "true".to_string(),
            "--cluster-node-id".to_string(),
            " node-a ".to_string(),
            "--cluster-bind".to_string(),
            " 127.0.0.1:9301 ".to_string(),
            "--cluster-seeds".to_string(),
            " node-b@127.0.0.1:9302 , , node-c@127.0.0.1:9303 ".to_string(),
            "--cluster-internal-auth-token".to_string(),
            " cluster-secret ".to_string(),
        ];

        let config = parse_server_args(args.into_iter())
            .expect("shared config path should normalize and validate");
        assert_eq!(config.listen, "127.0.0.1:9201");
        assert_eq!(config.auth_token.as_deref(), Some("public-secret"));
        assert_eq!(config.admin_auth_token.as_deref(), Some("admin-secret"));
        assert_eq!(config.data_path, Some(PathBuf::from("/tmp/tsink-data")));
        assert_eq!(config.tls_cert, Some(PathBuf::from("/tmp/server.pem")));
        assert_eq!(config.tls_key, Some(PathBuf::from("/tmp/server.key")));
        assert_eq!(config.cluster.node_id.as_deref(), Some("node-a"));
        assert_eq!(config.cluster.bind.as_deref(), Some("127.0.0.1:9301"));
        assert_eq!(
            config.cluster.seeds,
            vec![
                "node-b@127.0.0.1:9302".to_string(),
                "node-c@127.0.0.1:9303".to_string(),
            ]
        );
        assert_eq!(
            config.cluster.internal_auth_token.as_deref(),
            Some("cluster-secret")
        );
    }

    #[test]
    fn tls_cert_requires_tls_key_during_parse() {
        let args = vec!["--tls-cert".to_string(), "/tmp/server.pem".to_string()];
        let err = parse_server_args(args.into_iter()).expect_err("tls key should be required");
        assert!(err.contains("--tls-cert requires --tls-key"));
    }

    #[test]
    fn admin_api_requires_auth_token() {
        let args = vec!["--enable-admin-api".to_string()];
        let err =
            parse_server_args(args.into_iter()).expect_err("admin API without auth must fail");
        assert!(err.contains(
            "--enable-admin-api requires --admin-auth-token, --auth-token, or --rbac-config"
        ));
    }

    #[test]
    fn admin_api_accepts_admin_auth_token_without_public_auth_token() {
        let args = vec![
            "--enable-admin-api".to_string(),
            "--admin-auth-token".to_string(),
            "admin-secret".to_string(),
        ];
        let config = parse_server_args(args.into_iter())
            .expect("admin token should satisfy admin API auth requirement");
        assert_eq!(config.admin_auth_token.as_deref(), Some("admin-secret"));
        assert!(config.auth_token.is_none());
    }

    #[test]
    fn admin_api_accepts_rbac_config_without_legacy_tokens() {
        let args = vec![
            "--enable-admin-api".to_string(),
            "--rbac-config".to_string(),
            "/tmp/rbac.json".to_string(),
        ];
        let config =
            parse_server_args(args.into_iter()).expect("rbac config should satisfy admin auth");
        assert_eq!(
            config.rbac_config_path.as_deref(),
            Some(std::path::Path::new("/tmp/rbac.json"))
        );
        assert!(config.auth_token.is_none());
        assert!(config.admin_auth_token.is_none());
    }

    #[test]
    fn tenant_config_path_parses() {
        let args = vec![
            "--tenant-config".to_string(),
            "/tmp/tenant-policy.json".to_string(),
        ];
        let config = parse_server_args(args.into_iter()).expect("tenant config path should parse");
        assert_eq!(
            config.tenant_config_path,
            Some(PathBuf::from("/tmp/tenant-policy.json"))
        );
    }

    #[test]
    fn admin_path_prefix_requires_admin_api() {
        let args = vec![
            "--admin-path-prefix".to_string(),
            "/tmp/admin".to_string(),
            "--auth-token".to_string(),
            "secret".to_string(),
        ];
        let err =
            parse_server_args(args.into_iter()).expect_err("admin path prefix without admin API");
        assert!(err.contains("--admin-path-prefix requires --enable-admin-api"));
    }

    #[test]
    fn edge_sync_flags_parse() {
        let args = vec![
            "--data-path".to_string(),
            "/tmp/tsink-edge".to_string(),
            "--edge-sync-upstream".to_string(),
            "127.0.0.1:9301".to_string(),
            "--edge-sync-auth-token".to_string(),
            "edge-secret".to_string(),
            "--edge-sync-source-id".to_string(),
            "edge-a".to_string(),
            "--edge-sync-static-tenant".to_string(),
            "tenant-central".to_string(),
        ];
        let config =
            parse_server_args(args.into_iter()).expect("edge sync args should parse successfully");
        assert_eq!(
            config.edge_sync_upstream_endpoint.as_deref(),
            Some("127.0.0.1:9301")
        );
        assert_eq!(config.edge_sync_auth_token.as_deref(), Some("edge-secret"));
        assert_eq!(config.edge_sync_source_id.as_deref(), Some("edge-a"));
        assert_eq!(
            config.edge_sync_static_tenant.as_deref(),
            Some("tenant-central")
        );
    }

    #[test]
    fn edge_sync_upstream_requires_auth_token() {
        let args = vec![
            "--data-path".to_string(),
            "/tmp/tsink-edge".to_string(),
            "--edge-sync-upstream".to_string(),
            "127.0.0.1:9301".to_string(),
        ];
        let err = parse_server_args(args.into_iter())
            .expect_err("edge sync upstream without auth token must fail");
        assert!(err.contains("--edge-sync-upstream requires --edge-sync-auth-token"));
    }

    #[test]
    fn cluster_enabled_requires_node_id() {
        let args = vec![
            "--cluster-enabled".to_string(),
            "true".to_string(),
            "--cluster-bind".to_string(),
            "127.0.0.1:9301".to_string(),
        ];
        let err = parse_server_args(args.into_iter()).expect_err("node id must be required");
        assert!(err.contains("--cluster-node-id"));
    }

    #[test]
    fn cluster_enabled_requires_bind_address() {
        let args = vec![
            "--cluster-enabled".to_string(),
            "true".to_string(),
            "--cluster-node-id".to_string(),
            "node-a".to_string(),
        ];
        let err = parse_server_args(args.into_iter()).expect_err("bind must be required");
        assert!(err.contains("--cluster-bind"));
    }

    #[test]
    fn cluster_args_are_accepted_when_cluster_is_disabled() {
        let args = vec![
            "--cluster-enabled".to_string(),
            "false".to_string(),
            "--cluster-node-id".to_string(),
            "node-a".to_string(),
            "--cluster-bind".to_string(),
            "invalid-endpoint".to_string(),
            "--cluster-shards".to_string(),
            "0".to_string(),
            "--cluster-replication-factor".to_string(),
            "0".to_string(),
            "--cluster-seeds".to_string(),
            "also-invalid".to_string(),
            "--cluster-internal-mtls-enabled".to_string(),
            "true".to_string(),
        ];
        let config = parse_server_args(args.into_iter())
            .expect("cluster-disabled mode should ignore cluster validation");
        assert!(!config.cluster.enabled);
        assert_eq!(config.cluster.node_id.as_deref(), Some("node-a"));
        assert!(config.cluster.internal_mtls.enabled);
    }

    #[test]
    fn cluster_consistency_modes_parse() {
        let args = vec![
            "--cluster-enabled".to_string(),
            "true".to_string(),
            "--cluster-node-id".to_string(),
            "node-a".to_string(),
            "--cluster-bind".to_string(),
            "127.0.0.1:9301".to_string(),
            "--cluster-write-consistency".to_string(),
            "all".to_string(),
            "--cluster-read-consistency".to_string(),
            "strict".to_string(),
            "--cluster-read-partial-response".to_string(),
            "deny".to_string(),
            "--cluster-internal-mtls-enabled".to_string(),
            "true".to_string(),
            "--cluster-internal-mtls-ca-cert".to_string(),
            "/tmp/cluster-ca.pem".to_string(),
            "--cluster-internal-mtls-cert".to_string(),
            "/tmp/node-a.pem".to_string(),
            "--cluster-internal-mtls-key".to_string(),
            "/tmp/node-a.key".to_string(),
        ];
        let config =
            parse_server_args(args.into_iter()).expect("consistency flags should parse cleanly");
        assert_eq!(
            config.cluster.write_consistency,
            ClusterWriteConsistency::All
        );
        assert_eq!(
            config.cluster.read_consistency,
            ClusterReadConsistency::Strict
        );
        assert_eq!(
            config.cluster.read_partial_response,
            ClusterReadPartialResponsePolicy::Deny
        );
        assert!(config.cluster.internal_mtls.enabled);
        assert_eq!(
            config.cluster.internal_mtls.ca_cert.as_deref(),
            Some(std::path::Path::new("/tmp/cluster-ca.pem"))
        );
    }

    #[test]
    fn cluster_internal_auth_token_parses() {
        let args = vec![
            "--cluster-enabled".to_string(),
            "true".to_string(),
            "--cluster-node-id".to_string(),
            "node-a".to_string(),
            "--cluster-bind".to_string(),
            "127.0.0.1:9301".to_string(),
            "--cluster-internal-auth-token".to_string(),
            "cluster-secret".to_string(),
        ];
        let config =
            parse_server_args(args.into_iter()).expect("cluster internal auth token should parse");
        assert_eq!(
            config.cluster.internal_auth_token.as_deref(),
            Some("cluster-secret")
        );
    }

    #[test]
    fn cluster_internal_auth_token_rejects_empty_value() {
        let args = vec![
            "--cluster-internal-auth-token".to_string(),
            "   ".to_string(),
        ];
        let err = parse_server_args(args.into_iter()).expect_err("empty token should fail");
        assert!(err.contains("--cluster-internal-auth-token must not be empty"));
    }

    #[test]
    fn compute_only_storage_mode_requires_object_store_path() {
        let args = vec!["--storage-mode".to_string(), "compute-only".to_string()];
        let err = parse_server_args(args.into_iter())
            .expect_err("compute-only mode without object store");
        assert!(err.contains("--storage-mode=compute-only requires --object-store-path"));
    }

    #[test]
    fn cluster_query_role_requires_compute_only_storage_mode() {
        let args = vec![
            "--cluster-enabled".to_string(),
            "true".to_string(),
            "--cluster-node-id".to_string(),
            "query-a".to_string(),
            "--cluster-bind".to_string(),
            "127.0.0.1:9301".to_string(),
            "--cluster-node-role".to_string(),
            "query".to_string(),
            "--cluster-seeds".to_string(),
            "node-b@127.0.0.1:9302".to_string(),
            "--cluster-internal-auth-token".to_string(),
            "cluster-secret".to_string(),
            "--object-store-path".to_string(),
            "/tmp/object-store".to_string(),
        ];
        let err = parse_server_args(args.into_iter())
            .expect_err("query role should require compute-only mode");
        assert!(err.contains("--cluster-node-role=query requires --storage-mode=compute-only"));
    }

    #[test]
    fn cluster_query_role_and_compute_only_storage_mode_parse_together() {
        let args = vec![
            "--storage-mode".to_string(),
            "compute-only".to_string(),
            "--object-store-path".to_string(),
            "/tmp/object-store".to_string(),
            "--cluster-enabled".to_string(),
            "true".to_string(),
            "--cluster-node-id".to_string(),
            "query-a".to_string(),
            "--cluster-bind".to_string(),
            "127.0.0.1:9301".to_string(),
            "--cluster-node-role".to_string(),
            "query".to_string(),
            "--cluster-seeds".to_string(),
            "node-b@127.0.0.1:9302".to_string(),
            "--cluster-internal-auth-token".to_string(),
            "cluster-secret".to_string(),
        ];
        let config =
            parse_server_args(args.into_iter()).expect("query-only compute node should parse");
        assert_eq!(config.storage_mode, StorageRuntimeMode::ComputeOnly);
        assert_eq!(config.cluster.node_role, ClusterNodeRole::Query);
        assert_eq!(
            config.object_store_path.as_deref(),
            Some(std::path::Path::new("/tmp/object-store"))
        );
    }

    #[test]
    fn parses_legacy_listener_flags() {
        let args = vec![
            "--statsd-listen".to_string(),
            "127.0.0.1:8125".to_string(),
            "--statsd-tenant".to_string(),
            "statsd-team".to_string(),
            "--graphite-listen".to_string(),
            "127.0.0.1:2003".to_string(),
            "--graphite-tenant".to_string(),
            "graphite-team".to_string(),
        ];
        let config =
            parse_server_args(args.into_iter()).expect("legacy listener flags should parse");
        assert_eq!(config.statsd_listen.as_deref(), Some("127.0.0.1:8125"));
        assert_eq!(config.statsd_tenant_id, "statsd-team");
        assert_eq!(config.graphite_listen.as_deref(), Some("127.0.0.1:2003"));
        assert_eq!(config.graphite_tenant_id, "graphite-team");
    }

    #[test]
    fn rejects_empty_legacy_listener_tenant_ids() {
        let args = vec!["--statsd-tenant".to_string(), "   ".to_string()];
        let err = parse_server_args(args.into_iter()).expect_err("empty statsd tenant should fail");
        assert!(err.contains("--statsd-tenant must not be empty"));
    }
}
