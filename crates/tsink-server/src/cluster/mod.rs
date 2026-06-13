pub mod audit;
pub mod config;
pub mod consensus;
pub mod control;
pub mod dedupe;
pub mod distributed_storage;
pub mod hotspot;
pub mod membership;
pub mod outbox;
pub mod planner;
pub mod query;
pub mod query_merge;
pub mod repair;
pub mod replication;
pub mod ring;
pub mod rpc;

use audit::ClusterAuditLog;
use config::{ClusterConfig, ClusterNodeRole};
use consensus::ControlConsensusRuntime;
use control::ControlStateStore;
use dedupe::DedupeWindowStore;
use membership::MembershipView;
use outbox::HintedHandoffOutbox;
use query::{ReadFanoutExecutor, ReadResourceGuardrails};
use query_merge::ReadMergeLimits;
use repair::DigestExchangeRuntime;
use replication::{WriteRouter, WriteRouterTuning};
use ring::ShardRing;
use rpc::{InternalApiConfig, RpcClient, RpcClientConfig};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_FANOUT_CONCURRENCY: usize = 16;
const FANOUT_CONCURRENCY_ENV: &str = "TSINK_CLUSTER_FANOUT_CONCURRENCY";
const RPC_TIMEOUT_MS_ENV: &str = "TSINK_CLUSTER_RPC_TIMEOUT_MS";
const RPC_MAX_RETRIES_ENV: &str = "TSINK_CLUSTER_RPC_MAX_RETRIES";
const READ_MERGE_MAX_SERIES_ENV: &str = "TSINK_CLUSTER_READ_MAX_MERGED_SERIES";
const READ_MERGE_MAX_POINTS_PER_SERIES_ENV: &str =
    "TSINK_CLUSTER_READ_MAX_MERGED_POINTS_PER_SERIES";
const READ_MERGE_MAX_TOTAL_POINTS_ENV: &str = "TSINK_CLUSTER_READ_MAX_MERGED_POINTS_TOTAL";
const READ_MAX_INFLIGHT_QUERIES_ENV: &str = "TSINK_CLUSTER_READ_MAX_INFLIGHT_QUERIES";
const READ_MAX_INFLIGHT_MERGED_POINTS_ENV: &str = "TSINK_CLUSTER_READ_MAX_INFLIGHT_MERGED_POINTS";
const READ_RESOURCE_ACQUIRE_TIMEOUT_MS_ENV: &str = "TSINK_CLUSTER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS";

#[derive(Debug, Clone)]
pub struct ClusterRuntime {
    pub membership: MembershipView,
    pub ring: ShardRing,
    pub internal_api: InternalApiConfig,
    pub internal_mtls: Option<config::ClusterInternalMtlsResolved>,
    pub local_node_role: ClusterNodeRole,
    pub local_reads_serve_global_queries: bool,
    pub fanout_concurrency: usize,
    pub write_consistency: config::ClusterWriteConsistency,
    pub read_consistency: config::ClusterReadConsistency,
    pub read_partial_response: config::ClusterReadPartialResponsePolicy,
    pub read_merge_limits: ReadMergeLimits,
    pub read_resource_guardrails: ReadResourceGuardrails,
}

#[derive(Debug, Clone)]
pub struct ClusterRequestContext {
    pub runtime: ClusterRuntime,
    pub rpc_client: RpcClient,
    pub write_router: WriteRouter,
    pub read_fanout: ReadFanoutExecutor,
    pub control_state_store: Option<Arc<ControlStateStore>>,
    pub control_consensus: Option<Arc<ControlConsensusRuntime>>,
    pub audit_log: Option<Arc<ClusterAuditLog>>,
    pub dedupe_store: Option<Arc<DedupeWindowStore>>,
    pub outbox: Option<Arc<HintedHandoffOutbox>>,
    pub digest_runtime: Option<Arc<DigestExchangeRuntime>>,
}

impl ClusterRuntime {
    pub fn bootstrap(config: &ClusterConfig) -> Result<Option<Self>, String> {
        if !config.enabled {
            return Ok(None);
        }

        config.validate()?;
        let membership = MembershipView::from_config(config)?;
        let ring_membership = membership_for_ownership(&membership, config.node_role);
        let ring = ShardRing::build(config.shards, config.replication_factor, &ring_membership)?;
        let internal_mtls = config.resolved_internal_mtls()?;
        let internal_auth_token = config.load_cluster_internal_auth_token()?;
        let internal_api = InternalApiConfig::from_membership(
            &membership,
            internal_mtls.is_some(),
            internal_auth_token.as_deref(),
        )?;
        let fanout_concurrency = read_fanout_concurrency_from_env()?;
        let read_merge_limits = read_merge_limits_from_env()?;
        let read_resource_guardrails = read_resource_guardrails_from_env()?;

        Ok(Some(Self {
            membership,
            ring,
            internal_api,
            internal_mtls,
            local_node_role: config.node_role,
            local_reads_serve_global_queries: false,
            fanout_concurrency,
            write_consistency: config.write_consistency,
            read_consistency: config.read_consistency,
            read_partial_response: config.read_partial_response,
            read_merge_limits,
            read_resource_guardrails,
        }))
    }
}

fn membership_for_ownership(membership: &MembershipView, role: ClusterNodeRole) -> MembershipView {
    let mut nodes = membership.nodes.clone();
    if !role.owns_shards() {
        nodes.retain(|node| node.id != membership.local_node_id);
    }
    MembershipView {
        local_node_id: membership.local_node_id.clone(),
        nodes,
    }
}

impl ClusterRequestContext {
    pub fn from_runtime(runtime: ClusterRuntime) -> Result<Self, String> {
        let rpc_timeout = read_rpc_timeout_from_env()?;
        let rpc_max_retries = read_rpc_max_retries_from_env()?;
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: rpc_timeout,
            max_retries: rpc_max_retries,
            protocol_version: runtime.internal_api.protocol_version.clone(),
            internal_auth_token: runtime.internal_api.auth_token.clone(),
            internal_auth_runtime: None,
            local_node_id: runtime.membership.local_node_id.clone(),
            compatibility: runtime.internal_api.compatibility.clone(),
            internal_mtls: runtime.internal_mtls.as_ref().map(|mtls| {
                rpc::RpcClientInternalMtlsConfig {
                    ca_cert: mtls.ca_cert.clone(),
                    cert: mtls.cert.clone(),
                    key: mtls.key.clone(),
                }
            }),
        });
        let write_router_tuning = WriteRouterTuning::from_env()?;
        let write_router = WriteRouter::new(
            runtime.membership.local_node_id.clone(),
            runtime.ring.clone(),
            &runtime.membership,
            runtime.write_consistency,
        )?
        .with_tuning(write_router_tuning);
        let read_fanout = ReadFanoutExecutor::new(
            runtime.membership.local_node_id.clone(),
            runtime.ring.clone(),
            &runtime.membership,
            runtime.fanout_concurrency,
            runtime.read_consistency,
            runtime.read_partial_response,
            runtime.read_merge_limits,
            runtime.read_resource_guardrails,
        )?;

        Ok(Self {
            runtime,
            rpc_client,
            write_router,
            read_fanout,
            control_state_store: None,
            control_consensus: None,
            audit_log: None,
            dedupe_store: None,
            outbox: None,
            digest_runtime: None,
        })
    }
}

fn read_fanout_concurrency_from_env() -> Result<usize, String> {
    parse_env_usize(FANOUT_CONCURRENCY_ENV, DEFAULT_FANOUT_CONCURRENCY, true)
}

fn read_rpc_timeout_from_env() -> Result<Duration, String> {
    let timeout_ms = parse_env_u64(RPC_TIMEOUT_MS_ENV, rpc::DEFAULT_RPC_TIMEOUT_MS, true)?;
    Ok(Duration::from_millis(timeout_ms))
}

fn read_rpc_max_retries_from_env() -> Result<usize, String> {
    parse_env_usize(RPC_MAX_RETRIES_ENV, rpc::DEFAULT_RPC_MAX_RETRIES, false)
}

fn read_merge_limits_from_env() -> Result<ReadMergeLimits, String> {
    let limits = ReadMergeLimits {
        max_series: parse_env_usize(
            READ_MERGE_MAX_SERIES_ENV,
            ReadMergeLimits::DEFAULT_MAX_SERIES,
            true,
        )?,
        max_points_per_series: parse_env_usize(
            READ_MERGE_MAX_POINTS_PER_SERIES_ENV,
            ReadMergeLimits::DEFAULT_MAX_POINTS_PER_SERIES,
            true,
        )?,
        max_total_points: parse_env_usize(
            READ_MERGE_MAX_TOTAL_POINTS_ENV,
            ReadMergeLimits::DEFAULT_MAX_TOTAL_POINTS,
            true,
        )?,
    };
    limits.validate()?;
    Ok(limits)
}

fn read_resource_guardrails_from_env() -> Result<ReadResourceGuardrails, String> {
    let guardrails = ReadResourceGuardrails {
        max_inflight_queries: parse_env_usize(
            READ_MAX_INFLIGHT_QUERIES_ENV,
            query::DEFAULT_READ_MAX_INFLIGHT_QUERIES,
            true,
        )?,
        max_inflight_merged_points: parse_env_usize(
            READ_MAX_INFLIGHT_MERGED_POINTS_ENV,
            query::DEFAULT_READ_MAX_INFLIGHT_MERGED_POINTS,
            true,
        )?,
        acquire_timeout: Duration::from_millis(parse_env_u64(
            READ_RESOURCE_ACQUIRE_TIMEOUT_MS_ENV,
            query::DEFAULT_READ_RESOURCE_ACQUIRE_TIMEOUT_MS,
            false,
        )?),
    };
    guardrails.validate()?;
    Ok(guardrails)
}

fn parse_env_u64(var: &str, default: u64, enforce_positive: bool) -> Result<u64, String> {
    let value = match std::env::var(var) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{var} must be valid UTF-8 when set"));
        }
    };
    let parsed = value
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{var} must be an integer, got '{value}'"))?;
    if enforce_positive && parsed == 0 {
        return Err(format!("{var} must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_env_usize(var: &str, default: usize, enforce_positive: bool) -> Result<usize, String> {
    let value = match std::env::var(var) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{var} must be valid UTF-8 when set"));
        }
    };
    let parsed = value
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("{var} must be an integer, got '{value}'"))?;
    if enforce_positive && parsed == 0 {
        return Err(format!("{var} must be greater than zero"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_returns_none_when_disabled() {
        let cfg = ClusterConfig::default();
        let runtime = ClusterRuntime::bootstrap(&cfg).expect("bootstrap should not fail");
        assert!(runtime.is_none());
    }

    #[test]
    fn bootstrap_builds_runtime_when_enabled() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["127.0.0.1:9302".to_string()],
            internal_auth_token: Some("cluster-test-token".to_string()),
            ..ClusterConfig::default()
        };

        let runtime = ClusterRuntime::bootstrap(&cfg)
            .expect("bootstrap should succeed")
            .expect("runtime should exist");
        assert_eq!(runtime.ring.shard_count(), cfg.shards);
        assert_eq!(runtime.membership.nodes.len(), 2);
        assert_eq!(runtime.internal_api.protocol_version, "1");
        assert!(!runtime.internal_api.auth_token.is_empty());
        assert!(!runtime.internal_api.require_mtls);
        assert!(runtime.internal_mtls.is_none());
        assert!(runtime.fanout_concurrency > 0);
        assert!(runtime.read_resource_guardrails.max_inflight_queries > 0);
        assert!(runtime.read_resource_guardrails.max_inflight_merged_points > 0);
    }

    #[test]
    fn bootstrap_requires_explicit_internal_auth_token_without_mtls() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["127.0.0.1:9302".to_string()],
            ..ClusterConfig::default()
        };
        let err = ClusterRuntime::bootstrap(&cfg)
            .expect_err("bootstrap should fail without a plaintext internal auth token");
        assert!(err.contains("--cluster-internal-auth-token"));
    }

    #[test]
    fn cluster_request_context_builds_routing_components() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            internal_auth_token: Some("cluster-test-token".to_string()),
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&cfg)
            .expect("bootstrap should succeed")
            .expect("runtime should exist");

        let context =
            ClusterRequestContext::from_runtime(runtime).expect("request context should build");
        assert_eq!(context.write_router.local_node_id(), "node-a");
        assert_eq!(context.read_fanout.local_node_id(), "node-a");
    }

    #[test]
    fn query_only_node_role_excludes_local_node_from_shard_ownership() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("query-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            node_role: ClusterNodeRole::Query,
            internal_auth_token: Some("cluster-test-token".to_string()),
            ..ClusterConfig::default()
        };

        let runtime = ClusterRuntime::bootstrap(&cfg)
            .expect("bootstrap should succeed")
            .expect("runtime should exist");

        assert_eq!(runtime.local_node_role, ClusterNodeRole::Query);
        assert!(runtime
            .membership
            .nodes
            .iter()
            .any(|node| node.id == "query-a"));
        for shard in 0..runtime.ring.shard_count() {
            assert!(
                !runtime
                    .ring
                    .owners_for_shard(shard)
                    .unwrap()
                    .iter()
                    .any(|owner| owner == "query-a"),
                "query-only nodes must not appear in shard ownership"
            );
        }
    }
}
