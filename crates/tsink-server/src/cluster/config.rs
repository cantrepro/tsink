use serde::Deserialize;
use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

pub const DEFAULT_CLUSTER_SHARDS: u32 = 128;
pub const DEFAULT_CLUSTER_REPLICATION_FACTOR: u16 = 1;

const UNKNOWN_NODE_ID: &str = "unknown";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterWriteConsistency {
    One,
    #[default]
    Quorum,
    All,
}

impl ClusterWriteConsistency {
    pub fn required_acks(self, replication_factor: u16) -> u16 {
        let rf = replication_factor.max(1);
        match self {
            Self::One => 1,
            Self::Quorum => (rf / 2) + 1,
            Self::All => rf,
        }
    }
}

impl fmt::Display for ClusterWriteConsistency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::One => "one",
            Self::Quorum => "quorum",
            Self::All => "all",
        };
        f.write_str(value)
    }
}

impl FromStr for ClusterWriteConsistency {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "one" => Ok(Self::One),
            "quorum" => Ok(Self::Quorum),
            "all" => Ok(Self::All),
            _ => Err(format!(
                "invalid write consistency '{value}', expected one of: one, quorum, all"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterReadConsistency {
    #[default]
    Eventual,
    Quorum,
    Strict,
}

impl fmt::Display for ClusterReadConsistency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Eventual => "eventual",
            Self::Quorum => "quorum",
            Self::Strict => "strict",
        };
        f.write_str(value)
    }
}

impl FromStr for ClusterReadConsistency {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "eventual" => Ok(Self::Eventual),
            "quorum" => Ok(Self::Quorum),
            "strict" => Ok(Self::Strict),
            _ => Err(format!(
                "invalid read consistency '{value}', expected one of: eventual, quorum, strict"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterReadPartialResponsePolicy {
    #[default]
    Allow,
    Deny,
}

impl fmt::Display for ClusterReadPartialResponsePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        };
        f.write_str(value)
    }
}

impl FromStr for ClusterReadPartialResponsePolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "allow" => Ok(Self::Allow),
            "deny" => Ok(Self::Deny),
            _ => Err(format!(
                "invalid read partial response policy '{value}', expected one of: allow, deny"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterNodeRole {
    Storage,
    Query,
    #[default]
    Hybrid,
}

impl ClusterNodeRole {
    pub fn owns_shards(self) -> bool {
        matches!(self, Self::Storage | Self::Hybrid)
    }
}

impl fmt::Display for ClusterNodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Storage => "storage",
            Self::Query => "query",
            Self::Hybrid => "hybrid",
        };
        f.write_str(value)
    }
}

impl FromStr for ClusterNodeRole {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "storage" => Ok(Self::Storage),
            "query" => Ok(Self::Query),
            "hybrid" => Ok(Self::Hybrid),
            _ => Err(format!(
                "invalid cluster node role '{value}', expected one of: storage, query, hybrid"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: Option<String>,
    pub bind: Option<String>,
    pub seeds: Vec<String>,
    pub node_role: ClusterNodeRole,
    pub internal_auth_token: Option<String>,
    pub internal_auth_token_file: Option<PathBuf>,
    pub shards: u32,
    pub replication_factor: u16,
    pub write_consistency: ClusterWriteConsistency,
    pub read_consistency: ClusterReadConsistency,
    pub read_partial_response: ClusterReadPartialResponsePolicy,
    pub internal_mtls: ClusterInternalMtlsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterInternalMtlsConfig {
    pub enabled: bool,
    pub ca_cert: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterInternalMtlsResolved {
    pub ca_cert: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SeedNode {
    pub id: String,
    pub endpoint: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: None,
            bind: None,
            seeds: Vec::new(),
            node_role: ClusterNodeRole::default(),
            internal_auth_token: None,
            internal_auth_token_file: None,
            shards: DEFAULT_CLUSTER_SHARDS,
            replication_factor: DEFAULT_CLUSTER_REPLICATION_FACTOR,
            write_consistency: ClusterWriteConsistency::default(),
            read_consistency: ClusterReadConsistency::default(),
            read_partial_response: ClusterReadPartialResponsePolicy::default(),
            internal_mtls: ClusterInternalMtlsConfig::default(),
        }
    }
}

impl ClusterConfig {
    #[cfg(test)]
    pub fn add_seeds_from_arg(&mut self, value: &str) {
        for seed in value.split(',') {
            let trimmed = seed.trim();
            if !trimmed.is_empty() {
                self.seeds.push(trimmed.to_string());
            }
        }
    }

    pub fn normalize(mut self) -> Result<Self, String> {
        self.node_id = normalize_optional_string("--cluster-node-id", self.node_id)?;
        self.bind = normalize_optional_string("--cluster-bind", self.bind)?;
        self.seeds = self
            .seeds
            .into_iter()
            .map(|seed| seed.trim().to_string())
            .filter(|seed| !seed.is_empty())
            .collect();
        self.internal_auth_token =
            normalize_optional_string("--cluster-internal-auth-token", self.internal_auth_token)?;
        self.internal_auth_token_file = normalize_trimmed_optional_path(
            "--cluster-internal-auth-token-file",
            self.internal_auth_token_file,
        )?;
        self.internal_mtls.ca_cert = normalize_trimmed_optional_path(
            "--cluster-internal-mtls-ca-cert",
            self.internal_mtls.ca_cert,
        )?;
        self.internal_mtls.cert = normalize_trimmed_optional_path(
            "--cluster-internal-mtls-cert",
            self.internal_mtls.cert,
        )?;
        self.internal_mtls.key =
            normalize_trimmed_optional_path("--cluster-internal-mtls-key", self.internal_mtls.key)?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        let node_id = self
            .node_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                "--cluster-enabled=true requires --cluster-node-id with a non-empty value"
                    .to_string()
            })?;

        if node_id.eq_ignore_ascii_case(UNKNOWN_NODE_ID) {
            return Err("--cluster-node-id must not be 'unknown'".to_string());
        }

        if self.shards == 0 {
            return Err("--cluster-shards must be greater than zero".to_string());
        }

        if self.replication_factor == 0 {
            return Err("--cluster-replication-factor must be greater than zero".to_string());
        }

        if self
            .internal_auth_token
            .as_deref()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err("--cluster-internal-auth-token must not be empty".to_string());
        }
        if self.internal_auth_token.is_some() && self.internal_auth_token_file.is_some() {
            return Err(
                "--cluster-internal-auth-token and --cluster-internal-auth-token-file are mutually exclusive"
                    .to_string(),
            );
        }

        let bind = self.normalized_bind_endpoint()?;
        let seeds = self.normalized_seed_nodes()?;

        let mut unique_endpoints = BTreeSet::new();
        let mut unique_node_ids = BTreeSet::new();
        unique_endpoints.insert(endpoint_identity(&bind));
        unique_node_ids.insert(node_identity(node_id));

        for seed in &seeds {
            let endpoint_key = endpoint_identity(&seed.endpoint);
            if !unique_endpoints.insert(endpoint_key) {
                return Err(format!(
                    "--cluster-seeds contains duplicate endpoint '{}'",
                    seed.endpoint
                ));
            }

            let node_key = node_identity(&seed.id);
            if !unique_node_ids.insert(node_key) {
                return Err(format!(
                    "--cluster-seeds contains duplicate node id '{}'",
                    seed.id
                ));
            }
        }

        let storage_capable_nodes = seeds.len() + usize::from(self.node_role.owns_shards());
        if storage_capable_nodes == 0 {
            return Err(
                "--cluster-node-role=query requires at least one storage-capable seed node"
                    .to_string(),
            );
        }

        if usize::from(self.replication_factor) > storage_capable_nodes {
            return Err(format!(
                "--cluster-replication-factor ({}) cannot exceed storage-capable cluster node count ({storage_capable_nodes})",
                self.replication_factor,
            ));
        }

        let _ = self.resolved_internal_mtls()?;

        Ok(())
    }

    pub fn normalized_bind_endpoint(&self) -> Result<String, String> {
        let bind = self
            .bind
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                "--cluster-enabled=true requires --cluster-bind with a host:port value".to_string()
            })?;

        normalize_endpoint(bind, "--cluster-bind")
    }

    pub fn normalized_seed_nodes(&self) -> Result<Vec<SeedNode>, String> {
        let mut nodes = Vec::new();
        for raw_seed in &self.seeds {
            let raw_seed = raw_seed.trim();
            if raw_seed.is_empty() {
                continue;
            }
            let parsed = parse_seed(raw_seed)?;
            nodes.push(SeedNode {
                id: parsed
                    .node_id
                    .unwrap_or_else(|| infer_seed_node_id(&parsed.endpoint)),
                endpoint: parsed.endpoint,
            });
        }
        nodes.sort();
        Ok(nodes)
    }

    #[cfg(test)]
    pub fn cluster_internal_auth_token(&self) -> Option<&str> {
        self.internal_auth_token
            .as_deref()
            .map(str::trim)
            .filter(|token| !token.is_empty())
    }

    pub fn load_cluster_internal_auth_token(&self) -> Result<Option<String>, String> {
        crate::security::load_cluster_internal_auth_token_from_config(self)
    }

    pub fn resolved_internal_mtls(&self) -> Result<Option<ClusterInternalMtlsResolved>, String> {
        if !self.enabled || !self.internal_mtls.enabled {
            return Ok(None);
        }

        let ca_cert = self.internal_mtls.ca_cert.clone().ok_or_else(|| {
            "--cluster-internal-mtls-enabled=true requires --cluster-internal-mtls-ca-cert"
                .to_string()
        })?;
        let cert = self.internal_mtls.cert.clone().ok_or_else(|| {
            "--cluster-internal-mtls-enabled=true requires --cluster-internal-mtls-cert".to_string()
        })?;
        let key = self.internal_mtls.key.clone().ok_or_else(|| {
            "--cluster-internal-mtls-enabled=true requires --cluster-internal-mtls-key".to_string()
        })?;

        Ok(Some(ClusterInternalMtlsResolved { ca_cert, cert, key }))
    }
}

fn normalize_optional_string(flag: &str, value: Option<String>) -> Result<Option<String>, String> {
    value
        .map(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(format!("{flag} must not be empty"));
            }
            Ok(trimmed.to_string())
        })
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSeed {
    node_id: Option<String>,
    endpoint: String,
}

fn parse_seed(value: &str) -> Result<ParsedSeed, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("--cluster-seeds entries must not be empty".to_string());
    }

    if value.matches('@').count() > 1 {
        return Err(format!(
            "--cluster-seeds entry '{value}' has invalid format; expected host:port or node-id@host:port"
        ));
    }

    let (node_id, endpoint_raw) = if let Some((node_id_raw, endpoint_raw)) = value.split_once('@') {
        let node_id = node_id_raw.trim();
        if node_id.is_empty() {
            return Err(format!(
                "--cluster-seeds entry '{value}' has empty node id before '@'"
            ));
        }
        if node_id.eq_ignore_ascii_case(UNKNOWN_NODE_ID) {
            return Err("--cluster-seeds node id must not be 'unknown'".to_string());
        }

        let endpoint_raw = endpoint_raw.trim();
        if endpoint_raw.is_empty() {
            return Err(format!(
                "--cluster-seeds entry '{value}' has empty endpoint after '@'"
            ));
        }

        (Some(node_id.to_string()), endpoint_raw)
    } else {
        (None, value)
    };

    let endpoint = normalize_endpoint(endpoint_raw, "--cluster-seeds")?;
    Ok(ParsedSeed { node_id, endpoint })
}

fn normalize_endpoint(value: &str, flag: &str) -> Result<String, String> {
    let Some((host, port)) = value.rsplit_once(':') else {
        return Err(format!(
            "{flag} endpoint '{value}' must use host:port syntax"
        ));
    };

    if host.trim().is_empty() {
        return Err(format!("{flag} endpoint '{value}' has an empty host"));
    }

    port.parse::<u16>()
        .map_err(|_| format!("{flag} endpoint '{value}' has an invalid port"))?;

    Ok(format!(
        "{}:{}",
        host.trim().to_ascii_lowercase(),
        port.trim()
    ))
}

pub fn infer_seed_node_id(endpoint: &str) -> String {
    format!("seed@{}", endpoint_identity(endpoint))
}

fn endpoint_identity(endpoint: &str) -> String {
    endpoint.trim().to_ascii_lowercase()
}

fn node_identity(node_id: &str) -> String {
    node_id.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_deterministic_and_disabled() {
        let cfg = ClusterConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.shards, DEFAULT_CLUSTER_SHARDS);
        assert_eq!(cfg.replication_factor, DEFAULT_CLUSTER_REPLICATION_FACTOR);
        assert_eq!(cfg.write_consistency, ClusterWriteConsistency::Quorum);
        assert_eq!(cfg.read_consistency, ClusterReadConsistency::Eventual);
        assert_eq!(
            cfg.read_partial_response,
            ClusterReadPartialResponsePolicy::Allow
        );
        assert!(cfg.internal_auth_token.is_none());
        assert!(cfg.internal_auth_token_file.is_none());
        assert!(!cfg.internal_mtls.enabled);
        assert!(cfg.internal_mtls.ca_cert.is_none());
        assert!(cfg.internal_mtls.cert.is_none());
        assert!(cfg.internal_mtls.key.is_none());
    }

    #[test]
    fn disabled_mode_skips_cluster_validation() {
        let cfg = ClusterConfig {
            enabled: false,
            node_id: None,
            bind: None,
            seeds: vec!["not-an-endpoint".to_string()],
            node_role: ClusterNodeRole::Hybrid,
            internal_auth_token: None,
            internal_auth_token_file: None,
            shards: 0,
            replication_factor: 0,
            write_consistency: ClusterWriteConsistency::All,
            read_consistency: ClusterReadConsistency::Strict,
            read_partial_response: ClusterReadPartialResponsePolicy::Deny,
            internal_mtls: ClusterInternalMtlsConfig {
                enabled: true,
                ca_cert: None,
                cert: None,
                key: None,
            },
        };

        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn enabled_mode_requires_node_id() {
        let cfg = ClusterConfig {
            enabled: true,
            bind: Some("127.0.0.1:9301".to_string()),
            ..ClusterConfig::default()
        };

        let err = cfg
            .validate()
            .expect_err("missing node id should be rejected");
        assert!(err.contains("--cluster-node-id"));
    }

    #[test]
    fn enabled_mode_requires_bind() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            ..ClusterConfig::default()
        };

        let err = cfg.validate().expect_err("missing bind should be rejected");
        assert!(err.contains("--cluster-bind"));
    }

    #[test]
    fn rejects_duplicate_seed_endpoints() {
        let mut cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            ..ClusterConfig::default()
        };
        cfg.add_seeds_from_arg("node-b@127.0.0.1:9302,127.0.0.1:9302");

        let err = cfg.validate().expect_err("duplicate endpoints should fail");
        assert!(err.contains("duplicate endpoint"));
    }

    #[test]
    fn rejects_duplicate_seed_node_ids() {
        let mut cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            ..ClusterConfig::default()
        };
        cfg.add_seeds_from_arg("node-b@127.0.0.1:9302,node-b@127.0.0.1:9303");

        let err = cfg.validate().expect_err("duplicate node ids should fail");
        assert!(err.contains("duplicate node id"));
    }

    #[test]
    fn rejects_seed_node_id_that_conflicts_with_local_node_id() {
        let mut cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            ..ClusterConfig::default()
        };
        cfg.add_seeds_from_arg("node-a@127.0.0.1:9302");

        let err = cfg
            .validate()
            .expect_err("seed node id must not conflict with local node id");
        assert!(err.contains("duplicate node id"));
    }

    #[test]
    fn rejects_replication_factor_larger_than_cluster_size() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["127.0.0.1:9302".to_string()],
            replication_factor: 3,
            ..ClusterConfig::default()
        };

        let err = cfg
            .validate()
            .expect_err("rf greater than node count should fail");
        assert!(err.contains("--cluster-replication-factor"));
    }

    #[test]
    fn accepts_valid_enabled_config() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["127.0.0.1:9302".to_string()],
            node_role: ClusterNodeRole::Hybrid,
            internal_auth_token: None,
            internal_auth_token_file: None,
            shards: 256,
            replication_factor: 2,
            write_consistency: ClusterWriteConsistency::Quorum,
            read_consistency: ClusterReadConsistency::Eventual,
            read_partial_response: ClusterReadPartialResponsePolicy::Allow,
            internal_mtls: ClusterInternalMtlsConfig::default(),
        };

        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn query_only_role_requires_at_least_one_storage_seed() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("query-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            node_role: ClusterNodeRole::Query,
            internal_auth_token: Some("cluster-test-token".to_string()),
            ..ClusterConfig::default()
        };

        let err = cfg
            .validate()
            .expect_err("query-only nodes require at least one storage seed");
        assert!(err.contains("storage-capable seed node"));
    }

    #[test]
    fn parses_consistency_modes_case_insensitively() {
        assert_eq!(
            ClusterWriteConsistency::from_str("QuOrUm").expect("must parse"),
            ClusterWriteConsistency::Quorum
        );
        assert_eq!(
            ClusterReadConsistency::from_str("STRICT").expect("must parse"),
            ClusterReadConsistency::Strict
        );
        assert_eq!(
            ClusterReadPartialResponsePolicy::from_str("dEnY").expect("must parse"),
            ClusterReadPartialResponsePolicy::Deny
        );
    }

    #[test]
    fn add_seeds_from_arg_ignores_empty_entries() {
        let mut cfg = ClusterConfig::default();
        cfg.add_seeds_from_arg("127.0.0.1:9302, ,127.0.0.1:9303");
        assert_eq!(
            cfg.seeds,
            vec!["127.0.0.1:9302".to_string(), "127.0.0.1:9303".to_string()]
        );
    }

    #[test]
    fn normalized_seed_nodes_supports_optional_node_ids() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "  node-c@EXAMPLE.internal:9303".to_string(),
                "127.0.0.1:9302".to_string(),
            ],
            ..ClusterConfig::default()
        };

        let seeds = cfg
            .normalized_seed_nodes()
            .expect("seed normalization should succeed");
        assert_eq!(
            seeds,
            vec![
                SeedNode {
                    id: "node-c".to_string(),
                    endpoint: "example.internal:9303".to_string()
                },
                SeedNode {
                    id: "seed@127.0.0.1:9302".to_string(),
                    endpoint: "127.0.0.1:9302".to_string()
                },
            ]
        );
    }

    #[test]
    fn rejects_invalid_seed_format() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node@id@127.0.0.1:9302".to_string()],
            ..ClusterConfig::default()
        };

        let err = cfg
            .validate()
            .expect_err("invalid seed format must be rejected");
        assert!(err.contains("expected host:port or node-id@host:port"));
    }

    #[test]
    fn write_consistency_ack_thresholds_match_mode() {
        assert_eq!(ClusterWriteConsistency::One.required_acks(3), 1);
        assert_eq!(ClusterWriteConsistency::Quorum.required_acks(3), 2);
        assert_eq!(ClusterWriteConsistency::All.required_acks(3), 3);
    }

    #[test]
    fn enabled_mode_requires_internal_mtls_paths_when_enabled() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            internal_mtls: ClusterInternalMtlsConfig {
                enabled: true,
                ca_cert: None,
                cert: None,
                key: None,
            },
            ..ClusterConfig::default()
        };

        let err = cfg
            .validate()
            .expect_err("internal mTLS path validation should fail");
        assert!(err.contains("--cluster-internal-mtls-ca-cert"));
    }

    #[test]
    fn enabled_mode_accepts_internal_mtls_paths_when_enabled() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            internal_mtls: ClusterInternalMtlsConfig {
                enabled: true,
                ca_cert: Some(PathBuf::from("/tmp/ca.pem")),
                cert: Some(PathBuf::from("/tmp/node-a.pem")),
                key: Some(PathBuf::from("/tmp/node-a.key")),
            },
            ..ClusterConfig::default()
        };

        let resolved = cfg
            .resolved_internal_mtls()
            .expect("internal mTLS config should resolve")
            .expect("internal mTLS should be enabled");
        assert_eq!(resolved.ca_cert, PathBuf::from("/tmp/ca.pem"));
        assert_eq!(resolved.cert, PathBuf::from("/tmp/node-a.pem"));
        assert_eq!(resolved.key, PathBuf::from("/tmp/node-a.key"));
    }
}
