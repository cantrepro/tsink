use crate::cluster::membership::MembershipView;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use xxhash_rust::xxh64::xxh64;

const DEFAULT_VIRTUAL_NODES_PER_NODE: u16 = 128;
pub const STABLE_RING_HASH_VERSION_V1: u16 = 1;
pub const CURRENT_RING_HASH_VERSION: u16 = STABLE_RING_HASH_VERSION_V1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRing {
    hash_version: u16,
    shard_count: u32,
    replication_factor: u16,
    virtual_nodes_per_node: u16,
    assignments: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRingSnapshot {
    pub hash_version: u16,
    pub shard_count: u32,
    pub replication_factor: u16,
    pub virtual_nodes_per_node: u16,
    pub assignments: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct VirtualNodeToken {
    token: u64,
    node_id: String,
}

impl ShardRing {
    pub fn build(
        shard_count: u32,
        replication_factor: u16,
        membership: &MembershipView,
    ) -> Result<Self, String> {
        Self::build_with_hash_version(
            shard_count,
            replication_factor,
            membership,
            CURRENT_RING_HASH_VERSION,
            DEFAULT_VIRTUAL_NODES_PER_NODE,
        )
    }

    fn build_with_hash_version(
        shard_count: u32,
        replication_factor: u16,
        membership: &MembershipView,
        hash_version: u16,
        virtual_nodes_per_node: u16,
    ) -> Result<Self, String> {
        validate_build_hash_version(hash_version)?;
        if shard_count == 0 {
            return Err("ring requires shard_count > 0".to_string());
        }
        if replication_factor == 0 {
            return Err("ring requires replication_factor > 0".to_string());
        }
        if membership.nodes.is_empty() {
            return Err("ring requires at least one node".to_string());
        }
        if virtual_nodes_per_node == 0 {
            return Err("ring requires at least one virtual node per physical node".to_string());
        }

        let node_count = membership.nodes.len();
        let replicas = usize::from(replication_factor).min(node_count);
        let virtual_nodes = build_virtual_nodes(membership, virtual_nodes_per_node, hash_version);

        let mut assignments = Vec::with_capacity(shard_count as usize);
        for shard in 0..shard_count {
            let shard_token = stable_hash_u64(format!("shard:{shard}").as_bytes(), hash_version);
            assignments.push(assign_shard_owners(shard_token, replicas, &virtual_nodes));
        }

        Ok(Self {
            hash_version,
            shard_count,
            replication_factor,
            virtual_nodes_per_node,
            assignments,
        })
    }

    pub fn from_snapshot(snapshot: ShardRingSnapshot) -> Result<Self, String> {
        validate_snapshot_hash_version(snapshot.hash_version)?;
        if snapshot.shard_count == 0 {
            return Err("ring snapshot requires shard_count > 0".to_string());
        }
        if snapshot.replication_factor == 0 {
            return Err("ring snapshot requires replication_factor > 0".to_string());
        }
        if snapshot.virtual_nodes_per_node == 0 {
            return Err("ring snapshot requires virtual_nodes_per_node > 0".to_string());
        }
        if snapshot.assignments.len() != snapshot.shard_count as usize {
            return Err(format!(
                "ring snapshot assignments length ({}) does not match shard_count ({})",
                snapshot.assignments.len(),
                snapshot.shard_count
            ));
        }
        if snapshot.assignments.is_empty() {
            return Err("ring snapshot requires at least one shard assignment".to_string());
        }

        let mut known_nodes = BTreeSet::new();
        for (idx, owners) in snapshot.assignments.iter().enumerate() {
            if owners.is_empty() {
                return Err(format!("ring snapshot shard {idx} has no owners"));
            }
            let mut owners_set = BTreeSet::new();
            for owner in owners {
                if owner.trim().is_empty() {
                    return Err(format!("ring snapshot shard {idx} has empty owner id"));
                }
                if !owners_set.insert(owner.as_str()) {
                    return Err(format!(
                        "ring snapshot shard {idx} has duplicate owner id '{owner}'"
                    ));
                }
                known_nodes.insert(owner.as_str());
            }
            if owners.len() > usize::from(snapshot.replication_factor) {
                return Err(format!(
                    "ring snapshot shard {idx} has {} owners which exceeds replication factor {}",
                    owners.len(),
                    snapshot.replication_factor
                ));
            }
        }
        if known_nodes.is_empty() {
            return Err("ring snapshot requires at least one node owner".to_string());
        }

        Ok(Self {
            hash_version: snapshot.hash_version,
            shard_count: snapshot.shard_count,
            replication_factor: snapshot.replication_factor,
            virtual_nodes_per_node: snapshot.virtual_nodes_per_node,
            assignments: snapshot.assignments,
        })
    }

    pub fn snapshot(&self) -> ShardRingSnapshot {
        ShardRingSnapshot {
            hash_version: self.hash_version,
            shard_count: self.shard_count,
            replication_factor: self.replication_factor,
            virtual_nodes_per_node: self.virtual_nodes_per_node,
            assignments: self.assignments.clone(),
        }
    }

    pub fn shard_for_series_id(&self, series_id: u64) -> u32 {
        (series_id % u64::from(self.shard_count)) as u32
    }

    pub fn owners_for_shard(&self, shard: u32) -> Option<&[String]> {
        self.assignments.get(shard as usize).map(Vec::as_slice)
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    pub fn hash_version(&self) -> u16 {
        self.hash_version
    }

    #[allow(dead_code)]
    pub fn replication_factor(&self) -> u16 {
        self.replication_factor
    }

    #[allow(dead_code)]
    pub fn virtual_nodes_per_node(&self) -> u16 {
        self.virtual_nodes_per_node
    }
}

fn build_virtual_nodes(
    membership: &MembershipView,
    virtual_nodes_per_node: u16,
    hash_version: u16,
) -> Vec<VirtualNodeToken> {
    let mut virtual_nodes =
        Vec::with_capacity(membership.nodes.len() * usize::from(virtual_nodes_per_node));
    for node in &membership.nodes {
        for vnode_idx in 0..virtual_nodes_per_node {
            let token_source = format!("{}#{vnode_idx}", node.id);
            virtual_nodes.push(VirtualNodeToken {
                token: stable_hash_u64(token_source.as_bytes(), hash_version),
                node_id: node.id.clone(),
            });
        }
    }
    virtual_nodes.sort();
    virtual_nodes
}

fn assign_shard_owners(
    shard_token: u64,
    replica_count: usize,
    virtual_nodes: &[VirtualNodeToken],
) -> Vec<String> {
    let mut owners = Vec::with_capacity(replica_count);
    let mut seen = BTreeSet::new();
    let mut idx = virtual_nodes.partition_point(|token| token.token < shard_token);
    if idx == virtual_nodes.len() {
        idx = 0;
    }

    for _ in 0..virtual_nodes.len() {
        let owner = &virtual_nodes[idx].node_id;
        if seen.insert(owner.as_str()) {
            owners.push(owner.clone());
            if owners.len() == replica_count {
                break;
            }
        }
        idx = (idx + 1) % virtual_nodes.len();
    }

    if owners.len() < replica_count {
        for vnode in virtual_nodes {
            let owner = vnode.node_id.as_str();
            if seen.insert(owner) {
                owners.push(vnode.node_id.clone());
                if owners.len() == replica_count {
                    break;
                }
            }
        }
    }

    owners
}

pub fn ring_hash_version_name(hash_version: u16) -> Option<&'static str> {
    match hash_version {
        STABLE_RING_HASH_VERSION_V1 => Some("xxh64_seed0"),
        _ => None,
    }
}

fn validate_snapshot_hash_version(hash_version: u16) -> Result<(), String> {
    if hash_version != CURRENT_RING_HASH_VERSION {
        return Err(format!(
            "ring snapshot hash_version {} is unsupported; expected {} ({})",
            hash_version,
            CURRENT_RING_HASH_VERSION,
            ring_hash_version_name(CURRENT_RING_HASH_VERSION).unwrap_or("unknown")
        ));
    }
    Ok(())
}

fn validate_build_hash_version(hash_version: u16) -> Result<(), String> {
    if hash_version != CURRENT_RING_HASH_VERSION {
        return Err(format!(
            "ring hash_version {} is unsupported; expected {} ({})",
            hash_version,
            CURRENT_RING_HASH_VERSION,
            ring_hash_version_name(CURRENT_RING_HASH_VERSION).unwrap_or("unknown")
        ));
    }
    Ok(())
}

fn stable_hash_u64(input: &[u8], hash_version: u16) -> u64 {
    match hash_version {
        STABLE_RING_HASH_VERSION_V1 => stable_xxh64_seed0(input),
        other => unreachable!("unsupported ring hash version {other}"),
    }
}

fn stable_xxh64_seed0(bytes: &[u8]) -> u64 {
    xxh64(bytes, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;
    use crate::cluster::membership::MembershipView;
    use std::collections::BTreeMap;

    #[test]
    fn ring_assignments_are_deterministic() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-b@127.0.0.1:9302".to_string(),
                "node-c@127.0.0.1:9303".to_string(),
            ],
            ..ClusterConfig::default()
        };

        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let ring_a = ShardRing::build(256, 2, &membership).expect("ring should build");
        let ring_b = ShardRing::build(256, 2, &membership).expect("ring should build");

        assert_eq!(ring_a, ring_b);
        assert_eq!(ring_a.hash_version(), CURRENT_RING_HASH_VERSION);
        assert_eq!(ring_a.shard_for_series_id(257), 1);
        assert_eq!(ring_a.owners_for_shard(0).map(|v| v.len()), Some(2));
        assert_eq!(ring_a.replication_factor(), 2);
        assert_eq!(ring_a.virtual_nodes_per_node(), 128);
    }

    #[test]
    fn ring_distribution_skew_is_bounded() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-b@127.0.0.1:9302".to_string(),
                "node-c@127.0.0.1:9303".to_string(),
                "node-d@127.0.0.1:9304".to_string(),
                "node-e@127.0.0.1:9305".to_string(),
            ],
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let ring = ShardRing::build(1024, 1, &membership).expect("ring should build");

        let mut primary_counts: BTreeMap<String, usize> = membership
            .nodes
            .iter()
            .map(|node| (node.id.clone(), 0))
            .collect();
        for shard in 0..ring.shard_count() {
            let owner = ring
                .owners_for_shard(shard)
                .and_then(|owners| owners.first())
                .expect("every shard has an owner");
            *primary_counts.get_mut(owner).expect("owner must be known") += 1;
        }

        let ideal = ring.shard_count() as f64 / membership.nodes.len() as f64;
        for count in primary_counts.values() {
            let skew = (*count as f64 - ideal).abs() / ideal;
            assert!(
                skew <= 0.40,
                "primary-owner skew {skew:.3} exceeded threshold for count {count} with ideal {ideal:.2}"
            );
        }
    }

    #[test]
    fn ring_snapshot_roundtrip_preserves_assignments() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-b@127.0.0.1:9302".to_string(),
                "node-c@127.0.0.1:9303".to_string(),
            ],
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let ring = ShardRing::build(128, 2, &membership).expect("ring should build");

        let snapshot = ring.snapshot();
        let encoded = serde_json::to_vec(&snapshot).expect("snapshot should serialize");
        let decoded: ShardRingSnapshot =
            serde_json::from_slice(&encoded).expect("snapshot should deserialize");
        let restored = ShardRing::from_snapshot(decoded).expect("snapshot should restore");

        assert_eq!(ring, restored);
    }

    #[test]
    fn snapshot_requires_explicit_hash_version() {
        let err = serde_json::from_value::<ShardRingSnapshot>(serde_json::json!({
            "shard_count": 2,
            "replication_factor": 1,
            "virtual_nodes_per_node": 128,
            "assignments": [["node-a"], ["node-b"]]
        }))
        .expect_err("snapshot without hash_version must fail");

        assert!(err.to_string().contains("hash_version"));
    }

    #[test]
    fn snapshot_rejects_unknown_hash_version() {
        let err = ShardRing::from_snapshot(ShardRingSnapshot {
            hash_version: 0,
            shard_count: 1,
            replication_factor: 1,
            virtual_nodes_per_node: 128,
            assignments: vec![vec!["node-a".to_string()]],
        })
        .expect_err("unknown ring hash version must fail");

        assert!(err.contains("hash_version 0"));
        assert!(err.contains("expected 1"));
    }
}
