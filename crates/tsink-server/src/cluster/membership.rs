use crate::cluster::config::ClusterConfig;
use crate::cluster::rpc::{InternalControlAutoJoinRequest, RpcClient};
use std::time::Duration;
use tokio::task::JoinHandle;

const AUTO_JOIN_INTERVAL_SECS_ENV: &str = "TSINK_CLUSTER_AUTO_JOIN_INTERVAL_SECS";
const DEFAULT_AUTO_JOIN_INTERVAL_SECS: u64 = 3;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClusterNode {
    pub id: String,
    pub endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipView {
    pub local_node_id: String,
    pub nodes: Vec<ClusterNode>,
}

impl MembershipView {
    pub fn from_config(config: &ClusterConfig) -> Result<Self, String> {
        if !config.enabled {
            return Err("cluster membership requested while cluster mode is disabled".to_string());
        }
        config.validate()?;

        let local_node_id = config
            .node_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "missing local cluster node id".to_string())?
            .to_string();

        let local_endpoint = config.normalized_bind_endpoint()?;

        let mut nodes = vec![ClusterNode {
            id: local_node_id.clone(),
            endpoint: local_endpoint,
        }];

        for seed in config.normalized_seed_nodes()? {
            nodes.push(ClusterNode {
                id: seed.id,
                endpoint: seed.endpoint,
            });
        }

        nodes.sort();

        Ok(Self {
            local_node_id,
            nodes,
        })
    }

    pub fn local_endpoint(&self) -> Option<&str> {
        self.nodes
            .iter()
            .find(|node| node.id == self.local_node_id)
            .map(|node| node.endpoint.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct AutoJoinRuntime {
    local_node_id: String,
    local_endpoint: String,
    seed_endpoints: Vec<String>,
    rpc_client: RpcClient,
    interval: Duration,
}

impl AutoJoinRuntime {
    pub fn from_membership(
        membership: &MembershipView,
        rpc_client: RpcClient,
    ) -> Result<Option<Self>, String> {
        let Some(local_endpoint) = membership.local_endpoint() else {
            return Err(format!(
                "cluster membership is missing local endpoint for node '{}'",
                membership.local_node_id
            ));
        };
        let seed_endpoints = membership
            .nodes
            .iter()
            .filter(|node| node.id != membership.local_node_id)
            .map(|node| node.endpoint.clone())
            .collect::<Vec<_>>();
        if seed_endpoints.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            local_node_id: membership.local_node_id.clone(),
            local_endpoint: local_endpoint.to_string(),
            seed_endpoints,
            rpc_client,
            interval: Duration::from_secs(read_auto_join_interval_secs()?),
        }))
    }

    pub async fn attempt_once(&self) -> bool {
        let request = InternalControlAutoJoinRequest {
            node_id: self.local_node_id.clone(),
            endpoint: self.local_endpoint.clone(),
        };
        for endpoint in &self.seed_endpoints {
            match self.rpc_client.control_auto_join(endpoint, &request).await {
                Ok(response) if response.result == "accepted" || response.result == "noop" => {
                    return true;
                }
                Ok(_) => {}
                Err(_) => {}
            }
        }
        false
    }

    pub fn start_worker(&self) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                if runtime.attempt_once().await {
                    break;
                }
                tokio::time::sleep(runtime.interval).await;
            }
        })
    }
}

fn read_auto_join_interval_secs() -> Result<u64, String> {
    match std::env::var(AUTO_JOIN_INTERVAL_SECS_ENV) {
        Ok(value) => {
            let parsed = value
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("{AUTO_JOIN_INTERVAL_SECS_ENV} must be an integer"))?;
            if parsed == 0 {
                return Err(format!(
                    "{AUTO_JOIN_INTERVAL_SECS_ENV} must be greater than zero"
                ));
            }
            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_AUTO_JOIN_INTERVAL_SECS),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!(
            "{AUTO_JOIN_INTERVAL_SECS_ENV} must be valid UTF-8 when set"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;

    #[test]
    fn builds_sorted_static_membership_from_config() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-c@127.0.0.1:9303".to_string(),
                "127.0.0.1:9302".to_string(),
            ],
            ..ClusterConfig::default()
        };

        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        assert_eq!(membership.nodes.len(), 3);
        assert_eq!(membership.local_node_id, "node-a");
        assert_eq!(
            membership
                .nodes
                .iter()
                .map(|node| (node.id.clone(), node.endpoint.clone()))
                .collect::<Vec<_>>(),
            vec![
                ("node-a".to_string(), "127.0.0.1:9301".to_string()),
                ("node-c".to_string(), "127.0.0.1:9303".to_string()),
                (
                    "seed@127.0.0.1:9302".to_string(),
                    "127.0.0.1:9302".to_string(),
                )
            ]
        );
    }

    #[test]
    fn seed_order_does_not_change_membership() {
        let cfg_a = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-b@127.0.0.1:9302".to_string(),
                "node-c@127.0.0.1:9303".to_string(),
            ],
            ..ClusterConfig::default()
        };

        let cfg_b = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-c@127.0.0.1:9303".to_string(),
                "node-b@127.0.0.1:9302".to_string(),
            ],
            ..ClusterConfig::default()
        };

        let membership_a = MembershipView::from_config(&cfg_a).expect("membership should build");
        let membership_b = MembershipView::from_config(&cfg_b).expect("membership should build");
        assert_eq!(membership_a, membership_b);
    }
}
