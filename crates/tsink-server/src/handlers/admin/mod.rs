use super::*;

mod cluster;
mod control_plane;
mod rbac_security;
mod rules_rollups;
mod snapshot_restore;
mod usage_support;

pub(crate) use self::cluster::*;
pub(crate) use self::control_plane::*;
pub(crate) use self::rbac_security::*;
pub(crate) use self::rules_rollups::*;
pub(crate) use self::snapshot_restore::*;
pub(crate) use self::usage_support::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
    }

    fn make_rules_runtime(storage: &Arc<dyn Storage>) -> Arc<RulesRuntime> {
        RulesRuntime::open(
            None,
            Arc::clone(storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("rules runtime should build")
    }

    #[tokio::test]
    async fn admin_rules_handlers_apply_run_and_report_directly() {
        let storage = make_storage();
        let rules_runtime = make_rules_runtime(&storage);
        let now = unix_timestamp_millis() as i64;
        let seed_timestamp = now.saturating_sub(1_000);

        tenant::scoped_storage(Arc::clone(&storage), "team-a")
            .insert_rows(&[Row::with_labels(
                "source_metric",
                vec![Label::new("host", "a")],
                DataPoint::new(seed_timestamp, 3.0),
            )])
            .expect("seed write should succeed");

        let apply_response = handle_admin_rules_apply(
            &HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/rules/apply".to_string(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                body: serde_json::to_vec(&json!({
                    "groups": [{
                        "name": "recording",
                        "tenantId": "team-a",
                        "interval": "1s",
                        "rules": [{
                            "kind": "recording",
                            "record": "recorded_metric",
                            "expr": "source_metric{host=\"a\"}"
                        }]
                    }]
                }))
                .expect("json should encode"),
            },
            Some(rules_runtime.as_ref()),
        )
        .await;
        assert_eq!(apply_response.status, 200);

        let status_response = handle_admin_rules_status(Some(rules_runtime.as_ref())).await;
        assert_eq!(status_response.status, 200);
        let status_body: JsonValue =
            serde_json::from_slice(&status_response.body).expect("status body should decode");
        assert_eq!(status_body["data"]["metrics"]["configuredGroups"], 1);
        assert_eq!(
            status_body["data"]["groups"][0]["rules"][0]["name"],
            "recorded_metric"
        );

        let run_response = handle_admin_rules_run(Some(rules_runtime.as_ref())).await;
        assert_eq!(run_response.status, 200);

        let points = tenant::scoped_storage(Arc::clone(&storage), "team-a")
            .select(
                "recorded_metric",
                &[Label::new("host", "a")],
                seed_timestamp.saturating_sub(120_000),
                now.saturating_add(120_000),
            )
            .expect("recorded metric should be queryable");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value_as_f64(), Some(3.0));
    }

    #[tokio::test]
    async fn admin_usage_report_handler_reconciles_storage_directly() {
        let storage = make_storage();
        let usage_accounting = UsageAccounting::open(None).expect("usage accounting should open");

        tenant::scoped_storage(Arc::clone(&storage), "team-a")
            .insert_rows(&[Row::with_labels(
                "usage_metric",
                vec![Label::new("host", "a")],
                DataPoint::new(1_700_000_000_000, 7.0),
            )])
            .expect("seed write should succeed");

        let response = handle_admin_usage_report(
            &storage,
            &HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/admin/usage/report?tenant=team-a&bucket=none&reconcile=true"
                    .to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            Some(usage_accounting.as_ref()),
        );
        assert_eq!(response.status, 200);

        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should decode");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["report"]["tenants"][0]["tenantId"], "team-a");
        assert_eq!(
            body["data"]["reconciledStorageSnapshots"][0]["tenantId"],
            "team-a"
        );
        assert!(
            body["data"]["report"]["tenants"][0]["latestStorageSnapshot"]["seriesTotal"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
    }
}
