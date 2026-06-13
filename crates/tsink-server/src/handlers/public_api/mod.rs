mod ingest;
mod metadata;
mod promql;
mod remote_read;

#[cfg(test)]
pub(super) use self::ingest::handle_remote_write_with_admission;
pub(super) use self::ingest::{handle_otlp_metrics, handle_prometheus_import, handle_remote_write};
#[cfg(test)]
pub(super) use self::ingest::{parse_prom_labels, parse_prometheus_text};
#[cfg(test)]
pub(super) use self::metadata::handle_series_with_admission;
pub(super) use self::metadata::{
    handle_label_values, handle_labels, handle_metadata, handle_series,
};
#[cfg(test)]
pub(super) use self::promql::handle_instant_query_with_admission;
pub(super) use self::promql::{handle_instant_query, handle_query_exemplars, handle_range_query};
pub(super) use self::remote_read::handle_remote_read;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exemplar_store::ExemplarStore;
    use crate::http::HttpRequest;
    use crate::metadata_store::MetricMetadataStore;
    use crate::prom_remote::{
        Label as PromLabel, MetricMetadata, MetricType, Sample as PromSample, TimeSeries,
        WriteRequest,
    };
    use crate::tenant;
    use prost::Message;
    use serde_json::Value as JsonValue;
    use snap::raw::Encoder as SnappyEncoder;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tsink::promql::Engine;
    use tsink::{DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision};

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
    }

    fn make_engine(storage: &Arc<dyn Storage>) -> Engine {
        Engine::with_precision(Arc::clone(storage), TimestampPrecision::Milliseconds)
    }

    fn make_metadata_store() -> Arc<MetricMetadataStore> {
        Arc::new(MetricMetadataStore::in_memory())
    }

    fn make_exemplar_store() -> Arc<ExemplarStore> {
        Arc::new(ExemplarStore::in_memory())
    }

    #[tokio::test]
    async fn public_ingest_handlers_accept_remote_write_directly() {
        let storage = make_storage();
        let metadata_store = make_metadata_store();
        let exemplar_store = make_exemplar_store();

        let mut encoded = Vec::new();
        WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    PromLabel {
                        name: "__name__".to_string(),
                        value: "cpu_usage".to_string(),
                    },
                    PromLabel {
                        name: "host".to_string(),
                        value: "server-a".to_string(),
                    },
                ],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        }
        .encode(&mut encoded)
        .expect("protobuf encode should work");

        let response = handle_remote_write(
            &storage,
            &metadata_store,
            &exemplar_store,
            &HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/write".to_string(),
                headers: HashMap::from([
                    ("content-encoding".to_string(), "snappy".to_string()),
                    (
                        "content-type".to_string(),
                        "application/x-protobuf".to_string(),
                    ),
                ]),
                body: SnappyEncoder::new()
                    .compress_vec(&encoded)
                    .expect("snappy encode should succeed"),
            },
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(response.status, 200);

        let points = storage
            .select(
                "cpu_usage",
                &[
                    Label::new("host", "server-a"),
                    Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID),
                ],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("point must be persisted");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value.as_f64(), Some(11.5));
    }

    #[tokio::test]
    async fn public_metadata_handler_keeps_results_tenant_scoped() {
        let storage = make_storage();
        let metadata_store = make_metadata_store();
        let exemplar_store = make_exemplar_store();

        for (tenant_id, metric_name) in [("tenant-a", "alpha_metric"), ("tenant-b", "omega_metric")]
        {
            let mut encoded = Vec::new();
            WriteRequest {
                timeseries: Vec::new(),
                metadata: vec![MetricMetadata {
                    r#type: MetricType::Counter as i32,
                    metric_family_name: metric_name.to_string(),
                    help: format!("{metric_name} help"),
                    unit: "requests".to_string(),
                }],
            }
            .encode(&mut encoded)
            .expect("protobuf encode should work");

            let response = handle_remote_write(
                &storage,
                &metadata_store,
                &exemplar_store,
                &HttpRequest {
                    method: "POST".to_string(),
                    path: "/api/v1/write".to_string(),
                    headers: HashMap::from([
                        ("content-encoding".to_string(), "snappy".to_string()),
                        (
                            "content-type".to_string(),
                            "application/x-protobuf".to_string(),
                        ),
                        (tenant::TENANT_HEADER.to_string(), tenant_id.to_string()),
                    ]),
                    body: SnappyEncoder::new()
                        .compress_vec(&encoded)
                        .expect("snappy encode should succeed"),
                },
                TimestampPrecision::Milliseconds,
                None,
                None,
                None,
                None,
                None,
            )
            .await;
            assert_eq!(response.status, 200);
        }

        let response = handle_metadata(
            &metadata_store,
            &HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/metadata?limit=10".to_string(),
                headers: HashMap::from([(
                    tenant::TENANT_HEADER.to_string(),
                    "tenant-a".to_string(),
                )]),
                body: Vec::new(),
            },
            None,
            None,
            None,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should decode");
        assert!(body["data"]["alpha_metric"].is_array());
        assert!(body["data"]["omega_metric"].is_null());
    }

    #[tokio::test]
    async fn public_promql_handler_returns_results_without_router_indirection() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        tenant::scoped_storage(Arc::clone(&storage), tenant::DEFAULT_TENANT_ID)
            .insert_rows(&[Row::with_labels(
                "cpu_usage",
                vec![Label::new("host", "query-a")],
                DataPoint::new(1_700_000_000_000, 2.5),
            )])
            .expect("seed write should succeed");

        let response = handle_instant_query(
            &storage,
            &engine,
            &HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query?query=cpu_usage{host=\"query-a\"}&time=1700000000000"
                    .to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            TimestampPrecision::Milliseconds,
            super::super::PublicReadContext::new(None, None, None, None),
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should decode");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "vector");
        assert_eq!(body["data"]["result"][0]["metric"]["host"], "query-a");
    }
}
