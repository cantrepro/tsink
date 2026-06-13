use std::time::Duration;

use tempfile::tempdir;
use tsink_uniffi::*;

fn label(name: &str, value: &str) -> ULabel {
    ULabel {
        name: name.into(),
        value: value.into(),
    }
}

fn row(metric: &str, labels: Vec<ULabel>, timestamp: i64, value: UValue) -> URow {
    URow {
        metric: metric.into(),
        labels,
        data_point: UDataPoint { value, timestamp },
    }
}

#[test]
fn test_full_lifecycle() {
    let builder = TsinkStorageBuilder::new();
    builder.with_wal_enabled(false).unwrap();

    let db = builder.build().unwrap();
    let rows = vec![
        URow {
            metric: "cpu.usage".into(),
            labels: vec![ULabel {
                name: "host".into(),
                value: "server1".into(),
            }],
            data_point: UDataPoint {
                value: UValue::F64 { v: 75.5 },
                timestamp: 1000,
            },
        },
        URow {
            metric: "cpu.usage".into(),
            labels: vec![ULabel {
                name: "host".into(),
                value: "server1".into(),
            }],
            data_point: UDataPoint {
                value: UValue::F64 { v: 82.3 },
                timestamp: 2000,
            },
        },
        URow {
            metric: "cpu.usage".into(),
            labels: vec![ULabel {
                name: "host".into(),
                value: "server2".into(),
            }],
            data_point: UDataPoint {
                value: UValue::F64 { v: 55.0 },
                timestamp: 1500,
            },
        },
    ];
    db.insert_rows(rows).unwrap();
    let results = db
        .select(
            "cpu.usage".into(),
            vec![ULabel {
                name: "host".into(),
                value: "server1".into(),
            }],
            0,
            3000,
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].timestamp, 1000);
    assert_eq!(results[1].timestamp, 2000);
    let all = db.select_all("cpu.usage".into(), 0, 3000).unwrap();
    assert_eq!(all.len(), 2);
    let opts = UQueryOptions {
        labels: vec![ULabel {
            name: "host".into(),
            value: "server1".into(),
        }],
        start: 0,
        end: 3000,
        aggregation: UAggregation::None,
        downsample: None,
        limit: Some(1),
        offset: 0,
    };
    let limited = db.select_with_options("cpu.usage".into(), opts).unwrap();
    assert_eq!(limited.len(), 1);
    let metrics = db.list_metrics().unwrap();
    assert!(!metrics.is_empty());
    assert!(metrics.iter().any(|m| m.name == "cpu.usage"));
    let _used = db.memory_used();
    let _budget = db.memory_budget();
    db.close().unwrap();
}

#[test]
fn test_select_series() {
    let builder = TsinkStorageBuilder::new();
    builder.with_wal_enabled(false).unwrap();
    let db = builder.build().unwrap();

    db.insert_rows(vec![
        URow {
            metric: "mem.free".into(),
            labels: vec![ULabel {
                name: "host".into(),
                value: "a".into(),
            }],
            data_point: UDataPoint {
                value: UValue::I64 { v: 1024 },
                timestamp: 100,
            },
        },
        URow {
            metric: "disk.io".into(),
            labels: vec![],
            data_point: UDataPoint {
                value: UValue::U64 { v: 500 },
                timestamp: 100,
            },
        },
    ])
    .unwrap();

    let selection = USeriesSelection {
        metric: Some("mem.free".into()),
        matchers: vec![],
        start: None,
        end: None,
    };
    let series = db.select_series(selection).unwrap();
    assert!(!series.is_empty());
    assert!(series.iter().all(|s| s.name == "mem.free"));

    db.close().unwrap();
}

#[test]
fn test_builder_with_data_path() {
    let dir = tempfile::tempdir().unwrap();
    let builder = TsinkStorageBuilder::new();
    builder
        .with_data_path(dir.path().to_str().unwrap().into())
        .unwrap();
    builder.with_wal_enabled(false).unwrap();

    let db = builder.build().unwrap();

    db.insert_rows(vec![URow {
        metric: "test".into(),
        labels: vec![],
        data_point: UDataPoint {
            value: UValue::F64 { v: 1.0 },
            timestamp: 100,
        },
    }])
    .unwrap();

    let results = db.select("test".into(), vec![], 0, 200).unwrap();
    assert_eq!(results.len(), 1);

    db.close().unwrap();
}

#[test]
fn test_builder_consume_once_semantics() {
    let builder = TsinkStorageBuilder::new();
    let _db = builder.build().unwrap();
    let err = builder.build().unwrap_err();
    assert!(err.to_string().contains("already consumed"));
}

#[test]
fn test_empty_select_returns_empty() {
    let builder = TsinkStorageBuilder::new();
    builder.with_wal_enabled(false).unwrap();
    let db = builder.build().unwrap();

    let result = db.select("nonexistent".into(), vec![], 0, 1000).unwrap();
    assert!(result.is_empty());

    db.close().unwrap();
}

#[test]
fn test_value_types() {
    let builder = TsinkStorageBuilder::new();
    builder.with_wal_enabled(false).unwrap();
    let db = builder.build().unwrap();

    let rows = vec![
        URow {
            metric: "test.f64".into(),
            labels: vec![],
            data_point: UDataPoint {
                value: UValue::F64 { v: 3.125 },
                timestamp: 1,
            },
        },
        URow {
            metric: "test.i64".into(),
            labels: vec![],
            data_point: UDataPoint {
                value: UValue::I64 { v: -42 },
                timestamp: 1,
            },
        },
        URow {
            metric: "test.u64".into(),
            labels: vec![],
            data_point: UDataPoint {
                value: UValue::U64 { v: 999 },
                timestamp: 1,
            },
        },
        URow {
            metric: "test.bool".into(),
            labels: vec![],
            data_point: UDataPoint {
                value: UValue::Bool { v: true },
                timestamp: 1,
            },
        },
        URow {
            metric: "test.bytes".into(),
            labels: vec![],
            data_point: UDataPoint {
                value: UValue::Bytes {
                    v: vec![0xDE, 0xAD],
                },
                timestamp: 1,
            },
        },
        URow {
            metric: "test.str".into(),
            labels: vec![],
            data_point: UDataPoint {
                value: UValue::Str { v: "hello".into() },
                timestamp: 1,
            },
        },
    ];
    db.insert_rows(rows).unwrap();
    let r = db.select("test.f64".into(), vec![], 0, 10).unwrap();
    assert!(matches!(r[0].value, UValue::F64 { v } if (v - 3.125).abs() < 1e-10));
    let r = db.select("test.i64".into(), vec![], 0, 10).unwrap();
    assert!(matches!(r[0].value, UValue::I64 { v: -42 }));
    let r = db.select("test.bool".into(), vec![], 0, 10).unwrap();
    assert!(matches!(r[0].value, UValue::Bool { v: true }));
    let r = db.select("test.str".into(), vec![], 0, 10).unwrap();
    assert!(matches!(&r[0].value, UValue::Str { v } if v == "hello"));

    db.close().unwrap();
}

#[test]
fn test_embedded_surface_snapshot_shards_and_restore() {
    let data_dir = tempdir().unwrap();
    let snapshot_dir = tempdir().unwrap();
    let restored_dir = tempdir().unwrap();

    let builder = TsinkStorageBuilder::new();
    builder
        .with_data_path(data_dir.path().to_str().unwrap().into())
        .unwrap();
    builder
        .with_timestamp_precision(UTimestampPrecision::Seconds)
        .unwrap();
    builder
        .with_wal_sync_mode(UWalSyncMode::Periodic {
            interval: Duration::from_secs(60),
        })
        .unwrap();
    builder
        .with_wal_replay_mode(UWalReplayMode::Salvage)
        .unwrap();
    builder.with_background_fail_fast(false).unwrap();
    builder.with_metadata_shard_count(1).unwrap();

    let db = builder.build().unwrap();
    let labels_a = vec![label("host", "a")];
    let labels_b = vec![label("host", "b")];
    let rows = vec![
        row("cpu_usage", labels_a.clone(), 10, UValue::F64 { v: 1.0 }),
        row("cpu_usage", labels_a.clone(), 20, UValue::F64 { v: 2.0 }),
        row("cpu_usage", labels_b.clone(), 15, UValue::F64 { v: 3.0 }),
    ];

    let write_result = db.insert_rows_with_result(rows).unwrap();
    assert!(matches!(
        write_result.acknowledgement,
        UWriteAcknowledgement::Appended
    ));

    let series = vec![
        UMetricSeries {
            name: "cpu_usage".into(),
            labels: labels_a.clone(),
        },
        UMetricSeries {
            name: "cpu_usage".into(),
            labels: labels_b.clone(),
        },
    ];
    let selected = db.select_many(series.clone(), 0, 100).unwrap();
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].points.len(), 2);
    assert_eq!(selected[1].points.len(), 1);

    let scope = UMetadataShardScope {
        shard_count: 1,
        shards: vec![0],
    };
    let shard_metrics = db.list_metrics_in_shards(scope.clone()).unwrap();
    assert_eq!(shard_metrics.len(), 2);

    let shard_selection = db
        .select_series_in_shards(
            USeriesSelection {
                metric: Some("cpu_usage".into()),
                matchers: vec![],
                start: None,
                end: None,
            },
            scope,
        )
        .unwrap();
    assert_eq!(shard_selection.len(), 2);

    let digest = db.compute_shard_window_digest(0, 1, 0, 100).unwrap();
    assert_eq!(digest.series_count, 2);
    assert_eq!(digest.point_count, 3);

    let first_window_page = db
        .scan_shard_window_rows(
            0,
            1,
            0,
            100,
            UShardWindowScanOptions {
                max_series: None,
                max_rows: Some(2),
                row_offset: None,
            },
        )
        .unwrap();
    assert_eq!(first_window_page.rows.len(), 2);
    assert!(first_window_page.truncated);
    let second_window_page = db
        .scan_shard_window_rows(
            0,
            1,
            0,
            100,
            UShardWindowScanOptions {
                max_series: None,
                max_rows: None,
                row_offset: first_window_page.next_row_offset,
            },
        )
        .unwrap();
    assert_eq!(second_window_page.rows.len(), 1);

    let metric_rows = db
        .scan_metric_rows(
            "cpu_usage".into(),
            0,
            100,
            UQueryRowsScanOptions {
                max_rows: None,
                row_offset: None,
            },
        )
        .unwrap();
    assert_eq!(metric_rows.rows.len(), 3);

    let series_rows = db
        .scan_series_rows(
            series,
            0,
            100,
            UQueryRowsScanOptions {
                max_rows: None,
                row_offset: None,
            },
        )
        .unwrap();
    assert_eq!(series_rows.rows.len(), 3);

    let snapshot_path = snapshot_dir.path().join("db.snapshot");
    db.snapshot(snapshot_path.to_str().unwrap().into()).unwrap();
    let snapshot = db.observability_snapshot();
    assert!(!snapshot.health.fail_fast_enabled);
    assert!(snapshot.wal.enabled);

    db.close().unwrap();

    restore_from_snapshot(
        snapshot_path.to_str().unwrap().into(),
        restored_dir.path().to_str().unwrap().into(),
    )
    .unwrap();

    let restored_builder = TsinkStorageBuilder::new();
    restored_builder
        .with_data_path(restored_dir.path().to_str().unwrap().into())
        .unwrap();
    restored_builder
        .with_timestamp_precision(UTimestampPrecision::Seconds)
        .unwrap();
    let restored = restored_builder.build().unwrap();
    let restored_points = restored
        .select("cpu_usage".into(), labels_a, 0, 100)
        .unwrap();
    assert_eq!(restored_points.len(), 2);
    restored.close().unwrap();
}

#[test]
fn test_rollups_delete_and_observability_are_exposed() {
    let data_dir = tempdir().unwrap();

    let builder = TsinkStorageBuilder::new();
    builder
        .with_data_path(data_dir.path().to_str().unwrap().into())
        .unwrap();
    builder
        .with_timestamp_precision(UTimestampPrecision::Milliseconds)
        .unwrap();
    builder.with_wal_enabled(false).unwrap();

    let db = builder.build().unwrap();
    let labels = vec![label("host", "a")];
    db.insert_rows(vec![
        row("cpu_usage", labels.clone(), 0, UValue::F64 { v: 1.0 }),
        row("cpu_usage", labels.clone(), 1_000, UValue::F64 { v: 3.0 }),
        row("cpu_usage", labels.clone(), 2_000, UValue::F64 { v: 5.0 }),
        row("cpu_usage", labels.clone(), 3_000, UValue::F64 { v: 7.0 }),
    ])
    .unwrap();

    let rollups = db
        .apply_rollup_policies(vec![URollupPolicy {
            id: "cpu_2s_avg".into(),
            metric: "cpu_usage".into(),
            match_labels: vec![],
            interval: 2_000,
            aggregation: UAggregation::Avg,
            bucket_origin: 0,
        }])
        .unwrap();
    assert_eq!(rollups.policies.len(), 1);
    assert_eq!(rollups.policies[0].matched_series, 1);
    assert_eq!(rollups.policies[0].materialized_series, 1);

    let points = db
        .select_with_options(
            "cpu_usage".into(),
            UQueryOptions {
                labels: labels.clone(),
                start: 0,
                end: 4_000,
                aggregation: UAggregation::Avg,
                downsample: Some(UDownsampleOptions { interval: 2_000 }),
                limit: None,
                offset: 0,
            },
        )
        .unwrap();
    assert_eq!(points.len(), 2);
    assert!(matches!(points[0].value, UValue::F64 { v } if (v - 2.0).abs() < 1e-12));
    assert!(matches!(points[1].value, UValue::F64 { v } if (v - 6.0).abs() < 1e-12));

    let snapshot = db.observability_snapshot();
    assert_eq!(snapshot.rollups.policies.len(), 1);
    assert!(snapshot.query.rollup_query_plans_total >= 1);

    let delete_result = db
        .delete_series(USeriesSelection {
            metric: Some("cpu_usage".into()),
            matchers: vec![USeriesMatcher {
                name: "host".into(),
                op: USeriesMatcherOp::Equal,
                value: "a".into(),
            }],
            start: None,
            end: None,
        })
        .unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);
    assert!(db
        .select("cpu_usage".into(), labels, 0, 4_000)
        .unwrap()
        .is_empty());

    let rerun = db.trigger_rollup_run().unwrap();
    assert_eq!(rerun.policies.len(), 1);

    db.close().unwrap();
}

#[test]
fn test_tiered_remote_builder_configuration_is_exposed() {
    let data_dir = tempdir().unwrap();
    let object_store_dir = tempdir().unwrap();

    let builder = TsinkStorageBuilder::new();
    builder
        .with_data_path(data_dir.path().to_str().unwrap().into())
        .unwrap();
    builder
        .with_object_store_path(object_store_dir.path().to_str().unwrap().into())
        .unwrap();
    builder.with_retention(Duration::from_secs(600)).unwrap();
    builder
        .with_tiered_retention_policy(Duration::from_secs(60), Duration::from_secs(300))
        .unwrap();
    builder
        .with_runtime_mode(UStorageRuntimeMode::ReadWrite)
        .unwrap();
    builder
        .with_remote_segment_cache_policy(URemoteSegmentCachePolicy::MetadataOnly)
        .unwrap();
    builder
        .with_remote_segment_refresh_interval(Duration::from_millis(25))
        .unwrap();
    builder
        .with_mirror_hot_segments_to_object_store(true)
        .unwrap();
    builder.with_wal_enabled(false).unwrap();

    let db = builder.build().unwrap();
    let snapshot = db.observability_snapshot();
    assert!(matches!(
        snapshot.remote.runtime_mode,
        UStorageRuntimeMode::ReadWrite
    ));
    assert!(matches!(
        snapshot.remote.cache_policy,
        URemoteSegmentCachePolicy::MetadataOnly
    ));
    assert_eq!(snapshot.remote.metadata_refresh_interval_ms, 25);
    assert!(snapshot.remote.mirror_hot_segments);
    db.close().unwrap();
}
