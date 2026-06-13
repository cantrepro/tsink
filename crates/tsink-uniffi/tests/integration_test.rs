use tsink_uniffi::*;

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
    assert_eq!(all.len(), 2); // two label sets
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
                value: UValue::F64 { v: 3.14 },
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
    assert!(matches!(r[0].value, UValue::F64 { v } if (v - 3.14).abs() < 1e-10));
    let r = db.select("test.i64".into(), vec![], 0, 10).unwrap();
    assert!(matches!(r[0].value, UValue::I64 { v: -42 }));
    let r = db.select("test.bool".into(), vec![], 0, 10).unwrap();
    assert!(matches!(r[0].value, UValue::Bool { v: true }));
    let r = db.select("test.str".into(), vec![], 0, 10).unwrap();
    assert!(matches!(&r[0].value, UValue::Str { v } if v == "hello"));

    db.close().unwrap();
}
