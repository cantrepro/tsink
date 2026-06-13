//! Benchmarks for tsink storage operations
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision};

/// Benchmark storage insertions
fn bench_insert_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_rows");

    for size in [1, 10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let storage = StorageBuilder::new()
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .build()
                .unwrap();

            let rows: Vec<Row> = (0..size)
                .map(|i| {
                    Row::new(
                        "bench_metric",
                        DataPoint::new(1600000000 + i as i64, i as f64),
                    )
                })
                .collect();

            b.iter(|| {
                storage.insert_rows(black_box(&rows)).unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmark selecting among thousands of points
fn bench_select_among_thousand_points(c: &mut Criterion) {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let rows: Vec<Row> = (0..1000)
        .map(|i| {
            Row::new(
                "select_metric",
                DataPoint::new(1600000000 + i as i64, i as f64),
            )
        })
        .collect();

    storage.insert_rows(&rows).unwrap();

    c.bench_function("select_1000_points", |b| {
        let mut points = Vec::with_capacity(1024);
        b.iter(|| {
            storage
                .select_into(
                    black_box("select_metric"),
                    black_box(&[]),
                    black_box(1600000000),
                    black_box(1600001000),
                    &mut points,
                )
                .unwrap();
            black_box(&points);
        });
    });
}

/// Benchmark selecting among millions of points
fn bench_select_among_million_points(c: &mut Criterion) {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(150 * 3600))
        .build()
        .unwrap();

    for batch in 0..1000 {
        let rows: Vec<Row> = (0..1000)
            .map(|i| {
                let idx = batch * 1000 + i;
                Row::new(
                    "million_metric",
                    DataPoint::new(1600000000 + idx as i64, idx as f64),
                )
            })
            .collect();

        storage.insert_rows(&rows).unwrap();
    }

    let _all_points = storage
        .select("million_metric", &[], 1600000000, 1700000000)
        .unwrap();

    c.bench_function("select_1M_points", |b| {
        let mut points = Vec::with_capacity(1024);
        b.iter(|| {
            storage
                .select_into(
                    black_box("million_metric"),
                    black_box(&[]),
                    black_box(1600500000),
                    black_box(1600501000),
                    &mut points,
                )
                .unwrap();
            black_box(&points);
        });
    });
}

/// Benchmark concurrent insertions
fn bench_concurrent_insertions(c: &mut Criterion) {
    c.bench_function("concurrent_insert_10_threads", |b| {
        b.iter(|| {
            let storage = Arc::new(
                StorageBuilder::new()
                    .with_timestamp_precision(TimestampPrecision::Seconds)
                    .build()
                    .unwrap(),
            );

            let mut handles = vec![];

            for thread_id in 0..10 {
                let storage = storage.clone();
                let handle = thread::spawn(move || {
                    for i in 0..100 {
                        let row = Row::new(
                            "concurrent",
                            DataPoint::new(1600000000 + (thread_id * 100 + i) as i64, i as f64),
                        );
                        storage.insert_rows(&[row]).unwrap();
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

/// Benchmark operations with labels
fn bench_with_labels(c: &mut Criterion) {
    let mut group = c.benchmark_group("with_labels");

    group.bench_function("insert_with_labels", |b| {
        let storage = StorageBuilder::new().build().unwrap();

        b.iter(|| {
            let row = Row::with_labels(
                "labeled_metric",
                vec![
                    Label::new("host", "server1"),
                    Label::new("region", "us-east"),
                    Label::new("env", "production"),
                ],
                DataPoint::new(1600000000, 42.0),
            );
            storage.insert_rows(&[row]).unwrap();
        });
    });

    group.bench_function("select_with_labels", |b| {
        let storage = StorageBuilder::new().build().unwrap();

        for i in 0..100 {
            let row = Row::with_labels(
                "labeled_metric",
                vec![
                    Label::new("host", format!("server{}", i % 10)),
                    Label::new("region", if i % 2 == 0 { "us-east" } else { "us-west" }),
                ],
                DataPoint::new(1600000000 + i as i64, i as f64),
            );
            storage.insert_rows(&[row]).unwrap();
        }

        b.iter(|| {
            let points = storage.select(
                black_box("labeled_metric"),
                black_box(&[
                    Label::new("host", "server5"),
                    Label::new("region", "us-east"),
                ]),
                black_box(1600000000),
                black_box(1600000100),
            );
            let _ = black_box(points);
        });
    });

    group.finish();
}

/// Benchmark different timestamp precisions
fn bench_timestamp_precisions(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp_precision");

    for precision in [
        TimestampPrecision::Seconds,
        TimestampPrecision::Milliseconds,
        TimestampPrecision::Microseconds,
        TimestampPrecision::Nanoseconds,
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", precision)),
            &precision,
            |b, precision| {
                let storage = StorageBuilder::new()
                    .with_timestamp_precision(*precision)
                    .build()
                    .unwrap();

                let rows: Vec<Row> = (0..100)
                    .map(|i| {
                        Row::new(
                            "precision_metric",
                            DataPoint::new(1600000000 + i as i64, i as f64),
                        )
                    })
                    .collect();

                b.iter(|| {
                    storage.insert_rows(black_box(&rows)).unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark memory vs disk partitions
fn bench_partition_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_types");

    group.bench_function("memory_partition", |b| {
        let storage = StorageBuilder::new().build().unwrap();

        let rows: Vec<Row> = (0..100)
            .map(|i| {
                Row::new(
                    "memory_metric",
                    DataPoint::new(1600000000 + i as i64, i as f64),
                )
            })
            .collect();

        b.iter(|| {
            storage.insert_rows(black_box(rows.as_slice())).unwrap();
        });
    });

    group.bench_function("disk_partition", |b| {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .build()
            .unwrap();

        let rows: Vec<Row> = (0..100)
            .map(|i| {
                Row::new(
                    "disk_metric",
                    DataPoint::new(1600000000 + i as i64, i as f64),
                )
            })
            .collect();

        b.iter(|| {
            storage.insert_rows(black_box(rows.as_slice())).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_rows,
    bench_select_among_thousand_points,
    bench_select_among_million_points,
    bench_concurrent_insertions,
    bench_with_labels,
    bench_timestamp_precisions,
    bench_partition_types
);

criterion_main!(benches);
