//! Focused benchmarks for `insert_rows` and `select`.
//!
//! Run all cases:
//!   cargo bench --bench storage_benchmarks -- "^(insert_rows|select|concurrent_rw)/"
//!
//! Quick check:
//!   cargo bench --bench storage_benchmarks -- "^(insert_rows|select|concurrent_rw)/" --quick --noplot

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tsink::{DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision};

const BASE_TS: i64 = 1_700_000_000;
const LARGE_SELECT_SIZE: usize = 1_000_000;
const LOAD_BATCH_SIZE: usize = 10_000;
const CONCURRENT_READ_POINTS: usize = 1000;
const CONCURRENT_WRITES_PER_WRITER: usize = 1000;
const CONCURRENT_READS_PER_READER: usize = 1000;
const CONCURRENT_WRITE_TS_OFFSET: i64 = 10_000_000;

fn build_storage() -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        // Keep all generated points in one partition to reduce cross-partition variance.
        .with_partition_duration(Duration::from_secs(10 * 365 * 24 * 60 * 60))
        .with_wal_enabled(false)
        .build()
        .unwrap()
}

fn make_rows(metric: &str, start_ts: i64, count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            let ts = start_ts + i as i64;
            Row::new(metric, DataPoint::new(ts, i as f64))
        })
        .collect()
}

fn load_series(storage: &Arc<dyn Storage>, metric: &str, points: usize) {
    let mut loaded = 0usize;
    while loaded < points {
        let chunk = (points - loaded).min(LOAD_BATCH_SIZE);
        let rows = make_rows(metric, BASE_TS + loaded as i64, chunk);
        storage.insert_rows(&rows).unwrap();
        loaded += chunk;
    }
}

fn bench_insert_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_rows");

    for size in [1usize, 10, 1000] {
        let rows = make_rows("insert_metric", BASE_TS, size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, {
            let rows = rows.clone();
            move |b, &_size| {
                b.iter_batched(
                    build_storage,
                    |storage| {
                        storage.insert_rows(black_box(rows.as_slice())).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }

    group.finish();
}

fn bench_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("select");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(8));

    for size in [1usize, 10, 1000, LARGE_SELECT_SIZE] {
        let metric = format!("select_metric_{size}");
        let storage = build_storage();
        load_series(&storage, &metric, size);

        let start = BASE_TS;
        let end = BASE_TS + size as i64;

        let mut warmup = Vec::with_capacity(size);
        storage
            .select_into(metric.as_str(), &[], start, end, &mut warmup)
            .unwrap();
        assert_eq!(warmup.len(), size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, {
            let metric = metric.clone();
            move |b, &_size| {
                let labels: &[Label] = &[];
                let mut out = Vec::with_capacity(size);
                b.iter(|| {
                    storage
                        .select_into(
                            black_box(metric.as_str()),
                            labels,
                            black_box(start),
                            black_box(end),
                            &mut out,
                        )
                        .unwrap();
                    black_box(out.len());
                });
            }
        });
    }

    group.finish();
}

fn prepare_concurrent_storage() -> Arc<dyn Storage> {
    let storage = build_storage();
    load_series(&storage, "concurrent_read_metric", CONCURRENT_READ_POINTS);
    storage
}

fn run_concurrent_rw_once(storage: Arc<dyn Storage>, writers: usize, readers: usize) {
    let total_threads = writers + readers;
    let barrier = Arc::new(Barrier::new(total_threads + 1));
    let mut handles = Vec::with_capacity(total_threads);

    for writer_id in 0..writers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let writer_base = BASE_TS
                + CONCURRENT_WRITE_TS_OFFSET
                + (writer_id as i64 * CONCURRENT_WRITES_PER_WRITER as i64);
            let mut row = Row::new(
                "concurrent_write_metric",
                DataPoint::new(writer_base, writer_id as f64),
            );

            barrier.wait();
            for i in 0..CONCURRENT_WRITES_PER_WRITER {
                row.set_data_point(DataPoint::new(writer_base + i as i64, i as f64));
                storage.insert_rows(std::slice::from_ref(&row)).unwrap();
            }
        }));
    }

    for _ in 0..readers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut out = Vec::with_capacity(CONCURRENT_READ_POINTS);
            let read_start = BASE_TS;
            let read_end = BASE_TS + CONCURRENT_READ_POINTS as i64;

            barrier.wait();
            for _ in 0..CONCURRENT_READS_PER_READER {
                storage
                    .select_into(
                        "concurrent_read_metric",
                        &[],
                        read_start,
                        read_end,
                        &mut out,
                    )
                    .unwrap();
                black_box(out.len());
            }
        }));
    }

    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }
}

fn bench_concurrent_rw(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_rw");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for (writers, readers) in [(1usize, 1usize), (2, 2), (4, 4)] {
        let id = format!("w{writers}_r{readers}");
        let operations =
            (writers * CONCURRENT_WRITES_PER_WRITER + readers * CONCURRENT_READS_PER_READER) as u64;

        group.throughput(Throughput::Elements(operations));
        group.bench_with_input(
            BenchmarkId::new("mix", id),
            &(writers, readers),
            |b, &(w, r)| {
                b.iter_batched(
                    prepare_concurrent_storage,
                    |storage| run_concurrent_rw_once(storage, w, r),
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_rows,
    bench_select,
    bench_concurrent_rw
);
criterion_main!(benches);
