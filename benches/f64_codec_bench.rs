use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use tsink::engine::encoder::Encoder;
use tsink::{DataPoint, Value};

fn generate_sensor_temperatures(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            let base = 21.5;
            let step = (i as f64) * 0.05;
            let cycle = ((i % 100) as f64) * 0.1;
            DataPoint::new(i as i64 * 1000, Value::F64(base + step + cycle))
        })
        .collect()
}

fn generate_financial_prices(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            let price = 150.25 + ((i % 50) as f64) * 0.01;
            DataPoint::new(i as i64 * 1000, Value::F64(price))
        })
        .collect()
}

fn generate_random_floats(count: usize) -> Vec<DataPoint> {
    let mut state = 123456789u64;
    (0..count)
        .map(|i| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let val = (state as f64) / (u64::MAX as f64);
            DataPoint::new(i as i64 * 1000, Value::F64(val))
        })
        .collect()
}

fn bench_f64_codecs(c: &mut Criterion) {
    let counts = [1024, 8192];

    for count in counts {
        let datasets = [
            ("sensor_telemetry", generate_sensor_temperatures(count)),
            ("financial_prices", generate_financial_prices(count)),
            ("random_floats", generate_random_floats(count)),
        ];

        for (name, points) in datasets {
            let mut group = c.benchmark_group(format!("{name}_{count}"));
            group.throughput(Throughput::Elements(count as u64));

            let gorilla_encoded = Encoder::encode_f64_gorilla(&points).unwrap();
            let alp_encoded = Encoder::encode_f64_alp(&points).unwrap();

            eprintln!(
                "[{name} N={count}] Gorilla: {} bytes ({:.2} bits/val) | fastalp: {} bytes ({:.2} bits/val) | Ratio: {:.2}x",
                gorilla_encoded.len(),
                (gorilla_encoded.len() * 8) as f64 / count as f64,
                alp_encoded.len(),
                (alp_encoded.len() * 8) as f64 / count as f64,
                gorilla_encoded.len() as f64 / alp_encoded.len() as f64,
            );

            group.bench_function(BenchmarkId::new("encode", "gorilla"), |b| {
                b.iter(|| black_box(Encoder::encode_f64_gorilla(black_box(&points)).unwrap()))
            });

            group.bench_function(BenchmarkId::new("encode", "fastalp"), |b| {
                b.iter(|| black_box(Encoder::encode_f64_alp(black_box(&points)).unwrap()))
            });

            group.bench_function(BenchmarkId::new("decode", "gorilla"), |b| {
                b.iter(|| {
                    black_box(
                        Encoder::decode_f64_gorilla(black_box(&gorilla_encoded), count).unwrap(),
                    )
                })
            });

            group.bench_function(BenchmarkId::new("decode", "fastalp"), |b| {
                b.iter(|| {
                    black_box(Encoder::decode_f64_alp(black_box(&alp_encoded), count).unwrap())
                })
            });

            group.finish();
        }
    }
}

criterion_group!(benches, bench_f64_codecs);
criterion_main!(benches);
