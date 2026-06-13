# Data Types & Native Histograms

This document describes every type that can be stored in a tsink data point, how mixed types are handled, how timestamps are represented, the limits that apply to metrics and labels, and how custom user types are embedded inside the `bytes` lane.

---

## Table of Contents

1. [Core Data Model](#core-data-model)
2. [Value Types](#value-types)
   - [float64](#float64)
   - [int64](#int64)
   - [uint64](#uint64)
   - [bool](#bool)
   - [bytes](#bytes)
   - [string](#string)
   - [Native Histogram](#native-histogram)
3. [Value Lanes](#value-lanes)
4. [Encoding Codecs by Type](#encoding-codecs-by-type)
5. [Timestamp Precision](#timestamp-precision)
6. [Metrics and Labels](#metrics-and-labels)
7. [Type Coercions](#type-coercions)
8. [Custom Types via the Codec Trait](#custom-types-via-the-codec-trait)
9. [Aggregation](#aggregation)
10. [Python Bindings Type Mapping](#python-bindings-type-mapping)

---

## Core Data Model

The smallest unit of storage is a **`DataPoint`**, which pairs a typed value with an `i64` timestamp. Multiple data points for the same named metric are grouped into a **`Row`**:

```rust
pub struct DataPoint {
    pub value: Value,
    pub timestamp: i64,
}

pub struct Row {
    // metric name
    // labels (key-value pairs)
    // data_point
}
```

A **series** is identified by a metric name together with the full, sorted set of label key-value pairs. Two rows with the same metric name but different labels belong to different series.

---

## Value Types

All sample payloads are represented by the `Value` enum:

```rust
pub enum Value {
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Bytes(Vec<u8>),
    String(String),
    Histogram(Box<NativeHistogram>),
}
```

Every variant in a chunk must belong to the same **value family** (see [Value Lanes](#value-lanes)). Mixing different numeric variants, or mixing a numeric variant with a blob variant, in the same write batch returns a `ValueTypeMismatch` error.

### float64

`Value::F64(f64)` is the default numeric type and the only type directly consumed by the PromQL evaluator. Use it for gauges, counters, latency histograms expressed as raw floats, and any metric that needs PromQL processing.

NaN is a valid stored value and is preserved across encoding and decoding. NaN equality is defined as `NaN == NaN` for storage index purposes (deduplication), which diverges from IEEE 754.

### int64

`Value::I64(i64)` covers signed counters, monotonic event IDs, and any domain where integer semantics matter (no rounding at extreme values). The codec is ZigZag-encoded delta bitpack, which is efficient for slowly changing integers.

When queried via `as_f64()`, the conversion succeeds only if the integer can be represented exactly as a 64-bit float (i.e., the absolute value fits in 53 bits). Large values like `i64::MAX` return `None` rather than silently rounding.

### uint64

`Value::U64(u64)` covers unsigned accumulators and bitmask values. The codec is delta-bitpack (no ZigZag needed for non-negative deltas). The same exactness rule as `i64` applies when converting to `f64`.

### bool

`Value::Bool(bool)` is stored with a bitpack codec — one bit per sample. It is not coercible to `f64`; use a `0.0`/`1.0` float if you need PromQL arithmetic on boolean flags.

### bytes

`Value::Bytes(Vec<u8>)` is the escape hatch for any payload that does not fit into a numeric type — serialized Protobuf, MessagePack, JSON, or any custom binary encoding. No interpretation is done on the byte payload by the engine.

`string` and `bytes` share the same blob value lane and the same on-disk codec (bytes delta block). From the engine's perspective they are interchangeable; the distinction is only at the API boundary.

Custom Rust types can be embedded as `bytes` via the `Codec` trait — see [Custom Types via the Codec Trait](#custom-types-via-the-codec-trait).

### string

`Value::String(String)` stores UTF-8 text. Like `bytes`, it is stored on the blob lane using the bytes delta block codec and does not participate in numeric aggregation.

### Native Histogram

`Value::Histogram(Box<NativeHistogram>)` stores a complete Prometheus-compatible native histogram sample. Histograms use the blob lane and are serialized with the bytes delta block codec.

#### NativeHistogram Structure

```rust
pub struct NativeHistogram {
    // Total sample count. Either an integer or a float (for weighted observations).
    pub count: Option<HistogramCount>,
    // Sum of all observed values.
    pub sum: f64,
    // Exponential bucket schema (-4..=8, or schema 0 for custom buckets).
    pub schema: i32,
    // Half-width of the zero bucket.
    pub zero_threshold: f64,
    // Count of samples in the zero bucket.
    pub zero_count: Option<HistogramCount>,

    // Negative-side sparse buckets.
    pub negative_spans: Vec<HistogramBucketSpan>,
    // Delta-encoded bucket counts (integer mode).
    pub negative_deltas: Vec<i64>,
    // Absolute bucket counts (float mode).
    pub negative_counts: Vec<f64>,

    // Positive-side sparse buckets.
    pub positive_spans: Vec<HistogramBucketSpan>,
    pub positive_deltas: Vec<i64>,
    pub positive_counts: Vec<f64>,

    // Reset hint communicated to downstream consumers.
    pub reset_hint: HistogramResetHint,
    // Explicit bucket boundaries for custom schema (schema = -53).
    pub custom_values: Vec<f64>,
}
```

**`HistogramCount`** — count and zero count can be either integer or float to accommodate both classical integer counting and weighted/scaled histograms:

```rust
pub enum HistogramCount {
    Int(u64),
    Float(f64),
}
```

**`HistogramBucketSpan`** — describes a contiguous run of populated buckets in the sparse representation:

```rust
pub struct HistogramBucketSpan {
    pub offset: i32,  // gap in bucket index from the previous span's end
    pub length: u32,  // number of consecutive populated buckets
}
```

**`HistogramResetHint`** — indicates whether the histogram was reset before this sample:

| Variant | Meaning |
|---|---|
| `Unknown` | Reset status is not known |
| `Yes` | A reset definitely occurred |
| `No` | No reset occurred |
| `Gauge` | This is a gauge histogram (not accumulated) |

#### Sparse bucket encoding

Buckets are stored sparsely as a sequence of (span, deltas/counts) pairs. The `negative_spans`/`positive_spans` arrays describe which bucket slots are populated; `negative_deltas`/`positive_deltas` give the delta-encoded integer counts for each slot; `negative_counts`/`positive_counts` give the absolute float counts (used instead of deltas when any count is non-integer).

Either the `*_deltas` fields or the `*_counts` fields are populated for a given sample — not both. When float counts are present, `*_deltas` should be empty, and vice versa.

#### NaN semantics

For purposes of stored equality (used by deduplication and the WAL), NaN values inside a histogram are considered equal to other NaN values of the same sign.

---

## Value Lanes

Internally, every series is assigned to one of two mutually exclusive **value lanes** based on the type of its first ingested sample:

| Lane | Types | On-disk directory |
|---|---|---|
| `Numeric` | `f64`, `i64`, `u64`, `bool` | `lane_numeric/` |
| `Blob` | `bytes`, `string`, `NativeHistogram` | `lane_blob/` |

The lane is derived at ingest time and persisted in the series registry. Once a series is assigned to a lane, all subsequent writes must use a compatible value type. Writing a numeric type to a blob-lane series, or vice versa, returns a `ValueTypeMismatch` error.

Keeping numeric and blob data physically separate allows their compaction jobs to run independently and avoids mixing integer/float codecs with variable-length blob codecs in the same segment file.

---

## Encoding Codecs by Type

tsink selects the most compact codec automatically for each chunk at flush time. The codec choice is stored in the chunk header and used verbatim during reads — no re-encoding occurs on read.

**Timestamp codecs** are chosen independently from value codecs:

| Codec | When selected |
|---|---|
| `FixedStepRle` | All timestamps are evenly spaced (constant scrape interval) |
| `DeltaOfDeltaBitpack` | Timestamps have a slowly drifting interval |
| `DeltaVarint` | Irregular timestamps; always applicable as a fallback |

**Value codecs** by type:

| Type | Codec | Notes |
|---|---|---|
| `f64` | Gorilla XOR | Facebook Gorilla XOR-based float compression |
| `i64` | ZigZag delta bitpack | Maps signed deltas to unsigned, then bitpacks |
| `u64` | Delta bitpack | Non-negative deltas bitpacked directly |
| `bool` | Bit-pack | 1 bit per sample |
| `bytes` / `string` / `NativeHistogram` | Bytes delta block | Variable-length records with delta compression |
| Any type | Constant RLE | Applied when all values in a chunk are identical; takes priority over type-specific codecs |

The engine evaluates all applicable candidates for a given chunk and selects the one producing the smallest payload.

---

## Timestamp Precision

The `TimestampPrecision` configuration setting tells the engine how to interpret the `i64` timestamp in each `DataPoint`:

| Variant | Unit | Maximum date |
|---|---|---|
| `Nanoseconds` | 1 ns | ~2262 |
| `Microseconds` | 1 µs | ~294246 |
| `Milliseconds` | 1 ms | ~292278994 |
| `Seconds` | 1 s | ~292277026596 |

Precision is configured once on `StorageBuilder` and applies to all timestamps written to that storage instance. Mixing precisions within a single instance is not supported — timestamps from different precisions are not automatically renormalized.

The `DataPoint::new` constructor accepts the timestamp as a raw `i64`; callers are responsible for ensuring the value matches the configured precision.

---

## Metrics and Labels

### Metric name

A metric name is an arbitrary UTF-8 string. The only restriction is length:

| Limit | Value |
|---|---|
| Maximum metric name length | 65 535 bytes (`u16::MAX`) |

An empty metric name is rejected at ingest time.

### Labels

A label is a UTF-8 key-value pair. Both name and value must be non-empty. Length limits:

| Field | Limit |
|---|---|
| Label name | 256 bytes |
| Label value | 16 384 bytes (16 KiB) |

Duplicate label names within a single row are rejected. Labels are normalized to lexicographic order by name before computing the series identity, so `{a="1", b="2"}` and `{b="2", a="1"}` refer to the same series.

### Series identity

The engine assigns each unique (metric, sorted-labels) combination a 64-bit `SeriesId`. The identity is computed as a stable FNV-1a hash over a canonical binary encoding of the metric name and sorted label pairs. This hash is used internally; callers always identify series by metric name and labels.

---

## Type Coercions

The `Value` type exposes explicit, lossless conversion accessors:

| Method | Returns | Applies to |
|---|---|---|
| `as_f64()` | `Option<f64>` | `F64`, `I64` (if fits in 53 bits), `U64` (if fits in 53 bits) |
| `as_i64()` | `Option<i64>` | `I64`, `U64` (if fits in `i64`) |
| `as_u64()` | `Option<u64>` | `U64`, `I64` (if non-negative and fits in `u64`) |
| `as_bool()` | `Option<bool>` | `Bool` only |
| `as_bytes()` | `Option<&[u8]>` | `Bytes` only |
| `as_str()` | `Option<&str>` | `String` only |
| `as_histogram()` | `Option<&NativeHistogram>` | `Histogram` only |

**`as_f64()` precision note**: an `i64` or `u64` value is only converted if its absolute value can be represented exactly with 53 mantissa bits (the precision of `f64`). Values like `i64::MAX` (63 significant bits) return `None`. This prevents silent precision loss in numeric pipelines.

The PromQL evaluator calls `as_f64()` on every sample. Series whose values cannot be losslessly represented as `f64` — including `bool`, `bytes`, `string`, and oversized integers — are excluded from PromQL evaluation.

---

## Custom Types via the Codec Trait

Any Rust type can be stored inside the `bytes` lane by implementing the `Codec` trait:

```rust
pub trait Codec: Send + Sync {
    type Item: Clone + Send + Sync + 'static;

    fn encode(&self, value: &Self::Item) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<Self::Item>;
}
```

Encoding and decoding a point:

```rust
// Write
let encoded = Value::encode_with(&my_value, &my_codec)?;  // → Value::Bytes(...)

// Read
let decoded: MyType = point.value.decode_with(&my_codec)?;
```

### Custom aggregation

To aggregate custom-typed series at query time, pair a `Codec` with an `Aggregator`:

```rust
pub trait Aggregator<T>: Send + Sync {
    fn aggregate(&self, values: &[T]) -> Option<T>;
}
```

The `CodecAggregator<C, A>` adapter bridges a `Codec` and a typed `Aggregator` into the `BytesAggregation` interface expected by `QueryOptions::custom_aggregation`:

```rust
let agg = Arc::new(CodecAggregator::new(MyCodec, MyAggregator));

let options = QueryOptions::new(start, end)
    .with_custom_aggregation(agg);
```

`CodecAggregator` decodes each `Value::Bytes` point using the codec, passes all decoded values to the aggregator, encodes the result back to bytes, and attaches the bucket start timestamp to the aggregate point.

---

## Aggregation

When using the built-in `Aggregation` enum with `QueryOptions`, the engine aggregates numeric values (`f64`, and integers coercible to `f64`) over the query time range or per-bucket when downsampling is enabled:

| Variant | Operation |
|---|---|
| `None` | No aggregation — raw samples returned |
| `Sum` | Sum of all values |
| `Min` | Minimum value |
| `Max` | Maximum value |
| `Avg` | Mean |
| `First` | Earliest sample in the window |
| `Last` | Latest sample in the window |
| `Count` | Number of samples |
| `Median` | Median (50th percentile) |
| `Range` | `max - min` |
| `Variance` | Population variance |
| `StdDev` | Population standard deviation |

Built-in aggregation operates on the `f64` projection of a value (`Value::as_f64()`). `Bytes`, `String`, and `NativeHistogram` series are not aggregated by the built-in variants; use `custom_aggregation` for those types.

---

## Python Bindings Type Mapping

The UniFFI Python bindings expose all types with a `U` prefix. The mapping to Rust types is direct:

| Python type | Rust equivalent |
|---|---|
| `UValue` (enum) | `Value` |
| `UDataPoint` | `DataPoint` |
| `URow` | `Row` |
| `ULabel` | `Label` |
| `UNativeHistogram` | `NativeHistogram` |
| `UHistogramBucketSpan` | `HistogramBucketSpan` |
| `UHistogramCount` (enum) | `HistogramCount` |
| `UHistogramResetHint` (enum) | `HistogramResetHint` |

`UValue` is a tagged-union enum with named fields per variant:

```python
from tsink_uniffi import UValue, UDataPoint, UNativeHistogram, UHistogramBucketSpan

# float64
dp = UDataPoint(timestamp=1_700_000_000_000, value=UValue.F64(v=1.5))

# int64
dp = UDataPoint(timestamp=1_700_000_000_000, value=UValue.I64(v=-7))

# uint64
dp = UDataPoint(timestamp=1_700_000_000_000, value=UValue.U64(v=42))

# bool
dp = UDataPoint(timestamp=1_700_000_000_000, value=UValue.Bool(v=True))

# bytes
dp = UDataPoint(timestamp=1_700_000_000_000, value=UValue.Bytes(v=b"\x01\x02"))

# string
dp = UDataPoint(timestamp=1_700_000_000_000, value=UValue.Str(v="hello"))

# native histogram
hist = UNativeHistogram(
    count=UHistogramCount.Int(v=10),
    sum=5.5,
    schema=1,
    zero_threshold=0.0,
    zero_count=UHistogramCount.Int(v=0),
    negative_spans=[],
    negative_deltas=[],
    negative_counts=[],
    positive_spans=[UHistogramBucketSpan(offset=0, length=2)],
    positive_deltas=[3, 2],
    positive_counts=[],
    reset_hint=UHistogramResetHint.NO,
    custom_values=[],
)
dp = UDataPoint(timestamp=1_700_000_000_000, value=UValue.Histogram(v=hist))
```

Note that in the Python bindings the `string` variant is named `Str` (not `String`) to avoid conflicting with the Python built-in.
