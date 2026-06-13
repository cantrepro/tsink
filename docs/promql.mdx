# PromQL implementation

tsink ships a native PromQL parser and evaluator with no external query layer.
The implementation lives entirely in `src/promql/` and is exposed through the
public `tsink::promql` module.

---

## Architecture

The PromQL pipeline has three stages:

```
query string → Lexer → Parser → Evaluator → PromqlValue
```

| Module | File | Responsibility |
|---|---|---|
| Lexer | `src/promql/lexer.rs` | Tokenise raw input into a flat `Vec<Token>` |
| Parser | `src/promql/parser.rs` | Turn tokens into an `Expr` AST (Pratt / precedence-climbing) |
| Evaluator | `src/promql/eval/` | Walk the AST and resolve values against the storage engine |
| Types | `src/promql/types.rs` | `PromqlValue`, `Sample`, `Series`, histogram helpers |
| Errors | `src/promql/error.rs` | `PromqlError` enum |

---

## Public API

### Parsing

```rust
use tsink::promql::{parse, ast::Expr};

let expr: Expr = parse("rate(http_requests_total[5m])")?;
```

### Query engine

```rust
use std::sync::Arc;
use tsink::promql::{Engine, PromqlValue};
use tsink::{Storage, TimestampPrecision};

// Build an engine from any Arc<dyn Storage>.
let engine = Engine::with_precision(storage, TimestampPrecision::Milliseconds);

// Instant query — evaluate at a single timestamp.
let result: PromqlValue = engine.instant_query("up", eval_time)?;

// Range query — evaluate over [start, end] at each step.
let result: PromqlValue = engine.range_query("rate(errors_total[1m])", start, end, step)?;
```

`Engine::new` defaults to `TimestampPrecision::Nanoseconds`.
Both methods parse the expression internally before evaluation.

---

## Value types

`PromqlValue` mirrors the four PromQL expression result types.

| Variant | Contents | When returned |
|---|---|---|
| `Scalar(f64, i64)` | A single float and its evaluation timestamp | Literals, `scalar()`, arithmetic on two scalars |
| `InstantVector(Vec<Sample>)` | Zero or more labelled samples at one timestamp | Vector selectors, most functions |
| `RangeVector(Vec<Series>)` | Labelled time series with multiple samples | Matrix selectors, range queries |
| `String(String, i64)` | A string value and its evaluation timestamp | String literals |

### `Sample`

```rust
pub struct Sample {
    pub metric: String,
    pub labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
    pub histogram: Option<Box<NativeHistogram>>,
}
```

### `Series`

```rust
pub struct Series {
    pub metric: String,
    pub labels: Vec<Label>,
    pub samples: Vec<(i64, f64)>,       // (timestamp, float value)
    pub histograms: Vec<(i64, Box<NativeHistogram>)>,
}
```

---

## Lexer

The lexer is a single-pass byte scanner.  It produces all tokens in one call
(`Lexer::new(input).tokenize()`), returning `Vec<Token>` or a `PromqlError::Parse`.

### Comments

`#` starts a line comment; everything until the next newline is discarded.

### Identifiers

Identifiers follow the usual `[a-zA-Z_][a-zA-Z0-9_]*` alphabet.  Colons (`:`)
are also accepted inside identifiers to support recording-rule naming
conventions such as `job:http_requests:rate5m`.

### Keywords (case-insensitive)

`by`, `without`, `offset`, `bool`, `and`, `or`, `unless`, `on`, `ignoring`,
`group_left`, `group_right`, `atan2`, `inf`, `nan`

### Duration literals

A duration is a sequence of one or more `<integer><unit>` segments.

| Suffix | Unit |
|---|---|
| `ms` | milliseconds |
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days (24 h) |
| `w` | weeks (7 d) |
| `y` | years (365 d) |

Segments can be combined: `1h30m`, `5m30s`, `2d12h`.
Durations are stored internally as milliseconds (`i64`).

### String literals

Double-quoted strings with `\"`, `\\`, `\n`, `\r`, `\t` escape sequences.

### Number literals

Decimal integers and floats.  The special identifiers `inf` and `nan` are
recognised as numeric tokens equivalent to `f64::INFINITY` and `f64::NAN`.

---

## Parser

The parser implements precedence-climbing (Pratt) parsing for expressions.
The entry point is `parser::parse(input)`.

### Expression grammar (summary)

```
expr        := unary ( binary_op modifiers expr )*
unary       := ('+' | '-')? primary postfix*
primary     := number | string | inf | nan
             | '(' expr ')'
             | '{' matchers '}'
             | ident [ aggregation | call | vector_selector ]
postfix     := '[' duration (':' duration?)? ']'   -- matrix selector or subquery
             | 'offset' signed_duration
             | '@' (number | 'start()' | 'end()')
```

### Operator precedence

Lower number = binds tighter.

| Precedence | Operators |
|---|---|
| 1 (highest) | `^` (right-associative) |
| 2 | `*`, `/`, `%`, `atan2` |
| 3 | `+`, `-` |
| 4 | `==`, `!=`, `<`, `>`, `<=`, `>=` |
| 5 | `and`, `unless` |
| 6 (lowest) | `or` |

### Label matchers

```
{label="value", label2!="value", label3=~"regex", label4!~"regex"}
```

| Operator | Semantics |
|---|---|
| `=` | Exact equality |
| `!=` | Exact inequality |
| `=~` | Regex match (anchored) |
| `!~` | Regex non-match (anchored) |

Regex matching compiles the pattern with the standard `regex` crate.

### Vector selector

```
metric_name
metric_name{labels}
{labels}
```

### Matrix selector

```
metric_name[5m]
metric_name{label="value"}[1h]
```

### Subquery

```
expr[range:step]
expr[range:]          # omit step → use query step or default (1m)
```

### @modifier and offset

```
metric @ 1700000000         # pin to Unix timestamp
metric @ start()            # pin to range query start
metric @ end()              # pin to range query end
metric offset 5m            # shift evaluation back 5 minutes
metric[10m] offset 1h       # combine range + offset
```

### Aggregations

```
sum(expr)
sum by  (label1, label2) (expr)
sum without (label1)     (expr)
```

The grouping clause can be placed either before or after the argument list.

### Binary operator modifiers

```
a + on(job)             b       # match only on "job"
a + ignoring(instance)  b       # ignore "instance" when matching
a * on(job) group_left  b       # many-to-one: keep left-side labels
a * on(job) group_right b       # one-to-many: keep right-side labels
a * on(job) group_left(region) b  # also copy "region" from right
a == bool   b                   # return 0/1 instead of filtering
```

`group_left` and `group_right` cannot be combined with set operators
(`and`, `or`, `unless`).

---

## Evaluator

The evaluator is in `src/promql/eval/` and is split across several files:

| File | Contents |
|---|---|
| `mod.rs` | `Engine`, instant and range query entry points, prefetch cache, `@` resolution |
| `selector.rs` | Instant vector and matrix selector evaluation |
| `functions.rs` | All built-in function implementations |
| `aggregation.rs` | Aggregation operator implementations |
| `binary.rs` | Binary operator evaluation and vector matching |
| `subquery.rs` | Subquery evaluation |
| `time.rs` | Duration/timestamp utilities (`duration_to_units`, `step_times`) |

### Default parameters

| Parameter | Default |
|---|---|
| Lookback delta | 5 minutes |
| Subquery step | 1 minute |

The lookback delta controls how far back an instant vector selector looks for
the most recent sample.

### Range query prefetch

For range queries the engine checks whether any selector uses a dynamic `@`
modifier or is wrapped in a subquery.  When no dynamic time is involved it
pre-fetches all required metric data from storage in a single pass before
iterating over steps.  This significantly reduces storage I/O for wide time
ranges.  Subqueries and `@` modifiers disable prefetch for accuracy.

---

## Aggregation operators

All aggregation operators accept an optional `by (labels)` or
`without (labels)` grouping clause.

| Operator | Parameter | Description |
|---|---|---|
| `sum` | — | Sum of values |
| `avg` | — | Average of values |
| `min` | — | Minimum value |
| `max` | — | Maximum value |
| `count` | — | Number of series |
| `group` | — | 1 for each group (existence aggregation) |
| `stddev` | — | Population standard deviation |
| `stdvar` | — | Population variance |
| `count_values` | `label` (string) | Count series per distinct value; adds a `label` dimension |
| `quantile` | `φ` (scalar) | φ-quantile across the group |
| `topk` | `k` (scalar) | Top k series by value |
| `bottomk` | `k` (scalar) | Bottom k series by value |
| `limitk` | `k` (scalar) | Deterministically select k series (hash-stable) |
| `limit_ratio` | `ratio` (scalar) | Deterministically select a ratio of series |

`sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`, and `stdvar` support
native histograms for `sum`.  `count_values`, `quantile`, `topk`, and
`bottomk` require float samples.

---

## Functions

### Counter and gauge range functions

| Function | Input | Description |
|---|---|---|
| `rate(v[d])` | range vector | Per-second rate of counter increase (extrapolated to fit `d`) |
| `irate(v[d])` | range vector | Per-second instant rate using the last two samples |
| `increase(v[d])` | range vector | Total counter increase over `d` (extrapolated) |
| `delta(v[d])` | range vector | Value change over `d` (extrapolated, for gauges) |
| `idelta(v[d])` | range vector | Instant delta between the last two samples |
| `changes(v[d])` | range vector | Number of value changes within `d` |
| `resets(v[d])` | range vector | Number of counter resets within `d` |

`rate` and `increase` support native histogram series and produce a histogram
result.  The other range functions require float samples.

**Extrapolation**: `rate`, `increase`, and `delta` use the same boundary
extrapolation algorithm as Prometheus — the sampled interval is extended toward
the range boundaries when the gap is within 110% of the average sample interval.

### Over-time aggregations

All take a range vector and return an instant vector.

| Function | Description |
|---|---|
| `avg_over_time(v[d])` | Average of samples in window |
| `sum_over_time(v[d])` | Sum |
| `min_over_time(v[d])` | Minimum |
| `max_over_time(v[d])` | Maximum |
| `count_over_time(v[d])` | Count of samples |
| `last_over_time(v[d])` | Most recent sample |
| `present_over_time(v[d])` | 1 if any sample exists |
| `stddev_over_time(v[d])` | Standard deviation |
| `stdvar_over_time(v[d])` | Variance |
| `mad_over_time(v[d])` | Median absolute deviation |
| `quantile_over_time(φ, v[d])` | φ-quantile of samples |

### Histogram functions

| Function | Description |
|---|---|
| `histogram_quantile(φ, v)` | φ-quantile from classic (bucket-based) or native histograms |
| `histogram_avg(v)` | Average from native histograms |
| `histogram_count(v)` | Observation count from native histograms |
| `histogram_sum(v)` | Sum of observations from native histograms |
| `histogram_stddev(v)` | Standard deviation from native histograms |
| `histogram_stdvar(v)` | Variance from native histograms |
| `histogram_fraction(lower, upper, v)` | Fraction of observations in `(lower, upper]` from native histograms |

### Regression and prediction

| Function | Description |
|---|---|
| `deriv(v[d])` | Estimated per-second derivative by linear regression |
| `predict_linear(v[d], t)` | Predicted value `t` seconds from now using linear regression |
| `double_exponential_smoothing(v[d], sf, tf)` | Double exponential smoothing; `sf` = smoothing factor, `tf` = trend factor; also callable as `holt_winters` |

### Math functions

| Function | Description |
|---|---|
| `abs(v)` | Absolute value |
| `ceil(v)` | Ceiling |
| `floor(v)` | Floor |
| `round(v)` | Round to nearest integer |
| `round(v, to_nearest)` | Round to nearest multiple of `to_nearest` |
| `sqrt(v)` | Square root |
| `exp(v)` | e^v |
| `ln(v)` | Natural logarithm |
| `log2(v)` | Base-2 logarithm |
| `log10(v)` | Base-10 logarithm |
| `sgn(v)` | Sign (−1, 0, or 1) |
| `clamp(v, min, max)` | Clamp value to `[min, max]` |
| `clamp_min(v, min)` | Lower-clamp |
| `clamp_max(v, max)` | Upper-clamp |

### Trigonometry

| Function | Function |
|---|---|
| `cos(v)` | `acos(v)` |
| `cosh(v)` | `acosh(v)` |
| `sin(v)` | `asin(v)` |
| `sinh(v)` | `asinh(v)` |
| `tan(v)` | `atan(v)` |
| `tanh(v)` | `atanh(v)` |
| `deg(v)` — radians to degrees | `rad(v)` — degrees to radians |
| `pi()` — π as a scalar | |

### Date and time

When called with no argument these functions use the eval-time timestamp.
When called with an instant vector they use each sample's timestamp.

| Function | Description |
|---|---|
| `time()` | Current evaluation time in seconds since epoch (scalar) |
| `timestamp(v)` | Timestamp of each sample in seconds since epoch |
| `minute(v?)` | Minute of the hour (0–59) |
| `hour(v?)` | Hour of the day (0–23) |
| `day_of_week(v?)` | Day of the week (0=Sunday–6=Saturday) |
| `day_of_month(v?)` | Day of the month (1–31) |
| `day_of_year(v?)` | Day of the year (1–366) |
| `days_in_month(v?)` | Number of days in the month (28–31) |
| `month(v?)` | Month (1–12) |
| `year(v?)` | Year |

### Label manipulation

| Function | Description |
|---|---|
| `label_replace(v, dst, repl, src, regex)` | Rewrite label `src` into `dst` using a capture-aware `regex` and `repl` |
| `label_join(v, dst, sep, src1, src2, ...)` | Concatenate source labels into `dst` with `sep` as separator |
| `drop_common_labels(v)` | Remove labels that are identical across all series in the vector |

### Type coercion

| Function | Description |
|---|---|
| `scalar(v)` | Convert a single-element instant vector to a scalar; `NaN` if more than one element |
| `vector(s)` | Convert a scalar to a single-element instant vector with no labels |

### Sorting

| Function | Description |
|---|---|
| `sort(v)` | Sort by value ascending |
| `sort_desc(v)` | Sort by value descending |
| `sort_by_label(v, l1, ...)` | Sort by the specified label names, ascending |
| `sort_by_label_desc(v, l1, ...)` | Sort by the specified label names, descending |

### Absence detection

| Function | Description |
|---|---|
| `absent(v)` | Returns `{} 1` when the instant vector is empty; nothing otherwise |
| `absent_over_time(v[d])` | Returns `{} 1` when the range vector is empty; nothing otherwise |

### Miscellaneous

| Function | Description |
|---|---|
| `info(v)` | Experimental: fetches info-metric labels and merges them into each series |
| `count_scalar(v)` | Returns the element count of a vector as a scalar |

---

## Supported features vs. standard PromQL

| Feature | Supported |
|---|---|
| Instant and range queries | Yes |
| All arithmetic and set operators | Yes |
| `bool` comparison modifier | Yes |
| Vector matching (`on` / `ignoring` / `group_left` / `group_right`) | Yes |
| `offset` modifier | Yes |
| `@` modifier with literal timestamp, `start()`, `end()` | Yes |
| Subqueries `expr[range:step]` | Yes |
| Native histograms | Yes (float samples only for most functions; `rate` and `increase` supported) |
| Stale NaN markers (Prometheus compatibility) | Yes |
| `limitk` / `limit_ratio` aggregations (VictoriaMetrics extension) | Yes |
| `mad_over_time` | Yes |
| `double_exponential_smoothing` / `holt_winters` | Yes |
| `sort_by_label` / `sort_by_label_desc` | Yes |
| `info` | Yes (experimental) |
| UTF-8 / non-ASCII metric names | No — identifiers are ASCII only |
| Backtick string literals | No |

---

## Error types

```rust
pub enum PromqlError {
    Parse(String),                                  // invalid syntax
    UnexpectedToken { expected: String, found: String },
    UnknownFunction(String),                        // unrecognised function name
    ArgumentCount { func, expected, got },          // wrong arity
    Type(String),                                   // type mismatch at evaluation
    Eval(String),                                   // runtime evaluation error
    Regex(String),                                  // invalid regex in matcher
    Storage(TsinkError),                            // underlying storage error
}
```

`PromqlError::Storage` is constructed automatically from `TsinkError` via a
`From` impl, so storage errors surface transparently through the query result.

---

## Examples

```rust
use std::sync::Arc;
use tsink::{StorageBuilder, TimestampPrecision};
use tsink::promql::Engine;

let storage = Arc::new(
    StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()?
);
let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);

// Instant queries
let v = engine.instant_query("up", 1_700_000_000)?;
let v = engine.instant_query(r#"http_requests_total{method="GET"}"#, now)?;
let v = engine.instant_query("rate(http_requests_total[5m])", now)?;
let v = engine.instant_query("sum by (job) (rate(errors_total[1m]))", now)?;

// Range query (returns PromqlValue::RangeVector)
let v = engine.range_query(
    "rate(http_requests_total[5m])",
    start,   // inclusive
    end,     // inclusive
    step,    // interval between evaluation points
)?;

// Parse only (no storage required)
let expr = tsink::promql::parse("histogram_quantile(0.99, rate(latency_seconds_bucket[5m]))")?;
```
