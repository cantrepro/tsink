# PromQL reference

This page is a complete reference for the PromQL dialect supported by tsink.
For architecture and implementation details see [docs/promql.md](promql.md).

---

## Data types

Every PromQL expression evaluates to one of four types.

| Type | Description |
|---|---|
| **Scalar** | A single floating-point number with no labels. |
| **Instant vector** | A set of time series, each with one sample at the evaluation timestamp. |
| **Range vector** | A set of time series, each with multiple samples over a time range. |
| **String** | A string literal value (rarely used outside of `label_replace`/`label_join` arguments). |

---

## Selectors

### Vector selector

Returns an instant vector containing the most recent sample for each matching series within the lookback window (default 5 minutes).

```
metric_name
metric_name{label="value"}
{label="value", label2=~"regex"}
```

When no metric name is given the `{...}` form matches across all metrics.

### Matrix selector

Returns a range vector containing all samples within the given duration window.

```
metric_name[5m]
metric_name{label="value"}[1h30m]
```

### Label matchers

| Operator | Semantics |
|---|---|
| `=` | Exact string equality |
| `!=` | Exact string inequality |
| `=~` | Regular expression match (implicitly anchored) |
| `!~` | Regular expression non-match (implicitly anchored) |

Multiple matchers are combined with AND semantics. Regex patterns use the Rust `regex` crate syntax.

```
http_requests_total{method="GET", status=~"2.."}
http_requests_total{job!="alertmanager"}
{__name__=~"http_.+", method="POST"}
```

### Duration syntax

Durations are composed of one or more `<integer><unit>` segments.

| Suffix | Unit |
|---|---|
| `ms` | milliseconds |
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days (24 h) |
| `w` | weeks (7 d) |
| `y` | years (365 d) |

Segments can be combined in any order: `1h30m`, `2d12h`, `5m30s`.

---

## Modifiers

### `offset`

Shifts the evaluation time backward by the given duration.

```
http_requests_total offset 5m
rate(http_requests_total[5m]) offset 1h
```

### `@` modifier

Pins the selector to a fixed evaluation time regardless of the query's range.

```
http_requests_total @ 1700000000          # Unix timestamp
http_requests_total @ start()             # range query start
http_requests_total @ end()               # range query end
```

`@` and `offset` can be combined; `offset` is applied after the `@` timestamp.

### Subquery

Evaluates a non-range expression over a time range and step, producing a range vector as input to range functions.

```
max_over_time(rate(http_requests_total[1m])[1h:5m])
avg_over_time(some_gauge[30m:])           # omit step → query step or default 1 minute
```

---

## Operators

### Arithmetic operators

| Operator | Description |
|---|---|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |
| `^` | Power (right-associative) |
| `atan2` | Two-argument arctangent (binary operator form) |

### Comparison operators

| Operator | Description |
|---|---|
| `==` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

When applied between two scalars, comparisons require the `bool` modifier and return `0` or `1`. Between a vector and a scalar (or two vectors), the default behaviour is to filter — only matching samples are kept. Adding `bool` instead returns `0`/`1` for every sample.

```
http_requests_total > 100                 # filter: only series above 100
http_requests_total > bool 100            # 0 or 1 for every series
```

### Set (logical) operators

Set operators operate on label sets and require instant vector operands.

| Operator | Description |
|---|---|
| `and` | Intersection — returns samples from the left side that have a matching label set on the right side. |
| `or` | Union — returns all samples from both sides; left side takes precedence on conflicts. |
| `unless` | Difference — returns samples from the left side that have **no** matching label set on the right side. |

### Operator precedence

Higher rows bind tighter.

| Precedence | Operators | Associativity |
|---|---|---|
| 1 (tightest) | `^` | Right |
| 2 | `*`, `/`, `%`, `atan2` | Left |
| 3 | `+`, `-` | Left |
| 4 | `==`, `!=`, `<`, `>`, `<=`, `>=` | Left |
| 5 | `and`, `unless` | Left |
| 6 (loosest) | `or` | Left |

Use parentheses to override precedence.

---

## Vector matching

Binary operators between two instant vectors must be told how to align their label sets.

### One-to-one matching

By default, two samples match if their label sets are identical.

```
method_code:http_errors:rate5m / ignoring(code) method:http_requests:rate5m
method_code:http_errors:rate5m / on(method)     method:http_requests:rate5m
```

| Keyword | Effect |
|---|---|
| `on(label, ...)` | Match **only** on the listed labels. |
| `ignoring(label, ...)` | Match on all labels **except** those listed. |

### Many-to-one and one-to-many matching

When one side has more series per group, declare the higher-cardinality side explicitly.

```
# Each left-side series for a method matches one right-side series per job
metric_left * on(job) group_left  metric_right

# Copy extra labels from the right side into the result
metric_left * on(job) group_left(region) metric_right

# Right side has higher cardinality
metric_left * on(job) group_right metric_right
```

`group_left` and `group_right` cannot be used with set operators (`and`, `or`, `unless`).

---

## Aggregation operators

Aggregations collapse an instant vector into grouped results.  The optional `by` or `without` clause controls which labels define the groups.

```
sum(http_requests_total)
sum by  (job, method) (http_requests_total)
sum without (instance) (http_requests_total)
```

The grouping clause may also appear before the argument:

```
sum by (job) (http_requests_total)
```

### Aggregation operator catalogue

| Operator | Parameter | Description |
|---|---|---|
| `sum` | — | Sum of values in each group. Supports native histograms. |
| `avg` | — | Average of values in each group. |
| `min` | — | Minimum value in each group. |
| `max` | — | Maximum value in each group. |
| `count` | — | Number of series in each group. |
| `group` | — | Returns `1` for each group (existence check). |
| `stddev` | — | Population standard deviation within each group. |
| `stdvar` | — | Population variance within each group. |
| `count_values` | `label` (string) | Counts series per distinct value; adds a new label dimension named `label`. Float samples only. |
| `quantile` | `φ` (scalar, 0–1) | φ-quantile across each group. Float samples only. |
| `topk` | `k` (scalar) | Top-k series by value; retains original labels. Float samples only. |
| `bottomk` | `k` (scalar) | Bottom-k series by value; retains original labels. Float samples only. |
| `limitk` | `k` (scalar) | Deterministically selects k series per group using a stable hash. |
| `limit_ratio` | `ratio` (scalar, 0–1) | Deterministically selects a ratio of series per group using a stable hash. |

---

## Functions

### Counter and gauge range functions

These functions take a **range vector** and return an **instant vector**.

| Function | Description |
|---|---|
| `rate(v[d])` | Per-second average rate of counter increase over `d`, extrapolated to the range boundaries. Supports native histograms. |
| `irate(v[d])` | Per-second instant rate calculated from the last two samples. |
| `increase(v[d])` | Total counter increase over `d`, extrapolated. Supports native histograms. |
| `delta(v[d])` | Value change over `d`, extrapolated. Intended for gauges. |
| `idelta(v[d])` | Instant delta between the last two samples. |
| `changes(v[d])` | Number of times the value changed within `d`. |
| `resets(v[d])` | Number of counter resets (value decreases) within `d`. |

**Extrapolation**: `rate`, `increase`, and `delta` extend the sampled interval toward the range boundaries when the boundary gap is within 110 % of the average sample interval. This matches the Prometheus algorithm.

**`rate` with native histograms**: when a series contains native histogram samples, `rate` and `increase` return a histogram result rather than a scalar. Mixed float+histogram series produce an error.

### Over-time aggregation functions

These functions take a **range vector** and return an **instant vector**. All require float samples.

| Function | Description |
|---|---|
| `avg_over_time(v[d])` | Average of all samples within `d`. |
| `sum_over_time(v[d])` | Sum of all samples. |
| `min_over_time(v[d])` | Minimum sample value. |
| `max_over_time(v[d])` | Maximum sample value. |
| `count_over_time(v[d])` | Number of samples. |
| `last_over_time(v[d])` | Value of the most recent sample. |
| `present_over_time(v[d])` | Returns `1` if any sample exists in the window. |
| `stddev_over_time(v[d])` | Standard deviation of samples. |
| `stdvar_over_time(v[d])` | Variance of samples. |
| `mad_over_time(v[d])` | Median absolute deviation (MAD) of samples. |
| `quantile_over_time(φ, v[d])` | φ-quantile (`0`–`1`) of samples within `d`. |

### Histogram functions

| Function | Input | Description |
|---|---|---|
| `histogram_quantile(φ, v)` | Instant vector | φ-quantile from classic bucket-based histograms (using the `le` label) or native histograms. |
| `histogram_count(v)` | Instant vector (native histograms only) | Observation count extracted from a native histogram. |
| `histogram_sum(v)` | Instant vector (native histograms only) | Sum of observations extracted from a native histogram. |
| `histogram_avg(v)` | Instant vector (native histograms only) | Average of observations (`sum / count`). |
| `histogram_stddev(v)` | Instant vector (native histograms only) | Standard deviation derived from a native histogram. |
| `histogram_stdvar(v)` | Instant vector (native histograms only) | Variance derived from a native histogram. |
| `histogram_fraction(lower, upper, v)` | Scalars + instant vector | Fraction of observations in `(lower, upper]`. Classic bucket histograms only (native histogram support pending). |

**Classic histogram quantile**: `histogram_quantile` expects an instant vector whose series carry an `le` label. It groups series by all labels except `le`, interpolates the quantile within each bucket group, and strips the `le` and `_bucket` suffix from the result metric name.

### Regression and prediction

These functions take a **range vector** and return an **instant vector**. All require float samples.

| Function | Description |
|---|---|
| `deriv(v[d])` | Per-second derivative estimated by ordinary least-squares linear regression over `d`. |
| `predict_linear(v[d], t)` | Predicted value `t` seconds in the future using linear regression over `d`. `t` is a scalar. |
| `double_exponential_smoothing(v[d], sf, tf)` | Double exponential (Holt–Winters) smoothing. `sf` is the smoothing factor and `tf` is the trend factor; both must be in `[0, 1]`. Also callable as `holt_winters(v[d], sf, tf)`. |

### Math functions

These functions accept a **scalar** or **instant vector** and apply an element-wise transformation.

| Function | Description |
|---|---|
| `abs(v)` | Absolute value. |
| `ceil(v)` | Ceiling (round up to nearest integer). |
| `floor(v)` | Floor (round down to nearest integer). |
| `round(v)` | Round to nearest integer. |
| `round(v, to_nearest)` | Round to the nearest multiple of `to_nearest`. Returns `NaN` when `to_nearest` is `0` or non-finite. |
| `sqrt(v)` | Square root. |
| `exp(v)` | e^v |
| `ln(v)` | Natural logarithm (base e). |
| `log2(v)` | Base-2 logarithm. |
| `log10(v)` | Base-10 logarithm. |
| `sgn(v)` | Sign function: `-1` for negative, `0` for zero, `1` for positive, `NaN` for `NaN`. |
| `clamp(v, min, max)` | Clamp values to `[min, max]`. Returns empty vector when `min > max`. |
| `clamp_min(v, min)` | Clamp values to at least `min`. |
| `clamp_max(v, max)` | Clamp values to at most `max`. |
| `pi()` | Returns π as a scalar (no argument). |

### Trigonometric functions

All accept a **scalar** or **instant vector** (angles in radians unless noted).

| Function | Description |
|---|---|
| `sin(v)` | Sine. |
| `cos(v)` | Cosine. |
| `tan(v)` | Tangent. |
| `asin(v)` | Arcsine. |
| `acos(v)` | Arccosine. |
| `atan(v)` | Arctangent. |
| `sinh(v)` | Hyperbolic sine. |
| `cosh(v)` | Hyperbolic cosine. |
| `tanh(v)` | Hyperbolic tangent. |
| `asinh(v)` | Inverse hyperbolic sine. |
| `acosh(v)` | Inverse hyperbolic cosine. |
| `atanh(v)` | Inverse hyperbolic tangent. |
| `deg(v)` | Convert radians to degrees. |
| `rad(v)` | Convert degrees to radians. |

The binary operator `atan2` computes `y.atan2(x)` with vector matching semantics.

### Date and time functions

When called with **no argument** these functions use the query evaluation timestamp.  When called with an **instant vector** they extract the component from each sample's timestamp value (interpreted as seconds since the Unix epoch).

| Function | Description |
|---|---|
| `time()` | Evaluation time in seconds since the Unix epoch. Returns a scalar. |
| `timestamp(v)` | Timestamp of each sample in seconds since the Unix epoch. |
| `minute(v?)` | Minute of the hour, UTC (0–59). |
| `hour(v?)` | Hour of the day, UTC (0–23). |
| `day_of_week(v?)` | Day of the week, UTC (0 = Sunday, 6 = Saturday). |
| `day_of_month(v?)` | Day of the month, UTC (1–31). |
| `day_of_year(v?)` | Day of the year, UTC (1–366). |
| `days_in_month(v?)` | Number of days in the current month, UTC (28–31). |
| `month(v?)` | Month, UTC (1–12). |
| `year(v?)` | Year, UTC. |

### Label manipulation functions

| Function | Description |
|---|---|
| `label_replace(v, dst_label, replacement, src_label, regex)` | If the value of `src_label` matches `regex`, write `replacement` (with `$1`, `$2` … capture references) into `dst_label`. The label is set on every sample; if the regex does not match the sample is passed through unchanged. |
| `label_join(v, dst_label, separator, src_label1, src_label2, ...)` | Concatenates the values of the source labels with `separator` and writes the result into `dst_label`. At least one source label is required. |
| `drop_common_labels(v)` | Removes every label whose value is identical across all series in the input vector. |

**`label_replace` example**:

```
label_replace(up{job="api"}, "short_job", "$1", "job", "(.*)-svc")
```

### Type coercion functions

| Function | Description |
|---|---|
| `scalar(v)` | Converts a single-element instant vector to a scalar. Returns `NaN` if the vector has zero or more than one element, or if the element is a native histogram. |
| `vector(s)` | Converts a scalar `s` to a single-element instant vector with no labels. |

### Sorting functions

Sorting affects only the presentation order; the returned instant vector is otherwise unchanged.

| Function | Description |
|---|---|
| `sort(v)` | Sort by value ascending. Float samples only. |
| `sort_desc(v)` | Sort by value descending. Float samples only. |
| `sort_by_label(v, label1, ...)` | Sort by the specified label names lexicographically (natural order), ascending. Ties broken by full series identity. Float samples only. |
| `sort_by_label_desc(v, label1, ...)` | Same as `sort_by_label` but descending. Float samples only. |

### Absence detection functions

| Function | Description |
|---|---|
| `absent(v)` | Returns `{} 1` at the evaluation timestamp when the instant vector `v` is empty; returns nothing otherwise. When `v` is a simple vector selector the result inherits any equality-matched labels. |
| `absent_over_time(v[d])` | Returns `{} 1` when the range vector `v[d]` is empty; nothing otherwise. |

### Miscellaneous functions

| Function | Description |
|---|---|
| `count_scalar(v)` | Returns the number of elements in the instant vector as a scalar. Equivalent to `scalar(count(v))`. |
| `info(v)` | **Experimental.** Looks up the `target_info` metric (or a custom info selector passed as the optional second argument) and merges its labels into each sample in `v`, matched on `job` and `instance`. |

---

## Special values

| Value | Meaning |
|---|---|
| `inf`, `+inf` | Positive infinity |
| `-inf` | Negative infinity |
| `nan` | Not a number |

These identifiers are recognised by the lexer as numeric literals, not function calls.

---

## Stale NaN markers

tsink recognises the Prometheus stale NaN bit pattern (`0x7ff0000000000002`) and silently drops such samples from vector selectors, matching Prometheus staleness semantics.

---

## Limitations

| Feature | Status |
|---|---|
| UTF-8 / non-ASCII characters in metric names or label names | Not supported — identifiers are ASCII only |
| Backtick string literals | Not supported |
| `histogram_fraction` with native histograms | Pending — works with classic histograms only |
| `histogram_stddev` / `histogram_stdvar` evaluation | Native histogram support pending |
| `irate`, `delta`, `idelta`, `changes`, `resets` with native histograms | Not supported |

---

## Query examples

```promql
# Per-second HTTP request rate over the last 5 minutes
rate(http_requests_total[5m])

# Rate broken out by method and status, grouped by job
sum by (job) (rate(http_requests_total[5m]))

# Percentage of 5xx responses
sum(rate(http_requests_total{status=~"5.."}[5m]))
  / sum(rate(http_requests_total[5m])) * 100

# 99th-percentile latency from a classic histogram
histogram_quantile(0.99,
  sum by (le) (rate(request_duration_seconds_bucket[5m]))
)

# Alert: predict disk full in 4 hours
predict_linear(node_filesystem_avail_bytes[1h], 4 * 3600) < 0

# Absent metric detection
absent(up{job="api"})

# Month-over-month comparison using offset
rate(http_requests_total[5m])
  / rate(http_requests_total[5m] offset 30d)

# Subquery: maximum per-second rate over the last hour, evaluated every 5 minutes
max_over_time(rate(http_requests_total[5m])[1h:5m])

# Label rewrite: strip "-svc" suffix
label_replace(
  up{job=~".+-svc"},
  "short_job", "$1", "job", "(.+)-svc"
)

# Many-to-one join to enrich metrics with team labels
http_requests_total * on(job) group_left(team) job_info
```
