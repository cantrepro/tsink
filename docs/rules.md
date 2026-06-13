# Recording & Alerting Rules

tsink has a built-in rules engine that can both **record** PromQL expressions as new metric series and **detect alert conditions** using a configurable evaluation interval. Rules are organised into groups, persisted across restarts, and evaluated by a background scheduler that runs on the server — no external rule evaluation process is needed.

---

## Contents

- [Concepts](#concepts)
- [Rule groups](#rule-groups)
- [Recording rules](#recording-rules)
- [Alerting rules](#alerting-rules)
- [Duration format](#duration-format)
- [Label merging](#label-merging)
- [Scheduler and evaluation](#scheduler-and-evaluation)
- [Cluster mode](#cluster-mode)
- [Persistence](#persistence)
- [HTTP API reference](#http-api-reference)
- [RBAC permissions](#rbac-permissions)
- [Environment variables](#environment-variables)
- [Constraints and limits](#constraints-and-limits)

---

## Concepts

### Recording rules

A recording rule evaluates a PromQL expression on a schedule and writes the result back into storage as a new metric. This pre-computes expensive aggregations so that dashboards and queries can read from cheap point lookups instead of reprocessing the raw data on every request.

### Alerting rules

An alerting rule evaluates a PromQL expression on a schedule. When the expression returns a non-empty result set, instances of the alert become **active**. Each active instance goes through a two-stage lifecycle:

- **Pending** — the condition has been observed but has not been firing for the full `for` duration yet.
- **Firing** — the condition has been active for at least the `for` duration.

If `for` is zero (or not specified), instances become firing immediately.

Alert state is persisted across server restarts. Instances that disappear from the expression result are removed automatically on the next evaluation cycle.

---

## Rule groups

All rules are declared inside **rule groups**. A group bundles a set of rules that share a tenant, an evaluation interval, and optional extra labels that are appended to every result.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `string` | Yes | Unique group name. Must not be empty. |
| `tenantId` | `string` | Yes | Tenant the group belongs to. Rules read from and write to this tenant's data. |
| `interval` | `duration` | No | Evaluation interval for every rule in the group. Default: `60s`. |
| `labels` | `object` | No | Key-value labels appended to every result produced by rules in this group. Overridden by per-rule labels. |
| `rules` | `Rule[]` | Yes | At least one rule. |

Every rule group must contain at least one rule. Group names must be non-empty, and interval must be > 0 when specified.

---

## Recording rules

A recording rule has `"kind": "recording"`.

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | `"recording"` | Yes | Discriminator. |
| `record` | `string` | Yes | Name of the output metric. Must be a valid metric name (≤ 256 bytes). |
| `expr` | `string` | Yes | PromQL expression. Must evaluate to a scalar or instant vector. |
| `interval` | `duration` | No | Override the group interval for this rule only. |
| `labels` | `object` | No | Extra labels added to all output rows. Override group-level labels. |

**Example**

```json
{
  "kind": "recording",
  "record": "job:http_requests:rate5m",
  "expr": "rate(http_requests_total[5m])",
  "labels": {
    "aggregation": "rate"
  }
}
```

The expression result determines the shape of the output:

- **Scalar** — a single row is written with the labels from the group and rule.
- **Instant vector** — one row per sample, carrying the sample's original labels merged with group and rule labels.
- **Range vector or string** — rejected; the rule is marked as an error.

---

## Alerting rules

An alerting rule has `"kind": "alert"`.

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | `"alert"` | Yes | Discriminator. |
| `alert` | `string` | Yes | Alert name. Stored as the `alertname` label on every instance. |
| `expr` | `string` | Yes | PromQL expression. Must evaluate to a scalar or instant vector. A non-empty result set means the alert is active. |
| `interval` | `duration` | No | Override the group interval for this rule only. |
| `for` | `duration` | No | Minimum time the condition must hold before the alert fires. Default: `0s` (fire immediately). Also accepted as `forDuration`. |
| `labels` | `object` | No | Extra labels added to every alert instance. Override group-level labels. |
| `annotations` | `object` | No | Human-readable key-value metadata attached to each instance. Not stored as series labels. |

**Example**

```json
{
  "kind": "alert",
  "alert": "HighMemoryUsage",
  "expr": "process_resident_memory_bytes > 500000000",
  "for": "5m",
  "labels": {
    "severity": "warning"
  },
  "annotations": {
    "summary": "Process memory above 500 MB for 5 minutes",
    "runbook": "https://wiki.example.com/runbooks/high-memory"
  }
}
```

### Alert instance status

Each active instance exposes the following fields in the status snapshot:

| Field | Type | Description |
|---|---|---|
| `key` | `string` | Stable identifier derived from metric name + labels. |
| `sourceMetric` | `string` | Original metric name from the expression result. |
| `labels` | `Label[]` | Merged labels on this instance, including `alertname`. |
| `activeSinceTimestamp` | `i64` | Evaluation timestamp when the instance first became active. |
| `lastSeenTimestamp` | `i64` | Evaluation timestamp of the most recent evaluation that observed this instance. |
| `firingSinceTimestamp` | `i64 \| null` | Evaluation timestamp when the instance entered the firing state, or `null` if still pending. |
| `state` | `"pending" \| "firing"` | Current lifecycle state. |
| `sampleType` | `string` | `"scalar"` or `"histogram"`. |
| `sampleValue` | `string \| null` | Stringified sample value at the last evaluation. |

---

## Duration format

Durations can be expressed as a bare integer (interpreted as seconds), an integer string `"60"`, or a string with a unit suffix:

| Suffix | Unit |
|---|---|
| `ms` | milliseconds |
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days |
| `w` | weeks |
| `y` | years (365.25 days) |

**Examples:** `"30s"`, `"5m"`, `"1h"`, `"2d"`, `90` (= 90 seconds), `"90"` (= 90 seconds).

Fractional values are supported for string durations: `"1.5h"` = 5400 seconds. The result is rounded up to the nearest whole second. Durations must be > 0.

---

## Label merging

Labels on each output row (recording rules) or alert instance (alerting rules) are produced by merging three sources in priority order — later sources win on conflict:

1. **Sample labels** — labels carried by the PromQL result sample.
2. **Group labels** — labels declared at the rule group level.
3. **Rule labels** — labels declared on the individual rule.

Merged label names are validated: names must be non-empty, must not be `__name__` or the internal tenant label, and both names and values must not exceed their length limits.

For alert rules, an `alertname` label equal to the rule's `alert` field is always present in the final label set.

---

## Scheduler and evaluation

The rules scheduler runs as a background tokio task on the server process. On each tick it evaluates every rule whose aligned evaluation timestamp has advanced since the previous run.

**Aligned timestamps** — Each rule's effective evaluation timestamp is snapped to the nearest multiple of its interval, aligned to the Unix epoch. This ensures that rules with the same interval always evaluate at the same timestamps across restarts.

```
aligned_ts = now - (now mod interval)
```

Rules are skipped on a given tick if the aligned timestamp has not changed from the last recorded evaluation. This means rules are evaluated at most once per interval, even if the scheduler ticks more frequently.

**Error handling** — Evaluation errors (PromQL parse failure, query error, row limit exceeded) are recorded on the rule's runtime state and visible in the status snapshot. The next tick retries the rule normally.

---

## Cluster mode

In a clustered deployment only the **control-plane leader** runs the rules scheduler. All other nodes skip evaluation silently (`schedulerSkippedNotLeaderTotal` counter increments). This prevents duplicate recording-rule writes and duplicate alert state across replicas.

Recording rules in cluster mode route their output rows through the cluster write router using the same consistency and ring-version semantics as normal ingestion.

PromQL evaluation for rules uses the distributed storage adapter so that the expression can read from all shards.

---

## Persistence

Rule group configurations and per-rule runtime state (last evaluation timestamp, last error, alert instances) are persisted in `rules-store.json` in the data directory alongside the storage files. If no data path is configured the rules runtime operates in-memory only — rules are cleared on restart.

The store uses schema versioning and an integrity magic string. On startup the existing store is loaded and runtime state for rules that still match the configured fingerprint (group + rule specification hash) is carried forward. Runtime state for removed or modified rules is discarded automatically.

**Snapshots** — The rules store is included in cluster snapshots created through the admin snapshot endpoint so it can be restored consistently with the rest of the data.

---

## HTTP API reference

All rules endpoints are under the admin API and require `--enable-admin-api` to be set. See [Security model](security.md) for authentication details.

### Apply rule groups

```
POST /api/v1/admin/rules/apply
Content-Type: application/json
```

Replaces the complete set of configured rule groups with the groups in the request body. Sending an empty groups array (or an empty body) clears all rules.

Runtime state (last evaluation timestamp, alert instances) is **preserved** for rules that exist in both the old and new configuration and whose fingerprint (full specification hash) has not changed. Rules that are new or modified start with a clean runtime state.

**Request body**

```json
{
  "groups": [
    {
      "name": "latency-rules",
      "tenantId": "default",
      "interval": "60s",
      "labels": {
        "env": "prod"
      },
      "rules": [
        {
          "kind": "recording",
          "record": "job:request_duration_seconds:p99",
          "expr": "histogram_quantile(0.99, rate(request_duration_seconds_bucket[5m]))"
        },
        {
          "kind": "alert",
          "alert": "HighLatency",
          "expr": "job:request_duration_seconds:p99 > 1.0",
          "for": "2m",
          "labels": {
            "severity": "critical"
          },
          "annotations": {
            "summary": "p99 latency above 1 s for 2 minutes"
          }
        }
      ]
    }
  ]
}
```

**Response — 200 OK**

```json
{
  "status": "success",
  "data": { <RulesStatusSnapshot> }
}
```

**Error responses**

| Code | Cause |
|---|---|
| 400 | Invalid JSON, invalid PromQL expression, duplicate rule identifier, empty group name, empty rules list, invalid label, reserved label name |
| 503 | Rules runtime not available |

---

### Trigger immediate evaluation

```
POST /api/v1/admin/rules/run
```

Triggers a one-shot synchronous evaluation of all rules that are due at the current timestamp. Returns after the run completes. Returns a 409 if a scheduler run is already in progress.

**Response — 200 OK**

```json
{
  "status": "success",
  "data": { <RulesStatusSnapshot> }
}
```

**Error responses**

| Code | Cause |
|---|---|
| 409 | Scheduler is already running |
| 500 | Evaluation failed to persist state |
| 503 | Rules runtime not available |

---

### Query rules status

```
GET /api/v1/admin/rules/status
```

Returns the current configuration and runtime state for all configured rule groups and rules.

**Response — 200 OK**

```json
{
  "status": "success",
  "data": {
    "schedulerTickMs": 1000,
    "maxRecordingRowsPerEval": 10000,
    "maxAlertInstancesPerRule": 10000,
    "clusterEnabled": false,
    "clusterLeader": true,
    "metrics": {
      "schedulerRunsTotal": 42,
      "schedulerSkippedNotLeaderTotal": 0,
      "schedulerSkippedInflightTotal": 0,
      "evaluatedRulesTotal": 84,
      "evaluationFailuresTotal": 1,
      "recordingRowsWrittenTotal": 240,
      "lastRunUnixMs": 1700000060000,
      "lastError": null,
      "configuredGroups": 1,
      "configuredRules": 2,
      "pendingAlerts": 0,
      "firingAlerts": 1,
      "localSchedulerActive": true
    },
    "groups": [
      {
        "name": "latency-rules",
        "tenantId": "default",
        "intervalSecs": 60,
        "labels": { "env": "prod" },
        "rules": [
          {
            "id": "default/latency-rules/recording/job:request_duration_seconds:p99",
            "name": "job:request_duration_seconds:p99",
            "kind": "recording",
            "expr": "histogram_quantile(0.99, rate(request_duration_seconds_bucket[5m]))",
            "intervalSecs": 60,
            "labels": {},
            "annotations": {},
            "lastEvalTimestamp": 1700000060,
            "lastEvalUnixMs": 1700000060123,
            "lastSuccessUnixMs": 1700000060123,
            "lastDurationMs": 4,
            "lastError": null,
            "lastSampleCount": 12,
            "lastRecordedRows": 12,
            "state": "ok",
            "alertInstances": []
          },
          {
            "id": "default/latency-rules/alert/HighLatency",
            "name": "HighLatency",
            "kind": "alert",
            "expr": "job:request_duration_seconds:p99 > 1.0",
            "intervalSecs": 60,
            "forSecs": 120,
            "labels": { "severity": "critical" },
            "annotations": { "summary": "p99 latency above 1 s for 2 minutes" },
            "lastEvalTimestamp": 1700000060,
            "lastEvalUnixMs": 1700000060124,
            "lastSuccessUnixMs": 1700000060124,
            "lastDurationMs": 2,
            "lastError": null,
            "lastSampleCount": 1,
            "lastRecordedRows": 0,
            "state": "firing",
            "alertInstances": [
              {
                "key": "HighLatency{alertname=\"HighLatency\",env=\"prod\",job=\"api\",severity=\"critical\"}",
                "sourceMetric": "job:request_duration_seconds:p99",
                "labels": [
                  { "name": "alertname", "value": "HighLatency" },
                  { "name": "env", "value": "prod" },
                  { "name": "job", "value": "api" },
                  { "name": "severity", "value": "critical" }
                ],
                "activeSinceTimestamp": 1700000000,
                "lastSeenTimestamp": 1700000060,
                "firingSinceTimestamp": 1700000120,
                "state": "firing",
                "sampleType": "scalar",
                "sampleValue": "1.42"
              }
            ]
          }
        ]
      }
    ]
  }
}
```

**Rule state field summary**

| `state` | Meaning |
|---|---|
| `"ok"` | Recording rule has been evaluated at least once without error. |
| `"inactive"` | Rule has not been evaluated yet. |
| `"pending"` | Alert rule has at least one pending instance and no firing instances. |
| `"firing"` | Alert rule has at least one firing instance. |
| `"error"` | Last evaluation produced an error. |

---

## RBAC permissions

| Action | Endpoint | Required resource |
|---|---|---|
| Read | `GET /api/v1/admin/rules/status` | `admin:rules` (read) |
| Write | `POST /api/v1/admin/rules/apply` | `admin:rules` (write) |
| Write | `POST /api/v1/admin/rules/run` | `admin:rules` (write) |

See [Security model](security.md) for how to assign these permissions to a service account.

---

## Environment variables

The following environment variables tune the rules runtime. They are read once at startup.

| Variable | Default | Description |
|---|---|---|
| `TSINK_RULES_SCHEDULER_TICK_MS` | `1000` | Background scheduler tick interval in milliseconds. Must be > 0. This only controls how often the scheduler wakes up to check for due rules — actual rule evaluation still follows each rule's configured interval. |
| `TSINK_RULES_MAX_RECORDING_ROWS_PER_EVAL` | `10000` | Maximum number of rows a single recording rule evaluation is allowed to write. Evaluations producing more rows are rejected with an error. |
| `TSINK_RULES_MAX_ALERT_INSTANCES_PER_RULE` | `10000` | Maximum number of concurrent active alert instances for a single alerting rule. Evaluations that exceed this limit are rejected with an error. |

---

## Constraints and limits

- **Duplicate rule identifiers** — Rule identifiers are formed as `{tenantId}/{groupName}/{kind}/{name}`. All identifiers across the entire apply payload must be unique.
- **Reserved labels** — Rule and group labels must not use `__name__`, the internal tenant isolation label, or other reserved names. Annotation keys do not have this restriction.
- **Expression type** — Both recording and alerting rule expressions must evaluate to a scalar or instant vector. Range vectors and strings are rejected.
- **Recording rule metric names** — Output metric names must be valid (non-empty, ≤ 256 bytes).
- **Group requirements** — Every group must have a non-empty name, a valid tenant ID, a positive interval, and at least one rule.
- **In-flight concurrency** — Only one scheduler run (background or manually triggered) executes at a time. A `POST /api/v1/admin/rules/run` request returns 409 when a run is already in progress.
