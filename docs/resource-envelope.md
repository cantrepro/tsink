# Resource envelope

This document is the status ledger for the resource-envelope scenarios required by
[`GOAL.md` §16](../GOAL.md#16-phase-8-resource-envelope-and-reproducible-benchmarks). It reports
numeric observations where they exist and labels missing evidence as unmeasured. The standard
resource profiles remain enforceable provisional configurations, not measured capacity promises.
The qualification gaps are tracked in
[`resource-profile-measurements.md`](resource-profile-measurements.md#remaining-clean-environment-gate)
and [`goal-progress.md`](goal-progress.md#current-risks-and-blockers).

## Reading the numbers

The measurement surfaces are not interchangeable:

- **Modeled bytes** are tsink's internal retained-state or query-work accounting. Retained query
  memory follows owned collection capacities and documented allocation allowances; logical
  returned-byte work follows fixed result slots and content lengths, independent of allocator
  spare capacity. A profile's configured ceiling is enforced against the applicable model, not
  against resident memory.
- **Requested Rust heap** is whole-process memory requested through the benchmark's `System`
  global-allocator wrapper. It includes benchmark and runtime allocations that use that allocator,
  but excludes allocator metadata and slack, memory mappings, thread stacks, and foreign
  allocations.
- **Phase-current RSS** is whole-process resident memory sampled at named workload phases. It
  includes tsink, the benchmark, the allocator, and the runtime; it is not engine-only memory.
- **Peak RSS** is the operating system's process-lifetime high-water. It is attributable to one
  benchmark repetition only when that repetition runs in a fresh child.
- **Peak write transient** is a historical maximum of one modeled foreground-write/replay scratch
  component. It is not a phase-coincident total and must not be added to an unrelated retained-state
  sample.

The complete field definitions and exclusions are in
[`What the harness measures`](resource-profile-measurements.md#what-the-harness-measures).

The continuation rows below used three repetitions in one benchmark process. Consequently, their
modeled values and persisted-size values remain usable, but the reported RSS values are cumulative
process high-waters that include earlier repetitions and allocator retention. They are neither
per-run RSS ranges nor calibration ratios. Those rows predate retained requested-heap and
phase-current-RSS results.

The fresh-process runner now starts one child per repetition and emits provenance, phase-current
RSS, peak RSS, phase-current requested heap, and peak requested heap:

```bash
TSINK_BPP_OUTPUT_DIR=benchmark-results/resource-profiles \
  scripts/measure_bpp_fresh.sh <preset>
```

No clean-revision fresh-process result matrix is interpreted here yet. Until those runs are
retained, requested Rust heap and attributable per-repetition RSS remain **pending measurement**.

## Current measured ranges

These are the modified-tree continuation observations from 2026-07-26. Each range is
**p50–p95 across three runs**, not minimum–maximum; with three repetitions, nearest-rank p95 is the
maximum observation. All four mixed rows completed with zero late rejections and zero suite
failures. The base revision was
`4b54297e94ca1c86b4416c9bdc4f161e41e67629`; each row's distinct working-diff digest is recorded
beside its source table in
[`resource-profile-measurements.md`](resource-profile-measurements.md#test-profile-calibration-row).

| Profile row | Active series | Retained points/run | Post-write modeled bytes, p50–p95 | Post-settle modeled bytes, p50–p95 | Effective persisted B/point, p50–p95 | Old cumulative peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| `test` | 1,000 | 251,000 | 5,947,719–5,963,340 | 4,115,711–4,146,673 | 2.332267–2.339904 | 46,104,576 |
| `edge` | 20,000 | 1,020,000 | 90,047,240–90,134,415 | 88,502,387–88,527,536 | 4.357009–4.366931 | 429,703,168 |
| `embedded` | 50,000 | 2,050,000 | 221,056,826–221,100,680 | 220,602,302–220,674,333 | 5.023767–5.025235 | 1,018,019,840 |
| `server` | 100,000 | 4,100,000 | 441,000,192–441,075,551 | 440,460,413–440,591,746 | 4.889525–4.893182 | 1,995,440,128 |

Source tables:
[`Test`](resource-profile-measurements.md#2026-07-26-test-continuation-rerun),
[`Edge`](resource-profile-measurements.md#2026-07-26-edge-continuation-rerun),
[`Embedded`](resource-profile-measurements.md#2026-07-26-embedded-continuation-rerun), and
[`Server`](resource-profile-measurements.md#2026-07-26-server-base-continuation-rerun).

The separate persisted-size target failed in every mixed row. The table therefore records
observed effective persisted bytes per retained point; it does not establish a target, a
type-specific bytes-per-sample result, or a profile qualification.

Two specialized Server rows also have three modified-tree repetitions:

| Workload | Completed work/run | Numeric range, p50–p95 | Modeled query range, p50–p95 | Old cumulative peak RSS | Boundary result |
|---|---|---|---|---:|---|
| 16 writers | 400,000 new series | 114,842.262–115,986.291 series/s; writer-p95 3,261.169–3,386.310 ms | Not emitted | 4,083,548,160 | 0 failures |
| 32 queries plus writer | 262,144 query points and 100,000 writer points | query-p95 6.944–8.415 ms; writer 438.050–457.891 ms | peak shared reservation 4,411,479–6,666,297 bytes | 168,181,760 | N admitted, N+1 rejected; active/shared reservations returned to 0/0 |

Source tables:
[`Server writers`](resource-profile-measurements.md#2026-07-26-server-writer-continuation-rerun)
and
[`Server query pressure`](resource-profile-measurements.md#2026-07-26-server-query-pressure-continuation-rerun).
The writer row did not emit a retained-state modeled-memory sample. Neither old cumulative RSS
number is a per-run or engine-only observation.

## Required-scenario status

`Measured` means the required scenario has a retained numeric result. `Partial` means a related
numeric result or deterministic boundary test exists but does not cover the required shape.
`Unmeasured` means no qualifying numeric result is recorded in the current evidence.

| Required scenario from §16.2 | Status | Current evidence and exact gap |
|---|---|---|
| RSS and internal accounting at 10k, 100k, and 1M active series where feasible | **Partial** | Modeled retained-state rows exist at 1k, 20k, 50k, and 100k active series. The 100k row reports 441,000,192–441,075,551 post-write modeled bytes. Exact 10k and 1M rows are absent. Existing RSS figures are cumulative same-process peaks; fresh phase-current and per-repetition peak RSS are pending. |
| Bytes per sample for counters, gauges, sparse series, and histograms | **Partial** | Mixed rows report 2.332267–5.025235 effective persisted B/point across their p50–p95 endpoints. They do not isolate the four required sample classes, and every mixed row missed the separate target. |
| Startup time with increasing series and segment counts | **Unmeasured** | No numeric startup series/segment curve is recorded. |
| Crash recovery time with increasing WAL sizes | **Unmeasured** | No numeric WAL-size recovery curve is recorded. |
| Write throughput and latency under fixed small memory budgets | **Partial** | The Server writer row reports 114,842.262–115,986.291 series/s and 3,261.169–3,386.310 ms writer-p95, but it uses the Server profile rather than a fixed small-memory profile and emits no retained-state modeled-memory sample. |
| Query latency during flush and compaction | **Partial** | The Server pressure row reports 6.944–8.415 ms query-p95 during a concurrent writer. It does not establish that flush and compaction overlap occurred, and it covers the core query path rather than async, PromQL, HTTP, and distributed entry points. |
| Behavior at cardinality, memory, WAL, and disk limits | **Partial** | Deterministic limit tests and finite enforcement are recorded in [`Phase 2`](goal-progress.md#phase-2--finite-resource-profiles-and-admission-control-in-progress) and [`post-change verification`](goal-progress.md#post-change-verification). The mixed rows exercise modeled memory, and the query row exercises exact N/N+1 concurrency. WAL, disk, and cardinality boundary tests are enforcement evidence, not capacity measurements. |
| Idle CPU and resident memory | **Unmeasured** | No idle CPU or phase-current idle RSS range is recorded. |
| Shutdown duration | **Unmeasured** | Shutdown is bounded and observable, but no numeric duration range is recorded. |
| Snapshot and restore throughput | **Unmeasured** | No numeric snapshot or restore throughput range is recorded. |
| Testkit startup and teardown time | **Unmeasured** | No numeric testkit startup or teardown range is recorded. |
| Binary/library size and dependency footprint | **Partial** | Package dry runs contained 264 core, 91 server, and 18 UniFFI files, as recorded in [`post-change verification`](goal-progress.md#post-change-verification). Byte sizes and dependency-footprint measurements are absent. |
| Synchronization catch-up under bandwidth limits later | **Unmeasured / deferred** | No bandwidth-limited catch-up range is recorded; §16.2 explicitly assigns this scenario to later work. |

No regression threshold is claimed from this matrix. The §16.3 gates for bytes per sample,
startup/recovery time, idle resource use, testkit startup, common query latency, and memory
accounting remain to be defined from stable, repeatable evidence.

## Bounded claims and evidence

The current evidence supports only the following bounded statements:

| Bounded statement | Enforcement/test evidence | Measurement evidence |
|---|---|---|
| Standard profiles install finite modeled-memory and maintenance ceilings, and the named mixed rows completed under those modeled ceilings. This is not an RSS ceiling. | [`Phase 2 finite resource profiles`](goal-progress.md#phase-2--finite-resource-profiles-and-admission-control-in-progress) records exact-boundary and workspace verification status. | The four continuation tables linked under [Current measured ranges](#current-measured-ranges) report modeled bytes and distinguish cumulative RSS. |
| The Server query-pressure row admits all 32 configured query slots, rejects N+1 structurally, and releases active/shared reservations to zero. | [`Post-change verification`](goal-progress.md#post-change-verification) records the exact N/N+1 and release checks. | The [`Server query-pressure table`](resource-profile-measurements.md#2026-07-26-server-query-pressure-continuation-rerun) reports 32/32 queries, one N+1 rejection per run, and 0/0 resources after each run. |
| Public remote-read requests use one shared execution across all contained queries, local or distributed guarded reads, protobuf transformation, response encoding, and compression. They enforce cumulative sample/byte and modeled-memory boundaries with stable release behavior. This is deterministic test evidence, not a capacity measurement. | [`Post-change verification`](goal-progress.md#post-change-verification) records seventeen focused handler tests plus the single-node distributed HTTP case, including exact/one-under byte and full-request memory boundaries, cumulative multi-query work, decoded/result guard overlap and release ordering, empty-request admission, fail-closed accounting contracts, malformed/oversized input, and zero resources after success/failure. | **No capacity row yet**; broader query entry-point calibration remains open in the [clean-environment gate](resource-profile-measurements.md#remaining-clean-environment-gate). |
| PromQL multi-series, range-prefetch, exact-label fast paths, and `info()` data reads adopt detailed metadata/point guards through row transformation and cache ownership. `info()` map entries, map-to-vector conversion, and label merges are pre-admitted; bounded point backends fail closed when accounting is incomplete. | [`Post-change verification`](goal-progress.md#post-change-verification) records exact/one-under prefetch and `info()` memory, false/missing-accounting, two-result guard-lifetime, and release tests. | **Deterministic boundary evidence only**; there is no PromQL capacity or process-memory calibration row, and caller/backend internals remain outside the portable model. |
| Built-in local, tenant, and distributed point/metadata reads retain modeled result guards; bounded server paths fail closed for `Unaccounted` backends. Bounded remote metadata and point calls run sequentially, reserve request/header/raw/decode and merge memory, and apply canonical logical result limits to the deduplicated final union independently of the per-peer physical response cap. | [`Phase 2 query accounting`](goal-progress.md#phase-2--finite-resource-profiles-and-admission-control-in-progress) and [`Post-change verification`](goal-progress.md#post-change-verification) record capacity-invariance, exact/one-under result-memory, bounded RPC-buffer, sequential fanout, and conservative zero-residual tests. | **Deterministic boundary evidence only**; there is no distributed/adapter capacity or process-memory calibration row, and caller/backend internals plus external allocator/runtime buffers remain excluded. |
| Finite disk, WAL, cardinality, async-queue, and maintenance controls have boundary tests, but their current constants are not measured capacity promises. | [`Phase 2`](goal-progress.md#phase-2--finite-resource-profiles-and-admission-control-in-progress) records exact N/N+1, restart, rollback, and failure-path evidence. | **No complete capacity matrix yet**; item 5 of the [remaining gate](resource-profile-measurements.md#remaining-clean-environment-gate) lists these calibrations as open. |

Broader wording such as “bounded process memory,” “qualified for 1M active series,” or “measured
recovery envelope” is not supported by the present data.

## Reproduction and evidence separation

The versioned workload entry points are:

- [`scripts/measure_bpp.sh`](../scripts/measure_bpp.sh) for one-process named rows;
- [`scripts/measure_bpp_fresh.sh`](../scripts/measure_bpp_fresh.sh) for fresh-child repetitions and
  provenance capture;
- [`scripts/summarize_bpp_fresh.py`](../scripts/summarize_bpp_fresh.py) for validated
  nearest-rank aggregation.

Supported fresh-run presets are `quick`, `full`, `mixed-order`, `test`, `edge`, `embedded`,
`server`, `server-writers`, and `server-queries`. The standard named rows default to three fresh
children; `quick` and `mixed-order` default to one. `TSINK_BPP_RUNS` can select another positive
repetition count.

Fresh logs are written without overwrite beneath
[`benchmark-results/resource-profiles`](../benchmark-results/resource-profiles) when
`TSINK_BPP_OUTPUT_DIR` selects that directory. The
[`raw-result policy`](../benchmark-results/README.md) distinguishes evidence from performance
promises:

- provenance lines describe the source revision/diff, harness hashes, host, filesystems, toolchain,
  build profile, strict-preset mode, and terminal source-state stability;
- streamed Cargo and benchmark output replaces checkout, temporary, and explicitly configured
  storage roots with stable placeholders before retention;
- `RUN_RESULT`, `NEW_SERIES_RESULT`, or `QUERY_SATURATION_RESULT` lines are per-child raw results;
- `BPP_PRESET_CONFIGURATION` rows must be identical across children, and
  `FRESH_PROCESS_METRIC` lines are either derived nearest-rank summaries or explicit unavailable
  counts;
- interpretation and qualification status live in this document and
  [`resource-profile-measurements.md`](resource-profile-measurements.md).

A dirty-tree log is development evidence. Final qualification still requires a frozen clean
revision, retained raw output for every named row, repeatable maintenance/settle behavior,
fresh-process RSS/requested-heap interpretation, Linux/cgroup-constrained and low-memory hosts,
and calibration of the remaining finite constants.
