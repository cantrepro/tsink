# Resource-profile measurements

This file records the reproducible measurements used to calibrate finite resource profiles. It
reports tsink's modeled accounting surfaces and, in new runs, a separately labeled process-wide
RSS high-water observation. The RSS value is not engine-only accounting. See
[`resource-limits.md`](resource-limits.md) for the included and excluded modeled-memory categories.

## Qualification status

The profile constants are **not final**. `Test` remains at 64 MiB, `Edge` at 256 MiB, and
`Embedded` at 512 MiB after all three complete their post-fix memory rows three times. The failed
larger-ceiling probes exposed admission and timed-flush defects rather than supporting larger
constants. Server's base, writer-saturation, and query-pressure rows also pass three repetitions;
complete RSS calibration, query entry-point breadth, and the non-memory constants remain open.

All results on this page were collected from a modified working tree while Phase 2 implementation
was still changing. They are useful for rejecting undersized values and improving the harness, but
the final gate still requires a clean post-implementation rerun.

## Measurement environment

- Date: 2026-07-22 through 2026-07-23
- Host: Apple arm64 laptop, 24 GiB physical memory
- OS: Darwin 25.5.0
- Rust: 1.97.1 (`aarch64-apple-darwin`)
- Repository base revision: `4b7aace259f86a334e501c76853a9278a7d7ce21`
- Harness: optimized `benches/workload.rs`, deterministic seed `0xC0DEC0DE`

## What the harness measures

`TSINK_RESOURCE_PROFILE` now selects `test`, `edge`, `embedded`, or `server` explicitly. Every
successful `RUN_RESULT` reports the selected profile and the effective memory, local-disk, WAL,
cardinality, writer, query-concurrency, and maintenance limits. Named presets in
`scripts/measure_bpp.sh` select the corresponding profile and workload row.

Memory terms have deliberately narrow names:

- `max_post_write_accounted_memory_bytes` is the largest modeled retained-state sample observed
  immediately after a submitted batch, plus the initial and post-settle samples. It does not see a
  transient lease while `insert_rows` is executing and is not process RSS.
- `post_settle_accounted_memory_bytes` is the one modeled sample taken after the configured settle
  delay and before close.
- `process_peak_rss_bytes_so_far` is the operating system's `getrusage(RUSAGE_SELF)` high-water for
  the complete benchmark process: bytes on Darwin and KiB converted to bytes on other supported
  Unix targets. It is `unavailable` on non-Unix targets. The observation includes the allocator,
  runtime, benchmark-owned data, and all earlier runs in the same process, so it is deliberately
  named “so far” and cannot be attributed exclusively to tsink. Suite results report the maximum
  available observation rather than a per-run RSS percentile.
- A `MemoryBudgetExceeded { required, ... }` value is the engine's projected requirement at the
  rejected admission point. It is a useful lower bound for that operation, not a workload-wide
  high-water mark.
- `p95` and `max` in `SUITE_RESULT` aggregate the per-run values. With only three repetitions, the
  nearest-rank p95 is the maximum observation.

The July 23 runs below predate the renamed post-write sampling fields. Their reported
`accounted_memory_bytes` values are post-settle samples and are labeled that way here.

## Superseded high-cardinality result

An earlier in-progress run recorded the following result:

| Date | Profile actually selected | Runs | Active series | Retained points | Failures | Post-settle accounted memory | Pre-close local disk | Final persisted bytes | Effective B/point |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-07-22 | implicit `Embedded` | 1 | 50,000 | 2,050,000 | 0 | 169,094,990 | 26,059,687 | 6,542,205 | 3.191320 |

That observation is **superseded**, not a profile baseline. On July 23, after expanded accounting
and bounded-maintenance changes, the same workload first exposed a maintenance dependency defect;
after that defect was fixed, the stock 512 MiB run progressed normally but was rejected at
541,973,214 modeled bytes. Later 1 GiB diagnostic runs produced 643-723 MiB post-settle accounting
and 116-128 MiB of final persisted data. The old memory and bytes-per-point figures therefore do
not describe the current engine.

The exact stock-profile rerun was:

```bash
TSINK_BPP_RUNS=1 \
TSINK_ACTIVE_SERIES=50000 \
TSINK_PRIME_ALL_SERIES=1 \
TSINK_MIN_POINTS_PER_SERIES=1 \
TSINK_WARMUP_POINTS=500000 \
TSINK_MEASURE_POINTS=1500000 \
TSINK_BATCH_SIZE=4096 \
TSINK_SETTLE_MILLIS=1500 \
TSINK_FAIL_ON_TARGET=0 \
scripts/measure_bpp.sh quick
```

The run selected `Embedded`; it failed during measured ingest with a 536,870,912-byte budget and a
projected requirement of at least 541,973,214 bytes. Storage health remained non-degraded with no
background or maintenance errors.

## Test profile calibration row

The named row is three repetitions, 1,000 active series, and 251,000 retained points per run:

```bash
scripts/measure_bpp.sh test
```

Before the exact head-growth and timed-flush fixes, the stock 64 MiB profile failed all three
repetitions on the initial 1,000-series prime batch. Each failure reported the same 86,447,496-byte
requirement against a 67,108,864-byte budget; health remained clean. Diagnostic runs paired each
memory override with the same maintenance-byte override because configuration validation correctly
refuses to admit a chunk larger than a maintenance pass can process. All other Test limits stayed
unchanged.

```bash
TSINK_MEMORY_LIMIT_BYTES=100663296 \
TSINK_MAINTENANCE_MAX_BYTES_PER_PASS=100663296 \
scripts/measure_bpp.sh test

TSINK_MEMORY_LIMIT_BYTES=134217728 \
TSINK_MAINTENANCE_MAX_BYTES_PER_PASS=134217728 \
scripts/measure_bpp.sh test
```

| Diagnostic memory/pass ceiling | Successful runs | Retained points/run | Late rejections | p95 post-settle accounted bytes | p95 pre-close local disk | p95 final persisted bytes | p95 effective B/point |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 MiB (pre-fix stock) | 0/3 | — | — | — | — | — | — |
| 84 MiB | 3/3 | 251,000 | 0 | 7,135,008 | 1,265,810 | 297,156 | 1.183888 |
| 88 MiB | 3/3 | 251,000 | 0 | 7,550,879 | 1,419,923 | 298,687 | 1.189988 |
| 96 MiB | 3/3 | 251,000 | 0 | 7,058,488 | 1,274,199 | 299,020 | 1.191315 |
| 128 MiB | 3/3 | 251,000 | 0 | 9,176,408 | 1,641,438 | 172,336 | 0.686598 |

Those pre-fix thresholds no longer describe the write head after exact growth accounting and the
fill-aware timed flush. With both fixes in place, the original 64 MiB profile completed the same
three-repetition row cleanly:

| Successful runs | Retained points/run | Late rejections | p50 / p95 peak accounted bytes | p50 / p95 post-settle accounted bytes | p50 / p95 pre-close local disk | p50 / p95 final persisted bytes | p50 / p95 effective B/point |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3/3 | 251,000 | 0 | 5,716,213 / 5,756,707 | 4,899,566 / 5,550,733 | 5,044,268 / 5,049,531 | 583,522 / 623,367 | 2.324789 / 2.483534 |

The largest measured peak is 8.6% of the 64 MiB ceiling, which supports restoring the original
`Test` memory and maintenance-pass defaults. This row calibrates accounted memory only; it does not
yet qualify process RSS or the profile's other bounded-resource dimensions.

A one-repetition harness verification after adding the RSS field completed with zero late
rejections and reported `process_peak_rss_bytes_so_far=45,563,904` on Darwin. That value validates
the output path only: it is a process-wide cumulative high-water from a modified working tree, not
a three-run Test-profile RSS calibration or an engine memory limit.

The large effective-B/point variation despite a deterministic data seed shows that background
maintenance scheduling materially affects the post-settle layout. Release qualification needs a
stable maintenance point or a longer, justified settle condition.

## Edge profile calibration row

The named row is three repetitions, 20,000 active series, and 1,020,000 retained points per run:

```bash
scripts/measure_bpp.sh edge
```

The stock 256 MiB profile failed all three repetitions on the initial 20,000-series prime phase.
Each failure reported the same 353,950,090-byte requirement against a 268,435,456-byte budget.
Storage health remained non-degraded with no background or maintenance errors.

The prescribed paired diagnostics were:

```bash
TSINK_MEMORY_LIMIT_BYTES=402653184 \
TSINK_MAINTENANCE_MAX_BYTES_PER_PASS=402653184 \
scripts/measure_bpp.sh edge

TSINK_MEMORY_LIMIT_BYTES=536870912 \
TSINK_MAINTENANCE_MAX_BYTES_PER_PASS=536870912 \
scripts/measure_bpp.sh edge
```

| Diagnostic memory/pass ceiling | Successful runs | Failure phase | Projected required bytes |
|---:|---:|---|---:|
| 256 MiB (stock) | 0/3 | prime | 353,950,090 in every run |
| 384 MiB | 0/3 | warmup, measured, warmup | 402,982,892; 413,084,560; 403,818,742 |
| 512 MiB | 0/3 | measured | 537,561,064; 539,862,140; 541,511,511 |

Those pre-fix probes did not establish a passing candidate. The increasing rejection requirement
showed that a larger ceiling let more retained/index state accumulate before admission stopped;
the prime-batch lower bound could not be used as the profile size. Further probing stopped at the
requested 512 MiB bound. All non-memory Edge limits were left unchanged, apart from the paired
maintenance-byte override required by configuration validation.

### Edge cap-chasing diagnosis

This diagnosis is based on one additional focused 512 MiB run with failure-side memory and flush
snapshots enabled. It is a diagnostic probe, not another qualification row. The measured ingest
failed at a projected requirement of 547,201,432 bytes. By the time the error snapshot was taken,
the rejected write's transient reservation had unwound and the retained accounted state was
225,627,048 bytes:

| Accounted category after rejection | Bytes |
|---|---:|
| Active and sealed chunks | 1,280,000 |
| Series registry | 3,436,590 |
| Metadata caches | 13,694,472 |
| Persisted indexes | 181,257,932 |
| Persisted mmaps | 25,958,054 |
| Tombstones, WAL series cache, current write transient | 0 |

Persisted indexes and mmaps therefore represented 207,215,986 bytes, about 92% of retained
accounted state. Current write-transient bytes were zero after unwind, but the run's transient peak
was 353,307,514 bytes across 189 reservations; no transient reservation itself was rejected. The
pressure counters recorded two backpressure events, one final memory rejection, two relief
requests, and one observed relief. The flush pipeline was healthy: 37 runs all succeeded, with no
errors or timeouts. Those runs inspected 1,850,000 active-series candidates, hit the 50,000-item
pass ceiling in all 37 runs, and finalized 362,063 chunks containing only 765,632 points, or about
2.11 points per chunk. Persistence completed 23 runs plus 14 no-op runs, published 46 segments, and
exactly evicted all 362,063 persisted sealed chunks. No background or maintenance error was
recorded.

Two independent mechanisms explain why the reported requirement follows the configured ceiling:

1. The 250 ms timed flush uses the `BackgroundBounded` policy, which is allowed to finalize the
   current partial head. Edge permits 50,000 inspected items per pass for a 20,000-series workload.
   During continuous ingest, the timed worker therefore repeatedly turns very young heads into
   tiny chunks. Exact sealed-chunk eviction removes the hot encoded copies after persistence, but
   the chunk references, per-segment series summaries, postings, timestamp-index maps, and mapped
   segment bytes remain accounted. This is the source of the large retained-index component.
2. Write preparation estimates every new or reopened partition head as
   `size_of::<ActivePartitionHead>() + 2048 * size_of::<ChunkPoint>()`. On this arm64 build a
   `ChunkPoint` is 40 bytes, so a 4,096-series batch can be charged 335,544,320 bytes of eventual
   point capacity before the smaller control and metadata terms. `ChunkBuilder::new`, however,
   initially allocates only `min(2048, 64)` points: 10,485,760 bytes across the same 4,096 heads.
   The estimator is thus reserving future full-chunk capacity that the current write does not
   allocate. Timed flush removes those heads, so a later batch pays the same projection again.

The paired maintenance-byte override does **not** allocate or reserve that many bytes. It is a work
selection ceiling and is paired only because configuration validation requires a maintenance pass
to be able to admit the largest possible chunk. The observed pattern is consequently not expected
steady-state headroom or an oversized maintenance allocation. It is a timed-flush fragmentation
interaction amplified by an admission/accounting mismatch.

There is also a secondary admission-relief defect. On a shortfall of
`used + staged + transient + estimated_growth > budget`, admission asks sealed-chunk eviction to
target `budget`. The eviction function immediately returns whenever `used <= budget`, which is the
normal state for this kind of projected shortfall. It should instead target the residual retained
allowance after staged, transient, and estimated-growth bytes are reserved. This did not cause the
focused failure—its persisted sealed chunks had already been exactly evicted, and persisted indexes
and mmaps are not reclaimable by that path—but it prevents relief when reclaimable sealed overlap
does exist.

The implementation now models the exact builder-capacity delta caused by planned points, beginning
with the 64-point initial block, and has exact one-point regressions across 4,096 new and reopened
series. Admission relief targets the residual retained allowance after staged, transient, and
growth reservations. Timed WAL-backed flushes defer a healthy current head until it reaches half
the initial block; memory/WAL pressure, WAL-disabled durability, tier freshness, explicit flush,
and close still force it when required.

The stock 256 MiB Edge row was then repeated without any memory or maintenance override:

| Runs | Retained points/run | Late rejections | p50/p95 peak post-write accounted bytes | p50/p95 post-settle accounted bytes | p50/p95 pre-close local disk | p50/p95 final persisted bytes | p50/p95 effective B/point |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | 1,020,000 | 0 | 90,011,548 / 90,011,984 | 88,693,875 / 88,890,540 | 44,286,853 / 44,291,025 | 4,453,082 / 4,457,150 | 4.365767 / 4.369755 |

All three runs completed, and the one-repeat A/B immediately before them also completed at
90,061,411 peak accounted bytes. Persisted-index accounting fell from 181,257,932 bytes in the
focused pre-fix failure to 8,356 bytes in every post-fix run. The full three-run peak used 33.5% of
the 268,435,456-byte modeled ceiling and was tightly clustered within 1,421 bytes. This evidence
supports retaining the existing 256 MiB provisional Edge constant; it does not qualify process
RSS, the non-memory profile dimensions, or the benchmark's separate bytes-per-point target (the
reported `TARGET_CHECK` remains false for this high-cardinality mixed row).

## Embedded profile calibration row

The named row is three repetitions, 50,000 active series, and 2,050,000 retained points per run.
Diagnostic commands were:

```bash
TSINK_MEMORY_LIMIT_BYTES=805306368 \
TSINK_MAINTENANCE_MAX_BYTES_PER_PASS=805306368 \
scripts/measure_bpp.sh embedded

TSINK_MEMORY_LIMIT_BYTES=1073741824 \
TSINK_MAINTENANCE_MAX_BYTES_PER_PASS=1073741824 \
scripts/measure_bpp.sh embedded
```

The 768 MiB diagnostic failed all three repetitions during measured ingest. The projected required
values were 829,421,944, 817,195,276, and 813,436,130 bytes; all failures left storage health clean.

The 1 GiB diagnostic completed all three repetitions before the head-growth and timed-flush fixes:

| Runs | Retained points/run | Late rejections | p50/p95 post-settle accounted bytes | p50/p95 pre-close local disk | p50/p95 final persisted bytes | p50/p95 effective B/point |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 2,050,000 | 0 | 671,922,153 / 722,523,612 | 124,461,221 / 131,807,825 | 121,476,347 / 128,261,375 | 59.256755 / 62.566524 |

The three post-settle accounted-memory samples were 643,073,810, 671,922,153, and 722,523,612
bytes. Like the earlier Edge probes, this evidence was dominated by inaccurate full-head growth
projection and two-point timed-flush fragmentation; it no longer determines the profile constant.

After both fixes, one stock 1 GiB confirmation run peaked at 221,064,725 modeled bytes. The original
512 MiB candidate then completed three repetitions with paired memory and maintenance overrides.
After restoring those values as the actual Embedded defaults, the exact named preset completed
another three repetitions; this table records that final stock-profile run:

| Runs | Retained points/run | Late rejections | p50/p95 peak post-write accounted bytes | p50/p95 post-settle accounted bytes | p50/p95 pre-close local disk | p50/p95 final persisted bytes | p50/p95 effective B/point |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | 2,050,000 | 0 | 221,101,030 / 221,164,931 | 220,752,733 / 220,786,284 | 95,345,592 / 95,360,617 | 10,262,132 / 10,290,638 | 5.005918 / 5.019823 |

Persisted-index accounting was 8,356 bytes in all three stock post-fix runs. The largest measured peak
used 41.2% of the 536,870,912-byte ceiling. This supports retaining 512 MiB as the provisional
Embedded memory and maintenance value; it does not qualify process RSS or the profile's other
resource dimensions.

## Named workload presets and remaining matrix

The script's exact default rows are:

| Preset | Selected profile | Runs | Active series | Warmup + measured points | Primed total | Batch | Active partition heads | Status |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `test` | Test | 3 | 1,000 | 50,000 + 200,000 | 251,000 | 2,048 | 4 | Post-fix 64 MiB value passed 3/3 |
| `edge` | Edge | 3 | 20,000 | 250,000 + 750,000 | 1,020,000 | 4,096 | 8 | Post-fix stock 256 MiB value passed 3/3 |
| `embedded` | Embedded | 3 | 50,000 | 500,000 + 1,500,000 | 2,050,000 | 4,096 | 8 | Post-fix provisional 512 MiB value passed 3/3 |
| `server` | Server | 3 | 100,000 | 1,000,000 + 3,000,000 | 4,100,000 | 8,192 | 16 | Passed 3/3 with zero late rejections |
| `server-writers` | Server | 3 | 16 writers x 25,000 new series | specialized writer row | 400,000 new series | 25,000 per writer | 16 | Passed 3/3; all 1.2 million submitted series admitted |
| `server-queries` | Server | 3 | 4,096 seeded series; 32 query workers | 64 points/series + 100,000 concurrent writer points | 262,144 seeded points | 4,096 | 16 | Passed 3/3 with exact N/N+1 admission and zero leaked query resources |

Reproduce the Server rows with:

```bash
scripts/measure_bpp.sh server
scripts/measure_bpp.sh server-writers
```

The earlier Edge failures are superseded by direct post-fix evidence. The stock 256 MiB row now
passes cleanly; no larger candidate is supported or needed by this memory row.

The Server base row doubles both the Embedded active-series and retained-point counts. A simple
doubling of the post-fix Embedded p95 post-settle sample is about 442 MB, below Server's 2 GiB
memory limit. The exact three-repetition base row completed all 4,100,000 retained points per run
with zero late rejections:

| Runs | Retained points/run | Late rejections | p50 / p95 peak accounted bytes | p50 / p95 post-settle accounted bytes | p50 / p95 pre-close local disk | p50 / p95 final persisted bytes | p50 / p95 effective B/point |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | 4,100,000 | 0 | 440,921,332 / 441,129,372 | 440,582,395 / 440,710,421 | 190,716,800 / 190,724,905 | 19,989,473 / 20,032,963 | 4.875481 / 4.886089 |

The largest measured peak is 20.5% of the 2 GiB ceiling, so the base row does not support a larger
Server memory default. It does not qualify the profile by itself because RSS, query entry-point
breadth, and non-memory constants remain open.

The Server writer row starts all 16 profile writer slots together, with 25,000 unique new series per
writer. All three repetitions admitted every one of the 400,000 submitted series:

| Runs | New series/run | Failures | p50 / p95 aggregate series/s | p50 / p95 per-run writer-p95 latency |
|---:|---:|---:|---:|---:|
| 3 | 400,000 | 0 | 67,658.867 / 80,287.976 | 5,519.521 / 5,578.106 ms |

This exercises the configured writer concurrency and new-series admission path. It is not a
process-memory measurement and does not establish a throughput service-level objective.

The deterministic Server query-pressure row now fills all 32 profile query slots before releasing
the workers, verifies that the N+1 admission returns `ConcurrentQueries`, and then starts those
queries together with a 100,000-point writer. Each run returned all 262,144 requested query points,
accepted every writer point, and ended with zero active queries and zero shared query-memory
reservation:

| Runs | Queries/run | Structured N+1 rejections/run | p50 / p95 query-p95 latency | p50 / p95 writer latency | p50 / p95 peak shared query reservation | p50 / p95 total overlap time |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 32/32 | 1 | 32.535 / 32.866 ms | 889.064 / 949.297 ms | 5,244,541 / 6,092,048 bytes | 913 / 950 ms |

This demonstrates the Server concurrency boundary and release invariant under simultaneous ingest.
It does not calibrate async, PromQL, or HTTP query entry points, process RSS, or larger query shapes.

## Remaining clean-environment gate

Before profile constants can be final:

1. Freeze the Phase 2 implementation and rerun every named row from a clean revision, retaining the
   complete machine-readable output.
2. Retain all passed Server base, writer, and query-pressure rows in the final clean rerun.
3. Establish a deterministic maintenance/settle point so persisted-layout variance is understood.
4. Retain the new post-write modeled samples and process-wide RSS high-water fields for every clean
   row, then characterize allocator and harness overhead before making any total-process claim.
5. Calibrate local disk, WAL, cardinality, identity, write-batch, query, async-queue, concurrency,
   cadence, and maintenance-item constants. The current mixed-workload runs only exercise a small
   subset and do not justify those published values.
6. Repeat on Linux/cgroup-constrained and genuinely low-memory hosts; the current evidence is one
   Apple arm64 machine.
7. Retain tiny-limit boundary, restart, and failure-injection tests as enforcement evidence, but do
   not treat them as capacity measurements.

Until those steps are complete, the standard profiles are enforceable provisional configurations,
not measured capacity promises.
