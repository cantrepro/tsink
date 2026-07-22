# Changelog

All notable user-visible changes to tsink are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases follow Semantic
Versioning for the public API. Storage-format compatibility is tracked separately from Rust API
compatibility and is not yet covered by a stable support promise.

## [Unreleased]

### Added

- A repository progress ledger and product-positioning documentation for the embedded metrics
  engine roadmap.
- A declared Rust 1.89 minimum supported Rust version (MSRV) and an MSRV CI check.
- A design-partner interview guide for validating embedded, test-database, and edge use cases.
- Canonical `Storage::write_batch` and async/UniFFI equivalents with explicit `Atomic` and
  `BestEffort` modes, indexed outcomes, structured rejection categories, and bounded diagnostics.
- An opt-in `with_max_future_skew` admission cutoff; the default future-timestamp behavior remains
  unchanged.
- A durability contract covering core WAL modes, lifecycle behavior, server sidecars, cluster
  acknowledgements, and platform limits.
- Structured write-rejection, acknowledgement, partial-effect, and indeterminate-outcome metrics
  and HTTP response headers across the principal ingest adapters.

### Changed

- Project and package descriptions now lead with the embeddable, Prometheus-native use case.
- Repository and package links now point to `github.com/cantrepro/tsink`.
- Cluster mode is documented as experimental and is no longer presented as the primary adoption
  path.
- The root crate package excludes repository-only CI, progress, roadmap, and maintenance-script
  files.
- Multi-shard writes stage every fallible active-state mutation before publication, and failed
  active-head finalization preserves query visibility and correct memory accounting.
- Principal HTTP and internal cluster ingest paths use canonical atomic writes and validate backend
  result invariants before reporting success.
- Metadata and exemplar sidecar stores publish staged state only after persistence succeeds; a
  later sidecar failure discloses already accepted components.
- StatsD relative-gauge state advances only after proven row acceptance, oversized UDP datagrams
  are rejected without truncation, and Graphite closes a connection when a line cannot be parsed or
  stored.
- Cluster write consistency now counts one acknowledgement per replica and shard only after all of
  that replica's bounded transport fragments succeed; successful fragments still contribute to
  physical commit accounting when a later fragment fails.
- Cluster dedupe completion persistence failures are retryable and disclose established row and
  sidecar effects instead of inventing success. Edge-sync and hinted-handoff replay retain entries
  until a complete canonical atomic result is validated, and edge-sync appends and flushes its
  queue acknowledgement record before removing an entry from memory.

### Compatibility

- Existing compatibility write methods remain available and retain their all-or-error signatures.
  The canonical methods and result types are additive; third-party `Storage` implementations get an
  explicit unsupported default rather than fabricated indexed outcomes.
- Successful HTTP writes now include durability/component headers. Rejected or partially applied
  envelopes can return more specific non-success statuses and error codes than before.
- Graphite clients now observe a failed line as connection closure. StatsD remains response-less by
  design, with failures exposed through bounded counters.
- The cluster dedupe log stores optional completion data for exact retry replay and remains able to
  read older markers without that field; retries of legacy markers now return an explicit conflict
  because their original result cannot be reconstructed.
- No core segment or WAL wire-format version changed in this work.
- Rust versions older than 1.89 are not supported beginning with the next release.

## 0.10.2 - 2026-06-13

- Baseline release predating this changelog. Consult Git history for earlier changes.

[Unreleased]: https://github.com/cantrepro/tsink/commits/master
