0.9.0
tsink
- BREAKING: Upgrade storage metadata/index format to disk-backed inverted index + roaring bitmaps + persisted series/label dictionaries (introduces `series_index.bin` and bumps segment format to v2; old on-disk segments are not compatible)
- Use roaring bitmap postings for metric/label matcher candidate resolution
- Persist and reload global series/label dictionaries and series definitions to accelerate reopen/startup
- Encode/decode `postings.bin` using roaring payloads
- Compress `chunks.bin` chunk payloads with fast zstd (level 1) when it reduces size; keep uncompressed payloads when not beneficial, and transparently decode both formats on read via chunk flags
- Add storage observability snapshot API (`Storage::observability_snapshot`) with structured WAL/flush/compaction/query internals
- Instrument WAL internals (replay runs/frames/points/errors/duration, append series/batches/points/bytes/errors, reset stats, active segment/highwater visibility)
- Instrument flush/persist internals (pipeline runs/success/timeouts/errors/duration, active flush series/chunks/points, persist success/noop/errors, persisted series/chunks/points/segments, eviction stats)
- Instrument compaction internals with per-run source/output segment/chunk/point accounting and expose `CompactionRunStats`
- Instrument query internals (`select`, `select_with_options`, `select_all`, `select_series`) with call/error/duration/result counts plus merge-path vs append/sort-path selection counters
- Replace admission-time full memory scans with incremental per-shard memory accounting updated on mutations; keep full scans for reconciliation/debug paths
- Remove duplicate write-path WAL serialization by pre-encoding series/sample frame payloads once and reusing encoded bytes for both admission sizing and append
- Merge insert-path cardinality projection and series resolution into one registry pass to shorten write-lock hold time and reduce contention
- Skip unconditional active-chunk finalize sort when append timestamps are already monotonic; only sort out-of-order chunks
- Add atomic snapshot/restore APIs (`Storage::snapshot`, `StorageBuilder::restore_from_snapshot`) with segment-consistent, WAL-aware backups
- Decouple background flush/compaction maintenance from WAL enablement so persistent storage keeps draining/compacting even when WAL is disabled
- Fix flaky close/persist overlap handling by preserving same-timestamp distinct values across segments while still preferring compacted generations after crash-recovery overlap
- Always stamp persisted segment manifests with WAL high-water marks whenever WAL is configured, including persist paths invoked with non-WAL flags
- Make compaction source replacement crash-recoverable via replacement markers finalized at startup and before compaction passes
- Add single-process `data_path` lock file (`.tsink.lock`) to prevent concurrent writers from opening the same storage root
- Surface background flush/compaction worker failures in observability health state, with optional fail-fast mode to stop new operations
- Strengthen fsync durability boundaries for snapshot/restore rename flows and WAL reset/rotation directory mutations
- Add configurable WAL replay policy (`Salvage` default, `Strict` optional) for mid-log corruption handling during startup replay
- Expand failure-mode verification with deterministic corruption fuzz tests for WAL replay tails and segment decoding robustness
- PromQL: fix metricless selector matcher semantics, enforce one-to-one vector matching for binary ops, and align metric-name dropping in vector binary results
tsink-server
- Deepen `/metrics` exposition with WAL/flush/compaction/query internal counters and gauges
- Deepen `/api/v1/status/tsdb` response with nested internal observability sections (`wal`, `flush`, `compaction`, `query`)
- Add admin snapshot/restore endpoints (`POST /api/v1/admin/snapshot`, `POST /api/v1/admin/restore`)

0.8.1
general
- Add CI publish jobs
tsink
- Reduce segment flush I/O by building manifest hashes from in-memory buffers instead of re-reading staged files
- Reduce segment flush peak memory by sorting chunk indices instead of cloning per-series chunk vectors
- Improve WAL open-time recovery cost by scanning only the active segment (and nearest prior non-empty segment when needed) for highwater
- Replace WAL frame header heap allocation with a fixed 24-byte stack buffer on append
- Reuse a single payload buffer while scanning WAL sequence numbers to avoid per-frame allocations
- Eliminate `DataPoint` value clones in `Encoder::encode` and `Encoder::choose_codecs` by encoding from borrowed points
- Make `Encoder::choose_codecs` select codec IDs directly instead of building a combined encoded payload
tsink-uniffi
- Initialize
tsink-server
- Fix HTTP 422 responses using wrong reason phrase ("Unknown" instead of "Unprocessable Entity")
- Validate end >= start in /api/v1/query_range, returning a clear error instead of empty results
- Add --version / -V flag to CLI

0.8.0
- Complete storage engine rewrite (LSM-tree with L0/L1/L2 compaction)
- Segmented WAL with CRC32 checksums, fsync, replay recovery
- Multi-type value system (f64, i64, u64, bool, bytes, string)
- PromQL query engine (lexer, parser, evaluator — 23 functions, 15 binary ops, 7 aggregations)
- Async storage wrapper (tokio-based)
- HTTP server binary with Prometheus remote read/write compatibility
- Sharded concurrency (64 shards), background flush/compaction threads
- Memory budget enforcement with graduated pressure relief
- Segment-level retention sweeper with physical disk reclamation
- CI pipeline (fmt, clippy, tests, benchmarks, BPP regression checks)
