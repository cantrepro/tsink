0.8.2
tsink
- Add storage observability snapshot API (`Storage::observability_snapshot`) with structured WAL/flush/compaction/query internals
- Instrument WAL internals (replay runs/frames/points/errors/duration, append series/batches/points/bytes/errors, reset stats, active segment/highwater visibility)
- Instrument flush/persist internals (pipeline runs/success/timeouts/errors/duration, active flush series/chunks/points, persist success/noop/errors, persisted series/chunks/points/segments, eviction stats)
- Instrument compaction internals with per-run source/output segment/chunk/point accounting and expose `CompactionRunStats`
- Instrument query internals (`select`, `select_with_options`, `select_all`, `select_series`) with call/error/duration/result counts plus merge-path vs append/sort-path selection counters
- Add atomic snapshot/restore APIs (`Storage::snapshot`, `StorageBuilder::restore_from_snapshot`) with segment-consistent, WAL-aware backups
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
