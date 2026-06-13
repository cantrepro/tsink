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
