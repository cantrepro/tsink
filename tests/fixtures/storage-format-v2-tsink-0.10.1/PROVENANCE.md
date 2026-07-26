# `storage-format-v2-tsink-0.10.1` provenance

- Historical writer: tsink package `0.10.1` built from local Git commit `00cc627df7b36ae1838f68da273c42949f0a5d52` (commit subject: `release version 0.10.1`).
- Source isolation: `git archive 00cc627df7b36ae1838f68da273c42949f0a5d52` was extracted beneath `/tmp`; the export contained no `.git` directory and the repository worktree was not switched or rewritten.
- Fixture driver: `tests/fixture-generators/generate_storage_format_v2_tsink_0_10_1.rs` was copied into the historical export's `examples/` directory and compiled against that export. The driver rejects every package version other than `0.10.1`.
- Logical storage format: `2` (`framed_segment_v2`, `framed_wal_v2`, `registry_snapshot_v2`, and the blob value lane).
- Data-directory manifest: absent because the historical release predates `tsink-manifest.json`; no file was removed from the generated data directory.
- Generated: `2026-07-26` on `aarch64-apple-darwin` with `cargo run --offline --locked`.
- Retention metadata: not applicable. Storage format 2 does not persist the builder's runtime retention policy. Generation disables retention and includes a named retained sample that proves ordinary non-tombstoned data remains visible.
- WAL recovery: after cleanly closing the segment/tombstone baseline, the driver starts a child, waits for its `PerAppend` write to return `Durable`, flushes a readiness record, and abruptly kills the child while storage remains live.
- Lock file: the historical writer's zero-byte `.tsink.lock` file is intentionally retained and covered by the inventory.
- Byte reproducibility: not promised. Segment creation timestamps and concurrent series registration can change equivalent bytes. `CONTENT_HASHES.txt` freezes this reviewed instance and is never updated automatically by tests.

Reproduction is manual and destructive regeneration is forbidden:

```console
export_dir=$(mktemp -d /tmp/tsink-0.10.1-fixture.XXXXXX)
mkdir "$export_dir/source"
git archive 00cc627df7b36ae1838f68da273c42949f0a5d52 | tar -x -C "$export_dir/source"
mkdir "$export_dir/source/examples"
cp tests/fixture-generators/generate_storage_format_v2_tsink_0_10_1.rs \
"$export_dir/source/examples/"
CARGO_TARGET_DIR="$export_dir/target" cargo run --offline --locked \
--manifest-path "$export_dir/source/Cargo.toml" -p tsink \
--example generate_storage_format_v2_tsink_0_10_1 -- \
"$export_dir/storage-format-v2-tsink-0.10.1"
```

Tests never invoke this driver. A successor must use a new fixture ID and directory; never replace these frozen bytes in place.
