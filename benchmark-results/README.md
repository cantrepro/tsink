# Benchmark results

This directory retains raw, machine-readable output from the reproducible benchmark runners in
[`scripts`](../scripts). Resource-profile logs live in `resource-profiles/`; their UTC timestamped
filenames are generated without overwrite by:

```bash
TSINK_BPP_OUTPUT_DIR=benchmark-results/resource-profiles \
  scripts/measure_bpp_fresh.sh <preset>
```

New logs record the base revision, dirty state, a deterministic hash of every tracked and
non-ignored untracked source file, the tracked binary diff, harness hashes, host, toolchain,
filesystem, and configuration provenance before the per-process result rows, followed by
nearest-rank aggregate metrics. The runner verifies that the source-state hash is unchanged after
the final child. Generated benchmark results and build output are excluded from that hash.
Checkout, temporary-storage, and explicitly configured storage roots are replaced with stable
placeholders before child and Cargo output reaches a retained log. Older logs that predate those
headers remain parseable with
`scripts/summarize_bpp_fresh.py`; their missing provenance is documented alongside the interpreted
result.

These files are evidence, not performance promises. A dirty-tree result must say so, whole-process
RSS and requested heap are not engine-only accounting, and three-run p95 is the maximum
observation. Standard fresh runs clear ambient workload overrides and validate the named preset,
result schema, profile, storage kind, and identical per-child configuration. Diagnostic preset
overrides require `TSINK_BPP_ALLOW_PRESET_OVERRIDES=1`; configured retained storage additionally
requires `TSINK_BPP_ALLOW_KEEP_DIR=1` and is labeled in provenance. The directory is excluded from
published runtime crates.
