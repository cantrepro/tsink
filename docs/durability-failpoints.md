# Durability failpoints

This matrix records deterministic failure-injection evidence for the durability boundaries listed
in `GOAL.md` Phase 3. The hooks are available only under `cfg(test)` or are reached through
test-only wrappers; normal builds contain no switch that can activate them. A write that returns an
error has no acknowledgement. A prior `Durable` acknowledgement must remain recoverable even when
later maintenance fails.

## Matrix

| Required point | Hook and exact boundary | Focused test | Expected result and recovery |
|---|---|---|---|
| Series-definition WAL append | `WalDurabilityFailpoint::SeriesDefinitionAppend`, immediately before the frame header write | `engine::storage_engine::tests::durability_failpoints::series_definition_wal_append_failpoint_rolls_back_identity_and_reopen` | The call returns an error and no acknowledgement. The new identity, WAL frame, and value are absent both in-process and after reopen. |
| Sample WAL append | `WalDurabilityFailpoint::SamplesAppend`, immediately before the sample frame header, after a new definition may already have been staged | `engine::storage_engine::tests::durability_failpoints::sample_wal_append_failpoint_rolls_back_staged_definition_and_reopen` | The complete logical write rolls back, including its earlier definition frame. Reopen exposes neither identity nor value. |
| WAL flush | `WalDurabilityFailpoint::Flush`, after frames are buffered and immediately before `BufWriter::flush` | `engine::storage_engine::tests::durability_failpoints::wal_flush_failpoint_rolls_back_only_the_unacknowledged_write` | The failed write returns no acknowledgement and is truncated. A preceding `Durable` write remains exact after reopen. |
| WAL sync | `FramedWal::set_append_sync_hook`, after flush and immediately before `sync_data` | `engine::storage_engine::tests::ingest_failures::wal_sync_failure_does_not_ingest_points_or_survive_reopen` | Per-append synchronization failure returns an error, rolls back the flushed frames, and publishes no in-memory or replay-visible value. |
| Chunk sealing | `ChunkStorage::set_ingest_post_chunk_seal_hook`, after every affected chunk has encoded successfully in staged state but before timestamps, active state, sealed state, or WAL publication change | `engine::storage_engine::tests::durability_failpoints::chunk_seal_failpoint_aborts_wal_and_reopens_without_partial_rotation` | The rotating write returns an error. Its staged WAL sample and active-to-sealed handoff roll back; the preceding `Durable` point remains the only point after reopen. |
| Segment file creation | `fail_tmp_write_after_bytes_once` on staged `chunks.bin`, after create-exclusive temporary creation and before payload progress | `engine::segment::tests::segment_file_creation_failpoint_cleans_staging_and_retry_recovers` | No final segment or owned staging tree remains. Retrying the same segment identity publishes a checksum-valid segment with the expected series and point counts. |
| Index writing | `fail_tmp_write_after_bytes_once` on staged `chunk_index.bin`, after a bounded prefix write | `engine::segment::tests::segment_index_write_failpoint_cleans_staging_and_retry_recovers` | Partial index state and its owned staging tree are removed. Retry publishes one valid segment; no partial index becomes discoverable. |
| File sync | `fail_file_sync_matching_once`, immediately before the real `sync_all` call | `engine::segment::tests::segment_file_sync_failpoint_cleans_staging_and_retry_recovers`; `engine::storage_engine::tests::durability_failpoints::snapshot_copy_file_sync_failpoint_retains_unverified_staging_and_preserves_source` | Segment publication leaves no visible/staged partial result and can retry. Snapshot copy publishes no destination and retains the partial staging tree because descendant ownership was not completely captured before the failure; the `Durable` source value remains exact after reopen. |
| Directory sync | `fail_directory_sync_once` / `fail_directory_sync_matching_once`, immediately before directory `sync_all` on non-Windows builds | `engine::segment::tests::segment_publish_returns_error_and_rolls_back_when_parent_sync_fails`; `engine::storage_engine::tests::persistence_background::snapshot_publication_sync_failure_retains_the_visible_destination` | A segment whose rename became visible is rolled back. A snapshot whose final rename became visible is retained and reported with indeterminate durability so cleanup cannot delete consumer-created descendants. |
| Catalog/manifest replacement | `SegmentCatalogPublishStage::PointerPrePublication`, immediately before the atomic pointer write after generation/legacy publication; file-sync hook before the data-directory manifest temporary is replaced | `engine::storage_engine::maintenance::catalog_refresh::publication::tests::catalog_stage_failures_keep_prior_pointer_release_memory_and_retry_after_restart`; `engine::storage_engine::data_directory_manifest::tests::manifest_replacement_file_sync_failure_preserves_prior_bytes_and_reopen_recovers` | Finite readers stay pinned to the prior catalog pointer and retry succeeds after restart. A failed manifest replacement preserves the prior bytes, cleans its temporary, and a later reopen recovers the stored value before updating the version. |
| Compaction publication | `interrupt_after_compaction_output` after an output under a durable `Preparing` marker; directory-sync hook after `Ready` marker replacement and after final marker unlink | `engine::compactor::tests::interrupted_multi_output_compaction_recovers_preparing_and_releases_reservation`; `engine::compactor::tests::ready_marker_parent_sync_failure_is_resolved_before_source_retirement`; `engine::compactor::tests::recovered_ready_returns_catalog_diff_even_when_final_marker_sync_fails` | Recovery rolls back outputs left under `Preparing` and keeps all sources. A visible `Ready` marker is made durable before source retirement. Once replacement commits, the catalog diff is returned even if final cleanup synchronization reports durability debt. |
| WAL truncation/reset | `WalDurabilityFailpoint::ResetAfterTruncate`, after the empty active replacement is synchronized and before older segment removal, WAL-directory sync, and boundary-marker replacement | `engine::storage_engine::tests::durability_failpoints::wal_reset_after_truncate_failpoint_reopens_from_published_segment` | Reset reports maintenance failure, not a new write acknowledgement. The already `Durable` sample reopens from its published segment, and later durable writes and reopens retain the same identity and exact values. |
| Snapshot copy | File-sync hook on the copied staged WAL file, before that destination file's `sync_all` | `engine::storage_engine::tests::durability_failpoints::snapshot_copy_file_sync_failpoint_retains_unverified_staging_and_preserves_source` | The snapshot returns an error, publishes no destination, retains the not-fully-identity-captured staging tree for explicit recovery, and does not mutate the live source. |
| Snapshot final rename | `ChunkStorage::set_snapshot_pre_publication_hook`, after staging is fully synchronized and immediately before `rename_noreplace_and_sync_parents`; the existing copy observer installs a raced destination, and the directory-sync hook fails after a successful no-replace rename | `engine::storage_engine::tests::durability_failpoints::snapshot_pre_final_rename_failpoint_cleans_staging_and_reopens_source`; `engine::storage_engine::tests::persistence_background::snapshot_destination_created_during_copy_is_not_replaced`; `engine::storage_engine::tests::persistence_background::snapshot_publication_sync_failure_retains_the_visible_destination` | A pre-rename error removes identity-captured staging and publishes nothing while the `Durable` source reopens exactly. The atomic no-replace rename preserves the other owner's destination and removes only tsink staging. A post-rename sync failure retains the visible destination with an explicit indeterminate-durability error, preserving any consumer-created descendant. |

## Acknowledgement boundaries

- Failures during WAL append, flush, sync, or chunk staging occur before commit and return an error,
  so no `WriteResult` acknowledgement exists.
- A logical WAL boundary failure after in-memory commit returns `Volatile`, as exercised by
  `wal_publication_failure_keeps_a_replay_safe_prefix_for_later_publication` and
  `wal_post_rename_sync_failure_preserves_an_ambiguously_published_prefix`.
- Segment flush, compaction, WAL reset, and snapshot are maintenance operations. Failure cannot
  revoke an earlier acknowledgement; their tests reopen and verify the earlier `Durable` identity
  and value or preserve the last committed catalog generation.

## Limits of this evidence

These hooks inject deterministic Rust errors or an in-process panic at named boundaries. They do
not simulate a kernel crash, sudden host power loss, torn sectors, a device that lies about flush
completion, controller-cache loss, or every filesystem's rename behavior. The process-based
abrupt-termination suite in `tests/crash_durability_test.rs` separately kills a child only after
the parent receives its returned acknowledgement; it does not yet drive every hook in this table
from another process.

On Windows, the current directory-sync implementation is a documented no-op; its hook verifies
error-path ordering and cleanup but cannot establish a directory-flush guarantee that the
implementation does not make. File-sync hooks fire immediately before the real syscall, so they
prove failure handling at that boundary, not what a particular device does after reporting
success. See [the durability contract](durability.md) for the platform-qualified public guarantee.
