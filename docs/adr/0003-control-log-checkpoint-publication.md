# ADR 0003: Authoritative control-log checkpoint publication

- Status: Accepted for staged implementation
- Date: 2026-07-22

## Context

The experimental cluster control plane persists two files that describe one logical state:

- `control-log.json`, which contains the consensus term, commit index, snapshot boundary, and
  retained log entries; and
- `control-state.json`, which contains the materialized membership, leader, ring, and handoff
  state through an applied log index.

The original implementation writes these files independently and, on commit and snapshot paths,
publishes the materialized state before the log. A process or filesystem failure between those
writes can leave the state file ahead of the durable commit index. Startup correctly refuses that
combination, but it cannot repair it. Independently reserving each replacement against the shared
disk budget would also allow the first publication to consume capacity needed by the second.

Publishing the log first is necessary but not sufficient. After snapshot installation or recovery
restore, the old state file may be older than the new compacted log and the commands needed to
reconstruct the snapshot may no longer be present. The authoritative file therefore needs to carry
the checkpoint it commits.

## Decision

### Authority and compatibility

`control-log.json` is the authoritative recovery record. Schema v2 requires an embedded
`checkpointState` field containing the materialized `ControlState` represented exactly through the
log's commit index. Existing schema-v1 files remain readable as migration input: they use the
external state file plus retained committed entries during their first successful open, then are
republished as schema v2 before mutations are accepted. A v2 file without a checkpoint is invalid.

`control-state.json` remains a durable mirror for inspection, recovery exports, and compatibility,
but it is not allowed to make a commit authoritative. On open, a valid embedded checkpoint wins
over an older mirror. The runtime validates that its applied index and term agree with the durable
log, replays any committed retained entries if necessary, and repairs the mirror before accepting
mutations. A mirror ahead of a legacy log remains an error because no authoritative checkpoint can
prove that state committed.

The schema bump deliberately makes downgrade fail closed. An older writer could otherwise ignore
and later erase the authoritative checkpoint field. Deployment documentation calls out that a
schema-v2 control log cannot be opened by software that only supports schema v1.

### Membership and leadership eligibility

Only nodes whose committed membership status is `Active` are control voters. A `Joining` or
`Leaving` node may receive replication traffic while membership changes converge, but its response
does not count toward quorum and it cannot raise the receiver's term or assert leadership through
append or snapshot RPCs. A local node likewise reports itself as leader only while it remains
`Active`. If the recorded leader is no longer `Active`, failover treats that leader as ineligible
and selects the deterministic next candidate from the remaining Active voters.

This strict receiver-local eligibility check creates a convergence prerequisite: activation and
leadership transfer must not strand a voter whose control log has not caught up far enough to know
that the new leader is Active. The current workflow must activate a node only after control-log
catch-up and transfer leadership only after that activation is committed. A membership certificate
or joint-configuration proof that lets a newly activated leader prove eligibility to a lagging
receiver remains Phase 2 work; this ADR does not claim that guarantee is complete.

As a crash-safe near-term rule, an Active leader cannot propose its own `LeaveNode` transition.
Leadership must first move to another Active voter, which then commits the former leader's leave.
This avoids making the only node authorized to finish commit propagation ineligible in the middle
of its own membership change.

### One admission and publication unit

A control commit, snapshot installation, or recovery restore prepares the complete candidate log
and state mirror in memory without mutating the live runtime. It then:

1. computes the checked peak replacement cost for both encoded files;
2. acquires one shared `Cluster` disk-budget reservation for that complete peak;
3. writes, flushes, and synchronizes both collision-safe temporary files;
4. publishes and parent-synchronizes the authoritative log;
5. publishes and parent-synchronizes the state mirror; and
6. only then installs the prepared candidate as ordinary live state.

The shared managed-file coordinator serializes this sequence with other managed replacements. No
file is published unless both files were successfully staged. A pre-admission quota or physical
headroom failure is therefore a definitive rejection with no logical commit.

Term and uncommitted-entry changes that do not advance the commit index publish only the log. They
still use shared disk admission and a cloned candidate so a pre-publication failure cannot modify
live consensus state. Volatile follower-progress and liveness fields are not rewritten to disk.

### Interruption semantics and fencing

The ordered pair has these recoverable interruption states:

- before log publication: the old pair remains authoritative;
- after log publication but before mirror publication: the new log checkpoint is authoritative and
  startup repairs the older mirror;
- after mirror publication: both files describe the new checkpoint.

If log publication is known to have succeeded but mirror publication or its durability
confirmation fails, the operation is reported as `committed_checkpoint_pending`, including the
durable term and index. The runtime adopts the authoritative candidate and enters a persistence
fence. It will not accept another control mutation until it has reloaded the log, reconciled shared
disk accounting, and repaired the mirror with recovery admission. A publication result that cannot
prove whether the log rename occurred is treated conservatively in the same fenced state.

Authoritative Recovery admission may recreate a missing mirror or grow a stale mirror even when
reconciled usage is at the logical quota. That exception does not admit new logical state: the
already-durable log checkpoint proves the bytes being materialized. The complete temporary peak is
still reserved against physical free space and the configured filesystem headroom. Ordinary
Recovery rewrites remain non-growing.

If both the log and mirror publications are durable but grouped-replacement finalization, owned-
temporary cleanup, or exact accounting reconciliation fails, the commit is reported as
`committed_cleanup_pending`. That condition records cleanup debt without fencing the authoritative
pair. The next repair attempt removes owned temporaries and reconciles accounting before attempting
any separate pending-candidate or mirror-fence repair. Cleanup failure cannot turn an already
durable commit into a rejection.

A separate case exists after the command is quorum-committed and its local checkpoint publication
has been processed: a commit-notice response can reveal a higher term that must revoke local
leadership. If the runtime cannot yet persist that required term and step-down floor, the command
still returns successful, degraded `committed_persistence_pending` with its committed index and
term. The runtime adopts the higher term in memory, fences leadership, retains the required log-
only candidate, and retries durable publication before accepting more leader work. This outcome
must not be treated as a safe command retry.

### Errors and observability

Control persistence retains structured local-disk limit information through its internal and HTTP
boundaries. A definitive pre-publication quota or headroom rejection maps to the resource-limit
response used by other managed server stores. A committed checkpoint-pending result is distinct
from a retryable pre-commit error and carries the established term and index so callers do not
blindly retry a non-idempotent mutation.

The successful `committed_persistence_pending` result has the same no-retry requirement: the
command is already quorum-committed even though durable recording of a subsequently observed
higher term is pending. Internal auto-join reports the analogous
`accepted_persistence_pending` result.

Status and metrics expose whether control persistence is fenced, the pending checkpoint position,
and cleanup debt. The cluster subsystem is degraded while either condition is present. A control
or cluster recovery-snapshot export requires unambiguous durable authority, so a fence, pending
durable candidate, or pending mirror checkpoint returns HTTP 503
`control_persistence_indeterminate`. Cleanup-only debt remains exportable because the durable pair
already describes one authoritative checkpoint.

### Required verification

Deterministic failpoints cover staging each file, log rename, log parent synchronization, state
rename, state parent synchronization, post-publication finalization and cleanup, and restart after
every publish boundary. Tiny-quota tests prove whole-pair peak admission, exact accounting after
replacement and restart, no live-state mutation on pre-publication rejection, authoritative mirror
repair at the logical quota, and typed HTTP propagation. Legacy-log migration,
corrupt/mismatched checkpoints, Active-voter quorum and failover, concurrent proposals, snapshot
installation, snapshot-export fencing, post-commit higher-term persistence, leader self-leave
rejection, and recovery restore are included in the matrix.

## Consequences

- The consensus log can recover every state it makes authoritative, including installed and
  restored snapshots.
- Control commits pay for two fully staged replacements at their peak rather than depending on
  publication order to free capacity.
- The mirror may temporarily lag, but it can no longer be durably ahead of an authoritative new
  log through the supported publication protocol.
- Callers can distinguish a safe resource retry from a commit whose mirror still needs repair.
- Cleanup-only degradation remains distinguishable from a fence and does not prevent recovery-
  snapshot export.
- Older control-log files remain readable; once a new checkpoint is written, rollback to software
  that predates this protocol is an operational compatibility boundary.
