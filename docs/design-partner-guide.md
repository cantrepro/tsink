# Design-partner guide

> **HUMAN GATE:** Recruiting design partners, conducting interviews, and
> validating that an external project would ship tsink require maintainer
> participation and real users. An automated coding agent may prepare this
> guide and address repository evidence, but it must not mark recruitment or
> adoption complete.

## Purpose

Use this guide to learn where tsink succeeds or fails in a real integration.
The goal is not a general feature wishlist. It is to identify concrete shipping
blockers, measure the constraints the embedded engine must honor, and turn
observed failures into focused product work.

Seek at least one partner from each archetype:

1. a project that currently starts Prometheus or another TSDB in integration
   tests;
2. a self-hosted application or developer tool that wants built-in local
   metrics;
3. an agent, gateway, or appliance that needs bounded offline retention.

Record permission before naming a project or publishing findings. Use an
anonymized identifier when appropriate, and never commit credentials, customer
data, private metric labels, or private logs.

## Suggested integration exercise

Use the partner's real repository and normal development environment where
possible. Ask them to attempt the smallest meaningful path:

1. install or add the relevant package;
2. compile and package their application or test;
3. open tsink and reach the first successful write;
4. run the direct query, PromQL query, or protocol exchange they actually need;
5. exercise clean shutdown and restart;
6. test one relevant pressure, recovery, or upgrade scenario;
7. decide whether they could ship the integration and, if not, name the blocker.

Observe rather than coaching immediately. Record time-to-first-query, every
unexpected engine concept they must learn, and any workaround. Separate a
failure in tsink from an unfamiliarity in the host project's own tooling.

## Integration questions

### Current workflow and job

- What problem does the current metrics database solve in this project?
- What starts it today: library code, Docker, a sidecar, a system service, or an
  external backend?
- Which part of that workflow is costly, flaky, slow, or hard to distribute?
- What would a successful tsink integration replace?
- Is this path used in tests, development, production, or more than one?

### Installation, packaging, and lifecycle

- Where does installation, compilation, linking, wheel packaging, cross-build,
  or startup first fail?
- How much setup is needed from a clean machine?
- Does tsink add an unacceptable binary-size, dependency, toolchain, or
  platform requirement?
- Can the host choose and own the data directory?
- Are startup, health inspection, close, and restart behavior clear?
- Does shutdown meet the host's time budget and leave no process, listener, or
  background thread behind?

### API and query model

- Which API names or types are confusing?
- Which calls force the user to understand WAL, compaction, partitions, tiers,
  or other engine internals?
- Does the integration need direct query APIs, PromQL, or both?
- Which exact PromQL expressions or protocol requests are required?
- Are write acceptance, partial failure, and durability results sufficient to
  make a safe retry decision?
- Which errors lack an actionable category or diagnostic?

### Actual resource constraints

Ask for measured or contractual values, not “small” or “lightweight”:

- maximum and typical RAM;
- maximum local data disk and WAL disk;
- required retention window;
- expected active-series cardinality and series-creation rate;
- sample rate and largest expected batch;
- query result and concurrency limits;
- acceptable idle and peak CPU;
- maximum startup and shutdown duration;
- number of threads the host can tolerate;
- behavior required when any limit is reached.

Record how each number was obtained and whether it is a hard limit, target, or
guess.

### Durability, recovery, and upgrades

- Which data-loss, corruption, recovery, or upgrade scenario concerns the user
  most?
- What does the user believe a successful write acknowledgement promises?
- Must acknowledged data survive a process kill, power loss, or host crash?
- How are snapshots, backup verification, and restore expected to work?
- For how many releases must an older data directory remain readable?
- Must safe read-only inspection be possible after a failed open or upgrade?
- What diagnostic would make a recovery failure actionable?

### Protocols and disconnected operation

- Which concrete protocol client and version must interoperate?
- Is direct embedded access enough, or is an ephemeral/local HTTP endpoint
  required?
- Is store-and-forward synchronization required now?
- If so, what destination, authentication, bandwidth, retry, and maximum
  disconnect interval apply?
- May local retention delete unsent data? Who is allowed to make that choice?
- What lag, backlog, or destructive-retention signal must the host expose?

### Shipping decision

- What prevents this project from shipping tsink today?
- Is the blocker correctness, compatibility, operability, packaging, resource
  use, missing evidence, or a genuinely missing subsystem?
- What is the smallest change that would remove it?
- Which requested subsystem is backed by an immediate real-world blocker rather
  than hypothetical appeal?
- What evidence would change the answer from “not yet” to “yes”?

## Findings template

Copy this section for each integration. Keep raw logs and private details outside
the repository unless the partner approves publication.

```markdown
# Design-partner finding: <anonymized project or approved name>

## Record

- Archetype: test suite | self-hosted app/tool | agent/gateway/appliance
- Date:
- Interviewer:
- Participant/project identifier:
- Permission to name publicly: yes | no
- tsink version and commit:
- Host OS/architecture:
- Rust/Python/toolchain versions:
- Integration branch or evidence link:

## Current workflow

- Current database/service:
- How it starts:
- Environment(s) where it runs:
- Problem the partner wants to remove:
- Concrete success criterion:

## Exercise result

- Task attempted:
- Outcome: succeeded | partially succeeded | blocked
- Time to install/build:
- Time to first successful write:
- Time to first required query:
- Startup time:
- Shutdown time:
- Restart/recovery result:

## Friction observed

| Stage | Observation or failure | User impact | Workaround | Repro/evidence |
|---|---|---|---|---|
| Install/build | | | | |
| Package/start | | | | |
| Write/query | | | | |
| Close/restart | | | | |
| Pressure/recovery/upgrade | | | | |

## API and query needs

- Confusing APIs:
- Engine internals exposed unnecessarily:
- Query need: direct API | PromQL | both
- Required queries/protocol operations:
- Missing error or write-outcome detail:

## Resource envelope

| Constraint | Typical | Hard maximum/target | Measured or estimated? | Required behavior at limit |
|---|---:|---:|---|---|
| RAM | | | | |
| Data disk | | | | |
| WAL disk | | | | |
| Retention | | | | |
| Active series | | | | |
| New series/time | | | | |
| Samples/time | | | | |
| Query concurrency/result size | | | | |
| CPU | | | | |
| Threads | | | | |
| Startup | | | | |
| Shutdown | | | | |

## Durability and lifecycle risk

- Required acknowledgement meaning:
- Most concerning data-loss/corruption/recovery/upgrade scenario:
- Snapshot/restore expectation:
- Oldest version that must upgrade:
- Required diagnostics:

## Synchronization

- Required now: yes | no
- Destination and protocol:
- Maximum disconnect:
- Bandwidth/concurrency/auth constraints:
- Unsent-data retention policy:
- Required lag/backlog signals:

## Shipping decision

- Would ship current tsink: yes | no | conditional
- Concrete blockers:
- Smallest change that removes each blocker:
- Requested major subsystem:
- Immediate real-world blocker proving it is needed:
- Evidence required for reconsideration:

## Follow-up

| Action | Owner | Priority | Evidence/issue | Due or review date |
|---|---|---|---|---|
| | | | | |
```

## Turning findings into roadmap work

Link each accepted action to a reproducible failure, measurement, or explicit
shipping decision. Correctness, security, compatibility, documentation, and
packaging work can proceed whenever evidence warrants it.

Apply this product rule:

> Do not add another major subsystem until at least one external user is
> concretely blocked without it.

A feature request alone is not proof. Record the user's current workflow, the
failed integration step, why a smaller change cannot solve it, and the shipping
decision the subsystem would change.

## Recruitment status

Recruitment remains a **HUMAN GATE** until maintainers have completed and
recorded real external integrations. Keep a private outreach tracker if needed;
do not fill this repository with personal contact information or mark an
archetype complete based on a synthetic exercise.
