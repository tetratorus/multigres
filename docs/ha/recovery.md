# When We Change the Rules: Recovery & Orchestration

This doc covers _when_ and _why_ a rule change is triggered — the
responsibility of **Multiorch**, the orchestrator. It assumes you've read
[the consensus overview](consensus-overview.md) and
[the state model](state-model.md). _How_ a rule change is carried out safely is
[rule-change.md](rule-change.md); this doc decides when to start one.

> [!NOTE]
> **Status:** skeleton. The high-level responsibilities below are accurate. The
> detailed mechanics are not yet documented and will be filled in incrementally.

## Purpose and boundary

Multiorch watches the health of every pooler in a shard and drives the cohort
back to a healthy state when something changes. It owns the _decision_ to change
the rules; it delegates the _mechanism_ to the rule-change protocol. It has
three most-important responsibilities:

### 1. Failover when the leader is dead — a coordinator-led rule change

When the consensus leader is unreachable or unhealthy, Multiorch acts as the
coordinator and runs a [coordinator-led rule change](rule-change.md) (recruit →
promote) to appoint a new leader. This is the failure path: the old leader
cannot cooperate, so an external coordinator must eliminate any rogue quorum and
elect the most-advanced surviving pooler.

Leader fitness reaches Multiorch on the **health stream**, not through
Kubernetes probes — a pooler stays Ready (and keeps its Service endpoint) even
when its Postgres is down, precisely so the coordinator can still observe it.
See [Readiness and Liveness](../kubernetes/integration.md#readiness-and-liveness)
for why probe state and component health are deliberately separated.

> [!NOTE]
> Not yet documented: how a dead leader is detected — the heartbeat timeout on
> the health stream and the `LEADERSHIP_SIGNAL_REQUESTING_DEMOTION` fast path.
> The entry point is `AppointLeaderAction` → `Coordinator.AppointLeader`.

### 2. Cohort membership changes — a leader-led rule change

As poolers are provisioned and deprovisioned, Multiorch adjusts cohort
membership. Unlike failover, the leader is healthy here, so the change goes
**through the primary pooler's update-rules endpoint** — a leader-led rule
change. The leader writes the new rule itself (new cohort, same term, bumped
`leader_subterm`), with no external coordinator needed.

> [!NOTE]
> Not yet documented: the exact update-rules RPC and its handler, the
> add-member versus remove-member flows, and how the durability policy's
> achievability is re-checked against the new cohort.

### 3. Keeping poolers pointed at the right primary — SetPrimary reconciliation

Even with no rule change in flight, Multiorch continually reconciles each
pooler's replication target, calling `SetPrimary` to keep every follower and
observer pointed at the current leader. This repairs poolers that drifted —
restarted, were partitioned, or were told about a primary while Postgres was
down — without electing anyone or bumping the rule. `SetPrimary`'s rule comparison
makes a redundant call a safe no-op, so this can run freely.

> [!NOTE]
> Not yet documented: the reconciliation trigger (`FixReplicationAction`), how
> the coordinator decides a pooler needs re-pointing (comparing published
> `ReplicationPrimary` against the current rule via `ReplicationPrimaryMatches`),
> and how this interacts with pg_rewind for diverged followers.

## The recovery loop

> [!NOTE]
> Not yet documented: the engine and its loops (healthcheck, recovery,
> maintenance), the analyze → detect → dedup/prioritize → validate → act cycle,
> and the action registry (`AppointLeaderAction`, `DemoteStaleLeaderAction`,
> `FixReplicationAction`, `ReconcileCohortAction`, `ShardInitAction`).
> Files: `go/services/multiorch/recovery/`.

## Health input and leadership signaling

Poolers report fitness on the **health stream** — the out-of-band channel that
carries Postgres up/down, replication lag, and quarantine lifecycle to
Multiorch and the operator. This is deliberately kept off the Kubernetes
readiness/liveness probes. See
[Where component health actually goes](../kubernetes/integration.md#where-component-health-actually-goes)
for the boundary and its consequences.

> [!NOTE]
> Not yet documented: the concrete signaling types — `LeadershipStatus` /
> `LeadershipSignal` (ACTIVE, REQUESTING_DEMOTION) and `CohortEligibilityStatus`.
> This is the signaling layer the state model defers to recovery.

## When automatic failover is unsafe

Multiorch only fails over when it can prove the outgoing rule is revoked
(`CheckSufficientRecruitment` in
[`durability.go`](../../go/common/consensus/durability.go)): a strict
majority of the outgoing cohort must be recruitable, and the unrecruitable
remainder must be unable to satisfy the durability policy on its own. If the
proof is not available, proceeding could commit a new term while a partitioned
old leader keeps accepting durable writes — split brain, and eventually data
loss. Multiorch refuses instead, and reports one of these alert-only problem
codes ([`types.go`](../../go/services/multiorch/recovery/types/types.go)):

| Problem code             | Meaning                                                                                                                                         | Who acts                                                                                                       |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `ShardStuck`             | The leader must be replaced, but the recruitable subset of the cohort is not a sufficient recruitment quorum. Writes are halted.                | **Operator / provisioner.** Automatic recovery resumes only if enough cohort members become recruitable again. |
| `NoHealthyCohortMembers` | No initialized pooler has a fresh, valid health report; Multiorch is blind and will not convict the leader on stale evidence.                   | Usually transient (cold start, health-stream outage). Investigate connectivity from Multiorch to the poolers.  |
| `LeaderHealthUnknown`    | The leader can neither be confirmed healthy nor convicted, but a recruitment quorum exists. Nothing is blocked yet.                             | Watch; investigate if persistent.                                                                              |
| `ShardAtRisk`            | The leader is healthy, but if it were lost the remaining cohort could not recover. Not an outage — a warning that the next failure will be one. | Restore or add cohort members before the leader fails.                                                         |

All four are surfaced the same way:

- a `WARN` log line `non-actionable problem detected; human intervention
required` with `problem_code`, shard identity and `description`
  ([`alert_only.go`](../../go/services/multiorch/recovery/actions/alert_only.go));
- the `multiorch.recovery.detected_problems` gauge, whose `problem_code`
  attribute is the dimension to page on — `analysis_type` alone is not enough,
  because the same analyzer (`LeaderNeedsReplacement`) also emits the
  actionable, self-healing codes.

### Responding to `ShardStuck`

The `ShardStuck` description names the leader cause, the exact shortfall
(`majority not satisfied: recruited 1 of 3 …` or `revocation not satisfied:
… could independently satisfy AT_LEAST_2`), and the **unrecruitable cohort
members**. Work from that list:

1. **Prefer restoring members.** If any listed pooler can be brought back
   (restart the pooler or its Postgres, fix the network partition, un-drain
   it), do that first. As soon as the recruitable subset is sufficient,
   Multiorch fails over on its own with the normal, proof-backed protocol.
   Nothing else is required.
2. **Only if members are permanently lost**, force the failover with an
   externally certified revocation:

   ```bash
   multigres cluster apply-rule-change \
     --database=<db> --table-group=<tg> --shard=<shard> \
     --leader=<cell_name> --cohort=<cell_name>,... --durability=AT_LEAST_2 \
     --outgoing-rule-term=<term> --outgoing-leader-subterm=<subterm> \
     --frozen-lsn=<lsn> \
     --reason="<why the lost members cannot come back>"
   ```

   Before running it, make sure the lost members really cannot accept
   writes again (destroyed, fenced, or their Postgres stopped). The
   `--frozen-lsn` you pass is your attestation that no outgoing-cohort member
   will commit past it; the command confirms the shard name interactively
   because that attestation, if wrong, means data loss. Use
   `--unsafe-derive-cert-from-reachable` only when you accept that the lost
   members may have held writes the survivors never saw. The safety argument
   and flags are explained in
   [Operator override](rule-change.md#operator-override-forced-failover-after-a-quorum-is-permanently-lost).

Do not try to shortcut either path by editing rule state directly or
restarting Multiorch: the refusal is the safety property working as intended,
not a stuck process.

## Scenarios

> [!NOTE]
> Not yet documented: concrete cases walked end to end — clean leader failover,
> stale-primary demotion, a pooler joining the cohort, a pooler leaving — and
> how the consensus rules apply in each.

## See also

- [How we change the rules safely](rule-change.md) — the mechanism this doc
  triggers.
- [The HA state model](state-model.md) — health/leadership signals and rule state.
- [Consensus overview](consensus-overview.md) — the underlying algorithm.
