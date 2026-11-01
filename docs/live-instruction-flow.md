# Live Instruction and Stop Flow

This document describes the Phase 3 sequence for changing a task while its
answer is still streaming. The durable rules live in SQLite; the coordinator's
runtime state is only an optimization for the current process.

## 1. Invariants

1. **Instructions are durable before HTTP 202.** The response is sent only after
   the instruction row, next `sequence_no`, and new desired
   `generation_version` commit.
2. **Original text is never mutated.** Classification is metadata. The model
   receives the user's exact instruction text.
3. **One worker per task.** A replacement cannot start until the prior worker
   and provider stream have finished cleanup, or until cleanup has been reported
   as timed out.
4. **Version and sequence fences are checked in SQL.** A stale worker may write
   only its own terminal generation row; it cannot commit an answer, change the
   current draft, or change current task status.
5. **Committed answer and working draft are separate.** Cancellation, failure,
   stop, and replacement never erase the last successful answer.
6. **`STOPPED` means cleanup finished.** A stop response may say `CANCELLING`;
   only the cleanup path may claim `STOPPED`.

## 2. Instruction acceptance

### Request

```http
POST /api/tasks/{task_id}/instructions
Content-Type: application/json

{
  "content": "Explain it using simple English.",
  "client_instruction_id": "optional-idempotency-key"
}
```

### Durable transaction

The repository performs these operations in one `BEGIN IMMEDIATE` transaction:

1. lookup the task;
2. if `client_instruction_id` already exists for this task, return the existing
   instruction with `created = false` (no version or sequence bump);
3. reject a stopped task with HTTP 409;
4. enforce `MAX_INSTRUCTIONS_PER_TASK`;
5. validate that the current task state may move to `UPDATE_PENDING`;
6. insert the instruction with the exact content, deterministic kind, next
   `sequence_no`, and optional client id;
7. increment `tasks.generation_version` by one and set
   `tasks.instruction_seq = sequence_no`;
8. set task status to `UPDATE_PENDING`;
9. append the durable `instruction_accepted` event.

The HTTP handler returns **202** with the instruction id, sequence, kind, new
generation version, and status.

Classification is deterministic and does not call a model:

| Kind | Examples of matching signals |
|---|---|
| `NEW_TASK` | "new task", "separate task", "start over/fresh/again" |
| `CORRECTION` | "actually", "correction", "I meant", "not X but Y" |
| `RESTRICTION` | "only", "avoid", "without", "concise", "at most" |
| `REFOCUS` | "focus on", "emphasize", "more about", "concentrate" |
| `MODIFY` | "rewrite", "simplify", "format", "make it", "using" |
| `ADDITION` | "add", "include", "also", "examples", or default |

`NEW_TASK` is an explicit advisory. The instruction is still stored in order and
the task is not silently redefined. The response contains:

```json
{
  "kind": "NEW_TASK",
  "requires_new_task": true,
  "message": "This instruction looks like a new task. The current task and its original prompt were preserved; create a separate task for a genuinely different request."
}
```

## 3. Debounce and replacement

```text
HTTP instruction accepted
        |
        v
runtime.desired_version = new version
runtime.instruction_seq = new sequence
publish instruction_accepted
publish generation_invalidated
cancel old worker (if any)
        |
        v
restart supervisor waits for INSTRUCTION_DEBOUNCE_MS
        |
        +-- newer instruction arrives --> update target, restart quiet window
        |
        +-- stop arrives --------------> abandon replacement, run stop cleanup
        |
        v
wait for old worker task and provider cleanup (bounded)
        |
        +-- timeout --> publish cancellation_timeout; no replacement starts
        |
        v
create_revision_generation(version, base_instruction_seq)
  - requires exact desired version
  - requires exact latest instruction sequence
  - atomically sets task REVISING
        |
        v
publish context_updated
publish generation_restarted
start exactly one replacement worker
```

Every accepted instruction is persisted, but a burst of instructions produces
one replacement. The replacement uses the latest sequence only. Intermediate
desired versions may have no generation row.

### Context reconstruction

The replacement request contains:

- the immutable original prompt;
- every instruction with `sequence_no <= base_instruction_seq`, in order;
- the persisted classification of each instruction;
- the previous attempt's draft, identified by generation version, as reference
  material;
- an instruction to produce a complete answer, not to append to the old draft.

A partial draft from a cancelled, failed, or completed attempt is reference
material. It is not treated as already committed output.

## 4. Fencing after invalidation

A worker captures its generation version when it starts. Every write checks the
fence inside SQLite:

| Write | Fence |
|---|---|
| checkpoint | generation exists, is `GENERATING`, and version equals the desired task version |
| commit | current task version, current task status, and exactly one updated row |
| terminal cleanup | generation exists and is still `GENERATING`; may complete its own row |
| replacement creation | desired version and exact instruction sequence match |
| stop finalization | stop was requested and `generation_version == stop_version` |
| instruction acceptance | task is not stopped; one transaction advances the version |

A stale worker may therefore persist its own `CANCELLED`, `FAILED`, `STALE`, or
`INTERRUPTED` generation row, but cannot alter the current task. Its live events
carry the old generation version, so subscribers can reject them as non-current.

## 5. Manual stop

### Request

```http
POST /api/tasks/{task_id}/stop
```

The repository transaction:

1. treats an existing stop request as idempotent (`changed = false`, no bump);
2. rejects terminal/non-active tasks with HTTP 409;
3. records `stop_requested = true`;
4. advances the desired generation version;
5. sets status to `STOPPED` only when the task was already idle, otherwise
   `CANCELLING`;
6. appends the durable status event (and `task_stopped` for the idle case).

The API returns **202**. If already `CANCELLING`, repeated stops do not restart
generation or bump the version.

```text
stop accepted
     |
     v
stop_requested = true
cancel restart/debounce supervisor
cancel active worker
     |
     v
publish generation_invalidated
     |
     v
wait for worker/provider cleanup
     |
     +-- timeout --> publish error cancellation_timeout
     |               remain CANCELLING; no replacement
     |               if worker later unwinds, finalize STOPPED then
     |
     v
finish the cancelled/failed/stale generation with latest partial draft
publish generation_cancelled/failed, draft_preserved (if non-empty)
finalize_stop(stop_version)
publish task_stopped + status STOPPED
```

Stop is terminal. A later instruction gets HTTP 409 and never schedules work.

## 6. Ordering between instruction acceptance and stop

SQLite's single write lock defines the durable order. There is no ambiguous
mixed state. `TaskManager` additionally holds a per-task lock across the
transaction and its coordinator handoff, so live event order follows commit
order.

- **Instruction commits first:** its version bump is durable. A subsequent stop
  bumps the version again and invalidates the accepted instruction before a
  replacement can become current. If a replacement already started, it is
  cancelled by the stop fence.
- **Stop commits first:** the task has `stop_requested = true`. A later
  instruction transaction raises `TaskStoppedError`, returns HTTP 409, and does
  not create an instruction row or schedule anything.
- **Delayed handoff:** if a coordinator call is somehow applied after a newer
  stop or instruction, `accept_instruction` treats it as superseded. It publishes
  the durable instruction acknowledgement but does not clear stop state, roll the
  desired version backwards, cancel with a stale reason, or schedule a worker.
  If the target is already current (a supervisor refreshed it from SQLite first),
  the handoff is idempotent and does not cancel the current replacement worker.

The same rule applies to rapid instructions: sequence numbers and versions are
allocated inside the transaction, so they are strictly increasing.

## 7. Snapshot draft provenance

`GET /api/tasks/{task_id}` and the SSE snapshot expose:

```json
{
  "working_draft": "partial text...",
  "working_draft_generation_version": 2,
  "working_draft_generation_status": "FAILED",
  "generation": { "version": 2, "status": "FAILED", ... }
}
```

Selection rules:

1. if the current desired generation exists and is not `COMPLETED`, show its
   draft;
2. if the current desired version has no row yet (debounce/cleanup or stop
   version bump) and no committed answer exists, show the latest previous
   attempt's draft;
3. if a committed answer exists, the committed answer is the visible final
   answer; draft fields may still identify a newer failed/cancelled attempt.

The draft version/status fields are authoritative even when `generation` is
`null` after a stop version bump.

## 8. Relevant SSE events

| Event | Meaning |
|---|---|
| `instruction_accepted` | durable instruction, current sequence/kind |
| `generation_invalidated` | target changed; old version must not commit |
| `context_updated` | replacement context is durably available |
| `generation_restarted` | a replacement generation row was created |
| `draft_preserved` | a cancelled/failed attempt kept its partial draft |
| `generation_cancelled` | terminal cancelled/stale generation outcome |
| `task_stopped` | stop cleanup completed and `STOPPED` was claimed |
| `error` | explicit operational issue such as `cancellation_timeout` |

Events are published only after their corresponding durable write. Token deltas
and checkpoints are live-only; a reconnect recovers from the snapshot.

## 9. Failure and timeout behavior

- **Replacement provider failure:** the replacement generation is `FAILED`, the
  committed answer is retained, the partial replacement draft is retained, and
  a new instruction may start another attempt.
- **Replacement cancellation timeout:** the old worker is still unwinding. The
  task is `CANCELLING`, an explicit error event is published, and that supervisor
  starts no replacement. A later instruction may retry the replacement: if the
  old worker has since finished cleanup, the new supervisor starts it; otherwise
  the new attempt times out truthfully again. If the instruction arrives while
  the timed-out supervisor is still winding down, its `finally` handoff starts a
  fresh supervisor for the newer target.
- **Stop cancellation timeout:** same truthful `CANCELLING` state. If the worker
  eventually unwinds, the pending stop is finalized as `STOPPED` with its draft.
- **Server shutdown:** local workers are cancelled with a `shutdown` reason,
  partial drafts are persisted, and startup recovery reconciles remaining active
  tasks as `INTERRUPTED` or `STOPPED` (if stop was requested).

## 10. Files to read

- `app/core/generation_coordinator.py` — runtime, debounce, replacement, stop.
- `app/database/repository.py` — atomic instruction/stop/replacement/fence SQL.
- `app/core/context_builder.py` — replacement request composition.
- `app/core/instruction_classifier.py` — deterministic classification.
- `app/core/task_manager.py` — API use cases and snapshot provenance.
- `tests/integration/test_live_instructions.py` — end-to-end race tests.
