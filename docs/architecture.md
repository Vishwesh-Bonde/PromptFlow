# PromptFlow Architecture

> Status: **Phase 3 complete** — live instruction classification and acceptance,
> debounce, safe replacement, manual stop, generation/instruction-sequence
> fencing, and the bounded SSE event system. The browser UI and the optional
> OpenAI adapter belong to later phases.

## 1. Process model

PromptFlow is a **single-process** application:

- one Uvicorn worker,
- one FastAPI app,
- one long-lived `aiosqlite` connection,
- one in-memory runtime registry containing worker handles, locks, and SSE
  subscriber queues.

Multiple worker processes are **not supported**. In-memory coordination is
process-local, so two processes could both try to run a generation for the same
task. SQLite is the durable source of truth; in-memory objects are only
operational state for the current process.

## 2. Components

| Component | Responsibility | Phase |
|---|---|---|
| `app/main.py` | app factory, lifespan, startup migration/recovery, provider wiring, shutdown cleanup | 1–3 |
| `app/config.py` | environment validation, provider selection, limits, debounce/checkpoint/cancellation/SSE settings | 1–3 |
| `app/api/health.py` | dependency health check | 1 |
| `app/api/tasks.py` | create task, instruction, stop, and snapshot endpoints | 2–3 |
| `app/api/events.py` | SSE endpoint, snapshot ordering, draft dedup, heartbeat, subscriber cleanup | 2–3 |
| `app/core/task_manager.py` | task-level use cases, classification, snapshot/draft provenance | 2–3 |
| `app/core/generation_coordinator.py` | worker lifecycle, streaming, debounce, replacement, stop cleanup, fencing, shutdown | 2–3 |
| `app/core/instruction_classifier.py` | deterministic metadata classification without an LLM | 3 |
| `app/core/event_bus.py` | bounded per-subscriber queues, fan-out, overflow resync | 2 |
| `app/core/context_builder.py` | provider request from immutable prompt, ordered instructions, classifications, previous draft | 2–3 |
| `app/core/state.py` | lifecycle enums and transition tables | 1–3 |
| `app/core/event_types.py` | durable and live event names | 1–3 |
| `app/database/repository.py` | all SQL, instruction/stop/replacement transactions, fences | 1–3 |
| `app/database/connection.py` | single connection, cancellation-safe begin/rollback/commit | 1–3 |
| `app/services/ai_provider.py` | provider protocol, request/error types | 2–3 |
| `app/services/mock_provider.py` | deterministic, barrier-controllable mock stream | 2 |
| `app/services/factory.py` | provider construction; OpenAI remains unavailable until Phase 5 | 2 |
| `app/static/*` | vanilla JS UI | 4 |

The API layer validates transport concerns and calls the task manager. It does
not contain generation orchestration logic, and route handlers never execute SQL.

## 3. Database model

```text
tasks (1) ───< instructions        ordered by sequence_no, unique per task
  │
  ├──────────< generations         keyed by (task_id, version)
  │
  └──────────< task_events         meaningful lifecycle events only
```

- `tasks.original_prompt` is immutable, enforced by a database trigger.
- `tasks.instruction_seq` is the highest accepted instruction sequence.
- `tasks.generation_version` is the **desired generation version** (fence). Both
  instruction acceptance and manual stop advance it.
- `tasks.stop_requested` is durable stop intent; a stop is terminal even if the
  generation worker is still unwinding.
- `tasks.committed_answer` is the last successfully completed answer. It is not
  part of any draft and is never changed by cancellation or failure.
- `instructions.content` stores the user's exact text; `kind` is deterministic
  classification and never rewrites content. `client_instruction_id` makes
  retries idempotent per task.
- `generations.base_instruction_seq` records exactly which accepted instruction
  prefix a generation used. A replacement is created only when this equals the
  task's latest sequence.
- `generations.draft_text` is the latest checkpoint or final partial draft of one
  attempt.
- `generations.status` is `GENERATING`, `COMPLETED`, `CANCELLED`, `FAILED`,
  `STALE`, or `INTERRUPTED`.
- `task_events` records lifecycle milestones, **not tokens**. Token and
  checkpoint deltas are live SSE events only.

Foreign keys are enabled on every connection. WAL, a 5-second busy timeout, and
short `BEGIN IMMEDIATE` transactions are used. One connection with one asyncio
lock removes writer contention and prevents reads from observing uncommitted
state. The lock is never held during model I/O, debounce waits, or SSE waits.

**Generation versions start at 1.** Task creation reserves version 1 for the
first attempt. Accepting an instruction advances the desired version by exactly
one and cancels the old worker; a replacement generation is then created for
that version using the latest instruction sequence. Manual stop also advances
the version so no in-flight attempt can commit afterward. Rapid instructions may
skip intermediate generation rows (for example v1 is cancelled and the single
replacement is v4), but every desired version in the task row remains monotonic.

## 4. Task lifecycle transitions

`app/core/state.py` is the single source of truth. Route handlers use the enum,
never raw strings. Same-state writes are idempotent no-ops and are not treated as
transitions.

| From | Allowed next states | Purpose |
|---|---|---|
| `IDLE` | `GENERATING`, `UPDATE_PENDING`, `FAILED`, `INTERRUPTED` | legacy/early repository creation; the Phase 2 API creates tasks directly in `GENERATING` |
| `GENERATING` | `UPDATE_PENDING`, `CANCELLING`, `COMPLETED`, `FAILED`, `INTERRUPTED` | initial answer in progress |
| `UPDATE_PENDING` | `REVISING`, `CANCELLING`, `FAILED`, `INTERRUPTED` | instruction accepted; waiting for debounce/cleanup |
| `CANCELLING` | `UPDATE_PENDING`, `REVISING`, `STOPPED`, `INTERRUPTED` | worker being stopped; a late non-stop instruction may still arrive |
| `REVISING` | `UPDATE_PENDING`, `CANCELLING`, `COMPLETED`, `FAILED`, `INTERRUPTED` | replacement answer in progress |
| `COMPLETED` | `UPDATE_PENDING` | last attempt succeeded; new instruction allowed |
| `FAILED` | `UPDATE_PENDING` | last attempt failed; committed answer retained; new instruction allowed |
| `INTERRUPTED` | `UPDATE_PENDING` | recovered after restart or graceful shutdown; resumable only by a new instruction |
| `STOPPED` | *(none)* | manual stop is terminal; further instructions return HTTP 409 |

Generation transitions are stricter: `GENERATING` may become `COMPLETED`,
`CANCELLED`, `FAILED`, `STALE`, or `INTERRUPTED`; every terminal state has no
outgoing transitions. `COMPLETED` is reachable only through the fenced commit
method.

## 5. Phase 2 generation pipeline

The API path is `POST /api/tasks` → **HTTP 202** immediately after durable
acceptance.

1. Validate the request and configured prompt limit.
2. In **one transaction**, insert:
   - the task in `GENERATING` with `generation_version = 1`;
   - generation row version 1 in `GENERATING`;
   - durable `task_created` and `generation_started` events.
3. Return `{task_id, status, generation_version}` without waiting for the model.
4. The coordinator schedules one `asyncio.Task` for the task and returns.
5. The worker re-reads a consistent task state and constructs a
   `GenerationRequest` from the original prompt. The initial attempt sends no
   instructions and no previous draft; replacement attempts use the Phase 3
   context described below.
6. The worker publishes `generation_started`, then consumes the provider stream:
   - append the chunk to its local buffer;
   - publish a `token_delta` event;
   - every configured checkpoint interval, persist the full draft through a
     version-fenced `save_generation_draft` call and publish a `checkpoint`
     event **after** the commit.
7. After the stream ends, reject empty output, re-check the worker version, and
   call `commit_generation`. That single transaction writes the full answer to
   `generations.draft_text`, marks the generation `COMPLETED`, writes
   `tasks.committed_answer`, and sets the task `COMPLETED`.
8. Only after that commit is the `generation_completed` SSE event published.

Failure paths are equally explicit:

- provider failure, timeout, empty output, or an unexpected error → persist the
  partial draft and generation `FAILED`, set the task `FAILED` in the same
  transaction, then publish `generation_failed` and a `status` event;
- shutdown cancellation → persist the partial draft and generation
  `INTERRUPTED`, set the task `INTERRUPTED` in the same transaction;
- a checkpoint or completion rejected by the version fence → mark that attempt
  `STALE` without touching the task's committed answer or current status.

The worker never writes token events to SQLite. A restart therefore reconstructs
from `draft_text`, `generations.status`, `tasks.committed_answer`, and the coarse
timeline.

## 6. Phase 3 live replacement and manual stop

See `docs/live-instruction-flow.md` for the full sequence diagrams and event
order. The durable rules are:

1. `POST /api/tasks/{id}/instructions` normalizes nothing. In one transaction it
   stores the exact content, deterministic `kind`, optional
   `client_instruction_id`, next `sequence_no`, and the new desired
   `generation_version`; it also sets `UPDATE_PENDING`. The coordinator is
   notified only after this commit, so **HTTP 202 always means the instruction
   is durable**.
2. A retry with the same `client_instruction_id` returns the existing row without
   advancing the sequence or version.
3. Classification is heuristic metadata. The instruction text is never dropped
   or rewritten. `NEW_TASK` is still persisted in order but the response includes
   `requires_new_task: true` and an explicit message; the original prompt is
   immutable.
4. `TaskManager` serializes instruction acceptance, stop, and their coordinator
   handoff per task. The coordinator sets the desired in-memory version/sequence,
   invalidates and cancels the old worker immediately, and starts one debounce
   supervisor. Each further accepted instruction updates the target and resets
   the quiet window. Every instruction is persisted; only the latest sequence is
   used to start one replacement. A delayed handoff is ignored if a stop or newer
   instruction already advanced the local target.
5. After the debounce, the supervisor waits for the old worker task (and therefore
   its provider iterator and terminal cleanup) to finish, bounded by
   `CANCELLATION_TIMEOUT_SECONDS`. Only then does it call
   `create_revision_generation` with an exact `base_instruction_seq` match and
   transition the task to `REVISING`.
6. The replacement request contains the immutable prompt, every instruction up
   to and including that sequence with its classification, and the previous
   attempt's draft as reference material. The model is asked for a complete
   answer, never an append.
7. `POST /api/tasks/{id}/stop` commits stop intent, advances the desired version,
   and returns **202** with `CANCELLING` (or `STOPPED` for an idle task). The
   coordinator cancels the worker and only calls `finalize_stop` after cleanup.
   A repeated stop is idempotent and does not bump the version again.
8. If cleanup exceeds the timeout, an `error` event with
   `code: cancellation_timeout` is published and the task remains `CANCELLING`;
   no replacement may start. If the worker eventually unwinds, a pending manual
   stop is finalized with the preserved draft.
9. SQLite serializes instruction acceptance and stop intent. Whichever
   transaction commits first wins: if the instruction wins, the stop invalidates
   it before any replacement commits; if the stop wins, the late instruction gets
   HTTP 409 and never schedules work. The per-task manager lock keeps the commit
   and coordinator handoff together, and the coordinator ignores any late handoff
   that would roll the local target back or revive a stop.

## 7. What is authoritative?

| State | Authority | Notes |
|---|---|---|
| task status, prompt, instruction sequence, desired version | SQLite `tasks` | never reconstructed from a UI or event log |
| accepted instructions | SQLite `instructions` | exact order by `sequence_no` |
| committed answer | SQLite `tasks.committed_answer` | written only by a fenced commit |
| working draft / checkpoints | SQLite `generations.draft_text` | updated by fenced checkpoints and final cleanup |
| generation outcome | SQLite `generations.status` | terminal states are immutable |
| timeline | SQLite `task_events` | coarse events only |
| locks, worker handles, subscriber queues, live token deltas | in-memory runtime | operational only; lost on restart |

### Reconstruction after restart or reconnect

1. Open SQLite, apply migrations, then run startup recovery **before serving**.
2. For each task in an active status (`IDLE`, `GENERATING`, `UPDATE_PENDING`,
   `CANCELLING`, `REVISING`):
   - if `stop_requested = 1`: task → `STOPPED`, active generation → `CANCELLED`;
   - otherwise: task → `INTERRUPTED`, active generation → `INTERRUPTED`;
   - keep any `draft_text` and keep `committed_answer` untouched;
   - append `task_stopped` or `task_interrupted` to the timeline.
3. Graceful shutdown normally persists `INTERRUPTED` itself. It also reconciles
   a worker cancelled before its coroutine ever started.
4. The runtime registry starts empty. Operational objects are created lazily.
5. A snapshot is read under **one database lock**: task row, instructions in
   order, the generation whose version equals `tasks.generation_version`, and
   recent events. A concurrent commit can therefore never produce a mixed
   snapshot.
6. `working_draft` is the current generation's draft while it is not
   `COMPLETED`. If the current desired version has no generation row yet (an
   instruction is waiting for debounce/cleanup or a stop bumped the version), the
   latest previous non-`COMPLETED` attempt's draft is shown, even when a committed
   answer also exists. A previous `COMPLETED` attempt is already represented by
   the committed answer. `working_draft_generation_version` and
   `working_draft_generation_status` identify exactly which attempt produced the
   visible text.

## 8. Concurrency, fencing, and cancellation ordering

The core invariant is: **at most one worker per task may change durable task
state, and only while its version is current.**

- `GenerationCoordinator` refuses to schedule a second live worker for a task.
  A runtime has at most one worker, one restart supervisor, and one stop
  supervisor. When a supervisor exits, its `finally` block hands off any
  unhandled newer target - including one accepted during cancellation-timeout
  cleanup - to a fresh supervisor. A delayed handoff for a target that is
  already current is idempotent: it does not cancel that target's worker or
  start a duplicate replacement.
- A per-task `TaskManager` lock spans each instruction/stop transaction and its
  coordinator handoff. A delayed instruction handoff cannot clear a newer stop,
  roll back the desired version, or publish a second invalidation.
- Runtime locks guard only worker bookkeeping. No database or provider I/O runs
  while a runtime lock is held.
- Checkpoints require `generation.status = GENERATING` **and**
  `generation.version = tasks.generation_version` inside the SQL transaction.
- `commit_generation` re-checks task status and version and verifies row counts;
  a mismatch rolls back and raises `StaleGenerationError`.
- `create_revision_generation` re-checks stop intent, the desired version, and
  exact equality with the latest `instruction_seq`. A delayed supervisor cannot
  create a replacement from an outdated context.
- Cancellation propagates into the provider generator and through `aclosing`.
  The mock provider records cancellation and re-raises `CancelledError`.
- A replacement is not started until the old worker task has terminated (and
  therefore its provider cleanup has unwound) or the cancellation timeout has
  been reported. A new worker is never started after a timeout.
- A cancelled worker writes its final draft and terminal status atomically
  before the coordinator considers it stopped.
- The worker task reference is cleared in its `finally` block. The small
  per-task runtime object remains for the process lifetime and is cleared at
  shutdown; completed `asyncio.Task` objects are not retained.
- A disconnected SSE client only removes its queue subscription. It never
  cancels the worker.

### What an invalidated attempt may still write

Once a newer version exists, an old worker's periodic checkpoints are rejected.
Its only allowed write is its own cleanup row: final draft plus `CANCELLED`,
`FAILED`, `STALE`, or `INTERRUPTED`. A current attempt's failure/interruption may
also update the task to `FAILED`/`INTERRUPTED` in the same transaction. No
cleanup path may change the committed answer or instruction sequence.

## 9. SSE protocol

Endpoint: `GET /api/tasks/{task_id}/events` (`text/event-stream`).

Every event is framed as:

```text
event: token_delta
id: 12
data: {"task_id":"...","type":"token_delta","generation_version":1,"text":"Mock ","offset":0,"chars":5}

```

### Connection sequence

1. The route subscribes the client **before** reading the snapshot.
2. It captures the bus sequence. Durable events published before that sequence
   are reflected in the snapshot because they are published only after their
   database transaction commits.
3. It reads one consistent snapshot and sends it as the first `snapshot` frame.
4. It forwards queued events newer than the captured sequence, then waits for
   live events. Older queued events are discarded because the snapshot
   supersedes them. Token deltas whose offset is already covered by the
   snapshot's working draft are also dropped, preventing duplicate text when a
   checkpoint committed during snapshot construction.
5. Every 15 seconds of idleness, a `: heartbeat` comment is sent.
6. On disconnect, the generator's `finally` unregisters only that subscription.

### Event types

| Event | Payload highlights |
|---|---|
| `snapshot` | full `TaskStateResponse`: task, instructions, current generation, draft provenance, recent durable events |
| `instruction_accepted` | instruction ID, sequence number, deterministic kind |
| `generation_invalidated` | old version and the replacement/stop target version |
| `context_updated` | target sequence, instruction count, previous generation/draft size |
| `generation_restarted` | base instruction sequence, previous generation version |
| `generation_started` | task ID, generation version, status, base instruction sequence |
| `token_delta` | text, character offset, length |
| `checkpoint` | draft character count |
| `draft_preserved` | generation version and retained draft character count |
| `generation_completed` | answer character count |
| `generation_failed` | terminal status, safe error code/message, draft length |
| `generation_cancelled` | terminal status, safe error code/message, draft length |
| `task_stopped` | `STOPPED`, stop version, invalidated generation version |
| `status` | task status after completion/failure/interruption/stop |
| `error` | explicit `cancellation_timeout` or other safe operational error |
| `resync_required` | reason `queue_overflow`, dropped sequence/type |

`task_id` is present in every payload. `generation_version` is present on all
generation-scoped events and on the snapshot. Heartbeats are comments, not data
events.

### Queue overflow and resynchronization

Each subscriber has its own bounded queue (default 256). Publishing is
non-blocking. If a queue is full, the server drains that subscriber's queue and
enqueues a single `resync_required` event. Other subscribers are unaffected. The
client responds by calling `GET /api/tasks/{id}` and replacing its local view
with the authoritative snapshot.

### Token replay limitation

Token deltas are intentionally not replayed and not persisted. A reconnect (or a
snapshot taken before the first periodic checkpoint) can show an older working
draft than the bytes already streamed. The next checkpoint or the final
completion event corrects the visible state. This is a deliberate trade-off to
keep SQLite writes bounded; the committed answer is always authoritative.

## 10. Schema migrations

- `schema.sql` is read at startup and split with `sqlite3.complete_statement`, so
  trigger bodies are handled correctly.
- Each migration runs inside one `BEGIN IMMEDIATE` transaction together with its
  `PRAGMA user_version` bump.
- Any failure rolls back both DDL and the version number. A retry starts from the
  same version with no partial schema.
- `executescript` is not used because it commits the open transaction first.
- A database newer than the code is refused with a clear `MigrationError`.

## 11. Known Phase 3 limitations

- No browser UI (Phase 4).
- Instruction classification is a deterministic heuristic, not semantic
  understanding. `NEW_TASK` is advisory: the instruction is still persisted in
  the current task and the response tells the client to create a separate task.
- Provider cancellation is cooperative. A provider that ignores cancellation
  beyond the timeout leaves the task `CANCELLING` with an explicit error and no
  replacement. If it eventually unwinds, a pending manual stop is finalized; a
  pending replacement still requires a fresh instruction.
- A failed or cancelled replacement is not retried automatically. Submit another
  instruction to resume from the retained draft/committed answer.
- `AI_PROVIDER=openai` is validated by configuration but deliberately fails at
  startup until the adapter is implemented in Phase 5. Tests never contact a real
  provider.
- SSE has no `Last-Event-ID` replay. Recovery is snapshot-based by design.
- Token deltas can be missing after reconnect until the next checkpoint or
  completion.
- Single process, one connection, SQLite only. This is an educational
  correctness model, not a scalability claim.
- SQLite over OneDrive can experience file-lock issues; set `DATABASE_PATH` to a
  non-synced directory if WAL files cause problems.
