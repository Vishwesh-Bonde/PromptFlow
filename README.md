# PromptFlow

**Think while AI thinks.**

PromptFlow is an educational mini-project about a hard asynchronous-engineering
problem: a user receives a streaming AI answer and submits an additional
instruction *while it is still being generated*. The application must accept the
instruction immediately, invalidate the running attempt safely, keep the useful
partial draft, and produce one coherent replacement answer — without losing the
original request, duplicating output, or letting a stale attempt overwrite newer
state.

A normal model request cannot have its input mutated after generation starts.
PromptFlow therefore uses **cancellation, monotonic generation versions,
context reconstruction, and a replacement generation**. It does not pretend to
edit a request in flight.

## Current status

**Phase 3 is complete: live instruction replacement, safe manual stop, and
generation fencing during streaming.**

Implemented and tested:

- pinned Python environment, verified on Python 3.14.7;
- validated environment configuration and `.env` support;
- transactional, retry-safe SQLite migrations with `PRAGMA user_version`;
- schema for tasks, instructions, generation attempts, and event timeline;
- repository layer with ordered instructions, idempotency keys, version and
  instruction-sequence fencing, immutable original prompts, and atomic
  checkpoint/finish/stop operations;
- explicit task and generation lifecycle state machines;
- startup recovery that marks previously active tasks `INTERRUPTED` (or
  `STOPPED` when a stop was requested) before serving traffic;
- a provider-neutral async streaming interface and a deterministic mock provider
  with configurable chunk size, delays, failures, and test barriers;
- one-worker-per-task generation orchestration with version-fenced checkpoints,
  final commit, failure cleanup, and graceful shutdown;
- deterministic, no-LLM instruction classification (`ADDITION`, `MODIFY`,
  `CORRECTION`, `RESTRICTION`, `REFOCUS`, `NEW_TASK`) that never rewrites or
  discards user text;
- live instruction acceptance with idempotency, debounce, cancellation of the
  old attempt, provider-cleanup waiting, and one coherent replacement;
- context reconstruction from the original prompt, ordered instructions,
  classifications, and the previous partial draft;
- safe manual stop that preserves the latest draft and never claims `STOPPED`
  before worker cleanup finishes;
- bounded per-subscriber SSE queues with snapshots, token deltas, checkpoints,
  instruction/invalidation/restart/stop events, heartbeats, overflow
  resynchronization, and disconnect-safe generation;
- `POST /api/tasks`, `GET /api/tasks/{task_id}`,
  `POST /api/tasks/{task_id}/instructions`, `POST /api/tasks/{task_id}/stop`,
  and `GET /api/tasks/{task_id}/events`;
- 130 automated offline tests.

Not implemented yet (planned for Phases 4–5): the browser UI and the optional
real AI provider.

## Requirements

- Python 3.11 or newer. The current environment was verified with **3.14.7**.
- SQLite is included with Python.
- No API key or paid service is required. The default provider is `mock`.

## Setup (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

> Use `python -m pip` rather than a bare `pip` command. This also works on
> machines where the `pip.exe` launcher is restricted by policy.

Run the development server:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Check it:

- health: <http://127.0.0.1:8000/api/health>
- interactive API docs: <http://127.0.0.1:8000/docs>

Expected health response:

```json
{"status":"ok","database":"ok","provider":"mock","version":"0.3.0"}
```

The SQLite file and its parent directory are created automatically. WAL mode is
enabled; expect `promptflow.db`, `promptflow.db-wal`, and `promptflow.db-shm`
under `data/`.

### macOS/Linux

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
./.venv/bin/python -m uvicorn app.main:app --reload
```

## Run the tests

```powershell
.\.venv\Scripts\python.exe -m pytest
```

The suite uses temporary SQLite files and anyio's asyncio backend. It never
contacts a real AI provider and needs no credentials. A deterministic mock
provider with event barriers exercises streaming, cancellation, failure,
checkpoint, debounce, replacement, stop, and queue-overflow paths. See
`docs/testing.md` for coverage details and `docs/live-instruction-flow.md` for
the Phase 3 sequence.

## Configuration

All settings are read from environment variables or `.env`. See `.env.example`.

| Variable | Default | Purpose |
|---|---|---|
| `AI_PROVIDER` | `mock` | `mock` or `openai` |
| `OPENAI_API_KEY` | empty | backend-only secret; required only for `openai` |
| `OPENAI_MODEL` | empty | configurable model name; required only for `openai` |
| `MOCK_STREAM_DELAY_MS` | `40` | delay between deterministic mock chunks |
| `MOCK_STREAM_CHUNK_CHARS` | `24` | mock chunk size |
| `MOCK_INITIAL_DELAY_MS` | `0` | delay before the mock stream starts |
| `MOCK_FAILURE_AFTER_CHUNKS` | empty | optional deterministic failure point (0 = before first chunk) |
| `DATABASE_PATH` | `./data/promptflow.db` | SQLite file location |
| `INSTRUCTION_DEBOUNCE_MS` | `250` | quiet window before a replacement starts |
| `CHECKPOINT_INTERVAL_MS` | `500` | draft checkpoint interval |
| `MAX_PROMPT_CHARS` | `12000` | maximum initial prompt |
| `MAX_INSTRUCTION_CHARS` | `6000` | maximum live instruction |
| `MAX_INSTRUCTIONS_PER_TASK` | `100` | accepted instruction cap per task |
| `PROVIDER_TIMEOUT_SECONDS` | `120` | model request/stream timeout |
| `CANCELLATION_TIMEOUT_SECONDS` | `5` | local clean-up wait before reporting truthfully |
| `SSE_QUEUE_SIZE` | `256` | per-subscriber bounded queue |
| `SSE_HEARTBEAT_SECONDS` | `15` | SSE keep-alive interval |

Starting with `AI_PROVIDER=openai` without a key or model fails at startup with a
clear configuration error. Keys are never sent to the frontend, logged, or stored
in SQLite.

## Project layout

```text
app/
  main.py                  app factory, lifespan, provider/coordinator wiring
  config.py                validated settings
  api/health.py            health endpoint
  api/tasks.py             create/snapshot/instruction/stop endpoints
  api/events.py            SSE endpoint and frame generator
  core/state.py            lifecycle enums and transition tables
  core/errors.py           domain exceptions
  core/clock.py            UTC timestamps
  core/event_types.py      durable and live event names
  core/event_bus.py        bounded SSE fan-out and overflow resync
  core/task_manager.py     task use cases and snapshot assembly
  core/generation_coordinator.py  workers, debounce, replacement, stop, fencing
  core/context_builder.py  provider request from persisted state
  core/instruction_classifier.py deterministic instruction metadata
  services/ai_provider.py  provider protocol and request/error types
  services/mock_provider.py deterministic test provider
  services/factory.py      provider selection
  database/connection.py   aiosqlite lifecycle and cancellation-safe transactions
  database/migrations.py   transactional migrations
  database/schema.sql      schema (migration 1)
  database/records.py      typed row records
  database/repository.py   all SQL
  schemas/task.py          request/response models and size checks
tests/
  unit/                    state, config, validation, events, provider, classifier
  api/                     health and task HTTP endpoints
  integration/             repository, migrations, recovery, generation/SSE,
                           live instruction replacement and stop races
docs/architecture.md       component design, state table, fencing/recovery, SSE
docs/live-instruction-flow.md  replacement/stop sequence and event ordering
docs/testing.md            coverage and traceability
```

## API (implemented so far)

| Method | Path | Behaviour |
|---|---|---|
| `GET` | `/api/health` | process + database readiness; reveals no secrets |
| `POST` | `/api/tasks` | validate prompt, atomically create task + generation v1, start one worker, return **202** |
| `GET` | `/api/tasks/{task_id}` | authoritative snapshot: task, instructions, current generation, draft provenance, recent durable events |
| `POST` | `/api/tasks/{task_id}/instructions` | persist an instruction, classify it deterministically, invalidate the current attempt, return **202** without waiting for the replacement |
| `POST` | `/api/tasks/{task_id}/stop` | persist stop intent, cancel and clean up the worker, return **202**; status is `CANCELLING` until cleanup finishes |
| `GET` | `/api/tasks/{task_id}/events` | SSE stream: snapshot, token deltas, checkpoints, instruction/replacement/stop events, heartbeat, overflow resync |

Create a task:

```powershell
$response = Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/api/tasks `
  -ContentType 'application/json' `
  -Body '{"prompt":"Explain event loops in one paragraph."}'
$response
```

```json
{"task_id":"...","status":"GENERATING","generation_version":1}
```

Send a live instruction (use `client_instruction_id` for safe retries):

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/api/tasks/$response.task_id/instructions `
  -ContentType 'application/json' `
  -Body '{"content":"Explain it using simple English.","client_instruction_id":"ui-1"}'
```

```json
{"task_id":"...","instruction_id":"...","sequence_no":1,"kind":"MODIFY","generation_version":2,"status":"UPDATE_PENDING","duplicate":false,"requires_new_task":false,"message":null}
```

`NEW_TASK` does not silently rewrite the current task. It is persisted and
reported with `requires_new_task: true`; the API message tells the client to
create a separate task, and the original prompt remains immutable.

Stop a running generation:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/tasks/$response.task_id/stop
```

```json
{"task_id":"...","status":"CANCELLING","generation_version":2,"stop_requested":true,"changed":true}
```

A repeated stop is idempotent and returns `changed: false`. `STOPPED` is written
only after provider cleanup and the worker have terminated. The latest partial
draft remains visible through `working_draft` and
`working_draft_generation_version`.

SSE frames look like this:

```text
event: snapshot
id: 3
data: {"task_id":"...","type":"snapshot","generation_version":1,"task":{...},"working_draft":"","events":[...]}

event: token_delta
id: 4
data: {"task_id":"...","type":"token_delta","generation_version":1,"text":"Mock ","offset":0,"chars":5}

event: checkpoint
id: 5
data: {"task_id":"...","type":"checkpoint","generation_version":1,"draft_chars":120,"checkpointed_at":123.4}

event: generation_completed
id: 9
data: {"task_id":"...","type":"generation_completed","generation_version":1,"answer_chars":227}
```

A live instruction produces this ordering:

```text
instruction_accepted       -> newly accepted sequence/version
generation_invalidated     -> old generation version is no longer current
draft_preserved            -> the old partial draft is retained
context_updated            -> replacement context is ready
generation_restarted       -> replacement attempt is durably created
generation_started         -> replacement worker starts
...token_delta/checkpoint... -> replacement answer streams
generation_completed       -> replacement answer is committed
```

A manual stop publishes `generation_invalidated`, then after cleanup
`generation_cancelled`, `draft_preserved`, `task_stopped`, and
`status: STOPPED`. If cleanup exceeds `CANCELLATION_TIMEOUT_SECONDS`, an `error`
event with `code: cancellation_timeout` is sent and the task remains truthfully
`CANCELLING` until cleanup resolves.

Token deltas are live only. Reconnect and read the snapshot; `token_delta` and
`checkpoint` events are never stored in SQLite.

## Engineering highlights

- **Generation IDs are fences.** Every attempt has a per-task version. Periodic
  checkpoints, final commits, replacement creation, and terminal writes re-check
  the desired version and instruction sequence inside SQL, so a delayed
  completion from an old attempt cannot overwrite a newer answer.
- **Instructions are data, not mutations.** An instruction is persisted in order
  with its exact text and deterministic classification before HTTP 202. The
  server never tries to mutate an in-flight model request; it cancels, preserves
  the partial draft, reconstructs context, and asks for a complete replacement.
- **One worker at a time.** A per-task runtime allows one worker and one
  debounce/restart coordinator. A replacement waits for the old provider and
  worker to finish cleanup before starting, so streams never overlap.
- **Committed answer and working draft are separate.** A failed or cancelled
  replacement keeps its partial draft and leaves the last successful answer
  untouched. Snapshot draft fields identify which generation produced the
  visible text.
- **Atomic terminal writes.** A current attempt's failure/interruption persists
  the final partial draft, generation status, and task status in one transaction.
  A snapshot cannot observe a terminal generation attached to a task that still
  claims to be generating; stale cleanup cannot change the newer task state.
- **Manual stop is truthful.** Stop intent and version invalidation commit before
  the response. `STOPPED` is claimed only after cleanup; a timeout produces an
  explicit `cancellation_timeout` event and never starts another worker.
- **Snapshot-first SSE.** Each subscriber has an independent bounded queue. Token
  deltas are never persisted or replayed; overflow replaces queued deltas with
  `resync_required`, and the client recovers from the authoritative snapshot.
- **Disconnect is not cancellation.** Closing an SSE connection only unregisters
  that subscriber; the worker continues.
- **Deterministic testing.** The mock provider uses `asyncio.Event` barriers for
  start, blocked chunks, release, cancellation, cleanup, and debounce instead of
  arbitrary sleeps.
- **Transactional migrations.** Schema changes and `user_version` commit
  together, so a failed migration leaves no partial schema and can be retried.
- **Bounded recovery surface.** On restart, no in-memory state is trusted;
  active tasks are reconciled from SQLite before the server starts serving.

## Roadmap

1. **Phase 2 (complete):** mock provider, first generation, checkpoints, SSE
   snapshot and token deltas, version-safe completion.
2. **Phase 3 (complete):** live instructions, deterministic classification,
   debounce, cancellation, replacement context, manual stop, fencing, and
   race-condition tests.
3. **Phase 4:** vanilla JS UI with reconnect and snapshot reconciliation.
4. **Phase 5:** optional OpenAI Responses provider behind the same interface.
5. **Phase 6:** hardening, full documentation, interview preparation.

## Known limitations

- Single process, single Uvicorn worker. In-memory coordination does not work
  across processes. Do not run `--workers 2`.
- SQLite is sufficient for this local mini-project, not for horizontal scaling.
- No authentication, multi-tenancy, or billing.
- No vector search, embeddings, RAG, or multi-agent workflows.
- SSE does not implement `Last-Event-ID` replay. Recovery is snapshot-based.
- Token deltas can be missing after reconnect until the next checkpoint or the
  final completion; the committed answer remains authoritative.
- Instruction classification is heuristic metadata only. It is deliberately not
  a semantic model: ambiguous instructions still regenerate, and `NEW_TASK` only
  adds an explicit advisory response rather than creating a task.
- Provider cleanup is cooperative. If a provider ignores cancellation past
  `CANCELLATION_TIMEOUT_SECONDS`, the task remains `CANCELLING`, no replacement
  starts, and an explicit `cancellation_timeout` error is published. A worker
  that finishes later completes a pending manual stop then.
- A replacement is not started automatically after a failed replacement; submit
  another instruction to resume from the retained draft/committed answer.
- `AI_PROVIDER=openai` is not usable until Phase 5. The configuration validates
  the key/model, then startup fails with a clear "not implemented" error rather
  than making a real API call.
- The database lives under OneDrive by default. If SQLite locking misbehaves,
  point `DATABASE_PATH` at a local, non-synced directory.
