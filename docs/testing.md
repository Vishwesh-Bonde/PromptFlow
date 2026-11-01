# Testing Notes

## Commands

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m compileall -q app tests
```

The suite runs entirely offline against temporary SQLite databases. Async tests
use anyio's first-party pytest plugin restricted to asyncio. No browser, network,
API key, or paid service is involved. The mock provider is the only provider
constructed by the application in this phase.

## Current coverage (130 tests)

| Area | File | Tests | What is verified |
|---|---|---|---|
| Lifecycle | `tests/unit/test_state.py` | 16 | transition tables, active/terminal sets, valid/invalid transitions, generation terminal rules |
| Configuration | `tests/unit/test_config.py` | 12 | mock defaults, env overrides, bounded values including instruction cap, provider validation, secret masking, directory creation |
| Request validation | `tests/unit/test_schemas.py` | 8 | blank input, whitespace preservation, extra fields, size-limit errors that do not echo text |
| Event bus | `tests/unit/test_event_bus.py` | 4 | independent subscriber queues, slow-subscriber overflow/resync, unsubscribe isolation, sequence advancement |
| Mock provider | `tests/unit/test_mock_provider.py` | 3 | deterministic chunk order/content, failure after N chunks, cancellation at an event barrier |
| Provider factory | `tests/unit/test_provider_factory.py` | 1 | `openai` is unavailable in this phase and fails without any network call |
| Instruction classifier | `tests/unit/test_instruction_classifier.py` | 8 | all six deterministic kinds, repeated-call stability, safe default for blank input |
| Health API | `tests/api/test_health.py` | 3 | 200 shape, no path/secret leakage, 503 when the database is unavailable |
| Task API | `tests/api/test_tasks.py` | 9 | 202 creation without awaiting generation, completion snapshot, 404/422 paths, retrieval while generating, SSE response headers and first frame |
| Migrations | `tests/integration/test_migrations.py` | 6 | fresh schema, pragmas, idempotent rerun, rollback, retry, newer/non-contiguous schema rejection |
| Repository | `tests/integration/test_repository.py` | 24 | prompt immutability, ordering, idempotency, concurrent sequence allocation, stopped-task rejection, stale checkpoint/completion rejection, stale cleanup not changing newer task state, failed revision preserving committed answer, atomic cleanup, cancelled transaction recovery, history, reconstruction |
| Recovery | `tests/integration/test_recovery.py` | 5 | active → `INTERRUPTED`, stop → `STOPPED`, drafts/committed answers retained, idempotence, startup-before-serving |
| Generation + SSE | `tests/integration/test_sse.py` | 11 | initial snapshot, ordered token stream, independent subscribers, disconnect does not cancel, overflow → resync → snapshot recovery, heartbeat, checkpoints, partial failure, cancellation in a blocked chunk, shutdown cleanup, snapshot/token deduplication |
| Live instructions + stop | `tests/integration/test_live_instructions.py` | 20 | replacement context and fencing, debounce coalescing, cleanup barrier, cancellation timeout, late stop finalization, failed replacement recovery, manual/repeated stop, idempotent HTTP acceptance, `NEW_TASK`, instruction limits, SSE ordering, stale failure/status suppression, stop with committed answer, supervisor start/timeout/duplicate handoff races, delayed handoff vs stop, stop-after-replacement-timeout cleanup, restart persistence, stale commit rejection |

## Phase 2 requirement traceability

| Requested test | Status | Location / notes |
|---|---|---|
| Normal successful generation | Implemented | `test_get_task_returns_committed_answer_and_timeline` |
| Correct ordering of streamed chunks | Implemented | `test_stream_orders_chunks_and_ends_with_completion` (text and offsets) |
| Initial task snapshot | Implemented | `test_initial_snapshot_and_client_disconnect_do_not_cancel`, `test_event_endpoint_builds_sse_response` |
| Multiple independent SSE subscribers | Implemented | `test_multiple_sse_subscribers_are_independent`; bus-level test also checks queue isolation |
| Client disconnect without cancelling generation | Implemented | Same SSE test closes the frame generator, asserts zero subscribers, then verifies completion |
| Queue overflow and resynchronization | Implemented | `test_slow_subscriber_gets_resync_without_consuming_fast_subscriber` and `test_queue_overflow_emits_resync_and_snapshot_recovers` |
| Provider failure after partial output | Implemented | `test_provider_failure_persists_partial_draft_and_preserves_committed` |
| Cancellation during a blocked provider chunk | Implemented | `test_shutdown_cancels_a_blocked_chunk_and_persists_draft`; provider records propagated cancellation |
| Checkpoint persistence | Implemented | `test_checkpoint_persists_a_prefix_of_the_final_answer` waits for the committed checkpoint event, then reads SQLite |
| Final answer persistence | Implemented | Task API success test and final snapshot assertion after SSE completion |
| Stale generation checkpoint rejection | Implemented | `test_checkpoint_after_invalidation_is_rejected` |
| Stale generation completion rejection | Implemented | `test_stale_generation_cannot_commit` and `test_current_revision_can_commit_and_stale_completion_is_ignored` |
| Task retrieval during generation | Implemented | `test_task_retrieval_during_generation_reports_active_state` |
| Server shutdown with an active worker | Implemented | `test_shutdown_with_active_worker_before_first_chunk`, plus immediate-shutdown reconciliation |
| No real API calls in tests | Implemented | Only `MockAIProvider` is used; `test_phase_two_does_not_call_a_real_provider` proves the OpenAI path fails without network access |

## Phase 3 requirement traceability

| Requested test | Status | Location / notes |
|---|---|---|
| Instruction accepted during streaming replaces the attempt | Implemented | `test_instruction_during_generation_cancels_and_replaces`: old draft retained, new complete answer committed |
| Persist before HTTP 202 and preserve order/text | Implemented | Same test plus `test_duplicate_instruction_id_is_idempotent_over_http`; response returns before the replacement finishes |
| Rapid instructions coalesce into one replacement | Implemented | `test_three_instructions_during_debounce_make_one_replacement`; all three persisted, only generation v4 created |
| Context reconstruction | Implemented | Replacement request assertions cover immutable prompt, ordered instructions, kinds, previous draft/version |
| Wait for provider cleanup before replacement | Implemented | `test_replacement_waits_for_provider_cleanup`; a cancellation gate proves no second stream starts early and `max_active == 1` |
| Cancellation timeout blocks replacement | Implemented | `test_instruction_cancellation_timeout_does_not_start_replacement`; explicit error and `CANCELLING` |
| No stale commit/checkpoint after invalidation | Implemented | `test_stale_generation_cannot_commit_after_instruction`; repository-level fences also cover cleanup |
| Stale failure cannot publish current status | Implemented | `test_stale_provider_error_does_not_publish_current_failed_status`; old generation may be `FAILED`, task stays in replacement flow |
| Failed replacement preserves committed answer and draft | Implemented | `test_failed_replacement_preserves_committed_and_partial_draft`; a later instruction recovers from the failed draft |
| Manual stop preserves latest draft | Implemented | `test_manual_stop_cancels_worker_and_prevents_restart` |
| Repeated stop is idempotent and does not restart | Implemented | Same test; repeated response has `changed: false` and stays `STOPPED` |
| Stop during a pending replacement | Implemented | `test_stop_during_debounce_cancels_pending_replacement` |
| Stop cleanup timeout then late completion | Implemented | `test_stop_timeout_stays_cancelling_then_late_cleanup_stops`; never claims `STOPPED` early |
| Stop after an earlier replacement timeout | Implemented | `test_stop_after_replacement_timeout_finalizes_on_late_cleanup`; stop reason wins, so late provider cleanup finalizes `STOPPED` |
| Instruction ordering vs stop | Implemented | Stop-after-instruction and instruction-after-stop paths in `test_manual_stop_...`; SQLite transaction order is the rule, with a delayed-handoff test |
| Coordinator handoff race | Implemented | `test_instruction_during_supervisor_handoff_starts_latest_replacement`; an instruction accepted in the start-return window is not stranded |\n| Timeout handoff race | Implemented | `test_instruction_during_timeout_handoff_starts_latest_replacement`; an instruction accepted while the timed-out supervisor winds down still starts the latest replacement |\n| Duplicate coordinator handoff | Implemented | `test_duplicate_coordinator_handoff_does_not_cancel_replacement`; a delayed handoff for an already-current target does not cancel its worker, restart a completed task, or start a duplicate replacement |
| Delayed handoff cannot revive stop | Implemented | `test_late_instruction_handoff_cannot_revive_a_stop`; target/stop flags stay monotonic |
| Idempotent `client_instruction_id` | Implemented | Repository test plus HTTP test; one row, no second version bump, persisted classification wins |
| `NEW_TASK` is explicit, not silent | Implemented | `test_instruction_validation_and_new_task_classification`; response advisory and immutable prompt |
| Instruction validation and per-task cap | Implemented | Same test: blank/oversized/limit/missing-task paths |
| SSE replacement and stop ordering | Implemented | `test_sse_reports_instruction_and_replacement_order`; live timeout tests assert `cancellation_timeout` and late `task_stopped` |
| Restart persistence | Implemented | `test_recovery_keeps_instructions_committed_answer_and_draft` |
| Independent subscribers / snapshot recovery | Implemented | Existing Phase 2 SSE suite remains green; new events use the same bounded bus |

## Notes on SSE and concurrency testing

Starlette's `TestClient` and httpx's `ASGITransport` buffer a complete ASGI
response body, so an intentionally infinite SSE stream cannot be consumed
through them. Tests therefore exercise the real SSE frame generator
(`prepare_event_stream` → `EventStream.frames`) directly and additionally call
the route function to verify the streaming response headers and first frame.
This avoids sleeps and hangs while testing the same production code path.

Concurrency tests use `asyncio.Event` barriers (`started`, `blocked`, `release`,
`cleanup_started`, `cleanup_gate`, `ManualDebounce`) rather than arbitrary
delays. The controlled provider also records overlap so a replacement that
started too early fails the test. The only time-based mechanism in a test is the
configured checkpoint interval and the deliberate short
`CANCELLATION_TIMEOUT_SECONDS` used to exercise timeout handling; tests wait for
the resulting event rather than sleeping for a guessed duration.

## Manual Phase 2 checks

1. Copy `.env.example` to `.env`; leave `AI_PROVIDER=mock`.
2. Start `python -m uvicorn app.main:app --reload`.
3. `POST /api/tasks` with `{"prompt":"Explain event loops."}`; expect HTTP 202.
4. Open `GET /api/tasks/{id}/events`; expect a `snapshot` event, then
   `generation_started`, token deltas, checkpoints, and `generation_completed`.
5. Open `GET /api/tasks/{id}` after completion; expect `COMPLETED` and a
   committed answer. Token events are not in `events` history.
6. Close an SSE connection while generation is still running and confirm the
   task still completes.
7. Stop the server during a slow generation; restart it and confirm the task is
   `INTERRUPTED` with its last draft retained.

## Manual Phase 3 checks

1. Create a task with a long prompt and open its SSE stream.
2. While tokens are arriving, `POST /instructions` with
   `{"content":"Use simple English."}`. Expect HTTP 202 immediately and SSE
   ordering: `instruction_accepted`, `generation_invalidated`,
   `draft_preserved`, `context_updated`, `generation_restarted`,
   `generation_started`, then a complete replacement answer.
3. Send three rapid instructions during the debounce window and confirm all are
   listed in order but only one replacement generation starts.
4. Retry the same instruction with the same `client_instruction_id`; expect one
   persisted instruction and `duplicate: true`.
5. Submit an instruction beginning "Start a new task..." and confirm the response
   has `requires_new_task: true` and the original prompt is unchanged.
6. During generation, `POST /stop`; expect HTTP 202 and `CANCELLING`, then
   `task_stopped` and `STOPPED` after cleanup. The latest partial draft must
   remain visible. Repeat `/stop` and confirm `changed: false`.
7. After `STOPPED`, try another instruction; expect HTTP 409.
8. Restart the server after an interrupted replacement; confirm instructions,
   committed answer, and the latest draft survive.
9. When a provider ignores cancellation, confirm the task remains `CANCELLING`
   with an explicit `cancellation_timeout` SSE error and no replacement starts.
