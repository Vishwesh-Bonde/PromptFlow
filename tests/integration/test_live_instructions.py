"""Phase 3 live-instruction, replacement, stop, and recovery tests."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import httpx
import pytest

from app.api.events import prepare_event_stream
from app.config import Settings
from app.core.state import GenerationStatus, InstructionKind, TaskStatus
from app.database.connection import Database
from app.database.repository import Repository
from app.schemas.task import HARD_MAX_TEXT_CHARS
from app.services.ai_provider import ProviderError
from tests.helpers import (
    ManualDebounce,
    ScriptedCall,
    ScriptedProvider,
    collect_until,
    next_sse,
    running_app,
)

pytestmark = pytest.mark.anyio


def make_settings(settings: Settings, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "database_path": settings.database_path,
        "instruction_debounce_ms": 0,
        "checkpoint_interval_ms": 50,
    }
    values.update(overrides)
    return Settings(**values)


async def read_state(path: Path, task_id: str):
    database = Database(path)
    await database.connect()
    try:
        return await Repository(database).get_task_state(task_id)
    finally:
        await database.close()


async def create_task(client: httpx.AsyncClient, prompt: str) -> str:
    response = await client.post("/api/tasks", json={"prompt": prompt})
    assert response.status_code == 202
    return response.json()["task_id"]


async def test_instruction_during_generation_cancels_and_replaces(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("First partial. ", "This should not finish."),
                block_after_chunks=1,
            ),
            ScriptedCall(chunks=("Replacement answer. ", "Complete.")),
        ]
    )
    debounce = ManualDebounce()
    app_settings = make_settings(settings)
    async with running_app(
        app_settings, provider, debounce_wait=debounce
    ) as app:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            prompt = "Write a detailed explanation of machine learning."
            task_id = await create_task(client, prompt)
            await asyncio.wait_for(provider.first_started.wait(), timeout=2)
            await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

            started_at = time.monotonic()
            response = await client.post(
                f"/api/tasks/{task_id}/instructions",
                json={
                    "content": (
                        "Explain it using simple English and "
                        "college-level examples."
                    ),
                    "client_instruction_id": "instruction-1",
                },
            )
            elapsed = time.monotonic() - started_at

            assert response.status_code == 202
            assert elapsed < 1.0
            accepted = response.json()
            assert accepted["sequence_no"] == 1
            assert accepted["generation_version"] == 2
            assert accepted["status"] == TaskStatus.UPDATE_PENDING.value
            assert accepted["duplicate"] is False

            await app.state.coordinator.wait_until_debouncing(task_id, timeout=2)
            debounce.release.set()
            await app.state.coordinator.wait_for_idle(task_id, timeout=5)

            snapshot = await app.state.task_manager.get_snapshot(task_id)
            assert snapshot.task.original_prompt == prompt
            assert snapshot.task.status is TaskStatus.COMPLETED
            assert snapshot.task.committed_answer == "Replacement answer. Complete."
            assert [instruction.sequence_no for instruction in snapshot.instructions] == [1]
            assert snapshot.instructions[0].content == (
                "Explain it using simple English and college-level examples."
            )

            generations = await app.state.repository.list_generations(task_id)
            assert [(item.version, item.status) for item in generations] == [
                (1, GenerationStatus.CANCELLED),
                (2, GenerationStatus.COMPLETED),
            ]
            assert generations[0].draft_text == "First partial. "
            assert generations[1].base_instruction_seq == 1

            assert len(provider.requests) == 2
            assert provider.requests[1].prompt == prompt
            assert provider.requests[1].instructions == (
                "Explain it using simple English and college-level examples.",
            )
            assert provider.requests[1].instruction_kinds == (
                InstructionKind.MODIFY.value,
            )
            assert provider.requests[1].previous_draft == "First partial. "
            assert provider.requests[1].previous_generation_version == 1
            assert provider.max_active == 1


async def test_three_instructions_during_debounce_make_one_replacement(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [
            ScriptedCall(chunks=("Initial draft. ", "unused"), block_after_chunks=1),
            ScriptedCall(chunks=("One combined replacement.",)),
        ]
    )
    debounce = ManualDebounce()
    app_settings = make_settings(settings)
    async with running_app(
        app_settings, provider, debounce_wait=debounce
    ) as app:
        created = await app.state.task_manager.create_task(
            "Explain machine learning."
        )
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        first = await app.state.task_manager.add_instruction(
            task_id, "Use simple English."
        )
        await app.state.coordinator.wait_until_debouncing(task_id, timeout=2)
        second = await app.state.task_manager.add_instruction(
            task_id, "Add college-level examples."
        )
        third = await app.state.task_manager.add_instruction(
            task_id, "Keep the explanation concise."
        )

        assert [first.sequence_no, second.sequence_no, third.sequence_no] == [1, 2, 3]
        assert second.generation_version == 3
        assert third.generation_version == 4

        debounce.release.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert [item.content for item in snapshot.instructions] == [
            "Use simple English.",
            "Add college-level examples.",
            "Keep the explanation concise.",
        ]
        generations = await app.state.repository.list_generations(task_id)
        assert [item.version for item in generations] == [1, 4]
        assert generations[1].base_instruction_seq == 3
        assert len(provider.requests) == 2
        assert provider.requests[1].instructions == (
            "Use simple English.",
            "Add college-level examples.",
            "Keep the explanation concise.",
        )
        assert provider.requests[1].previous_draft == "Initial draft. "
        assert provider.max_active == 1


async def test_replacement_waits_for_provider_cleanup(
    settings: Settings,
) -> None:
    cleanup_gate = asyncio.Event()
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Old partial.", "unused"),
                block_after_chunks=1,
                cleanup_gate=cleanup_gate,
            ),
            ScriptedCall(chunks=("Clean replacement.",)),
        ]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        await app.state.task_manager.add_instruction(task_id, "Use simple English.")
        await asyncio.wait_for(provider.cleanup_started.wait(), timeout=2)

        # The provider has observed cancellation but has not unwound. No
        # replacement stream may start until that cleanup barrier is released.
        assert len(provider.requests) == 1
        assert provider.active == 1

        cleanup_gate.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.task.committed_answer == "Clean replacement."
        assert len(provider.requests) == 2
        assert provider.max_active == 1


async def test_instruction_during_supervisor_handoff_starts_latest_replacement(
    settings: Settings,
) -> None:
    """An instruction in the start-return window must not strand the task."""
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Initial partial.", "unused"), block_after_chunks=1
            ),
            ScriptedCall(
                chunks=("V2 partial.", "unused"), block_after_chunks=1
            ),
            ScriptedCall(chunks=("V3 complete.",)),
        ]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        coordinator = app.state.coordinator
        original_start_worker = coordinator._start_worker
        supervisor_entered = asyncio.Event()
        release_supervisor = asyncio.Event()

        async def gated_start_worker(runtime, version: int) -> bool:
            started = await original_start_worker(runtime, version)
            if version == 2:
                supervisor_entered.set()
                await release_supervisor.wait()
            return started

        coordinator._start_worker = gated_start_worker  # type: ignore[method-assign]

        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        first = await app.state.task_manager.add_instruction(
            task_id, "Use simple English."
        )
        assert first.sequence_no == 1
        await asyncio.wait_for(supervisor_entered.wait(), timeout=3)
        await asyncio.wait_for(provider.call_started[1].wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[1].wait(), timeout=2)

        # The v2 supervisor has started its worker but has not returned yet.
        # accept_instruction therefore sees a live supervisor and does not make
        # a second one. The supervisor itself must notice the newer target.
        second = await app.state.task_manager.add_instruction(
            task_id, "Add a concrete example."
        )
        assert second.sequence_no == 2

        release_supervisor.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.task.committed_answer == "V3 complete."
        assert [item.sequence_no for item in snapshot.instructions] == [1, 2]
        assert len(provider.requests) == 3
        assert provider.max_active == 1

        generations = await app.state.repository.list_generations(task_id)
        assert [(item.version, item.status) for item in generations] == [
            (1, GenerationStatus.CANCELLED),
            (2, GenerationStatus.CANCELLED),
            (3, GenerationStatus.COMPLETED),
        ]


async def test_instruction_during_timeout_handoff_starts_latest_replacement(
    settings: Settings,
) -> None:
    """An instruction accepted during timeout cleanup must not be stranded."""
    cleanup_gate = asyncio.Event()
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Old partial.", "unused"),
                block_after_chunks=1,
                cleanup_gate=cleanup_gate,
            ),
            ScriptedCall(chunks=("Recovered complete.",)),
        ]
    )
    app_settings = make_settings(
        settings, cancellation_timeout_seconds=0.05
    )
    async with running_app(app_settings, provider) as app:
        coordinator = app.state.coordinator
        original_mark_timeout = coordinator._mark_cancellation_timeout
        timeout_entered = asyncio.Event()
        release_timeout = asyncio.Event()

        async def gated_mark_timeout(runtime, version: int | None) -> None:
            await original_mark_timeout(runtime, version)
            timeout_entered.set()
            await release_timeout.wait()

        coordinator._mark_cancellation_timeout = gated_mark_timeout  # type: ignore[method-assign]

        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        first = await app.state.task_manager.add_instruction(
            task_id, "Use simple English."
        )
        assert first.sequence_no == 1
        await asyncio.wait_for(timeout_entered.wait(), timeout=3)
        await asyncio.wait_for(provider.cleanup_started.wait(), timeout=2)

        # Let the timed-out v1 worker finish, but keep its supervisor in the
        # timeout path. The v2 supervisor is still the current restart task.
        cleanup_gate.set()

        async def wait_for_worker_cleanup() -> None:
            runtime = coordinator._runtimes[task_id]
            while runtime.worker is not None:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_worker_cleanup(), timeout=2)

        second = await app.state.task_manager.add_instruction(
            task_id, "Add a concrete example."
        )
        assert second.sequence_no == 2

        release_timeout.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.task.committed_answer == "Recovered complete."
        assert [item.sequence_no for item in snapshot.instructions] == [1, 2]
        assert len(provider.requests) == 2
        assert provider.requests[1].instructions == (
            "Use simple English.",
            "Add a concrete example.",
        )
        assert provider.requests[1].previous_draft == "Old partial."
        assert provider.max_active == 1

        generations = await app.state.repository.list_generations(task_id)
        assert [(item.version, item.status) for item in generations] == [
            (1, GenerationStatus.CANCELLED),
            (3, GenerationStatus.COMPLETED),
        ]


async def test_duplicate_coordinator_handoff_does_not_cancel_replacement(
    settings: Settings,
) -> None:
    """A delayed handoff for an already-current target must be a no-op."""
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Initial partial.", "unused"), block_after_chunks=1
            ),
            ScriptedCall(chunks=("V2 complete.",), block_before_start=True),
            ScriptedCall(chunks=("Unexpected third call.",)),
        ]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        accepted = await app.state.task_manager.add_instruction(
            task_id, "Use simple English."
        )
        await asyncio.wait_for(provider.call_started[1].wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[1].wait(), timeout=2)

        # Simulate a delayed duplicate of the same coordinator handoff after a
        # supervisor refreshed the target from SQLite. The v2 worker is already
        # current, so this must not invalidate it or create another supervisor.
        snapshot = await app.state.task_manager.get_snapshot(task_id)
        instruction = snapshot.instructions[0]
        await app.state.coordinator.accept_instruction(
            task_id=task_id,
            new_version=accepted.generation_version,
            instruction_seq=snapshot.task.instruction_seq,
            sequence_no=instruction.sequence_no,
            kind=instruction.kind,
            instruction_id=instruction.id,
        )
        await asyncio.sleep(0)
        runtime = app.state.coordinator._runtimes[task_id]
        assert runtime.restart_task is None
        assert runtime.worker is not None and not runtime.worker.done()
        assert len(provider.requests) == 2

        provider.release[1].set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        final = await app.state.task_manager.get_snapshot(task_id)
        assert final.task.status is TaskStatus.COMPLETED
        assert final.task.committed_answer == "V2 complete."
        assert len(provider.requests) == 2
        assert provider.max_active == 1

        generations = await app.state.repository.list_generations(task_id)
        assert [(item.version, item.status) for item in generations] == [
            (1, GenerationStatus.CANCELLED),
            (2, GenerationStatus.COMPLETED),
        ]

        # The same delayed handoff after completion must also be a no-op; it
        # must never turn a completed task back into CANCELLING.
        await app.state.coordinator.accept_instruction(
            task_id=task_id,
            new_version=accepted.generation_version,
            instruction_seq=snapshot.task.instruction_seq,
            sequence_no=instruction.sequence_no,
            kind=instruction.kind,
            instruction_id=instruction.id,
        )
        await asyncio.sleep(0)
        runtime = app.state.coordinator._runtimes[task_id]
        assert runtime.restart_task is None
        assert runtime.worker is None
        completed_again = await app.state.task_manager.get_snapshot(task_id)
        assert completed_again.task.status is TaskStatus.COMPLETED
        assert completed_again.task.committed_answer == "V2 complete."
        assert len(provider.requests) == 2


async def test_late_instruction_handoff_cannot_revive_a_stop(
    settings: Settings,
) -> None:
    """A delayed coordinator call must not undo a stop committed after it."""
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Old partial.", "unused"), block_after_chunks=1
            )
        ]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        # Durable order: instruction, then stop. Apply the coordinator handoff
        # in the opposite order to simulate a task that was descheduled between
        # SQLite commit and local bookkeeping.
        instruction = await app.state.repository.add_instruction(
            task_id,
            "Use simple English.",
            kind=InstructionKind.MODIFY,
        )
        stopped = await app.state.repository.request_stop(task_id)
        assert stopped.task.generation_version == 3

        await app.state.coordinator.accept_stop(
            task_id=task_id,
            stop_version=stopped.task.generation_version,
            invalidated_version=stopped.invalidated_generation_version,
        )
        await app.state.coordinator.accept_instruction(
            task_id=task_id,
            new_version=instruction.task.generation_version,
            instruction_seq=instruction.task.instruction_seq,
            sequence_no=instruction.instruction.sequence_no,
            kind=instruction.instruction.kind,
            instruction_id=instruction.instruction.id,
        )

        runtime = app.state.coordinator._runtimes[task_id]
        assert runtime.stop_requested is True
        assert runtime.stopping is True
        assert runtime.desired_version == stopped.task.generation_version

        await app.state.coordinator.wait_for_idle(task_id, timeout=5)
        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.STOPPED
        assert snapshot.task.stop_requested is True
        assert [item.sequence_no for item in snapshot.instructions] == [1]
        assert len(provider.requests) == 1

        generations = await app.state.repository.list_generations(task_id)
        assert [(item.version, item.status) for item in generations] == [
            (1, GenerationStatus.CANCELLED)
        ]


async def test_instruction_cancellation_timeout_does_not_start_replacement(
    settings: Settings,
) -> None:
    cleanup_gate = asyncio.Event()
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Timed out partial.", "unused"),
                block_after_chunks=1,
                cleanup_gate=cleanup_gate,
            ),
            ScriptedCall(chunks=("Must never start.",)),
        ]
    )
    app_settings = make_settings(
        settings, cancellation_timeout_seconds=0.05
    )
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        stream = await prepare_event_stream(
            task_manager=app.state.task_manager,
            event_bus=app.state.event_bus,
            task_id=task_id,
        )
        frames = stream.frames(heartbeat_seconds=10)
        assert (await next_sse(frames))["event"] == "snapshot"

        await app.state.task_manager.add_instruction(task_id, "Use simple English.")
        events = await asyncio.wait_for(
            collect_until(frames, "error"), timeout=5
        )
        await frames.aclose()
        assert events[-1]["data"]["code"] == "cancellation_timeout"

        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.CANCELLING
        assert snapshot.generation is None
        assert len(provider.requests) == 1

        cleanup_gate.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        # Late cleanup records the old draft but deliberately starts no new
        # worker. Only a fresh instruction may resume the task.
        generations = await app.state.repository.list_generations(task_id)
        assert [(item.version, item.status) for item in generations] == [
            (1, GenerationStatus.CANCELLED)
        ]
        assert generations[0].draft_text == "Timed out partial."
        assert len(provider.requests) == 1


async def test_stop_timeout_stays_cancelling_then_late_cleanup_stops(
    settings: Settings,
) -> None:
    cleanup_gate = asyncio.Event()
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Stop partial.", "unused"),
                block_after_chunks=1,
                cleanup_gate=cleanup_gate,
            )
        ]
    )
    app_settings = make_settings(
        settings, cancellation_timeout_seconds=0.05
    )
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        stream = await prepare_event_stream(
            task_manager=app.state.task_manager,
            event_bus=app.state.event_bus,
            task_id=task_id,
        )
        frames = stream.frames(heartbeat_seconds=10)
        assert (await next_sse(frames))["event"] == "snapshot"

        stopped = await app.state.task_manager.stop_task(task_id)
        assert stopped.status is TaskStatus.CANCELLING

        timeout_events = await asyncio.wait_for(
            collect_until(frames, "error"), timeout=5
        )
        assert timeout_events[-1]["data"]["code"] == "cancellation_timeout"
        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.CANCELLING

        cleanup_gate.set()
        events = await asyncio.wait_for(
            collect_until(frames, "task_stopped"), timeout=5
        )
        await frames.aclose()
        assert events[-1]["data"]["status"] == TaskStatus.STOPPED.value
        assert events[-1]["data"]["generation_version"] == 2

        await app.state.coordinator.wait_for_idle(task_id, timeout=5)
        final = await app.state.task_manager.get_snapshot(task_id)
        assert final.task.status is TaskStatus.STOPPED
        assert final.working_draft == "Stop partial."
        assert final.working_draft_generation_version == 1


async def test_stop_after_replacement_timeout_finalizes_on_late_cleanup(
    settings: Settings,
) -> None:
    """A stop must replace an earlier 'replaced' cleanup reason."""
    cleanup_gate = asyncio.Event()
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Old partial.", "unused"),
                block_after_chunks=1,
                cleanup_gate=cleanup_gate,
                error=ProviderError(
                    "cancel_error", "Provider failed during cancellation cleanup."
                ),
            )
        ]
    )
    app_settings = make_settings(
        settings, cancellation_timeout_seconds=0.05
    )
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        stream = await prepare_event_stream(
            task_manager=app.state.task_manager,
            event_bus=app.state.event_bus,
            task_id=task_id,
        )
        frames = stream.frames(heartbeat_seconds=10)
        assert (await next_sse(frames))["event"] == "snapshot"

        await app.state.task_manager.add_instruction(task_id, "Use simple English.")
        replacement_timeout = await asyncio.wait_for(
            collect_until(frames, "error"), timeout=5
        )
        assert replacement_timeout[-1]["data"]["code"] == "cancellation_timeout"

        stopped = await app.state.task_manager.stop_task(task_id)
        assert stopped.status is TaskStatus.CANCELLING
        stop_timeout = await asyncio.wait_for(
            collect_until(frames, "error"), timeout=5
        )
        assert stop_timeout[-1]["data"]["code"] == "cancellation_timeout"

        cleanup_gate.set()
        events = await asyncio.wait_for(
            collect_until(frames, "task_stopped"), timeout=5
        )
        await frames.aclose()
        assert events[-1]["data"]["status"] == TaskStatus.STOPPED.value
        assert events[-1]["data"]["generation_version"] == 3

        await app.state.coordinator.wait_for_idle(task_id, timeout=5)
        final = await app.state.task_manager.get_snapshot(task_id)
        assert final.task.status is TaskStatus.STOPPED
        assert final.task.committed_answer is None
        assert final.working_draft == "Old partial."
        assert final.working_draft_generation_version == 1

        generations = await app.state.repository.list_generations(task_id)
        assert [(item.version, item.status) for item in generations] == [
            (1, GenerationStatus.FAILED)
        ]


async def test_stale_provider_error_does_not_publish_current_failed_status(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [
            ScriptedCall(
                chunks=("Old partial.", "unused"),
                block_after_chunks=1,
                error=ProviderError("cancel_error", "Provider failed during cancel."),
            ),
            ScriptedCall(chunks=("Replacement complete.",)),
        ]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        stream = await prepare_event_stream(
            task_manager=app.state.task_manager,
            event_bus=app.state.event_bus,
            task_id=task_id,
        )
        frames = stream.frames(heartbeat_seconds=10)
        assert (await next_sse(frames))["event"] == "snapshot"

        await app.state.task_manager.add_instruction(task_id, "Use simple English.")
        events = await asyncio.wait_for(
            collect_until(frames, "generation_completed"), timeout=5
        )
        await frames.aclose()

        assert any(
            event["event"] == "generation_failed"
            and event["data"]["generation_version"] == 1
            for event in events
        )
        assert not any(
            event["event"] == "status"
            and event["data"].get("status") == TaskStatus.FAILED.value
            for event in events
        )
        completed = events[-1]
        assert completed["data"]["generation_version"] == 2

        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.task.committed_answer == "Replacement complete."
        generations = await app.state.repository.list_generations(task_id)
        assert [(item.version, item.status) for item in generations] == [
            (1, GenerationStatus.FAILED),
            (2, GenerationStatus.COMPLETED),
        ]


async def test_stop_after_committed_answer_keeps_replacement_draft(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [
            ScriptedCall(chunks=("Committed answer.",)),
            ScriptedCall(
                chunks=("Replacement partial.", "unused"), block_after_chunks=1
            ),
        ]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        await app.state.task_manager.add_instruction(task_id, "Use simple English.")
        await asyncio.wait_for(provider.call_started[1].wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[1].wait(), timeout=2)

        stopped = await app.state.task_manager.stop_task(task_id)
        assert stopped.status is TaskStatus.CANCELLING
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.STOPPED
        assert snapshot.task.committed_answer == "Committed answer."
        assert snapshot.generation is None
        assert snapshot.working_draft == "Replacement partial."
        assert snapshot.working_draft_generation_version == 2
        assert (
            snapshot.working_draft_generation_status
            is GenerationStatus.CANCELLED
        )


async def test_failed_replacement_preserves_committed_and_partial_draft(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [
            ScriptedCall(chunks=("First good answer.",)),
            ScriptedCall(
                chunks=("Partial replacement.",),
                error=ProviderError("provider_down", "The provider went away."),
            ),
            ScriptedCall(chunks=("Recovered answer.",)),
        ]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        await app.state.task_manager.add_instruction(task_id, "Use simple English.")
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        failed = await app.state.task_manager.get_snapshot(task_id)
        assert failed.task.status is TaskStatus.FAILED
        assert failed.task.committed_answer == "First good answer."
        assert failed.generation is not None
        assert failed.generation.version == 2
        assert failed.generation.status is GenerationStatus.FAILED
        assert failed.generation.error_code == "provider_down"
        assert failed.working_draft == "Partial replacement."
        assert failed.working_draft_generation_version == 2
        assert [item.sequence_no for item in failed.instructions] == [1]

        await app.state.task_manager.add_instruction(task_id, "Correct the answer.")
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        recovered = await app.state.task_manager.get_snapshot(task_id)
        assert recovered.task.status is TaskStatus.COMPLETED
        assert recovered.task.committed_answer == "Recovered answer."
        assert [item.sequence_no for item in recovered.instructions] == [1, 2]
        assert [item.version for item in await app.state.repository.list_generations(task_id)] == [
            1,
            2,
            3,
        ]
        assert provider.requests[2].previous_draft == "Partial replacement."
        assert provider.requests[2].previous_generation_version == 2


async def test_manual_stop_cancels_worker_and_prevents_restart(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [ScriptedCall(chunks=("Stopped partial.", "never"), block_after_chunks=1)]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            task_id = await create_task(client, "Stop this generation.")
            await asyncio.wait_for(provider.first_started.wait(), timeout=2)
            await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

            response = await client.post(f"/api/tasks/{task_id}/stop")
            assert response.status_code == 202
            body = response.json()
            assert body["status"] == TaskStatus.CANCELLING.value
            assert body["stop_requested"] is True
            assert body["generation_version"] == 2

            await app.state.coordinator.wait_for_idle(task_id, timeout=5)
            snapshot = await app.state.task_manager.get_snapshot(task_id)
            assert snapshot.task.status is TaskStatus.STOPPED
            assert snapshot.task.stop_requested is True
            assert snapshot.task.committed_answer is None
            assert snapshot.generation is None
            assert snapshot.working_draft == "Stopped partial."
            assert snapshot.working_draft_generation_version == 1
            assert (
                snapshot.working_draft_generation_status
                is GenerationStatus.CANCELLED
            )
            generations = await app.state.repository.list_generations(task_id)
            assert [(item.version, item.status) for item in generations] == [
                (1, GenerationStatus.CANCELLED)
            ]
            assert generations[0].draft_text == "Stopped partial."
            assert len(provider.requests) == 1

            late = await client.post(
                f"/api/tasks/{task_id}/instructions",
                json={"content": "This must not restart."},
            )
            assert late.status_code == 409

            repeated = await client.post(f"/api/tasks/{task_id}/stop")
            assert repeated.status_code == 202
            assert repeated.json()["changed"] is False
            assert repeated.json()["status"] == TaskStatus.STOPPED.value


async def test_stop_during_debounce_cancels_pending_replacement(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [ScriptedCall(chunks=("Initial partial.", "unused"), block_after_chunks=1)]
    )
    debounce = ManualDebounce()
    app_settings = make_settings(settings)
    async with running_app(
        app_settings, provider, debounce_wait=debounce
    ) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        await app.state.task_manager.add_instruction(task_id, "Use simple English.")
        await app.state.coordinator.wait_until_debouncing(task_id, timeout=2)
        stop = await app.state.task_manager.stop_task(task_id)

        assert stop.status is TaskStatus.CANCELLING
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.STOPPED
        assert snapshot.task.stop_requested is True
        assert [item.sequence_no for item in snapshot.instructions] == [1]
        assert [item.version for item in await app.state.repository.list_generations(task_id)] == [
            1
        ]
        assert len(provider.requests) == 1


async def test_duplicate_instruction_id_is_idempotent_over_http(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [
            ScriptedCall(chunks=("Partial.", "unused"), block_after_chunks=1),
            ScriptedCall(chunks=("Replacement.",)),
        ]
    )
    app_settings = make_settings(settings)
    async with running_app(app_settings, provider) as app:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            task_id = await create_task(client, "Explain ML.")
            await asyncio.wait_for(provider.first_started.wait(), timeout=2)
            await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)
            payload = {
                "content": "Use simple English.",
                "client_instruction_id": "same-client-key",
            }

            first = await client.post(
                f"/api/tasks/{task_id}/instructions", json=payload
            )
            # A retry may even carry a different body; the persisted row and
            # classification are authoritative.
            second = await client.post(
                f"/api/tasks/{task_id}/instructions",
                json={
                    "content": "Start a new task about databases.",
                    "client_instruction_id": "same-client-key",
                },
            )

            assert first.status_code == 202
            assert second.status_code == 202
            assert first.json()["sequence_no"] == 1
            assert second.json()["sequence_no"] == 1
            assert first.json()["duplicate"] is False
            assert second.json()["duplicate"] is True
            assert second.json()["kind"] == first.json()["kind"] == "MODIFY"
            assert second.json()["requires_new_task"] is False
            assert first.json()["generation_version"] == second.json()[
                "generation_version"
            ]

            await app.state.coordinator.wait_for_idle(task_id, timeout=5)
            snapshot = await app.state.task_manager.get_snapshot(task_id)
            assert len(snapshot.instructions) == 1
            assert snapshot.task.generation_version == 2


async def test_instruction_validation_and_new_task_classification(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [
            ScriptedCall(chunks=("Partial.", "unused"), block_after_chunks=1),
            ScriptedCall(chunks=("Replacement.",)),
        ]
    )
    app_settings = make_settings(
        settings, max_instruction_chars=100, max_instructions_per_task=1
    )
    async with running_app(app_settings, provider) as app:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            task_id = await create_task(client, "Explain ML.")
            await asyncio.wait_for(provider.first_started.wait(), timeout=2)
            await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

            blank = await client.post(
                f"/api/tasks/{task_id}/instructions",
                json={"content": "   "},
            )
            too_large = await client.post(
                f"/api/tasks/{task_id}/instructions",
                json={"content": "x" * 101},
            )
            missing = await client.post(
                "/api/tasks/missing/instructions",
                json={"content": "Use simple English."},
            )

            assert blank.status_code == 422
            assert too_large.status_code == 422
            assert "at most" in too_large.json()["detail"]
            assert missing.status_code == 404

            new_task = await client.post(
                f"/api/tasks/{task_id}/instructions",
                json={"content": "Start a new task about databases."},
            )
            assert new_task.status_code == 202
            assert new_task.json()["kind"] == "NEW_TASK"
            assert new_task.json()["requires_new_task"] is True
            assert new_task.json()["message"]

            over_limit = await client.post(
                f"/api/tasks/{task_id}/instructions",
                json={"content": "One instruction too many."},
            )
            assert over_limit.status_code == 422
            assert "maximum of 1" in over_limit.json()["detail"]

            await app.state.coordinator.wait_for_idle(task_id, timeout=5)
            snapshot = await app.state.task_manager.get_snapshot(task_id)
            assert snapshot.task.original_prompt == "Explain ML."


async def test_sse_reports_instruction_and_replacement_order(
    settings: Settings,
) -> None:
    provider = ScriptedProvider(
        [
            ScriptedCall(chunks=("Old partial.", "unused"), block_after_chunks=1),
            ScriptedCall(chunks=("New complete answer.",)),
        ]
    )
    debounce = ManualDebounce()
    app_settings = make_settings(settings)
    async with running_app(
        app_settings, provider, debounce_wait=debounce
    ) as app:
        created = await app.state.task_manager.create_task("Explain ML.")
        task_id = created.task_id
        await asyncio.wait_for(provider.first_started.wait(), timeout=2)
        await asyncio.wait_for(provider.blocked[0].wait(), timeout=2)

        stream = await prepare_event_stream(
            task_manager=app.state.task_manager,
            event_bus=app.state.event_bus,
            task_id=task_id,
        )
        frames = stream.frames(heartbeat_seconds=10)
        snapshot = await next_sse(frames)
        assert snapshot["event"] == "snapshot"

        await app.state.task_manager.add_instruction(task_id, "Use simple English.")
        await app.state.coordinator.wait_until_debouncing(task_id, timeout=2)
        debounce.release.set()

        events = await asyncio.wait_for(
            collect_until(frames, "generation_completed"), timeout=5
        )
        await frames.aclose()

        names = [event["event"] for event in events]
        for required in (
            "instruction_accepted",
            "generation_invalidated",
            "draft_preserved",
            "context_updated",
            "generation_restarted",
            "generation_started",
            "generation_completed",
        ):
            assert required in names

        accepted = next(event for event in events if event["event"] == "instruction_accepted")
        assert accepted["data"]["generation_version"] == 2
        assert accepted["data"]["sequence_no"] == 1
        invalidated = next(
            event for event in events if event["event"] == "generation_invalidated"
        )
        assert invalidated["data"]["generation_version"] == 1
        assert invalidated["data"]["new_generation_version"] == 2
        completed = events[-1]
        assert completed["data"]["generation_version"] == 2
        assert [event["id"] for event in events] == sorted(
            event["id"] for event in events
        )


async def test_recovery_keeps_instructions_committed_answer_and_draft(
    settings: Settings,
) -> None:
    setup_database = Database(settings.database_path)
    await setup_database.connect()
    try:
        await setup_database.migrate()
        repository = Repository(setup_database)
        task = await repository.create_task(
            "Explain machine learning.", status=TaskStatus.GENERATING
        )
        await repository.create_generation(task.id, 1, base_instruction_seq=0)
        assert await repository.commit_generation(task.id, 1, "Committed answer")
        await repository.add_instruction(
            task.id, "Use simple English.", client_instruction_id="survives-restart"
        )
        await repository.set_task_status(task.id, TaskStatus.REVISING)
        await repository.create_generation(task.id, 2, base_instruction_seq=1)
        await repository.save_generation_draft(task.id, 2, "interrupted draft")
    finally:
        await setup_database.close()

    provider = ScriptedProvider([])
    async with running_app(make_settings(settings), provider) as app:
        snapshot = await app.state.task_manager.get_snapshot(task.id)
        assert snapshot.task.status is TaskStatus.INTERRUPTED
        assert snapshot.task.committed_answer == "Committed answer"
        assert [item.content for item in snapshot.instructions] == [
            "Use simple English."
        ]
        assert snapshot.generation is not None
        assert snapshot.generation.version == 2
        assert snapshot.generation.status is GenerationStatus.INTERRUPTED
        assert snapshot.working_draft == "interrupted draft"
        assert provider.requests == []

        state = await read_state(settings.database_path, task.id)
        assert state is not None
        assert state.task.instruction_seq == 1


async def test_stale_generation_cannot_commit_after_instruction(
    settings: Settings,
) -> None:
    database = Database(settings.database_path)
    await database.connect()
    try:
        await database.migrate()
        repository = Repository(database)
        task = await repository.create_task(
            "Explain ML.", status=TaskStatus.GENERATING
        )
        await repository.create_generation(task.id, 1, base_instruction_seq=0)
        await repository.add_instruction(task.id, "Use simple English.")
        await repository.set_task_status(task.id, TaskStatus.REVISING)
        await repository.create_generation(task.id, 2, base_instruction_seq=1)

        assert await repository.save_generation_draft(task.id, 1, "late checkpoint") is False
        assert await repository.commit_generation(task.id, 1, "late answer") is False
        assert (
            await repository.finish_generation(
                task.id,
                1,
                GenerationStatus.FAILED,
                error_code="late_failure",
            )
            is True
        )

        loaded = await repository.get_task(task.id)
        assert loaded is not None
        assert loaded.committed_answer is None
        assert loaded.status is TaskStatus.REVISING
        assert loaded.generation_version == 2
        generation = await repository.get_generation(task.id, 1)
        assert generation is not None
        assert generation.status is GenerationStatus.FAILED
    finally:
        await database.close()
