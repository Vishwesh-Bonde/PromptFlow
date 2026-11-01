"""Generation SSE protocol, disconnect, overflow, and shutdown tests."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.api.events import EventStream, prepare_event_stream
from app.config import Settings
from app.core.event_bus import EventBus
from app.core.event_types import StreamEventType
from app.core.state import GenerationStatus, TaskStatus
from app.database.connection import Database
from app.database.repository import Repository
from app.schemas.task import GenerationResponse, TaskResponse, TaskStateResponse
from app.services.ai_provider import GenerationRequest
from app.services.mock_provider import MockAIProvider, build_mock_answer
from tests.helpers import collect_until, next_sse, running_app

pytestmark = pytest.mark.anyio


async def prepare(app, task_id: str):
    return await prepare_event_stream(
        task_manager=app.state.task_manager,
        event_bus=app.state.event_bus,
        task_id=task_id,
    )


async def read_state(path: Path, task_id: str):
    database = Database(path)
    await database.connect()
    try:
        return await Repository(database).get_task_state(task_id)
    finally:
        await database.close()


async def test_initial_snapshot_and_client_disconnect_do_not_cancel(
    settings: Settings,
) -> None:
    provider = MockAIProvider(block_before_start=True)
    provider.release.clear()
    async with running_app(settings, provider) as app:
        created = await app.state.task_manager.create_task("Hold the generation.")
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)

        stream = await prepare(app, task_id)
        frames = stream.frames(heartbeat_seconds=10)
        first = await asyncio.wait_for(next_sse(frames), timeout=2)

        assert first["event"] == "snapshot"
        assert first["data"]["task_id"] == task_id
        assert first["data"]["task"]["id"] == task_id
        assert first["data"]["task"]["status"] == TaskStatus.GENERATING.value
        assert first["data"]["generation"]["version"] == 1
        assert [event["type"] for event in first["data"]["events"]] == [
            "task_created",
            "generation_started",
        ]

        await frames.aclose()
        assert app.state.event_bus.subscriber_count(task_id) == 0
        assert not provider.cancelled.is_set()

        provider.release.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)
        snapshot = await app.state.task_manager.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.task.committed_answer == build_mock_answer(
            GenerationRequest(prompt="Hold the generation.")
        )


async def test_stream_orders_chunks_and_ends_with_completion(
    settings: Settings,
) -> None:
    provider = MockAIProvider(
        block_before_start=True,
        chunk_chars=13,
        chunk_delay_ms=0,
    )
    provider.release.clear()
    async with running_app(settings, provider) as app:
        created = await app.state.task_manager.create_task("Order these chunks.")
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)
        stream = await prepare(app, task_id)
        frames = stream.frames(heartbeat_seconds=10)
        provider.release.set()

        events = await asyncio.wait_for(
            collect_until(frames, "generation_completed"), timeout=5
        )
        await frames.aclose()

        token_events = [
            event for event in events if event["event"] == "token_delta"
        ]
        chunks = [event["data"]["text"] for event in token_events]
        expected = build_mock_answer(
            GenerationRequest(prompt="Order these chunks.")
        )
        assert "".join(chunks) == expected
        assert [event["id"] for event in events] == sorted(
            event["id"] for event in events
        )
        offsets = [event["data"]["offset"] for event in token_events]
        assert offsets == [sum(len(chunk) for chunk in chunks[:index]) for index in range(len(chunks))]

        completion = events[-1]
        assert completion["event"] == "generation_completed"
        assert completion["data"]["generation_version"] == 1
        assert completion["data"]["answer_chars"] == len(expected)
        assert completion["data"]["task_id"] == task_id


async def test_multiple_sse_subscribers_are_independent(
    settings: Settings,
) -> None:
    provider = MockAIProvider(block_before_start=True, chunk_delay_ms=0)
    provider.release.clear()
    async with running_app(settings, provider) as app:
        created = await app.state.task_manager.create_task("Fan out this answer.")
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)

        first_stream = await prepare(app, task_id)
        second_stream = await prepare(app, task_id)
        assert app.state.event_bus.subscriber_count(task_id) == 2

        first_frames = first_stream.frames(heartbeat_seconds=10)
        second_frames = second_stream.frames(heartbeat_seconds=10)
        provider.release.set()

        first_events, second_events = await asyncio.wait_for(
            asyncio.gather(
                collect_until(first_frames, "generation_completed"),
                collect_until(second_frames, "generation_completed"),
            ),
            timeout=5,
        )
        await first_frames.aclose()
        await second_frames.aclose()

        expected = build_mock_answer(
            GenerationRequest(prompt="Fan out this answer.")
        )
        for events in (first_events, second_events):
            assert "".join(
                event["data"]["text"]
                for event in events
                if event["event"] == "token_delta"
            ) == expected
            assert events[-1]["event"] == "generation_completed"
        assert app.state.event_bus.subscriber_count(task_id) == 0


async def test_queue_overflow_emits_resync_and_snapshot_recovers(
    settings: Settings,
) -> None:
    overflow_settings = Settings(
        _env_file=None,
        database_path=settings.database_path,
        sse_queue_size=16,
        mock_stream_chunk_chars=1,
        mock_stream_delay_ms=0,
    )
    provider = MockAIProvider(
        block_before_start=True,
        chunk_chars=1,
        chunk_delay_ms=0,
    )
    provider.release.clear()
    async with running_app(overflow_settings, provider) as app:
        created = await app.state.task_manager.create_task("Overflow me.")
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)

        stream = await prepare(app, task_id)
        frames = stream.frames(heartbeat_seconds=10)
        snapshot = await asyncio.wait_for(next_sse(frames), timeout=2)
        assert snapshot["event"] == "snapshot"

        provider.release.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        events = await asyncio.wait_for(
            collect_until(frames, "resync_required"), timeout=5
        )
        await frames.aclose()

        resync = events[-1]
        assert resync["event"] == "resync_required"
        assert resync["data"]["reason"] == "queue_overflow"

        recovered = await app.state.task_manager.get_snapshot(task_id)
        assert recovered.task.status is TaskStatus.COMPLETED
        assert recovered.task.committed_answer == build_mock_answer(
            GenerationRequest(prompt="Overflow me.")
        )


async def test_heartbeat_comment_is_emitted_when_idle(settings: Settings) -> None:
    provider = MockAIProvider(block_before_start=True)
    provider.release.clear()
    async with running_app(settings, provider) as app:
        created = await app.state.task_manager.create_task("Stay idle.")
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)
        stream = await prepare(app, task_id)
        frames = stream.frames(heartbeat_seconds=0.02)

        assert (await asyncio.wait_for(next_sse(frames), timeout=2))["event"] == (
            "snapshot"
        )
        heartbeat = await asyncio.wait_for(anext(frames), timeout=1)
        assert heartbeat == ": heartbeat\n\n"
        await frames.aclose()

        provider.release.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)


async def test_checkpoint_persists_a_prefix_of_the_final_answer(
    settings: Settings,
) -> None:
    provider = MockAIProvider(
        block_before_start=True,
        chunk_chars=20,
        chunk_delay_ms=30,
    )
    provider.release.clear()
    checkpoint_settings = Settings(
        _env_file=None,
        database_path=settings.database_path,
        checkpoint_interval_ms=50,
    )
    async with running_app(checkpoint_settings, provider) as app:
        prompt = "Checkpoint this draft."
        created = await app.state.task_manager.create_task(prompt)
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)
        stream = await prepare(app, task_id)
        frames = stream.frames(heartbeat_seconds=10)
        provider.release.set()

        events = await asyncio.wait_for(
            collect_until(frames, "checkpoint"), timeout=3
        )
        checkpoint = events[-1]
        snapshot = await app.state.task_manager.get_snapshot(task_id)

        assert checkpoint["event"] == "checkpoint"
        assert checkpoint["data"]["draft_chars"] > 0
        assert snapshot.working_draft
        assert build_mock_answer(GenerationRequest(prompt=prompt)).startswith(
            snapshot.working_draft
        )
        assert checkpoint["data"]["draft_chars"] <= len(snapshot.working_draft)
        assert snapshot.generation is not None
        assert snapshot.generation.status is GenerationStatus.GENERATING

        await frames.aclose()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)
        final = await app.state.task_manager.get_snapshot(task_id)
        assert final.task.committed_answer == build_mock_answer(
            GenerationRequest(prompt=prompt)
        )


async def test_provider_failure_persists_partial_draft_and_preserves_committed(
    settings: Settings,
) -> None:
    provider = MockAIProvider(
        block_before_start=True,
        chunk_chars=24,
        chunk_delay_ms=0,
        failure_after_chunks=2,
    )
    provider.release.clear()
    async with running_app(settings, provider) as app:
        prompt = "Fail after two chunks."
        created = await app.state.task_manager.create_task(prompt)
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)
        stream = await prepare(app, task_id)
        frames = stream.frames(heartbeat_seconds=10)
        provider.release.set()

        events = await asyncio.wait_for(
            collect_until(frames, "generation_failed"), timeout=5
        )
        await frames.aclose()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)

        failures = [
            event
            for event in events
            if event["event"] == "generation_failed"
            and event["data"].get("error_code") == "mock_failure"
        ]
        assert failures
        snapshot = await app.state.task_manager.get_snapshot(task_id)
        expected_prefix = build_mock_answer(
            GenerationRequest(prompt=prompt)
        )[:48]
        assert snapshot.task.status is TaskStatus.FAILED
        assert snapshot.task.committed_answer is None
        assert snapshot.generation is not None
        assert snapshot.generation.status is GenerationStatus.FAILED
        assert snapshot.working_draft == expected_prefix
        assert snapshot.generation.error_code == "mock_failure"


async def test_shutdown_cancels_a_blocked_chunk_and_persists_draft(
    settings: Settings,
) -> None:
    provider = MockAIProvider(
        block_after_chunks=2,
        chunk_chars=24,
        chunk_delay_ms=0,
    )
    provider.release.clear()
    task_id = ""
    prompt = "Cancel in a blocked chunk."
    async with running_app(settings, provider) as app:
        created = await app.state.task_manager.create_task(prompt)
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)
        assert not provider.cancelled.is_set()

    assert provider.cancelled.is_set()
    state = await read_state(settings.database_path, task_id)
    assert state is not None
    assert state.task.status is TaskStatus.INTERRUPTED
    assert state.generation is not None
    assert state.generation.status is GenerationStatus.INTERRUPTED
    assert state.generation.error_code == "server_shutdown"
    assert state.generation.draft_text == build_mock_answer(
        GenerationRequest(prompt=prompt)
    )[:48]
    assert state.task.committed_answer is None


async def test_shutdown_with_active_worker_before_first_chunk(
    settings: Settings,
) -> None:
    provider = MockAIProvider(block_before_start=True)
    provider.release.clear()
    task_id = ""
    async with running_app(settings, provider) as app:
        created = await app.state.task_manager.create_task("Stop before output.")
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)

    assert provider.cancelled.is_set()
    state = await read_state(settings.database_path, task_id)
    assert state is not None
    assert state.task.status is TaskStatus.INTERRUPTED
    assert state.generation is not None
    assert state.generation.status is GenerationStatus.INTERRUPTED
    assert state.generation.draft_text == ""
    assert state.task.committed_answer is None


async def test_shutdown_reconciles_a_worker_cancelled_before_it_starts(
    settings: Settings,
) -> None:
    provider = MockAIProvider(block_before_start=True)
    provider.release.clear()
    task_id = ""
    async with running_app(settings, provider) as app:
        created = await app.state.task_manager.create_task("Cancel immediately.")
        task_id = created.task_id

    state = await read_state(settings.database_path, task_id)
    assert state is not None
    assert state.task.status is TaskStatus.INTERRUPTED
    assert state.generation is not None
    assert state.generation.status is GenerationStatus.INTERRUPTED
    assert state.generation.error_code == "server_shutdown"
    assert state.task.committed_answer is None


async def test_tokens_already_covered_by_snapshot_are_not_duplicated() -> None:
    bus = EventBus(queue_size=8)
    subscription = bus.subscribe("task-1")
    timestamp = "2026-01-01T00:00:00Z"
    snapshot = TaskStateResponse(
        task=TaskResponse(
            id="task-1",
            original_prompt="prompt",
            status=TaskStatus.GENERATING,
            instruction_seq=0,
            generation_version=1,
            committed_answer=None,
            stop_requested=False,
            created_at=timestamp,
            updated_at=timestamp,
        ),
        instructions=[],
        generation=GenerationResponse(
            task_id="task-1",
            version=1,
            base_instruction_seq=0,
            status=GenerationStatus.GENERATING,
            error_code=None,
            error_message=None,
            started_at=timestamp,
            completed_at=None,
            created_at=timestamp,
        ),
        working_draft="abcdef",
        events=[],
    )
    bus.publish(
        "task-1",
        StreamEventType.TOKEN_DELTA,
        generation_version=1,
        data={"text": "abc", "offset": 0},
    )
    bus.publish(
        "task-1",
        StreamEventType.TOKEN_DELTA,
        generation_version=1,
        data={"text": "ghi", "offset": 6},
    )
    stream = EventStream(
        bus=bus,
        subscription=subscription,
        snapshot=snapshot,
        snapshot_seq=0,
    )
    frames = stream.frames(heartbeat_seconds=10)

    assert (await next_sse(frames))["event"] == "snapshot"
    forwarded = await asyncio.wait_for(next_sse(frames), timeout=2)
    assert forwarded["event"] == "token_delta"
    assert forwarded["data"]["offset"] == 6
    assert forwarded["data"]["text"] == "ghi"
    await frames.aclose()
