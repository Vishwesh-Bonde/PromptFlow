"""Repository and persistence behaviour tests."""

from __future__ import annotations

import asyncio

import aiosqlite
import pytest

from app.config import Settings
from app.core.errors import (
    InvalidTransitionError,
    TaskNotFoundError,
    TaskStoppedError,
)
from app.core.state import GenerationStatus, TaskStatus
from app.database.connection import Database
from app.database.repository import Repository

pytestmark = pytest.mark.anyio


async def test_task_round_trip_preserves_original_prompt(
    repository: Repository,
) -> None:
    task = await repository.create_task("  Explain machine learning.  ")

    loaded = await repository.get_task(task.id)

    assert loaded is not None
    assert loaded.original_prompt == "  Explain machine learning.  "
    assert loaded.status is TaskStatus.IDLE
    assert loaded.committed_answer is None


async def test_missing_task_returns_none_and_raises_on_write(
    repository: Repository,
) -> None:
    assert await repository.get_task("does-not-exist") is None
    with pytest.raises(TaskNotFoundError):
        await repository.add_instruction("does-not-exist", "hello")
    with pytest.raises(TaskNotFoundError):
        await repository.set_task_status("does-not-exist", TaskStatus.GENERATING)


async def test_instructions_are_all_retained_in_sequence_order(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain machine learning in detail.")

    for content in ("Use simple English.", "Add college examples.", "Keep it short."):
        await repository.add_instruction(task.id, content)

    instructions = await repository.list_instructions(task.id)
    loaded = await repository.get_task(task.id)

    assert [item.sequence_no for item in instructions] == [1, 2, 3]
    assert [item.content for item in instructions] == [
        "Use simple English.",
        "Add college examples.",
        "Keep it short.",
    ]
    assert loaded is not None
    assert loaded.instruction_seq == 3
    assert loaded.generation_version == 4
    assert loaded.status is TaskStatus.UPDATE_PENDING


async def test_duplicate_client_instruction_id_is_idempotent(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.")

    first = await repository.add_instruction(
        task.id, "Use simple English.", client_instruction_id="key-1"
    )
    second = await repository.add_instruction(
        task.id, "Use simple English.", client_instruction_id="key-1"
    )

    assert first.created is True
    assert second.created is False
    assert second.instruction.id == first.instruction.id
    assert len(await repository.list_instructions(task.id)) == 1
    loaded = await repository.get_task(task.id)
    assert loaded is not None
    assert loaded.generation_version == 2


async def test_concurrent_duplicate_client_ids_create_one_row(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.")

    results = await asyncio.gather(
        *[
            repository.add_instruction(
                task.id,
                "Use simple English.",
                client_instruction_id="same-key",
            )
            for _ in range(2)
        ]
    )

    assert sorted(result.created for result in results) == [False, True]
    assert len(await repository.list_instructions(task.id)) == 1
    loaded = await repository.get_task(task.id)
    assert loaded is not None
    assert loaded.generation_version == 2


async def test_simultaneous_instructions_get_distinct_ordered_sequences(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.")

    await asyncio.gather(
        *[
            repository.add_instruction(task.id, f"instruction {index}")
            for index in range(1, 9)
        ]
    )

    instructions = await repository.list_instructions(task.id)
    loaded = await repository.get_task(task.id)

    assert [item.sequence_no for item in instructions] == list(range(1, 9))
    assert {item.content for item in instructions} == {
        f"instruction {index}" for index in range(1, 9)
    }
    assert loaded is not None
    assert loaded.instruction_seq == 8
    assert loaded.generation_version == 9


async def test_stopped_task_rejects_new_instructions(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.STOPPED)

    with pytest.raises(TaskStoppedError):
        await repository.add_instruction(task.id, "One more thing.")

    assert await repository.list_instructions(task.id) == []


async def test_cancelled_transaction_rolls_back_and_recovers(
    repository: Repository, database: Database
) -> None:
    with pytest.raises(asyncio.CancelledError):
        async with database.transaction() as conn:
            await conn.execute(
                "UPDATE tasks SET status = ?", (TaskStatus.STOPPED.value,)
            )
            raise asyncio.CancelledError

    # The shared connection must not be left inside an uncommitted transaction.
    task = await repository.create_task("Explain ML.")
    assert task.status is TaskStatus.IDLE


async def test_stop_request_blocks_late_instruction(
    repository: Repository, database: Database
) -> None:
    task = await repository.create_task("Explain ML.")
    async with database.transaction() as conn:
        await conn.execute(
            "UPDATE tasks SET stop_requested = 1 WHERE id = ?", (task.id,)
        )

    with pytest.raises(TaskStoppedError):
        await repository.add_instruction(task.id, "A late instruction.")


async def test_foreign_keys_are_enabled(database: Database) -> None:
    with pytest.raises(aiosqlite.IntegrityError):
        async with database.transaction() as conn:
            await conn.execute(
                "INSERT INTO instructions "
                "(id, task_id, sequence_no, content, kind, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("i1", "missing-task", 1, "hello", "ADDITION", "2026-01-01T00:00:00Z"),
            )


async def test_original_prompt_cannot_be_updated(
    repository: Repository, database: Database
) -> None:
    task = await repository.create_task("Original prompt.")

    with pytest.raises(aiosqlite.IntegrityError):
        async with database.transaction() as conn:
            await conn.execute(
                "UPDATE tasks SET original_prompt = ? WHERE id = ?",
                ("Rewritten prompt.", task.id),
            )

    loaded = await repository.get_task(task.id)
    assert loaded is not None
    assert loaded.original_prompt == "Original prompt."


async def test_status_update_validates_transitions(repository: Repository) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.COMPLETED)

    with pytest.raises(InvalidTransitionError):
        await repository.set_task_status(task.id, TaskStatus.GENERATING)

    # Writing the current status is an idempotent no-op, not a transition.
    unchanged = await repository.set_task_status(task.id, TaskStatus.COMPLETED)
    assert unchanged.status is TaskStatus.COMPLETED


async def test_stale_generation_cannot_commit(repository: Repository) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)

    # A new instruction advances the desired version and invalidates attempt 1.
    await repository.add_instruction(task.id, "Use simple English.")

    committed = await repository.commit_generation(task.id, 1, "stale answer")

    assert committed is False
    loaded = await repository.get_task(task.id)
    generation = await repository.get_generation(task.id, 1)
    assert loaded is not None and loaded.committed_answer is None
    assert generation is not None
    assert generation.status is GenerationStatus.GENERATING


async def test_current_revision_can_commit_and_stale_completion_is_ignored(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    assert await repository.commit_generation(task.id, 1, "First answer")

    await repository.add_instruction(task.id, "Use simple English.")
    await repository.set_task_status(task.id, TaskStatus.REVISING)
    await repository.create_generation(task.id, 2, base_instruction_seq=1)

    assert await repository.commit_generation(task.id, 2, "Revised answer")
    # A very late event from attempt 1 must not overwrite attempt 2.
    assert await repository.commit_generation(task.id, 1, "Late stale answer") is False

    loaded = await repository.get_task(task.id)
    assert loaded is not None
    assert loaded.committed_answer == "Revised answer"
    assert loaded.status is TaskStatus.COMPLETED


async def test_failed_revision_preserves_committed_answer(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    await repository.commit_generation(task.id, 1, "First good answer")

    await repository.add_instruction(task.id, "Use simple English.")
    await repository.set_task_status(task.id, TaskStatus.REVISING)
    await repository.create_generation(task.id, 2, base_instruction_seq=1)

    finished = await repository.finish_generation(
        task.id,
        2,
        GenerationStatus.FAILED,
        error_code="provider_error",
        error_message="The provider failed during the revision.",
    )
    await repository.set_task_status(task.id, TaskStatus.FAILED)

    loaded = await repository.get_task(task.id)
    generation = await repository.get_generation(task.id, 2)
    assert finished is True
    assert loaded is not None
    assert loaded.committed_answer == "First good answer"
    assert loaded.status is TaskStatus.FAILED
    assert generation is not None
    assert generation.status is GenerationStatus.FAILED
    assert generation.error_code == "provider_error"


async def test_checkpoint_after_invalidation_is_rejected(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    assert await repository.save_generation_draft(task.id, 1, "before")

    await repository.add_instruction(task.id, "Use simple English.")

    assert await repository.save_generation_draft(task.id, 1, "too late") is False
    generation = await repository.get_generation(task.id, 1)
    assert generation is not None
    assert generation.draft_text == "before"

    # The explicit cleanup path is still allowed to persist the final draft.
    assert await repository.finish_generation(
        task.id,
        1,
        GenerationStatus.CANCELLED,
        draft_text="final cleanup checkpoint",
    )
    cleaned = await repository.get_generation(task.id, 1)
    assert cleaned is not None
    assert cleaned.draft_text == "final cleanup checkpoint"


async def test_stale_cleanup_cannot_change_newer_task_status(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    await repository.add_instruction(task.id, "Use simple English.")

    assert await repository.finish_generation(
        task.id,
        1,
        GenerationStatus.FAILED,
        draft_text="stale draft",
        task_status=TaskStatus.FAILED,
    )

    loaded = await repository.get_task(task.id)
    generation = await repository.get_generation(task.id, 1)
    assert loaded is not None
    assert loaded.status is TaskStatus.UPDATE_PENDING
    assert loaded.generation_version == 2
    assert generation is not None
    assert generation.status is GenerationStatus.FAILED
    assert generation.draft_text == "stale draft"


async def test_cancellation_persists_latest_draft_atomically(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    await repository.save_generation_draft(task.id, 1, "partial draft")

    finished = await repository.finish_generation(
        task.id,
        1,
        GenerationStatus.CANCELLED,
        draft_text="partial draft, final checkpoint",
        error_code="cancelled",
    )

    generation = await repository.get_generation(task.id, 1)
    loaded = await repository.get_task(task.id)
    assert finished is True
    assert generation is not None
    assert generation.status is GenerationStatus.CANCELLED
    assert generation.draft_text == "partial draft, final checkpoint"
    assert loaded is not None
    assert loaded.committed_answer is None


async def test_finished_generation_cannot_be_checkpointed_or_finished_again(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    await repository.finish_generation(
        task.id, 1, GenerationStatus.CANCELLED, draft_text="kept"
    )

    assert await repository.save_generation_draft(task.id, 1, "overwritten") is False
    assert (
        await repository.finish_generation(task.id, 1, GenerationStatus.FAILED)
        is False
    )

    generation = await repository.get_generation(task.id, 1)
    assert generation is not None
    assert generation.draft_text == "kept"


async def test_finish_generation_rejects_completed_status(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)

    with pytest.raises(ValueError):
        await repository.finish_generation(task.id, 1, GenerationStatus.COMPLETED)


async def test_generation_history_is_ordered(repository: Repository) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    await repository.add_instruction(task.id, "Use simple English.")
    await repository.set_task_status(task.id, TaskStatus.REVISING)
    await repository.create_generation(task.id, 2, base_instruction_seq=1)

    generations = await repository.list_generations(task.id)

    assert [generation.version for generation in generations] == [1, 2]
    assert [generation.base_instruction_seq for generation in generations] == [0, 1]


async def test_generation_version_must_match_desired_version(
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain ML.", status=TaskStatus.GENERATING)

    with pytest.raises(ValueError):
        await repository.create_generation(task.id, 2, base_instruction_seq=0)


async def test_event_timeline_is_chronological(repository: Repository) -> None:
    task = await repository.create_task("Explain ML.")
    await repository.set_task_status(task.id, TaskStatus.GENERATING)
    await repository.add_instruction(task.id, "Use simple English.")

    events = await repository.list_events(task.id)

    assert [event.type for event in events] == [
        "task_created",
        "status_changed",
        "instruction_accepted",
    ]
    assert [event.id for event in events] == sorted(event.id for event in events)


async def test_state_reconstructs_from_sqlite_after_reconnect(
    settings: Settings,
    database: Database,
    repository: Repository,
) -> None:
    task = await repository.create_task("Explain machine learning.")
    await repository.add_instruction(task.id, "Use simple English.")
    await repository.set_task_status(task.id, TaskStatus.REVISING)
    await repository.create_generation(task.id, 2, base_instruction_seq=1)
    await repository.save_generation_draft(task.id, 2, "draft after reconnect")

    await database.close()

    reopened = Database(settings.database_path)
    await reopened.connect()
    try:
        await reopened.migrate()
        reopened_repository = Repository(reopened)
        loaded_task = await reopened_repository.get_task(task.id)
        instructions = await reopened_repository.list_instructions(task.id)
        generation = await reopened_repository.get_current_generation(task.id)

        assert loaded_task is not None
        assert loaded_task.original_prompt == "Explain machine learning."
        assert loaded_task.generation_version == 2
        assert [item.content for item in instructions] == ["Use simple English."]
        assert generation is not None
        assert generation.version == 2
        assert generation.draft_text == "draft after reconnect"
        assert generation.status is GenerationStatus.GENERATING
    finally:
        await reopened.close()
