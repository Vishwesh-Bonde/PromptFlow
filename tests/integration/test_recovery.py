"""Startup recovery and restart-behaviour tests."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.core.state import GenerationStatus, TaskStatus
from app.database.connection import Database
from app.database.repository import Repository
from app.main import create_app

pytestmark = pytest.mark.anyio


async def test_active_tasks_become_interrupted_and_keep_drafts(
    repository: Repository,
) -> None:
    task = await repository.create_task(
        "Explain machine learning.", status=TaskStatus.GENERATING
    )
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    await repository.save_generation_draft(task.id, 1, "partial draft")

    recovered = await repository.recover_interrupted_tasks()

    loaded = await repository.get_task(task.id)
    generation = await repository.get_generation(task.id, 1)
    events = await repository.list_events(task.id)

    assert recovered == [task.id]
    assert loaded is not None and loaded.status is TaskStatus.INTERRUPTED
    assert loaded.stop_requested is False
    assert generation is not None
    assert generation.status is GenerationStatus.INTERRUPTED
    assert generation.draft_text == "partial draft"
    assert generation.error_code == "server_restart"
    assert events[-1].type == "task_interrupted"
    assert events[-1].payload["previous_status"] == TaskStatus.GENERATING.value


async def test_stop_requested_task_becomes_stopped_on_recovery(
    repository: Repository, database: Database
) -> None:
    task = await repository.create_task(
        "Explain machine learning.", status=TaskStatus.CANCELLING
    )
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    await repository.save_generation_draft(task.id, 1, "stopped partial")
    async with database.transaction() as conn:
        await conn.execute(
            "UPDATE tasks SET stop_requested = 1 WHERE id = ?", (task.id,)
        )

    recovered = await repository.recover_interrupted_tasks()

    loaded = await repository.get_task(task.id)
    generation = await repository.get_generation(task.id, 1)
    assert recovered == [task.id]
    assert loaded is not None and loaded.status is TaskStatus.STOPPED
    assert loaded.stop_requested is True
    assert generation is not None
    assert generation.status is GenerationStatus.CANCELLED
    assert generation.draft_text == "stopped partial"


async def test_recovery_preserves_last_committed_answer(
    repository: Repository,
) -> None:
    task = await repository.create_task(
        "Explain machine learning.", status=TaskStatus.GENERATING
    )
    await repository.create_generation(task.id, 1, base_instruction_seq=0)
    assert await repository.commit_generation(task.id, 1, "First good answer")

    await repository.add_instruction(task.id, "Use simple English.")
    await repository.set_task_status(task.id, TaskStatus.REVISING)
    await repository.create_generation(task.id, 2, base_instruction_seq=1)
    await repository.save_generation_draft(task.id, 2, "unfinished revision")

    await repository.recover_interrupted_tasks()

    loaded = await repository.get_task(task.id)
    generation = await repository.get_generation(task.id, 2)
    assert loaded is not None
    assert loaded.status is TaskStatus.INTERRUPTED
    assert loaded.committed_answer == "First good answer"
    assert generation is not None
    assert generation.status is GenerationStatus.INTERRUPTED
    assert generation.draft_text == "unfinished revision"


async def test_recovery_is_idempotent(repository: Repository) -> None:
    task = await repository.create_task(
        "Explain machine learning.", status=TaskStatus.GENERATING
    )
    await repository.create_generation(task.id, 1, base_instruction_seq=0)

    first = await repository.recover_interrupted_tasks()
    second = await repository.recover_interrupted_tasks()

    assert first == [task.id]
    assert second == []
    loaded = await repository.get_task(task.id)
    assert loaded is not None and loaded.status is TaskStatus.INTERRUPTED


async def test_application_startup_recovers_before_serving(
    settings: Settings,
) -> None:
    setup_database = Database(settings.database_path)
    await setup_database.connect()
    try:
        await setup_database.migrate()
        setup_repository = Repository(setup_database)
        task = await setup_repository.create_task(
            "Explain machine learning.", status=TaskStatus.GENERATING
        )
        await setup_repository.create_generation(
            task.id, 1, base_instruction_seq=0
        )
        await setup_repository.save_generation_draft(task.id, 1, "pre-restart")
    finally:
        await setup_database.close()

    app = create_app(settings)
    async with app.router.lifespan_context(app):
        loaded = await app.state.repository.get_task(task.id)
        generation = await app.state.repository.get_generation(task.id, 1)
        assert loaded is not None and loaded.status is TaskStatus.INTERRUPTED
        assert generation is not None
        assert generation.status is GenerationStatus.INTERRUPTED
        assert generation.draft_text == "pre-restart"
