"""Task read/write facade used by the API layer.

Routes stay thin: they validate transport concerns and delegate lifecycle work
here. Snapshots are assembled from one repository read lock. Instruction
acceptance commits to SQLite before returning and hands only scheduling work to
the coordinator, so the HTTP response never waits for a replacement model call.
"""

from __future__ import annotations

import asyncio
import logging

from app.core.errors import TaskNotFoundError
from app.core.generation_coordinator import GenerationCoordinator
from app.core.instruction_classifier import classify_instruction
from app.core.state import GenerationStatus, InstructionKind, TaskStatus
from app.database.repository import Repository
from app.schemas.task import (
    GenerationResponse,
    InstructionAcceptedResponse,
    InstructionResponse,
    TaskCreatedResponse,
    TaskEventResponse,
    TaskResponse,
    TaskStateResponse,
    TaskStopResponse,
)

logger = logging.getLogger(__name__)


class TaskManager:
    def __init__(
        self,
        *,
        repository: Repository,
        coordinator: GenerationCoordinator,
        max_instructions_per_task: int = 100,
    ) -> None:
        self._repository = repository
        self._coordinator = coordinator
        self._max_instructions_per_task = max_instructions_per_task
        self._task_locks: dict[str, asyncio.Lock] = {}

    def _task_lock(self, task_id: str) -> asyncio.Lock:
        lock = self._task_locks.get(task_id)
        if lock is None:
            lock = asyncio.Lock()
            self._task_locks[task_id] = lock
        return lock

    async def create_task(self, prompt: str) -> TaskCreatedResponse:
        task, generation = await self._repository.create_task_with_generation(prompt)
        try:
            await self._coordinator.schedule_initial(task.id, generation.version)
        except Exception:
            logger.exception("Could not schedule generation for task %s", task.id)
            try:
                await self._repository.finish_generation(
                    task.id,
                    generation.version,
                    GenerationStatus.FAILED,
                    error_code="scheduler_error",
                    error_message="The generation could not be scheduled.",
                    task_status=TaskStatus.FAILED,
                )
            except Exception:
                logger.exception(
                    "Could not mark unscheduled task %s as failed", task.id
                )
            raise
        return TaskCreatedResponse(
            task_id=task.id,
            status=task.status,
            generation_version=task.generation_version,
        )

    async def add_instruction(
        self,
        task_id: str,
        content: str,
        *,
        client_instruction_id: str | None = None,
    ) -> InstructionAcceptedResponse:
        # Serialize the durable commit and its coordinator handoff per task.
        # Otherwise a stop could commit between them and the late coordination
        # call could reorder live events or revive local stop state.
        async with self._task_lock(task_id):
            return await self._accept_instruction(
                task_id,
                content,
                client_instruction_id=client_instruction_id,
            )

    async def _accept_instruction(
        self,
        task_id: str,
        content: str,
        *,
        client_instruction_id: str | None = None,
    ) -> InstructionAcceptedResponse:
        kind = classify_instruction(content)
        result = await self._repository.add_instruction(
            task_id,
            content,
            kind=kind,
            client_instruction_id=client_instruction_id,
            max_instructions=self._max_instructions_per_task,
        )

        if result.created:
            await self._coordinator.accept_instruction(
                task_id=task_id,
                new_version=result.task.generation_version,
                instruction_seq=result.task.instruction_seq,
                sequence_no=result.instruction.sequence_no,
                kind=result.instruction.kind,
                instruction_id=result.instruction.id,
            )

        # Use the persisted instruction's classification. A retry with the same
        # client id returns the original row even if a retry body was different.
        requires_new_task = result.instruction.kind is InstructionKind.NEW_TASK
        return InstructionAcceptedResponse(
            task_id=task_id,
            instruction_id=result.instruction.id,
            sequence_no=result.instruction.sequence_no,
            kind=result.instruction.kind,
            generation_version=result.task.generation_version,
            status=result.task.status,
            duplicate=not result.created,
            requires_new_task=requires_new_task,
            message=(
                "This instruction looks like a new task. The current task and "
                "its original prompt were preserved; create a separate task for "
                "a genuinely different request."
                if requires_new_task
                else None
            ),
        )

    async def stop_task(self, task_id: str) -> TaskStopResponse:
        async with self._task_lock(task_id):
            return await self._request_stop(task_id)

    async def _request_stop(self, task_id: str) -> TaskStopResponse:
        result = await self._repository.request_stop(task_id)
        if result.task.status is not TaskStatus.STOPPED:
            await self._coordinator.accept_stop(
                task_id=task_id,
                stop_version=result.task.generation_version,
                invalidated_version=result.invalidated_generation_version,
            )
        return TaskStopResponse(
            task_id=task_id,
            status=result.task.status,
            generation_version=result.task.generation_version,
            stop_requested=result.task.stop_requested,
            changed=result.changed,
        )

    async def get_snapshot(self, task_id: str) -> TaskStateResponse:
        records = await self._repository.get_task_state(task_id)
        if records is None:
            raise TaskNotFoundError(task_id)

        generation = records.generation
        previous = records.previous_generation
        working_draft = ""
        draft_version: int | None = None
        draft_status: GenerationStatus | None = None

        if generation is not None:
            if generation.status is not GenerationStatus.COMPLETED:
                working_draft = generation.draft_text
                draft_version = generation.version
                draft_status = generation.status
        elif previous is not None and (
            records.task.committed_answer is None
            or previous.status is not GenerationStatus.COMPLETED
        ):
            # A replacement is pending or a stop bumped the desired version and
            # no attempt exists for that version yet. The most recent useful
            # non-completed draft remains visible for debugging/recovery. A
            # completed previous generation is already represented by the
            # committed answer.
            working_draft = previous.draft_text
            draft_version = previous.version
            draft_status = previous.status

        return TaskStateResponse(
            task=TaskResponse.model_validate(records.task),
            instructions=[
                InstructionResponse.model_validate(instruction)
                for instruction in records.instructions
            ],
            generation=(
                GenerationResponse.model_validate(generation)
                if generation is not None
                else None
            ),
            working_draft=working_draft,
            working_draft_generation_version=draft_version,
            working_draft_generation_status=draft_status,
            events=[
                TaskEventResponse.model_validate(event) for event in records.events
            ],
        )
