"""The only place that executes SQL.

Routes and orchestration code call these methods; they never build SQL. Every
related change runs in one short transaction. Business rules that protect
durable state (fencing, immutable prompt, ordered instructions) are enforced
here rather than trusted to callers.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import replace
from typing import Any

import aiosqlite

from app.core.clock import utc_now_iso
from app.core.errors import (
    InstructionLimitError,
    StaleGenerationError,
    TaskNotActiveError,
    TaskNotFoundError,
    TaskStoppedError,
)
from app.core.event_types import TaskEventType
from app.core.state import (
    ACTIVE_TASK_STATUSES,
    GenerationStatus,
    InstructionKind,
    TaskStatus,
    validate_generation_transition,
    validate_task_transition,
)
from app.database.connection import Database
from app.database.records import (
    GenerationRecord,
    InstructionRecord,
    InstructionResult,
    StopResult,
    TaskEventRecord,
    TaskRecord,
    TaskStateRecords,
)

_TASK_COLUMNS = (
    "id, original_prompt, status, instruction_seq, generation_version, "
    "committed_answer, stop_requested, created_at, updated_at"
)
_INSTRUCTION_COLUMNS = (
    "id, task_id, sequence_no, content, kind, client_instruction_id, created_at"
)
_GENERATION_COLUMNS = (
    "task_id, version, base_instruction_seq, status, draft_text, error_code, "
    "error_message, started_at, completed_at, created_at"
)

_GENERATION_TERMINAL_EVENTS: dict[GenerationStatus, TaskEventType] = {
    GenerationStatus.CANCELLED: TaskEventType.GENERATION_CANCELLED,
    GenerationStatus.FAILED: TaskEventType.GENERATION_FAILED,
    GenerationStatus.STALE: TaskEventType.GENERATION_STALE,
    GenerationStatus.INTERRUPTED: TaskEventType.GENERATION_INTERRUPTED,
}

# Statuses in which a generation is allowed to commit its final answer.
_COMMITTABLE_TASK_STATUS_SQL = "('GENERATING', 'REVISING')"


class Repository:
    def __init__(self, database: Database) -> None:
        self._db = database

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------
    async def create_task(
        self,
        prompt: str,
        *,
        status: TaskStatus = TaskStatus.IDLE,
        task_id: str | None = None,
        now: str | None = None,
    ) -> TaskRecord:
        if not prompt.strip():
            raise ValueError("prompt must not be blank")

        new_id = task_id or str(uuid.uuid4())
        timestamp = now or utc_now_iso()
        record = TaskRecord(
            id=new_id,
            original_prompt=prompt,
            status=status,
            instruction_seq=0,
            # Version 1 is reserved for the first generation attempt. The
            # desired version is durable even before that attempt starts, so a
            # quick first instruction advances it to 2 and cannot collide.
            generation_version=1,
            committed_answer=None,
            stop_requested=status is TaskStatus.STOPPED,
            created_at=timestamp,
            updated_at=timestamp,
        )

        async with self._db.transaction() as conn:
            await self._insert_task_row(conn, record)
            await self._insert_event(
                conn,
                record.id,
                TaskEventType.TASK_CREATED,
                payload={"status": record.status.value},
                now=timestamp,
            )
        return record

    async def create_task_with_generation(
        self,
        prompt: str,
        *,
        task_id: str | None = None,
        now: str | None = None,
    ) -> tuple[TaskRecord, GenerationRecord]:
        """Create the task and its first generation in one transaction.

        This is the API's creation path. No request can observe a task that is
        missing its initial attempt, and version 1 is reserved consistently.
        """
        if not prompt.strip():
            raise ValueError("prompt must not be blank")

        new_id = task_id or str(uuid.uuid4())
        timestamp = now or utc_now_iso()
        task = TaskRecord(
            id=new_id,
            original_prompt=prompt,
            status=TaskStatus.GENERATING,
            instruction_seq=0,
            generation_version=1,
            committed_answer=None,
            stop_requested=False,
            created_at=timestamp,
            updated_at=timestamp,
        )
        generation = GenerationRecord(
            task_id=new_id,
            version=1,
            base_instruction_seq=0,
            status=GenerationStatus.GENERATING,
            draft_text="",
            error_code=None,
            error_message=None,
            started_at=timestamp,
            completed_at=None,
            created_at=timestamp,
        )

        async with self._db.transaction() as conn:
            await self._insert_task_row(conn, task)
            await self._insert_generation_row(conn, generation)
            await self._insert_event(
                conn,
                new_id,
                TaskEventType.TASK_CREATED,
                payload={"status": task.status.value},
                now=timestamp,
            )
            await self._insert_event(
                conn,
                new_id,
                TaskEventType.GENERATION_STARTED,
                generation_version=1,
                payload={"base_instruction_seq": 0},
                now=timestamp,
            )
        return task, generation

    async def get_task(self, task_id: str) -> TaskRecord | None:
        async with self._db.read() as conn:
            return await self._fetch_task(conn, task_id)

    async def get_task_state(
        self, task_id: str, *, event_limit: int = 50
    ) -> TaskStateRecords | None:
        """Read task, instructions, current generation, and recent events under
        one lock so a concurrent commit cannot produce a mixed snapshot."""
        if event_limit < 1:
            raise ValueError("event_limit must be positive")

        async with self._db.read() as conn:
            task = await self._fetch_task(conn, task_id)
            if task is None:
                return None
            instructions = await self._fetch_instructions(conn, task_id)
            generation = await self._fetch_generation(
                conn, task_id, task.generation_version
            )
            previous_generation = await self._fetch_previous_generation(
                conn, task_id, task.generation_version
            )

            cursor = await conn.execute(
                "SELECT id, task_id, generation_version, type, payload, created_at "
                "FROM task_events WHERE task_id = ? ORDER BY id DESC LIMIT ?",
                (task_id, event_limit),
            )
            event_rows = await cursor.fetchall()
            await cursor.close()

            return TaskStateRecords(
                task=task,
                instructions=tuple(instructions),
                generation=generation,
                events=tuple(
                    TaskEventRecord.from_row(row) for row in reversed(event_rows)
                ),
                previous_generation=previous_generation,
            )

    async def set_task_status(
        self,
        task_id: str,
        new_status: TaskStatus,
        *,
        payload: dict[str, Any] | None = None,
        now: str | None = None,
    ) -> TaskRecord:
        timestamp = now or utc_now_iso()
        async with self._db.transaction() as conn:
            task = await self._require_task(conn, task_id)
            if task.status is new_status:
                return task

            validate_task_transition(task.status, new_status)
            stop_requested = task.stop_requested or new_status is TaskStatus.STOPPED
            await conn.execute(
                "UPDATE tasks SET status = ?, stop_requested = ?, updated_at = ? "
                "WHERE id = ?",
                (new_status.value, int(stop_requested), timestamp, task_id),
            )
            event_payload = {
                "from": task.status.value,
                "to": new_status.value,
                **(payload or {}),
            }
            await self._insert_event(
                conn,
                task_id,
                TaskEventType.STATUS_CHANGED,
                payload=event_payload,
                now=timestamp,
            )
            return replace(
                task,
                status=new_status,
                stop_requested=stop_requested,
                updated_at=timestamp,
            )

    # ------------------------------------------------------------------
    # Instructions
    # ------------------------------------------------------------------
    async def add_instruction(
        self,
        task_id: str,
        content: str,
        *,
        kind: InstructionKind = InstructionKind.ADDITION,
        client_instruction_id: str | None = None,
        max_instructions: int | None = None,
        now: str | None = None,
    ) -> InstructionResult:
        """Persist an instruction, bump the fence version, and return promptly.

        The original text is stored verbatim. When ``client_instruction_id`` is
        supplied, a retry returns the existing row instead of creating a
        duplicate or bumping the version a second time.
        """
        if not content.strip():
            raise ValueError("instruction content must not be blank")
        if client_instruction_id is not None and not client_instruction_id.strip():
            raise ValueError("client_instruction_id must not be blank")

        timestamp = now or utc_now_iso()
        async with self._db.transaction() as conn:
            task = await self._require_task(conn, task_id)

            if client_instruction_id is not None:
                existing = await self._fetch_instruction_by_client_id(
                    conn, task_id, client_instruction_id
                )
                if existing is not None:
                    return InstructionResult(
                        instruction=existing, task=task, created=False
                    )

            if task.stop_requested or task.status is TaskStatus.STOPPED:
                raise TaskStoppedError(task_id)

            if (
                max_instructions is not None
                and task.instruction_seq >= max_instructions
            ):
                raise InstructionLimitError(task_id, max_instructions)

            if task.status is not TaskStatus.UPDATE_PENDING:
                validate_task_transition(task.status, TaskStatus.UPDATE_PENDING)

            sequence_no = task.instruction_seq + 1
            generation_version = task.generation_version + 1
            instruction = InstructionRecord(
                id=str(uuid.uuid4()),
                task_id=task_id,
                sequence_no=sequence_no,
                content=content,
                kind=InstructionKind(kind),
                client_instruction_id=client_instruction_id,
                created_at=timestamp,
            )

            await conn.execute(
                f"INSERT INTO instructions ({_INSTRUCTION_COLUMNS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    instruction.id,
                    instruction.task_id,
                    instruction.sequence_no,
                    instruction.content,
                    instruction.kind.value,
                    instruction.client_instruction_id,
                    instruction.created_at,
                ),
            )
            await conn.execute(
                "UPDATE tasks SET instruction_seq = ?, generation_version = ?, "
                "status = ?, updated_at = ? WHERE id = ?",
                (
                    sequence_no,
                    generation_version,
                    TaskStatus.UPDATE_PENDING.value,
                    timestamp,
                    task_id,
                ),
            )
            await self._insert_event(
                conn,
                task_id,
                TaskEventType.INSTRUCTION_ACCEPTED,
                generation_version=generation_version,
                payload={
                    "instruction_id": instruction.id,
                    "sequence_no": sequence_no,
                    "kind": instruction.kind.value,
                },
                now=timestamp,
            )

            updated_task = replace(
                task,
                instruction_seq=sequence_no,
                generation_version=generation_version,
                status=TaskStatus.UPDATE_PENDING,
                updated_at=timestamp,
            )
            return InstructionResult(
                instruction=instruction, task=updated_task, created=True
            )

    async def request_stop(
        self, task_id: str, *, now: str | None = None
    ) -> StopResult:
        """Persist manual-stop intent and invalidate the desired generation.

        The version bump is the database-level ordering rule between stop and
        instruction acceptance. Whichever transaction commits first wins:
        an instruction accepted first is invalidated by this bump; a stop
        committed first makes the later instruction transaction raise
        :class:`TaskStoppedError`.

        The task is deliberately left in ``CANCELLING`` until the active worker
        has cleaned up. Only :meth:`finalize_stop` may claim ``STOPPED``.
        """
        timestamp = now or utc_now_iso()
        async with self._db.transaction() as conn:
            task = await self._require_task(conn, task_id)

            # Repeated stop is idempotent: no new version bump and no new
            # cancellation work. A caller can still inspect the current state.
            if task.stop_requested or task.status is TaskStatus.STOPPED:
                return StopResult(
                    task=task,
                    invalidated_generation_version=None,
                    changed=False,
                )

            if task.status not in ACTIVE_TASK_STATUSES:
                raise TaskNotActiveError(task_id, task.status.value)

            invalidated_version = task.generation_version
            stop_version = invalidated_version + 1
            next_status = (
                TaskStatus.STOPPED
                if task.status is TaskStatus.IDLE
                else TaskStatus.CANCELLING
            )
            await conn.execute(
                "UPDATE tasks SET status = ?, generation_version = ?, "
                "stop_requested = 1, updated_at = ? "
                "WHERE id = ? AND generation_version = ? AND stop_requested = 0",
                (
                    next_status.value,
                    stop_version,
                    timestamp,
                    task_id,
                    invalidated_version,
                ),
            )
            await self._insert_event(
                conn,
                task_id,
                TaskEventType.STATUS_CHANGED,
                generation_version=invalidated_version,
                payload={
                    "from": task.status.value,
                    "to": next_status.value,
                    "reason": "manual_stop",
                    "invalidated_generation_version": invalidated_version,
                },
                now=timestamp,
            )
            if next_status is TaskStatus.STOPPED:
                await self._insert_event(
                    conn,
                    task_id,
                    TaskEventType.TASK_STOPPED,
                    generation_version=stop_version,
                    payload={"reason": "manual_stop"},
                    now=timestamp,
                )

            updated = replace(
                task,
                status=next_status,
                generation_version=stop_version,
                stop_requested=True,
                updated_at=timestamp,
            )
            return StopResult(
                task=updated,
                invalidated_generation_version=invalidated_version,
                changed=True,
            )

    async def finalize_stop(
        self,
        task_id: str,
        stop_version: int,
        *,
        now: str | None = None,
    ) -> TaskRecord:
        """Claim ``STOPPED`` only after worker and draft cleanup completed.

        The version predicate prevents an old stop-cleanup path from changing a
        task that somehow moved on. Instruction acceptance is already blocked by
        ``stop_requested``, so this transition is safe and idempotent.
        """
        timestamp = now or utc_now_iso()
        async with self._db.transaction() as conn:
            task = await self._require_task(conn, task_id)
            if task.status is TaskStatus.STOPPED:
                return task
            if not task.stop_requested or task.generation_version != stop_version:
                return task

            cursor = await conn.execute(
                "UPDATE tasks SET status = ?, updated_at = ? "
                "WHERE id = ? AND stop_requested = 1 "
                "AND generation_version = ? AND status != ?",
                (
                    TaskStatus.STOPPED.value,
                    timestamp,
                    task_id,
                    stop_version,
                    TaskStatus.STOPPED.value,
                ),
            )
            if cursor.rowcount == 1:
                await self._insert_event(
                    conn,
                    task_id,
                    TaskEventType.TASK_STOPPED,
                    generation_version=stop_version,
                    payload={"reason": "manual_stop"},
                    now=timestamp,
                )
                return replace(
                    task,
                    status=TaskStatus.STOPPED,
                    updated_at=timestamp,
                )
            return task

    async def list_instructions(self, task_id: str) -> list[InstructionRecord]:
        async with self._db.read() as conn:
            return await self._fetch_instructions(conn, task_id)

    # ------------------------------------------------------------------
    # Generations
    # ------------------------------------------------------------------
    async def create_generation(
        self,
        task_id: str,
        version: int,
        *,
        base_instruction_seq: int,
        now: str | None = None,
    ) -> GenerationRecord:
        """Create a generation attempt for the task's current desired version.

        Task creation reserves version 1. Accepting an instruction advances the
        desired version, so a replacement uses the new value. A version that
        does not match the task's desired version is stale before it starts and
        is rejected.
        """
        if version < 1:
            raise ValueError("generation version must be positive")

        timestamp = now or utc_now_iso()
        async with self._db.transaction() as conn:
            task = await self._require_task(conn, task_id)
            if version != task.generation_version:
                raise ValueError(
                    f"generation version {version} does not match desired "
                    f"version {task.generation_version}"
                )
            if base_instruction_seq < 0 or base_instruction_seq > task.instruction_seq:
                raise ValueError(
                    "base_instruction_seq must be between 0 and the latest "
                    "accepted instruction"
                )

            record = GenerationRecord(
                task_id=task_id,
                version=version,
                base_instruction_seq=base_instruction_seq,
                status=GenerationStatus.GENERATING,
                draft_text="",
                error_code=None,
                error_message=None,
                started_at=timestamp,
                completed_at=None,
                created_at=timestamp,
            )
            await conn.execute(
                f"INSERT INTO generations ({_GENERATION_COLUMNS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.task_id,
                    record.version,
                    record.base_instruction_seq,
                    record.status.value,
                    record.draft_text,
                    record.error_code,
                    record.error_message,
                    record.started_at,
                    record.completed_at,
                    record.created_at,
                ),
            )
            await self._insert_event(
                conn,
                task_id,
                TaskEventType.GENERATION_STARTED,
                generation_version=version,
                payload={"base_instruction_seq": base_instruction_seq},
                now=timestamp,
            )
            return record

    async def create_revision_generation(
        self,
        task_id: str,
        version: int,
        *,
        base_instruction_seq: int,
        now: str | None = None,
    ) -> GenerationRecord:
        """Atomically start one replacement attempt for the current version.

        This is stronger than :meth:`create_generation`: it requires the
        requested base instruction sequence to equal the latest accepted
        sequence and moves the task to ``REVISING`` in the same transaction.
        A delayed restart that read an older sequence therefore cannot start a
        generation using an outdated instruction set.
        """
        if version < 1:
            raise ValueError("generation version must be positive")

        timestamp = now or utc_now_iso()
        async with self._db.transaction() as conn:
            task = await self._require_task(conn, task_id)
            if task.stop_requested or task.status is TaskStatus.STOPPED:
                raise TaskStoppedError(task_id)
            if task.generation_version != version:
                raise StaleGenerationError(task_id, version)
            if base_instruction_seq != task.instruction_seq:
                raise ValueError(
                    "base_instruction_seq must match the latest accepted "
                    "instruction sequence"
                )

            existing = await self._fetch_generation(conn, task_id, version)
            if existing is not None:
                if existing.status is not GenerationStatus.GENERATING:
                    raise ValueError(
                        f"generation {version} for task {task_id} is already terminal"
                    )
                return existing

            if task.status is not TaskStatus.REVISING:
                validate_task_transition(task.status, TaskStatus.REVISING)
                cursor = await conn.execute(
                    "UPDATE tasks SET status = ?, updated_at = ? "
                    "WHERE id = ? AND generation_version = ? "
                    "AND instruction_seq = ? AND stop_requested = 0",
                    (
                        TaskStatus.REVISING.value,
                        timestamp,
                        task_id,
                        version,
                        base_instruction_seq,
                    ),
                )
                if cursor.rowcount != 1:
                    raise StaleGenerationError(task_id, version)

            record = GenerationRecord(
                task_id=task_id,
                version=version,
                base_instruction_seq=base_instruction_seq,
                status=GenerationStatus.GENERATING,
                draft_text="",
                error_code=None,
                error_message=None,
                started_at=timestamp,
                completed_at=None,
                created_at=timestamp,
            )
            await self._insert_generation_row(conn, record)
            await self._insert_event(
                conn,
                task_id,
                TaskEventType.GENERATION_STARTED,
                generation_version=version,
                payload={"base_instruction_seq": base_instruction_seq},
                now=timestamp,
            )
            return record

    async def get_generation(
        self, task_id: str, version: int
    ) -> GenerationRecord | None:
        async with self._db.read() as conn:
            return await self._fetch_generation(conn, task_id, version)

    async def get_current_generation(
        self, task_id: str
    ) -> GenerationRecord | None:
        async with self._db.read() as conn:
            cursor = await conn.execute(
                f"SELECT {_GENERATION_COLUMNS} FROM generations "
                "WHERE task_id = ? ORDER BY version DESC LIMIT 1",
                (task_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            return GenerationRecord.from_row(row) if row else None

    async def list_generations(self, task_id: str) -> list[GenerationRecord]:
        async with self._db.read() as conn:
            cursor = await conn.execute(
                f"SELECT {_GENERATION_COLUMNS} FROM generations "
                "WHERE task_id = ? ORDER BY version ASC",
                (task_id,),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [GenerationRecord.from_row(row) for row in rows]

    async def save_generation_draft(
        self, task_id: str, version: int, draft_text: str
    ) -> bool:
        """Checkpoint a draft while its generation is still active and current.

        Periodic checkpoints are rejected as soon as a new instruction advances
        the desired version. If a worker is cancelled, its final partial draft
        is persisted through :meth:`finish_generation`, which is the explicit
        cleanup path and commits the draft together with the terminal status.
        Neither path can touch the task's committed answer.
        """
        async with self._db.transaction() as conn:
            cursor = await conn.execute(
                "UPDATE generations SET draft_text = ? "
                "WHERE task_id = ? AND version = ? AND status = ? "
                "AND version = (SELECT generation_version FROM tasks WHERE id = ?)",
                (
                    draft_text,
                    task_id,
                    version,
                    GenerationStatus.GENERATING.value,
                    task_id,
                ),
            )
            return cursor.rowcount == 1

    async def finish_generation(
        self,
        task_id: str,
        version: int,
        status: GenerationStatus,
        *,
        draft_text: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        task_status: TaskStatus | None = None,
        now: str | None = None,
    ) -> bool:
        """Atomically persist the final draft (if any) and a terminal status.

        ``COMPLETED`` is not accepted here; a successful answer must go through
        the fenced :meth:`commit_generation`. This method never changes the
        task's committed answer, so a failed or cancelled revision cannot erase
        previous success.

        When ``task_status`` is supplied and this generation is still current,
        the task transition and its event are committed in the *same*
        transaction as the generation terminal status. A snapshot can therefore
        never observe a terminal generation attached to a current task that
        still claims to be generating. A stale generation records only its own
        terminal row.
        """
        if status not in _GENERATION_TERMINAL_EVENTS:
            raise ValueError(
                "finish_generation only accepts CANCELLED, FAILED, STALE, "
                "or INTERRUPTED"
            )

        timestamp = now or utc_now_iso()
        async with self._db.transaction() as conn:
            generation = await self._fetch_generation(conn, task_id, version)
            if generation is None:
                raise ValueError(
                    f"generation {version} for task {task_id} does not exist"
                )
            if generation.status is not GenerationStatus.GENERATING:
                return False

            validate_generation_transition(generation.status, status)
            final_draft = (
                generation.draft_text if draft_text is None else draft_text
            )
            await conn.execute(
                "UPDATE generations SET status = ?, draft_text = ?, "
                "error_code = ?, error_message = ?, completed_at = ? "
                "WHERE task_id = ? AND version = ?",
                (
                    status.value,
                    final_draft,
                    error_code,
                    error_message,
                    timestamp,
                    task_id,
                    version,
                ),
            )
            await self._insert_event(
                conn,
                task_id,
                _GENERATION_TERMINAL_EVENTS[status],
                generation_version=version,
                payload={
                    key: value
                    for key, value in {
                        "status": status.value,
                        "error_code": error_code,
                        "error_message": error_message,
                    }.items()
                    if value is not None
                },
                now=timestamp,
            )

            if task_status is not None:
                task = await self._require_task(conn, task_id)
                # Only the current attempt may change the task's terminal
                # status. A stale worker may still record its own generation
                # outcome, but it must never clobber newer task state.
                if (
                    task.generation_version == version
                    and task.status is not task_status
                ):
                    validate_task_transition(task.status, task_status)
                    stop_requested = (
                        task.stop_requested or task_status is TaskStatus.STOPPED
                    )
                    await conn.execute(
                        "UPDATE tasks SET status = ?, stop_requested = ?, "
                        "updated_at = ? WHERE id = ? AND generation_version = ?",
                        (
                            task_status.value,
                            int(stop_requested),
                            timestamp,
                            task_id,
                            version,
                        ),
                    )
                    await self._insert_event(
                        conn,
                        task_id,
                        TaskEventType.STATUS_CHANGED,
                        generation_version=version,
                        payload={
                            "from": task.status.value,
                            "to": task_status.value,
                        },
                        now=timestamp,
                    )
            return True

    async def commit_generation(
        self, task_id: str, version: int, answer: str, *, now: str | None = None
    ) -> bool:
        """Commit a completed answer only if its version is still current.

        The SQL predicates re-check the fence inside the transaction. Even if a
        cancelled worker delivers a late completion, it cannot overwrite a newer
        task state. Returns False for a stale attempt; a partially applied
        commit raises and rolls back.
        """
        if not answer.strip():
            raise ValueError("committed answer must not be empty")

        timestamp = now or utc_now_iso()
        async with self._db.transaction() as conn:
            task = await self._require_task(conn, task_id)
            if task.generation_version != version:
                return False

            generation = await self._fetch_generation(conn, task_id, version)
            if generation is None:
                raise ValueError(
                    f"generation {version} for task {task_id} does not exist"
                )
            if generation.status is not GenerationStatus.GENERATING:
                return False

            cursor = await conn.execute(
                "UPDATE generations SET status = ?, draft_text = ?, "
                "error_code = NULL, error_message = NULL, completed_at = ? "
                "WHERE task_id = ? AND version = ? AND status = ?",
                (
                    GenerationStatus.COMPLETED.value,
                    answer,
                    timestamp,
                    task_id,
                    version,
                    GenerationStatus.GENERATING.value,
                ),
            )
            if cursor.rowcount != 1:
                return False

            cursor = await conn.execute(
                "UPDATE tasks SET committed_answer = ?, status = ?, updated_at = ? "
                f"WHERE id = ? AND generation_version = ? "
                f"AND status IN {_COMMITTABLE_TASK_STATUS_SQL}",
                (
                    answer,
                    TaskStatus.COMPLETED.value,
                    timestamp,
                    task_id,
                    version,
                ),
            )
            if cursor.rowcount != 1:
                # The task moved on between the two statements. Roll back so
                # the generation row is not left falsely completed.
                raise StaleGenerationError(task_id, version)

            await self._insert_event(
                conn,
                task_id,
                TaskEventType.GENERATION_COMPLETED,
                generation_version=version,
                payload={"answer_chars": len(answer)},
                now=timestamp,
            )
            return True

    # ------------------------------------------------------------------
    # Events and history
    # ------------------------------------------------------------------
    async def add_event(
        self,
        task_id: str,
        event_type: TaskEventType | str,
        *,
        generation_version: int | None = None,
        payload: dict[str, Any] | None = None,
        now: str | None = None,
    ) -> None:
        async with self._db.transaction() as conn:
            await self._require_task(conn, task_id)
            await self._insert_event(
                conn,
                task_id,
                event_type,
                generation_version=generation_version,
                payload=payload,
                now=now,
            )

    async def list_events(
        self, task_id: str, *, limit: int = 200
    ) -> list[TaskEventRecord]:
        if limit < 1:
            raise ValueError("limit must be positive")
        async with self._db.read() as conn:
            cursor = await conn.execute(
                "SELECT id, task_id, generation_version, type, payload, created_at "
                "FROM task_events WHERE task_id = ? ORDER BY id DESC LIMIT ?",
                (task_id, limit),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            events = [TaskEventRecord.from_row(row) for row in rows]
            events.reverse()
            return events

    # ------------------------------------------------------------------
    # Startup recovery
    # ------------------------------------------------------------------
    async def recover_interrupted_tasks(
        self, *, now: str | None = None
    ) -> list[str]:
        """Reconcile tasks that were active when the previous process died.

        The in-memory runtime did not survive the restart, so pretending the
        task is still generating would be false. Tasks with an explicit stop
        request become ``STOPPED`` (manual stop is terminal). Everything else
        becomes ``INTERRUPTED``, which can be resumed only by submitting a new
        instruction. Any draft checkpoint is kept.

        This is the deliberate exception to normal transition validation; it
        reconciles durable state at startup.
        """
        timestamp = now or utc_now_iso()
        active_values = tuple(status.value for status in ACTIVE_TASK_STATUSES)
        placeholders = ", ".join("?" for _ in active_values)

        async with self._db.transaction() as conn:
            cursor = await conn.execute(
                "SELECT id, status, stop_requested FROM tasks "
                f"WHERE status IN ({placeholders}) ORDER BY created_at ASC",
                active_values,
            )
            rows = await cursor.fetchall()
            await cursor.close()
            if not rows:
                return []

            recovered: list[str] = []
            for row in rows:
                task_id = str(row["id"])
                was_stopped = bool(row["stop_requested"])
                task_status = (
                    TaskStatus.STOPPED if was_stopped else TaskStatus.INTERRUPTED
                )
                generation_status = (
                    GenerationStatus.CANCELLED
                    if was_stopped
                    else GenerationStatus.INTERRUPTED
                )
                await conn.execute(
                    "UPDATE generations SET status = ?, error_code = ?, "
                    "error_message = ?, completed_at = ? "
                    "WHERE task_id = ? AND status = ?",
                    (
                        generation_status.value,
                        "server_restart",
                        "The server restarted before this generation finished.",
                        timestamp,
                        task_id,
                        GenerationStatus.GENERATING.value,
                    ),
                )
                await conn.execute(
                    "UPDATE tasks SET status = ?, stop_requested = ?, updated_at = ? "
                    "WHERE id = ?",
                    (
                        task_status.value,
                        int(was_stopped),
                        timestamp,
                        task_id,
                    ),
                )
                await self._insert_event(
                    conn,
                    task_id,
                    (
                        TaskEventType.TASK_STOPPED
                        if was_stopped
                        else TaskEventType.TASK_INTERRUPTED
                    ),
                    payload={"previous_status": str(row["status"])},
                    now=timestamp,
                )
                recovered.append(task_id)
            return recovered

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _require_task(
        self, conn: aiosqlite.Connection, task_id: str
    ) -> TaskRecord:
        task = await self._fetch_task(conn, task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        return task

    async def _fetch_task(
        self, conn: aiosqlite.Connection, task_id: str
    ) -> TaskRecord | None:
        cursor = await conn.execute(
            f"SELECT {_TASK_COLUMNS} FROM tasks WHERE id = ?", (task_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return TaskRecord.from_row(row) if row else None

    async def _fetch_instructions(
        self, conn: aiosqlite.Connection, task_id: str
    ) -> list[InstructionRecord]:
        cursor = await conn.execute(
            f"SELECT {_INSTRUCTION_COLUMNS} FROM instructions "
            "WHERE task_id = ? ORDER BY sequence_no ASC",
            (task_id,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [InstructionRecord.from_row(row) for row in rows]

    async def _fetch_instruction_by_client_id(
        self,
        conn: aiosqlite.Connection,
        task_id: str,
        client_instruction_id: str,
    ) -> InstructionRecord | None:
        cursor = await conn.execute(
            f"SELECT {_INSTRUCTION_COLUMNS} FROM instructions "
            "WHERE task_id = ? AND client_instruction_id = ?",
            (task_id, client_instruction_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return InstructionRecord.from_row(row) if row else None

    async def _fetch_generation(
        self,
        conn: aiosqlite.Connection,
        task_id: str,
        version: int,
    ) -> GenerationRecord | None:
        cursor = await conn.execute(
            f"SELECT {_GENERATION_COLUMNS} FROM generations "
            "WHERE task_id = ? AND version = ?",
            (task_id, version),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return GenerationRecord.from_row(row) if row else None

    async def _fetch_previous_generation(
        self,
        conn: aiosqlite.Connection,
        task_id: str,
        current_version: int,
    ) -> GenerationRecord | None:
        cursor = await conn.execute(
            f"SELECT {_GENERATION_COLUMNS} FROM generations "
            "WHERE task_id = ? AND version < ? "
            "ORDER BY version DESC LIMIT 1",
            (task_id, current_version),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return GenerationRecord.from_row(row) if row else None

    async def _insert_task_row(
        self, conn: aiosqlite.Connection, task: TaskRecord
    ) -> None:
        await conn.execute(
            f"INSERT INTO tasks ({_TASK_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task.id,
                task.original_prompt,
                task.status.value,
                task.instruction_seq,
                task.generation_version,
                task.committed_answer,
                int(task.stop_requested),
                task.created_at,
                task.updated_at,
            ),
        )

    async def _insert_generation_row(
        self, conn: aiosqlite.Connection, generation: GenerationRecord
    ) -> None:
        await conn.execute(
            f"INSERT INTO generations ({_GENERATION_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                generation.task_id,
                generation.version,
                generation.base_instruction_seq,
                generation.status.value,
                generation.draft_text,
                generation.error_code,
                generation.error_message,
                generation.started_at,
                generation.completed_at,
                generation.created_at,
            ),
        )

    async def _insert_event(
        self,
        conn: aiosqlite.Connection,
        task_id: str,
        event_type: TaskEventType | str,
        *,
        generation_version: int | None = None,
        payload: dict[str, Any] | None = None,
        now: str | None = None,
    ) -> None:
        event_name = (
            event_type.value
            if isinstance(event_type, TaskEventType)
            else str(event_type)
        )
        await conn.execute(
            "INSERT INTO task_events "
            "(task_id, generation_version, type, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                task_id,
                generation_version,
                event_name,
                json.dumps(payload or {}, separators=(",", ":"), sort_keys=True),
                now or utc_now_iso(),
            ),
        )
