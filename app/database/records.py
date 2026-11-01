"""Immutable database row representations used by the repository layer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from app.core.state import (
    GenerationStatus,
    InstructionKind,
    TaskStatus,
)


@dataclass(frozen=True, slots=True)
class TaskRecord:
    id: str
    original_prompt: str
    status: TaskStatus
    instruction_seq: int
    generation_version: int
    committed_answer: str | None
    stop_requested: bool
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "TaskRecord":
        return cls(
            id=str(row["id"]),
            original_prompt=str(row["original_prompt"]),
            status=TaskStatus(str(row["status"])),
            instruction_seq=int(row["instruction_seq"]),
            generation_version=int(row["generation_version"]),
            committed_answer=(
                str(row["committed_answer"])
                if row["committed_answer"] is not None
                else None
            ),
            stop_requested=bool(row["stop_requested"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )


@dataclass(frozen=True, slots=True)
class InstructionRecord:
    id: str
    task_id: str
    sequence_no: int
    content: str
    kind: InstructionKind
    client_instruction_id: str | None
    created_at: str

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "InstructionRecord":
        return cls(
            id=str(row["id"]),
            task_id=str(row["task_id"]),
            sequence_no=int(row["sequence_no"]),
            content=str(row["content"]),
            kind=InstructionKind(str(row["kind"])),
            client_instruction_id=(
                str(row["client_instruction_id"])
                if row["client_instruction_id"] is not None
                else None
            ),
            created_at=str(row["created_at"]),
        )


@dataclass(frozen=True, slots=True)
class InstructionResult:
    """Outcome of accepting an instruction.

    ``created`` is False when the request was an idempotent retry and the
    existing row was returned instead of inserting a duplicate.
    """

    instruction: InstructionRecord
    task: TaskRecord
    created: bool


@dataclass(frozen=True, slots=True)
class StopResult:
    """Outcome of persisting a manual stop request."""

    task: TaskRecord
    invalidated_generation_version: int | None
    changed: bool


@dataclass(frozen=True, slots=True)
class GenerationRecord:
    task_id: str
    version: int
    base_instruction_seq: int
    status: GenerationStatus
    draft_text: str
    error_code: str | None
    error_message: str | None
    started_at: str | None
    completed_at: str | None
    created_at: str

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "GenerationRecord":
        def optional_text(value: Any) -> str | None:
            return str(value) if value is not None else None

        return cls(
            task_id=str(row["task_id"]),
            version=int(row["version"]),
            base_instruction_seq=int(row["base_instruction_seq"]),
            status=GenerationStatus(str(row["status"])),
            draft_text=str(row["draft_text"]),
            error_code=optional_text(row["error_code"]),
            error_message=optional_text(row["error_message"]),
            started_at=optional_text(row["started_at"]),
            completed_at=optional_text(row["completed_at"]),
            created_at=str(row["created_at"]),
        )


@dataclass(frozen=True, slots=True)
class TaskEventRecord:
    id: int
    task_id: str
    generation_version: int | None
    type: str
    payload: dict[str, Any]
    created_at: str

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "TaskEventRecord":
        return cls(
            id=int(row["id"]),
            task_id=str(row["task_id"]),
            generation_version=(
                int(row["generation_version"])
                if row["generation_version"] is not None
                else None
            ),
            type=str(row["type"]),
            payload=json.loads(str(row["payload"])),
            created_at=str(row["created_at"]),
        )


@dataclass(frozen=True, slots=True)
class TaskStateRecords:
    """A consistent, single-lock read of everything a snapshot needs.

    ``generation`` is the attempt for the task's current desired version (it may
    be absent while a replacement is pending). ``previous_generation`` is the
    latest attempt below that version, whose draft is useful reference material
    for reconstruction and for showing a truthful visible draft.
    """

    task: TaskRecord
    instructions: tuple[InstructionRecord, ...]
    generation: GenerationRecord | None
    events: tuple[TaskEventRecord, ...]
    previous_generation: GenerationRecord | None = None

