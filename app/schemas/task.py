"""Pydantic request/response models shared by the API layer.

Configured size limits are applied by the API layer with
:func:`enforce_max_chars`, which returns HTTP 422 rather than silently
truncating user input. The hard ``max_length`` values here only protect the
process from absurdly large payloads.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.core.state import GenerationStatus, InstructionKind, TaskStatus

HARD_MAX_TEXT_CHARS = 100_000
HARD_MAX_CLIENT_ID_CHARS = 200


class TaskCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt: str = Field(min_length=1, max_length=HARD_MAX_TEXT_CHARS)

    @field_validator("prompt")
    @classmethod
    def _prompt_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("prompt must not be blank")
        return value


class InstructionCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content: str = Field(min_length=1, max_length=HARD_MAX_TEXT_CHARS)
    client_instruction_id: str | None = Field(
        default=None, max_length=HARD_MAX_CLIENT_ID_CHARS
    )

    @field_validator("content")
    @classmethod
    def _content_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("instruction content must not be blank")
        return value

    @field_validator("client_instruction_id")
    @classmethod
    def _client_id_must_not_be_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("client_instruction_id must not be blank")
        return value


class HealthResponse(BaseModel):
    status: Literal["ok"]
    database: Literal["ok"]
    provider: Literal["mock", "openai"]
    version: str


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    original_prompt: str
    status: TaskStatus
    instruction_seq: int
    generation_version: int
    committed_answer: str | None
    stop_requested: bool
    created_at: str
    updated_at: str


class InstructionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    task_id: str
    sequence_no: int
    content: str
    kind: InstructionKind
    client_instruction_id: str | None
    created_at: str


class GenerationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    task_id: str
    version: int
    base_instruction_seq: int
    status: GenerationStatus
    error_code: str | None
    error_message: str | None
    started_at: str | None
    completed_at: str | None
    created_at: str


class TaskEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    task_id: str
    generation_version: int | None
    type: str
    payload: dict[str, object]
    created_at: str


class TaskStateResponse(BaseModel):
    """Authoritative state returned by GET and by the first SSE frame."""

    task: TaskResponse
    instructions: list[InstructionResponse]
    generation: GenerationResponse | None
    working_draft: str
    working_draft_generation_version: int | None = None
    working_draft_generation_status: GenerationStatus | None = None
    events: list[TaskEventResponse]


class TaskCreatedResponse(BaseModel):
    task_id: str
    status: TaskStatus
    generation_version: int


class InstructionAcceptedResponse(BaseModel):
    """Acknowledgement returned after the instruction transaction commits."""

    task_id: str
    instruction_id: str
    sequence_no: int
    kind: InstructionKind
    generation_version: int
    status: TaskStatus
    duplicate: bool = False
    requires_new_task: bool = False
    message: str | None = None


class TaskStopResponse(BaseModel):
    task_id: str
    status: TaskStatus
    generation_version: int
    stop_requested: bool
    changed: bool


def enforce_max_chars(value: str, limit: int, field_name: str) -> str:
    """Return ``value`` if it fits; raise a safe size error otherwise.

    The error message contains character counts but never echoes user text.
    """
    length = len(value)
    if length > limit:
        raise ValueError(
            f"{field_name} must be at most {limit} characters (got {length})"
        )
    return value
