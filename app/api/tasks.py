"""Thin task HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app.core.errors import (
    InstructionLimitError,
    TaskNotActiveError,
    TaskNotFoundError,
    TaskStoppedError,
)
from app.core.task_manager import TaskManager
from app.schemas.task import (
    InstructionAcceptedResponse,
    InstructionCreateRequest,
    TaskCreatedResponse,
    TaskCreateRequest,
    TaskStateResponse,
    TaskStopResponse,
    enforce_max_chars,
)

router = APIRouter(prefix="/api", tags=["tasks"])


@router.post(
    "/tasks",
    response_model=TaskCreatedResponse,
    status_code=202,
    summary="Create a task and start its first generation",
)
async def create_task(
    payload: TaskCreateRequest, request: Request
) -> TaskCreatedResponse:
    settings = request.app.state.settings
    try:
        enforce_max_chars(payload.prompt, settings.max_prompt_chars, "prompt")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    manager: TaskManager = request.app.state.task_manager
    return await manager.create_task(payload.prompt)


@router.get(
    "/tasks/{task_id}",
    response_model=TaskStateResponse,
    summary="Read the authoritative task snapshot",
)
async def get_task(task_id: str, request: Request) -> TaskStateResponse:
    manager: TaskManager = request.app.state.task_manager
    try:
        return await manager.get_snapshot(task_id)
    except TaskNotFoundError:
        raise HTTPException(status_code=404, detail="Task not found") from None


@router.post(
    "/tasks/{task_id}/instructions",
    response_model=InstructionAcceptedResponse,
    status_code=202,
    summary="Accept a live instruction and request safe replacement",
)
async def add_instruction(
    task_id: str,
    payload: InstructionCreateRequest,
    request: Request,
) -> InstructionAcceptedResponse:
    settings = request.app.state.settings
    try:
        enforce_max_chars(
            payload.content,
            settings.max_instruction_chars,
            "instruction",
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    manager: TaskManager = request.app.state.task_manager
    try:
        return await manager.add_instruction(
            task_id,
            payload.content,
            client_instruction_id=payload.client_instruction_id,
        )
    except TaskNotFoundError:
        raise HTTPException(status_code=404, detail="Task not found") from None
    except TaskStoppedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except InstructionLimitError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None


@router.post(
    "/tasks/{task_id}/stop",
    response_model=TaskStopResponse,
    status_code=202,
    summary="Stop generation and preserve the latest useful draft",
)
async def stop_task(task_id: str, request: Request) -> TaskStopResponse:
    manager: TaskManager = request.app.state.task_manager
    try:
        return await manager.stop_task(task_id)
    except TaskNotFoundError:
        raise HTTPException(status_code=404, detail="Task not found") from None
    except TaskNotActiveError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
