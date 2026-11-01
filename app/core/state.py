"""Lifecycle states and the only allowed transitions.

The database stores the status strings defined here. Route handlers and
orchestration code must use these enums instead of ad-hoc strings.

A transition from a state back to itself is *not* listed. Callers performing an
idempotent write (for example, refreshing ``updated_at``) should skip the
transition check when the target state equals the current state. Recovery on
startup is the one deliberate exception: it force-reconciles tasks that were
active when the process died (see ``Repository.recover_interrupted_tasks``).
"""

from __future__ import annotations

from enum import StrEnum

from app.core.errors import InvalidTransitionError


class TaskStatus(StrEnum):
    IDLE = "IDLE"
    GENERATING = "GENERATING"
    UPDATE_PENDING = "UPDATE_PENDING"
    CANCELLING = "CANCELLING"
    REVISING = "REVISING"
    COMPLETED = "COMPLETED"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"


class GenerationStatus(StrEnum):
    GENERATING = "GENERATING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    STALE = "STALE"
    INTERRUPTED = "INTERRUPTED"


class InstructionKind(StrEnum):
    """Deterministic classification; the original text is always preserved."""

    ADDITION = "ADDITION"
    MODIFY = "MODIFY"
    CORRECTION = "CORRECTION"
    RESTRICTION = "RESTRICTION"
    REFOCUS = "REFOCUS"
    NEW_TASK = "NEW_TASK"


ALLOWED_TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.IDLE: frozenset(
        {
            TaskStatus.GENERATING,
            TaskStatus.UPDATE_PENDING,
            TaskStatus.FAILED,
            TaskStatus.INTERRUPTED,
        }
    ),
    TaskStatus.GENERATING: frozenset(
        {
            TaskStatus.UPDATE_PENDING,
            TaskStatus.CANCELLING,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.INTERRUPTED,
        }
    ),
    TaskStatus.UPDATE_PENDING: frozenset(
        {
            TaskStatus.REVISING,
            TaskStatus.CANCELLING,
            TaskStatus.FAILED,
            TaskStatus.INTERRUPTED,
        }
    ),
    TaskStatus.CANCELLING: frozenset(
        {
            # A late (non-stop) instruction may arrive during cleanup.
            TaskStatus.UPDATE_PENDING,
            TaskStatus.REVISING,
            TaskStatus.STOPPED,
            TaskStatus.INTERRUPTED,
        }
    ),
    TaskStatus.REVISING: frozenset(
        {
            TaskStatus.UPDATE_PENDING,
            TaskStatus.CANCELLING,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.INTERRUPTED,
        }
    ),
    TaskStatus.COMPLETED: frozenset({TaskStatus.UPDATE_PENDING}),
    TaskStatus.FAILED: frozenset({TaskStatus.UPDATE_PENDING}),
    TaskStatus.STOPPED: frozenset(),
    TaskStatus.INTERRUPTED: frozenset({TaskStatus.UPDATE_PENDING}),
}

# Statuses that represent work that cannot survive a process restart.
ACTIVE_TASK_STATUSES: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.IDLE,
        TaskStatus.GENERATING,
        TaskStatus.UPDATE_PENDING,
        TaskStatus.CANCELLING,
        TaskStatus.REVISING,
    }
)

ALLOWED_GENERATION_TRANSITIONS: dict[
    GenerationStatus, frozenset[GenerationStatus]
] = {
    GenerationStatus.GENERATING: frozenset(
        {
            GenerationStatus.COMPLETED,
            GenerationStatus.CANCELLED,
            GenerationStatus.FAILED,
            GenerationStatus.STALE,
            GenerationStatus.INTERRUPTED,
        }
    ),
    GenerationStatus.COMPLETED: frozenset(),
    GenerationStatus.CANCELLED: frozenset(),
    GenerationStatus.FAILED: frozenset(),
    GenerationStatus.STALE: frozenset(),
    GenerationStatus.INTERRUPTED: frozenset(),
}

GENERATION_TERMINAL_STATUSES: frozenset[GenerationStatus] = frozenset(
    {
        GenerationStatus.COMPLETED,
        GenerationStatus.CANCELLED,
        GenerationStatus.FAILED,
        GenerationStatus.STALE,
        GenerationStatus.INTERRUPTED,
    }
)


def validate_task_transition(current: TaskStatus, target: TaskStatus) -> None:
    """Raise if ``current -> target`` is not an allowed lifecycle transition."""
    if target not in ALLOWED_TASK_TRANSITIONS[current]:
        raise InvalidTransitionError("task", current.value, target.value)


def validate_generation_transition(
    current: GenerationStatus, target: GenerationStatus
) -> None:
    """Raise if a generation would move to an invalid terminal state."""
    if target not in ALLOWED_GENERATION_TRANSITIONS[current]:
        raise InvalidTransitionError("generation", current.value, target.value)


def is_active_status(status: TaskStatus) -> bool:
    return status in ACTIVE_TASK_STATUSES
