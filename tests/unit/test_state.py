"""Lifecycle state machine tests (no I/O)."""

from __future__ import annotations

import pytest

from app.core.errors import InvalidTransitionError
from app.core.state import (
    ACTIVE_TASK_STATUSES,
    ALLOWED_GENERATION_TRANSITIONS,
    ALLOWED_TASK_TRANSITIONS,
    GENERATION_TERMINAL_STATUSES,
    GenerationStatus,
    TaskStatus,
    is_active_status,
    validate_generation_transition,
    validate_task_transition,
)


def test_every_task_status_has_explicit_transition_entry() -> None:
    assert set(ALLOWED_TASK_TRANSITIONS) == set(TaskStatus)
    assert set(ALLOWED_GENERATION_TRANSITIONS) == set(GenerationStatus)


def test_active_statuses_are_the_non_terminal_working_states() -> None:
    assert ACTIVE_TASK_STATUSES == {
        TaskStatus.IDLE,
        TaskStatus.GENERATING,
        TaskStatus.UPDATE_PENDING,
        TaskStatus.CANCELLING,
        TaskStatus.REVISING,
    }
    assert not is_active_status(TaskStatus.COMPLETED)
    assert not is_active_status(TaskStatus.STOPPED)


def test_stopped_is_terminal() -> None:
    assert ALLOWED_TASK_TRANSITIONS[TaskStatus.STOPPED] == frozenset()


def test_generation_terminal_statuses_have_no_outgoing_transitions() -> None:
    for status in GENERATION_TERMINAL_STATUSES:
        assert ALLOWED_GENERATION_TRANSITIONS[status] == frozenset()


def test_all_declared_task_transitions_are_accepted() -> None:
    for current, targets in ALLOWED_TASK_TRANSITIONS.items():
        for target in targets:
            validate_task_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (TaskStatus.STOPPED, TaskStatus.GENERATING),
        (TaskStatus.STOPPED, TaskStatus.UPDATE_PENDING),
        (TaskStatus.COMPLETED, TaskStatus.GENERATING),
        (TaskStatus.COMPLETED, TaskStatus.FAILED),
        (TaskStatus.FAILED, TaskStatus.COMPLETED),
        (TaskStatus.GENERATING, TaskStatus.REVISING),
        (TaskStatus.CANCELLING, TaskStatus.COMPLETED),
        (TaskStatus.IDLE, TaskStatus.COMPLETED),
        (TaskStatus.UPDATE_PENDING, TaskStatus.GENERATING),
    ],
)
def test_invalid_task_transitions_are_rejected(
    current: TaskStatus, target: TaskStatus
) -> None:
    with pytest.raises(InvalidTransitionError):
        validate_task_transition(current, target)


def test_same_state_is_not_a_transition() -> None:
    with pytest.raises(InvalidTransitionError):
        validate_task_transition(TaskStatus.GENERATING, TaskStatus.GENERATING)


def test_generation_must_finish_from_generating() -> None:
    validate_generation_transition(
        GenerationStatus.GENERATING, GenerationStatus.COMPLETED
    )
    with pytest.raises(InvalidTransitionError):
        validate_generation_transition(
            GenerationStatus.COMPLETED, GenerationStatus.FAILED
        )
