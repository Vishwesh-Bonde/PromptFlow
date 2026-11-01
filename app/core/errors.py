"""Domain exceptions with safe, user-facing messages."""

from __future__ import annotations


class PromptFlowError(Exception):
    """Base class for expected application errors."""


class TaskNotFoundError(PromptFlowError):
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(f"Task {task_id} was not found")


class TaskStoppedError(PromptFlowError):
    """Raised when a stopped task is asked to accept a new instruction."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__("This task was stopped and cannot accept further instructions")


class TaskNotActiveError(PromptFlowError):
    """Raised when manual stop is requested for a terminal task."""

    def __init__(self, task_id: str, status: str) -> None:
        self.task_id = task_id
        self.status = status
        super().__init__(f"Task {task_id} is already {status} and cannot be stopped")


class InstructionLimitError(PromptFlowError):
    """Raised when a task has reached its bounded instruction allowance."""

    def __init__(self, task_id: str, limit: int) -> None:
        self.task_id = task_id
        self.limit = limit
        super().__init__(
            f"Task {task_id} already has the maximum of {limit} instructions"
        )


class InvalidTransitionError(PromptFlowError):
    def __init__(self, subject: str, current: str, target: str) -> None:
        self.subject = subject
        self.current = current
        self.target = target
        super().__init__(f"Invalid {subject} transition: {current} -> {target}")


class StaleGenerationError(PromptFlowError):
    """Raised when a generation tries to commit after being invalidated."""

    def __init__(self, task_id: str, version: int) -> None:
        self.task_id = task_id
        self.version = version
        super().__init__(
            f"Generation {version} for task {task_id} is no longer current"
        )


class MigrationError(PromptFlowError):
    """Raised when schema migrations cannot be applied safely."""
