"""Names of durable task events shown in the event timeline.

These are deliberately coarse-grained: tokens are streamed but never persisted
as events. Each event corresponds to a meaningful state change.
"""

from __future__ import annotations

from enum import StrEnum


class TaskEventType(StrEnum):
    TASK_CREATED = "task_created"
    INSTRUCTION_ACCEPTED = "instruction_accepted"
    STATUS_CHANGED = "status_changed"
    GENERATION_STARTED = "generation_started"
    GENERATION_COMPLETED = "generation_completed"
    GENERATION_FAILED = "generation_failed"
    GENERATION_CANCELLED = "generation_cancelled"
    GENERATION_STALE = "generation_stale"
    GENERATION_INTERRUPTED = "generation_interrupted"
    TASK_STOPPED = "task_stopped"
    TASK_INTERRUPTED = "task_interrupted"


class StreamEventType(StrEnum):
    """SSE event names sent to connected browsers.

    These are live events, not durable history. A reconnecting client gets a
    fresh snapshot and never expects token replay.
    """

    SNAPSHOT = "snapshot"
    STATUS = "status"
    INSTRUCTION_ACCEPTED = "instruction_accepted"
    GENERATION_INVALIDATED = "generation_invalidated"
    GENERATION_STARTED = "generation_started"
    GENERATION_RESTARTED = "generation_restarted"
    CONTEXT_UPDATED = "context_updated"
    DRAFT_PRESERVED = "draft_preserved"
    TOKEN_DELTA = "token_delta"
    CHECKPOINT = "checkpoint"
    GENERATION_COMPLETED = "generation_completed"
    GENERATION_FAILED = "generation_failed"
    GENERATION_CANCELLED = "generation_cancelled"
    TASK_STOPPED = "task_stopped"
    RESYNC_REQUIRED = "resync_required"
    ERROR = "error"
