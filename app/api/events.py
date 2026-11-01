"""SSE endpoint and frame generator.

Snapshot ordering is deliberate:

1. subscribe (so no event can be lost between snapshot and subscription);
2. capture ``latest_seq``;
3. read a consistent task snapshot from SQLite;
4. send the snapshot;
5. forward queued events newer than the captured sequence.

Durable events are published only after their database transaction commits, so
anything at or before the captured sequence is reflected in the snapshot. Token
deltas are live-only; a token already covered by the snapshot's checked-in draft
is dropped to avoid duplicated text.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.core.errors import TaskNotFoundError
from app.core.event_bus import EventBus, Subscription, TaskStreamEvent, format_sse
from app.core.event_types import StreamEventType
from app.schemas.task import TaskStateResponse

if TYPE_CHECKING:
    from app.core.task_manager import TaskManager

router = APIRouter(prefix="/api", tags=["tasks"])

SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


@dataclass(slots=True)
class EventStream:
    """A prepared SSE subscription plus its authoritative starting snapshot."""

    bus: EventBus
    subscription: Subscription
    snapshot: TaskStateResponse
    snapshot_seq: int

    async def frames(self, *, heartbeat_seconds: float) -> AsyncIterator[str]:
        """Yield SSE frames until the client disconnects.

        The ``finally`` block unregisters this subscriber. It never touches the
        generation worker: closing a browser connection is not a cancellation
        request.
        """
        try:
            generation_version = (
                self.snapshot.generation.version
                if self.snapshot.generation is not None
                else None
            )
            snapshot_event = TaskStreamEvent(
                seq=self.snapshot_seq,
                type=StreamEventType.SNAPSHOT,
                task_id=self.snapshot.task.id,
                generation_version=generation_version,
                data=self.snapshot.model_dump(mode="json"),
            )
            yield format_sse(snapshot_event)

            # Forward events that arrived while the snapshot was read. Older
            # events are represented by the snapshot itself. A token whose
            # offset is already covered by the snapshot draft is redundant: the
            # checkpoint may have been read after the delta was published.
            snapshot_draft_chars = len(self.snapshot.working_draft)
            snapshot_version = self.snapshot.working_draft_generation_version
            if (
                snapshot_version is None
                and self.snapshot.working_draft
                and self.snapshot.generation is not None
            ):
                # Backward-compatible fallback for snapshots built by older
                # callers that only supplied the generation object.
                snapshot_version = self.snapshot.generation.version
            pending: list[TaskStreamEvent] = []
            while True:
                try:
                    event = self.subscription.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if event.seq > self.snapshot_seq and not self._covered_by_snapshot(
                    event, snapshot_version, snapshot_draft_chars
                ):
                    pending.append(event)
            for event in pending:
                yield format_sse(event)

            while True:
                try:
                    event = await asyncio.wait_for(
                        self.subscription.queue.get(),
                        timeout=heartbeat_seconds,
                    )
                except TimeoutError:
                    yield ": heartbeat\n\n"
                    continue
                if self._covered_by_snapshot(
                    event, snapshot_version, snapshot_draft_chars
                ):
                    continue
                yield format_sse(event)
        finally:
            self.bus.unsubscribe(self.subscription)

    @staticmethod
    def _covered_by_snapshot(
        event: TaskStreamEvent,
        snapshot_version: int | None,
        snapshot_draft_chars: int,
    ) -> bool:
        if event.type is not StreamEventType.TOKEN_DELTA:
            return False
        if event.generation_version != snapshot_version:
            return False
        offset = event.data.get("offset")
        return isinstance(offset, int) and offset < snapshot_draft_chars


async def prepare_event_stream(
    *,
    task_manager: "TaskManager",
    event_bus: EventBus,
    task_id: str,
) -> EventStream:
    """Subscribe first, then build the snapshot. Caller must handle 404."""
    subscription = event_bus.subscribe(task_id)
    snapshot_seq = event_bus.latest_seq
    try:
        snapshot = await task_manager.get_snapshot(task_id)
    except BaseException:
        event_bus.unsubscribe(subscription)
        raise
    return EventStream(
        bus=event_bus,
        subscription=subscription,
        snapshot=snapshot,
        snapshot_seq=snapshot_seq,
    )


@router.get(
    "/tasks/{task_id}/events",
    response_class=StreamingResponse,
    summary="Stream task events with an initial authoritative snapshot",
)
async def task_events(task_id: str, request: Request) -> StreamingResponse:
    task_manager: TaskManager = request.app.state.task_manager
    event_bus: EventBus = request.app.state.event_bus
    try:
        stream = await prepare_event_stream(
            task_manager=task_manager,
            event_bus=event_bus,
            task_id=task_id,
        )
    except TaskNotFoundError:
        raise HTTPException(status_code=404, detail="Task not found") from None

    return StreamingResponse(
        stream.frames(
            heartbeat_seconds=request.app.state.settings.sse_heartbeat_seconds
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )
