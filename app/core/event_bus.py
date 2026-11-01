"""Bounded in-process event fan-out for SSE subscribers.

The bus is deliberately small and synchronous. ``publish`` never awaits, so a
slow browser cannot block the generation worker. Each subscriber owns an
independent bounded queue; one subscriber's overflow cannot consume or drop
events for another subscriber.

If a queue is full, the queued deltas are discarded and replaced with a single
``resync_required`` event. The client then fetches the authoritative task
snapshot, which includes the current instruction sequence, generation version,
committed answer, and working draft.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

from app.core.event_types import StreamEventType


@dataclass(frozen=True, slots=True)
class TaskStreamEvent:
    seq: int
    type: StreamEventType
    task_id: str
    generation_version: int | None = None
    data: Mapping[str, Any] = field(default_factory=dict)

    def payload(self) -> dict[str, Any]:
        body = dict(self.data)
        body["task_id"] = self.task_id
        body["type"] = self.type.value
        if self.generation_version is not None:
            body["generation_version"] = self.generation_version
        return body


@dataclass(eq=False, slots=True)
class Subscription:
    task_id: str
    queue: asyncio.Queue[TaskStreamEvent]


def format_sse(event: TaskStreamEvent) -> str:
    body = json.dumps(event.payload(), separators=(",", ":"), ensure_ascii=False)
    return f"event: {event.type.value}\nid: {event.seq}\ndata: {body}\n\n"


class EventBus:
    def __init__(self, queue_size: int = 256) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        self._queue_size = queue_size
        self._subscribers: dict[str, set[Subscription]] = {}
        self._sequence = 0

    @property
    def latest_seq(self) -> int:
        """Monotonic sequence of the most recently published event."""
        return self._sequence

    def subscriber_count(self, task_id: str) -> int:
        return len(self._subscribers.get(task_id, ()))

    def subscribe(self, task_id: str) -> Subscription:
        subscription = Subscription(
            task_id=task_id,
            queue=asyncio.Queue(maxsize=self._queue_size),
        )
        self._subscribers.setdefault(task_id, set()).add(subscription)
        return subscription

    def unsubscribe(self, subscription: Subscription) -> None:
        subscribers = self._subscribers.get(subscription.task_id)
        if subscribers is None:
            return
        subscribers.discard(subscription)
        if not subscribers:
            self._subscribers.pop(subscription.task_id, None)

    def publish(
        self,
        task_id: str,
        event_type: StreamEventType,
        *,
        generation_version: int | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> TaskStreamEvent:
        self._sequence += 1
        event = TaskStreamEvent(
            seq=self._sequence,
            type=event_type,
            task_id=task_id,
            generation_version=generation_version,
            data=dict(data or {}),
        )

        for subscription in tuple(self._subscribers.get(task_id, ())):
            if subscription.queue.full():
                self._drain(subscription.queue)
                resync = TaskStreamEvent(
                    seq=event.seq,
                    type=StreamEventType.RESYNC_REQUIRED,
                    task_id=task_id,
                    generation_version=generation_version,
                    data={
                        "reason": "queue_overflow",
                        "dropped_seq": event.seq,
                        "dropped_type": event.type.value,
                    },
                )
                subscription.queue.put_nowait(resync)
            else:
                subscription.queue.put_nowait(event)
        return event

    @staticmethod
    def _drain(queue: asyncio.Queue[TaskStreamEvent]) -> None:
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return

    def close(self) -> None:
        """Drop all subscribers. Used during application shutdown."""
        self._subscribers.clear()
