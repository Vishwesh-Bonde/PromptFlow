"""Bounded event bus fan-out and overflow behaviour."""

from __future__ import annotations

import pytest

from app.core.event_bus import EventBus
from app.core.event_types import StreamEventType

pytestmark = pytest.mark.anyio


def publish(bus: EventBus, index: int) -> None:
    bus.publish(
        "task-1",
        StreamEventType.TOKEN_DELTA,
        generation_version=1,
        data={"text": str(index)},
    )


async def test_subscribers_receive_independent_copies() -> None:
    bus = EventBus(queue_size=16)
    first = bus.subscribe("task-1")
    second = bus.subscribe("task-1")

    for index in range(5):
        publish(bus, index)

    first_events = [first.queue.get_nowait() for _ in range(5)]
    second_events = [second.queue.get_nowait() for _ in range(5)]

    assert [event.seq for event in first_events] == [1, 2, 3, 4, 5]
    assert [event.seq for event in second_events] == [1, 2, 3, 4, 5]
    assert all(event.type is StreamEventType.TOKEN_DELTA for event in first_events)


async def test_slow_subscriber_gets_resync_without_consuming_fast_subscriber() -> None:
    bus = EventBus(queue_size=4)
    slow = bus.subscribe("task-1")
    fast = bus.subscribe("task-1")

    fast_events = []
    for index in range(10):
        publish(bus, index)
        fast_events.append(fast.queue.get_nowait())

    assert [event.seq for event in fast_events] == list(range(1, 11))
    assert all(
        event.type is StreamEventType.TOKEN_DELTA for event in fast_events
    )
    assert slow.queue.qsize() <= 4

    slow_events = []
    while not slow.queue.empty():
        slow_events.append(slow.queue.get_nowait())
    assert any(
        event.type is StreamEventType.RESYNC_REQUIRED for event in slow_events
    )
    resync = next(
        event
        for event in slow_events
        if event.type is StreamEventType.RESYNC_REQUIRED
    )
    assert resync.data["reason"] == "queue_overflow"


async def test_unsubscribe_removes_only_that_subscriber() -> None:
    bus = EventBus(queue_size=8)
    first = bus.subscribe("task-1")
    second = bus.subscribe("task-1")

    bus.unsubscribe(first)
    publish(bus, 1)

    assert bus.subscriber_count("task-1") == 1
    assert first.queue.empty()
    assert second.queue.qsize() == 1


async def test_sequence_advances_without_subscribers() -> None:
    bus = EventBus(queue_size=8)
    publish(bus, 1)
    subscription = bus.subscribe("task-1")

    assert bus.latest_seq == 1
    publish(bus, 2)

    event = subscription.queue.get_nowait()
    assert event.seq == 2
