"""Test helpers for app lifecycles, SSE frames, and controlled providers."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI

from app.config import Settings
from app.main import create_app
from app.services.ai_provider import (
    AIProvider,
    GenerationRequest,
    ProviderError,
)


@asynccontextmanager
async def running_app(
    settings: Settings,
    provider: AIProvider | None = None,
    *,
    debounce_wait: Any = None,
) -> AsyncIterator[FastAPI]:
    app = create_app(settings, provider=provider, debounce_wait=debounce_wait)
    async with app.router.lifespan_context(app):
        yield app


class ManualDebounce:
    """Controllable debounce wait used instead of real-time sleeps in tests."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def __call__(self, seconds: float) -> None:
        self.calls += 1
        self.started.set()
        await self.release.wait()

    def reset(self) -> None:
        self.started.clear()
        self.release.clear()


@dataclass(slots=True)
class ScriptedCall:
    chunks: tuple[str, ...]
    error: ProviderError | None = None
    block_before_start: bool = False
    block_after_chunks: int | None = None
    cleanup_gate: asyncio.Event | None = None


@dataclass(slots=True)
class ScriptedProvider:
    """Provider with one controllable stream per call.

    It tracks overlap so tests can assert that a replacement never starts before
    the previous provider stream has unwound.
    """

    calls: list[ScriptedCall]
    requests: list[GenerationRequest] = field(default_factory=list)
    started: list[asyncio.Event] = field(default_factory=list)
    release: list[asyncio.Event] = field(default_factory=list)
    blocked: list[asyncio.Event] = field(default_factory=list)
    call_started: list[asyncio.Event] = field(init=False)
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    first_started: asyncio.Event = field(default_factory=asyncio.Event)
    cleanup_started: asyncio.Event = field(default_factory=asyncio.Event)
    active: int = 0
    max_active: int = 0

    def __post_init__(self) -> None:
        self.call_started = [asyncio.Event() for _ in self.calls]

    @property
    def name(self) -> str:
        return "scripted"

    async def stream(self, request: GenerationRequest) -> AsyncIterator[str]:
        index = len(self.requests)
        if index >= len(self.calls):
            raise ProviderError(
                "script_exhausted", "The scripted provider has no more calls."
            )
        script = self.calls[index]
        self.requests.append(request)

        started = asyncio.Event()
        release = asyncio.Event()
        blocked = asyncio.Event()
        if (
            not script.block_before_start
            and script.block_after_chunks is None
        ):
            release.set()
        self.started.append(started)
        self.release.append(release)
        self.blocked.append(blocked)

        self.active += 1
        self.max_active = max(self.max_active, self.active)
        started.set()
        self.first_started.set()
        self.call_started[index].set()
        try:
            if script.block_before_start:
                blocked.set()
                await release.wait()

            yielded = 0
            for chunk in script.chunks:
                if (
                    script.block_after_chunks is not None
                    and yielded >= script.block_after_chunks
                ):
                    blocked.set()
                    await release.wait()
                yielded += 1
                yield chunk

            if script.error is not None:
                raise script.error
        except asyncio.CancelledError:
            self.cancelled.set()
            if script.cleanup_gate is not None:
                self.cleanup_started.set()
                await script.cleanup_gate.wait()
            if script.error is not None:
                # Simulate a provider that surfaces a domain error while its
                # cancellation is unwinding instead of re-raising CancelledError.
                raise script.error
            raise
        finally:
            self.active -= 1

    async def aclose(self) -> None:
        return None


def parse_sse(frame: str) -> dict[str, Any]:
    if frame.startswith(":"):
        return {"comment": frame.strip()}
    event: dict[str, Any] = {}
    for line in frame.strip().splitlines():
        if line.startswith("event: "):
            event["event"] = line.removeprefix("event: ")
        elif line.startswith("id: "):
            event["id"] = int(line.removeprefix("id: "))
        elif line.startswith("data: "):
            event["data"] = json.loads(line.removeprefix("data: "))
    return event


async def next_sse(stream: AsyncIterator[str]) -> dict[str, Any]:
    while True:
        frame = await anext(stream)
        if frame.startswith(":"):
            continue
        return parse_sse(frame)


async def collect_until(
    stream: AsyncIterator[str], event_name: str
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    while True:
        event = await next_sse(stream)
        events.append(event)
        if event.get("event") == event_name:
            return events
