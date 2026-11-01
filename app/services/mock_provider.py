"""Deterministic mock provider for development and tests.

Test control is explicit rather than time-based where races matter:

- ``started`` is set when the stream begins.
- ``release`` blocks the stream while cleared.
- ``blocked`` is set when the configured block point is reached.
- ``cancelled`` is set when cancellation propagates into the provider.
- ``finished`` is set when the stream exits for any reason.

Tests wait on these events instead of sleeping.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from app.services.ai_provider import GenerationRequest, ProviderError

MOCK_ANSWER_SUFFIX = (
    "\n\nThis is a deterministic mock answer streamed in chunks. "
    "It exists so the generation pipeline can be tested without a real model. "
    "Every chunk, checkpoint, and completion in this run is reproducible."
)


def build_mock_answer(request: GenerationRequest) -> str:
    """Return the exact full text the mock provider will stream."""
    lines = [
        "Mock answer to:",
        request.prompt,
    ]
    if request.instructions:
        lines.append("")
        lines.append("Accepted instructions:")
        lines.extend(f"- {instruction}" for instruction in request.instructions)
    if request.previous_draft is not None:
        lines.append("")
        lines.append("Previous draft considered as reference material.")
    return "\n".join(lines) + MOCK_ANSWER_SUFFIX


def split_chunks(text: str, chunk_chars: int) -> tuple[str, ...]:
    if chunk_chars < 1:
        raise ValueError("chunk_chars must be positive")
    return tuple(text[index : index + chunk_chars] for index in range(0, len(text), chunk_chars))


class MockAIProvider:
    """A controllable, deterministic provider implementation."""

    def __init__(
        self,
        *,
        chunk_chars: int = 24,
        chunk_delay_ms: int = 40,
        initial_delay_ms: int = 0,
        failure_after_chunks: int | None = None,
        failure_message: str = "The mock provider failed on purpose.",
        block_before_start: bool = False,
        block_after_chunks: int | None = None,
    ) -> None:
        if chunk_chars < 1:
            raise ValueError("chunk_chars must be positive")
        if chunk_delay_ms < 0 or initial_delay_ms < 0:
            raise ValueError("delays must not be negative")
        if failure_after_chunks is not None and failure_after_chunks < 0:
            raise ValueError("failure_after_chunks must not be negative")
        if block_after_chunks is not None and block_after_chunks < 0:
            raise ValueError("block_after_chunks must not be negative")

        self._name = "mock"
        self.chunk_chars = chunk_chars
        self.chunk_delay_ms = chunk_delay_ms
        self.initial_delay_ms = initial_delay_ms
        self.failure_after_chunks = failure_after_chunks
        self.failure_message = failure_message
        self.block_before_start = block_before_start
        self.block_after_chunks = block_after_chunks

        self.started = asyncio.Event()
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.cancelled = asyncio.Event()
        self.finished = asyncio.Event()
        self.chunks_yielded = 0

    @property
    def name(self) -> str:
        return self._name

    async def stream(self, request: GenerationRequest) -> AsyncIterator[str]:
        self.started.set()
        yielded = 0
        try:
            if self.initial_delay_ms:
                await asyncio.sleep(self.initial_delay_ms / 1_000)

            if self.block_before_start:
                self.blocked.set()
                await self.release.wait()

            text = build_mock_answer(request)
            for index, chunk in enumerate(split_chunks(text, self.chunk_chars)):
                if (
                    self.failure_after_chunks is not None
                    and index >= self.failure_after_chunks
                ):
                    raise ProviderError("mock_failure", self.failure_message)
                if (
                    self.block_after_chunks is not None
                    and yielded >= self.block_after_chunks
                ):
                    self.blocked.set()
                    await self.release.wait()

                if self.chunk_delay_ms:
                    await asyncio.sleep(self.chunk_delay_ms / 1_000)
                yielded += 1
                self.chunks_yielded = yielded
                yield chunk
        except asyncio.CancelledError:
            # Record the cancellation for assertions, then propagate it. Never
            # convert cancellation into a normal completion or a ProviderError.
            self.cancelled.set()
            raise
        finally:
            self.finished.set()

    async def aclose(self) -> None:
        return None
