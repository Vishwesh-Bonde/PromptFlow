"""Mock provider determinism and cancellation propagation."""

from __future__ import annotations

import asyncio

import pytest

from app.services.ai_provider import GenerationRequest, ProviderError
from app.services.mock_provider import (
    MockAIProvider,
    build_mock_answer,
    split_chunks,
)

pytestmark = pytest.mark.anyio

REQUEST = GenerationRequest(prompt="Explain coroutines in one paragraph.")


async def test_mock_provider_is_deterministic_and_ordered() -> None:
    provider = MockAIProvider(
        chunk_chars=17,
        chunk_delay_ms=0,
        initial_delay_ms=0,
    )
    chunks = [chunk async for chunk in provider.stream(REQUEST)]

    assert "".join(chunks) == build_mock_answer(REQUEST)
    assert chunks == list(split_chunks(build_mock_answer(REQUEST), 17))
    assert provider.chunks_yielded == len(chunks)
    assert provider.finished.is_set()
    assert not provider.cancelled.is_set()


async def test_mock_provider_fails_after_requested_chunk_count() -> None:
    provider = MockAIProvider(
        chunk_chars=10,
        chunk_delay_ms=0,
        failure_after_chunks=3,
    )
    chunks: list[str] = []

    with pytest.raises(ProviderError) as exc_info:
        async for chunk in provider.stream(REQUEST):
            chunks.append(chunk)

    assert exc_info.value.code == "mock_failure"
    assert len(chunks) == 3
    assert "".join(chunks) == build_mock_answer(REQUEST)[:30]
    assert provider.finished.is_set()


async def test_mock_provider_propagates_cancellation_at_barrier() -> None:
    provider = MockAIProvider(block_before_start=True)

    async def consume() -> None:
        async for _ in provider.stream(REQUEST):
            pass

    task = asyncio.create_task(consume())
    await asyncio.wait_for(provider.started.wait(), timeout=2)
    await asyncio.wait_for(provider.blocked.wait(), timeout=2)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert provider.cancelled.is_set()
    assert provider.finished.is_set()
