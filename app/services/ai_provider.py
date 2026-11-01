"""Provider-neutral asynchronous streaming interface.

The orchestration layer depends only on this protocol. It never imports a
vendor SDK, so the mock provider can be used in every test and no test can
accidentally reach a paid API.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True, slots=True)
class GenerationRequest:
    """Model input reconstructed from persisted task state.

    ``prompt`` is user content and is never concatenated into an application
    system instruction. Phase 2 sends the original prompt; later phases add
    ordered instructions and a previous draft as reference material.
    """

    prompt: str
    instructions: tuple[str, ...] = field(default_factory=tuple)
    instruction_kinds: tuple[str, ...] = field(default_factory=tuple)
    previous_draft: str | None = None
    previous_generation_version: int | None = None


class ProviderError(Exception):
    """A safe, classified provider failure.

    ``code`` is stable and suitable for logs/UI; ``message`` is user-facing and
    must not contain credentials, stack traces, or provider internals.
    """

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class AIProvider(Protocol):
    """Minimal contract for an async text-streaming provider."""

    @property
    def name(self) -> str: ...

    def stream(self, request: GenerationRequest) -> AsyncIterator[str]:
        """Yield deterministic text chunks.

        Cancellation of the consuming task must propagate into the iterator and
        through any open network stream. Implementations must not swallow
        ``asyncio.CancelledError``.
        """
        ...

    async def aclose(self) -> None:
        """Release provider resources. Must be safe to call more than once."""
        ...
