"""Provider construction.

Phase 2 deliberately supports only the mock provider. The OpenAI adapter is
implemented in Phase 5 after verifying the installed SDK and streaming API.
"""

from __future__ import annotations

from app.config import Settings
from app.services.ai_provider import AIProvider
from app.services.mock_provider import MockAIProvider


def create_provider(settings: Settings) -> AIProvider:
    if settings.ai_provider == "mock":
        return MockAIProvider(
            chunk_chars=settings.mock_stream_chunk_chars,
            chunk_delay_ms=settings.mock_stream_delay_ms,
            initial_delay_ms=settings.mock_initial_delay_ms,
            failure_after_chunks=settings.mock_failure_after_chunks,
        )
    raise RuntimeError(
        "AI_PROVIDER=openai is not implemented in Phase 2. "
        "Use AI_PROVIDER=mock; the real provider is added in Phase 5."
    )
