"""Provider selection safety tests."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.services.factory import create_provider


def test_phase_two_does_not_call_a_real_provider(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        ai_provider="openai",
        openai_api_key="not-a-real-key",
        openai_model="not-a-real-model",
        database_path=tmp_path / "unused.db",
    )

    with pytest.raises(RuntimeError, match="not implemented in Phase 2"):
        create_provider(settings)
