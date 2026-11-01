"""Configuration validation tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from app.config import Settings

_ENV_NAMES = (
    "AI_PROVIDER",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "MOCK_STREAM_DELAY_MS",
    "MOCK_STREAM_CHUNK_CHARS",
    "DATABASE_PATH",
    "INSTRUCTION_DEBOUNCE_MS",
    "CHECKPOINT_INTERVAL_MS",
    "MAX_PROMPT_CHARS",
    "MAX_INSTRUCTION_CHARS",
    "MAX_INSTRUCTIONS_PER_TASK",
    "PROVIDER_TIMEOUT_SECONDS",
    "CANCELLATION_TIMEOUT_SECONDS",
    "SSE_QUEUE_SIZE",
    "SSE_HEARTBEAT_SECONDS",
)


@pytest.fixture(autouse=True)
def _clear_promptflow_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_defaults_work_without_configuration() -> None:
    settings = Settings(_env_file=None)
    assert settings.ai_provider == "mock"
    assert settings.database_path == Path("./data/promptflow.db")
    assert settings.mock_stream_delay_ms == 40
    assert settings.instruction_debounce_ms == 250
    assert settings.checkpoint_interval_ms == 500
    assert settings.max_prompt_chars == 12_000
    assert settings.max_instruction_chars == 6_000
    assert settings.max_instructions_per_task == 100
    assert settings.sse_queue_size == 256


def test_environment_variables_override_defaults(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AI_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-secret")
    monkeypatch.setenv("OPENAI_MODEL", "test-model")
    monkeypatch.setenv("INSTRUCTION_DEBOUNCE_MS", "100")
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "custom.db"))

    settings = Settings(_env_file=None)

    assert settings.ai_provider == "openai"
    assert settings.openai_model == "test-model"
    assert settings.instruction_debounce_ms == 100
    assert settings.database_path == tmp_path / "custom.db"
    assert settings.openai_api_key is not None
    assert settings.openai_api_key.get_secret_value() == "sk-test-secret"


def test_openai_provider_requires_api_key() -> None:
    with pytest.raises(ValidationError, match="OPENAI_API_KEY"):
        Settings(
            _env_file=None,
            ai_provider="openai",
            openai_model="test-model",
        )


def test_openai_provider_requires_model() -> None:
    with pytest.raises(ValidationError, match="OPENAI_MODEL"):
        Settings(
            _env_file=None,
            ai_provider="openai",
            openai_api_key="sk-test",
        )


def test_api_key_is_masked_in_settings_repr() -> None:
    settings = Settings(
        _env_file=None,
        ai_provider="openai",
        openai_api_key="sk-never-print-me",
        openai_model="test-model",
    )
    assert "sk-never-print-me" not in repr(settings)


@pytest.mark.parametrize("value", [-1, 10_001])
def test_stream_delay_is_bounded(value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, mock_stream_delay_ms=value)


@pytest.mark.parametrize(
    "field",
    [
        "max_prompt_chars",
        "max_instruction_chars",
        "max_instructions_per_task",
    ],
)
def test_size_limits_must_be_positive(field: str) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: 0})


def test_unknown_provider_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, ai_provider="other")


def test_database_directory_is_created(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "data" / "promptflow.db"
    settings = Settings(_env_file=None, database_path=target)
    settings.ensure_database_directory()
    assert target.parent.is_dir()
