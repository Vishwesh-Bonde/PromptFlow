"""Application configuration.

Every value can be supplied through an environment variable or a local `.env`
file. Tests construct ``Settings(_env_file=None, ...)`` so they never depend on
a developer's local file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ProviderName = Literal["mock", "openai"]


class Settings(BaseSettings):
    """Validated runtime settings.

    The API key is wrapped in ``SecretStr`` so that accidental logging or a
    traceback containing the settings object cannot leak it.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    ai_provider: ProviderName = "mock"
    openai_api_key: SecretStr | None = None
    openai_model: str | None = None

    mock_stream_delay_ms: int = Field(default=40, ge=0, le=10_000)
    mock_stream_chunk_chars: int = Field(default=24, ge=1, le=2_000)
    mock_initial_delay_ms: int = Field(default=0, ge=0, le=60_000)
    mock_failure_after_chunks: int | None = Field(default=None, ge=0)

    database_path: Path = Path("./data/promptflow.db")
    instruction_debounce_ms: int = Field(default=250, ge=0, le=5_000)
    checkpoint_interval_ms: int = Field(default=500, ge=50, le=60_000)

    max_prompt_chars: int = Field(default=12_000, ge=1, le=100_000)
    max_instruction_chars: int = Field(default=6_000, ge=1, le=100_000)
    max_instructions_per_task: int = Field(default=100, ge=1, le=10_000)

    provider_timeout_seconds: float = Field(default=120.0, gt=0, le=600)
    cancellation_timeout_seconds: float = Field(default=5.0, gt=0, le=60)

    sse_queue_size: int = Field(default=256, ge=16, le=10_000)
    sse_heartbeat_seconds: float = Field(default=15.0, gt=0, le=120)

    @model_validator(mode="after")
    def _validate_selected_provider(self) -> "Settings":
        if self.ai_provider == "openai":
            key = self.openai_api_key.get_secret_value() if self.openai_api_key else ""
            if not key.strip():
                raise ValueError("OPENAI_API_KEY must be set when AI_PROVIDER=openai")
            if not (self.openai_model or "").strip():
                raise ValueError("OPENAI_MODEL must be set when AI_PROVIDER=openai")
        return self

    def ensure_database_directory(self) -> None:
        """Create the parent directory for the SQLite file if it is missing."""
        self.database_path.parent.mkdir(parents=True, exist_ok=True)


def load_settings() -> Settings:
    """Load settings from the environment and optional `.env` file."""
    return Settings()
