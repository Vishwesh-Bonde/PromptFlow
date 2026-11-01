"""Health endpoint tests."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from app import __version__
from app.config import Settings
from app.main import create_app

pytestmark = pytest.mark.anyio


async def test_health_reports_ready_without_secrets(
    app_client: tuple[httpx.AsyncClient, FastAPI], settings: Settings
) -> None:
    client, _ = app_client

    response = await client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "database": "ok",
        "provider": "mock",
        "version": __version__,
    }
    assert str(settings.database_path) not in response.text


async def test_health_returns_503_when_database_is_unavailable(
    app_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = app_client
    await app.state.db.close()

    response = await client.get("/api/health")

    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}


async def test_health_never_echoes_api_key(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "secret-test.db",
        ai_provider="mock",
        openai_api_key="sk-this-must-never-appear",
        openai_model="test-model",
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            response = await client.get("/api/health")

    assert response.status_code == 200
    assert "sk-this-must-never-appear" not in response.text
