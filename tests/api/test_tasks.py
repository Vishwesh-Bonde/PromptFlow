"""Task creation and retrieval API behaviour."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI

from app.api.events import task_events
from app.config import Settings
from app.schemas.task import HARD_MAX_TEXT_CHARS
from app.services.ai_provider import GenerationRequest
from app.services.mock_provider import MockAIProvider, build_mock_answer
from tests.helpers import next_sse, running_app

pytestmark = pytest.mark.anyio


async def test_create_task_returns_202_without_waiting_for_generation(
    app_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = app_client

    response = await client.post(
        "/api/tasks", json={"prompt": "Explain event loops briefly."}
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "GENERATING"
    assert body["generation_version"] == 1
    assert body["task_id"]

    await app.state.coordinator.wait_for_idle(body["task_id"], timeout=5)


async def test_get_task_returns_committed_answer_and_timeline(
    app_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = app_client
    prompt = "Explain SQLite WAL mode."
    created = await client.post("/api/tasks", json={"prompt": prompt})
    task_id = created.json()["task_id"]
    await app.state.coordinator.wait_for_idle(task_id, timeout=5)

    response = await client.get(f"/api/tasks/{task_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["task"]["status"] == "COMPLETED"
    assert body["task"]["committed_answer"] == build_mock_answer(
        GenerationRequest(prompt=prompt)
    )
    assert body["generation"]["status"] == "COMPLETED"
    assert body["working_draft"] == ""
    assert [event["type"] for event in body["events"]] == [
        "task_created",
        "generation_started",
        "generation_completed",
    ]


async def test_get_missing_task_returns_404(
    app_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, _ = app_client
    response = await client.get("/api/tasks/does-not-exist")
    assert response.status_code == 404
    assert response.json() == {"detail": "Task not found"}


async def test_missing_task_event_stream_returns_404(
    app_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, _ = app_client
    response = await client.get("/api/tasks/does-not-exist/events")
    assert response.status_code == 404


async def test_blank_prompt_is_rejected(
    app_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, _ = app_client
    response = await client.post("/api/tasks", json={"prompt": "   "})
    assert response.status_code == 422


async def test_configured_prompt_limit_is_enforced(
    app_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = app_client
    prompt = "x" * (app.state.settings.max_prompt_chars + 1)
    response = await client.post("/api/tasks", json={"prompt": prompt})

    assert response.status_code == 422
    assert "at most" in response.json()["detail"]
    assert prompt not in response.json()["detail"]


async def test_hard_pydantic_limit_is_enforced(
    app_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, _ = app_client
    response = await client.post(
        "/api/tasks", json={"prompt": "x" * (HARD_MAX_TEXT_CHARS + 1)}
    )
    assert response.status_code == 422


async def test_task_retrieval_during_generation_reports_active_state(
    settings: Settings,
) -> None:
    provider = MockAIProvider(block_before_start=True)
    async with running_app(settings, provider) as app:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            created = await asyncio.wait_for(
                client.post("/api/tasks", json={"prompt": "Hold this open."}),
                timeout=2,
            )
            assert created.status_code == 202
            task_id = created.json()["task_id"]
            await asyncio.wait_for(provider.blocked.wait(), timeout=2)

            response = await client.get(f"/api/tasks/{task_id}")
            assert response.status_code == 200
            body = response.json()
            assert body["task"]["status"] == "GENERATING"
            assert body["generation"]["status"] == "GENERATING"
            assert body["working_draft"] == ""
            assert body["task"]["committed_answer"] is None

            provider.release.set()
            await app.state.coordinator.wait_for_idle(task_id, timeout=5)


async def test_event_endpoint_builds_sse_response(settings: Settings) -> None:
    provider = MockAIProvider(block_before_start=True)
    provider.release.clear()
    async with running_app(settings, provider) as app:
        created = await app.state.task_manager.create_task("Open the stream.")
        task_id = created.task_id
        await asyncio.wait_for(provider.blocked.wait(), timeout=2)

        class RequestStub:
            def __init__(self, application: FastAPI) -> None:
                self.app = application

        response = await task_events(task_id, RequestStub(app))
        assert response.media_type == "text/event-stream"
        assert response.headers["cache-control"] == "no-cache, no-transform"
        assert response.headers["x-accel-buffering"] == "no"

        frames = response.body_iterator
        first = await asyncio.wait_for(next_sse(frames), timeout=2)
        assert first["event"] == "snapshot"
        await frames.aclose()
        assert app.state.event_bus.subscriber_count(task_id) == 0

        provider.release.set()
        await app.state.coordinator.wait_for_idle(task_id, timeout=5)
