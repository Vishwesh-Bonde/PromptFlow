"""FastAPI application factory and process lifespan.

The app uses one Uvicorn worker by design: coordination state (locks, worker
handles, SSE subscribers) is process-local. Persisted task state lives in
SQLite. Startup reconciles work interrupted by a previous process; shutdown
cancels local workers and persists their partial drafts.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import __version__
from app.api.events import router as events_router
from app.api.health import router as health_router
from app.api.tasks import router as tasks_router
from app.config import Settings, load_settings
from app.core.event_bus import EventBus
from app.core.generation_coordinator import DebounceWait, GenerationCoordinator
from app.core.task_manager import TaskManager
from app.database.connection import Database
from app.database.repository import Repository
from app.services.ai_provider import AIProvider
from app.services.factory import create_provider

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    *,
    provider: AIProvider | None = None,
    debounce_wait: DebounceWait | None = None,
) -> FastAPI:
    app_settings = settings or load_settings()
    provider_override = provider

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app_settings.ensure_database_directory()
        database = Database(app_settings.database_path)
        await database.connect()
        coordinator: GenerationCoordinator | None = None
        event_bus: EventBus | None = None
        active_provider: AIProvider | None = None
        try:
            await database.migrate()
            repository = Repository(database)

            # Reconcile tasks that were active when the previous process died
            # before serving any request.
            recovered = await repository.recover_interrupted_tasks()
            if recovered:
                logger.warning(
                    "Reconciled %d task(s) interrupted by a server restart",
                    len(recovered),
                )

            active_provider = provider_override or create_provider(app_settings)
            event_bus = EventBus(queue_size=app_settings.sse_queue_size)
            coordinator = GenerationCoordinator(
                repository=repository,
                event_bus=event_bus,
                provider=active_provider,
                settings=app_settings,
                debounce_wait=debounce_wait,
            )
            task_manager = TaskManager(
                repository=repository,
                coordinator=coordinator,
                max_instructions_per_task=(
                    app_settings.max_instructions_per_task
                ),
            )

            app.state.db = database
            app.state.repository = repository
            app.state.provider = active_provider
            app.state.event_bus = event_bus
            app.state.coordinator = coordinator
            app.state.task_manager = task_manager
            logger.info(
                "PromptFlow ready (provider=%s, database=%s, journal=%s)",
                app_settings.ai_provider,
                database.path,
                database.journal_mode,
            )
            yield
        finally:
            try:
                if coordinator is not None:
                    await coordinator.shutdown()
            finally:
                if event_bus is not None:
                    event_bus.close()
                try:
                    if active_provider is not None:
                        await active_provider.aclose()
                finally:
                    await database.close()

    app = FastAPI(
        title="PromptFlow",
        description="Think while AI thinks.",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = app_settings
    app.include_router(health_router)
    app.include_router(tasks_router)
    app.include_router(events_router)
    return app


app = create_app()
