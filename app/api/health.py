"""Health endpoint: proves the process and database are usable.

It deliberately reveals only the provider name and app version. No paths,
environment variables, counts, or credentials are included.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from app import __version__
from app.config import Settings
from app.database.connection import Database
from app.schemas.task import HealthResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["system"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Check that the API and database are ready",
)
async def health(request: Request) -> HealthResponse:
    database: Database = request.app.state.db
    settings: Settings = request.app.state.settings
    try:
        await database.ping()
    except Exception:
        logger.exception("Health check failed: database is not available")
        raise HTTPException(status_code=503, detail="Database unavailable") from None

    return HealthResponse(
        status="ok",
        database="ok",
        provider=settings.ai_provider,
        version=__version__,
    )
