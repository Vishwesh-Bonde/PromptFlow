"""Async SQLite connection lifecycle.

The MVP runs one process with one long-lived connection. A single asyncio lock
serializes access, which keeps transaction boundaries unambiguous and removes
SQLite writer contention. The lock is never held while waiting for provider
streams, debounce timers, or HTTP clients.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from app.database.migrations import SCHEMA_MIGRATIONS, Migration, apply_migrations

logger = logging.getLogger(__name__)


class Database:
    """Owns the single aiosqlite connection used by the application."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self.journal_mode: str | None = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def is_connected(self) -> bool:
        return self._connection is not None

    def _require_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("Database is not connected")
        return self._connection

    async def connect(self) -> None:
        if self._connection is not None:
            return

        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(self._path, isolation_level=None)
        conn.row_factory = aiosqlite.Row
        try:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("PRAGMA busy_timeout = 5000")
            await conn.execute("PRAGMA synchronous = NORMAL")
            cursor = await conn.execute("PRAGMA journal_mode = WAL")
            row = await cursor.fetchone()
            await cursor.close()
            self.journal_mode = str(row[0]).lower() if row else None
            if self.journal_mode != "wal":
                logger.warning(
                    "SQLite did not enable WAL mode (reported %r); "
                    "the database may be on a filesystem that does not support it",
                    self.journal_mode,
                )
        except BaseException:
            await conn.close()
            raise

        self._connection = conn
        logger.info("Connected to SQLite database at %s", self._path)

    async def migrate(
        self, migrations: tuple[Migration, ...] = SCHEMA_MIGRATIONS
    ) -> int:
        """Apply schema migrations under the database lock."""
        conn = self._require_connection()
        async with self._lock:
            return await apply_migrations(conn, migrations)

    async def close(self) -> None:
        async with self._lock:
            connection, self._connection = self._connection, None
            if connection is not None:
                await connection.close()
                logger.info("Closed SQLite database at %s", self._path)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        """Run a short write transaction and commit or roll back atomically.

        The begin/yield/commit guard is cancellation-aware. Generation workers
        are cancelled while they may be checkpointing, and a cancellation that
        lands between ``BEGIN`` and the ``try`` block must not leave the shared
        connection stuck inside an uncommitted transaction.
        """
        conn = self._require_connection()
        async with self._lock:
            try:
                await conn.execute("BEGIN IMMEDIATE")
            except BaseException:
                await self._rollback_if_open(conn)
                raise
            try:
                yield conn
            except BaseException:
                await self._rollback_if_open(conn)
                raise
            else:
                try:
                    await conn.commit()
                except BaseException:
                    await self._rollback_if_open(conn)
                    raise

    @staticmethod
    async def _rollback_if_open(conn: aiosqlite.Connection) -> None:
        if conn.in_transaction:
            try:
                await conn.rollback()
            except BaseException:
                logger.exception("Could not roll back an interrupted transaction")

    @asynccontextmanager
    async def read(self) -> AsyncIterator[aiosqlite.Connection]:
        """Serialize reads on the same connection.

        Reads are short and this prevents a query from observing another
        coroutine's uncommitted transaction state.
        """
        conn = self._require_connection()
        async with self._lock:
            yield conn

    async def ping(self) -> None:
        async with self.read() as conn:
            cursor = await conn.execute("SELECT 1")
            row = await cursor.fetchone()
            await cursor.close()
            if row is None or row[0] != 1:
                raise RuntimeError("Database ping returned an unexpected result")
