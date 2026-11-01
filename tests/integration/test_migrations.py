"""Migration safety tests: atomicity, retry behaviour, and version checks."""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from app.core.errors import MigrationError
from app.database.connection import Database
from app.database.migrations import LATEST_SCHEMA_VERSION, Migration

pytestmark = pytest.mark.anyio


async def _user_version(database: Database) -> int:
    async with database.read() as conn:
        cursor = await conn.execute("PRAGMA user_version")
        row = await cursor.fetchone()
        await cursor.close()
    assert row is not None
    return int(row[0])


async def _table_names(database: Database) -> set[str]:
    async with database.read() as conn:
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
        rows = await cursor.fetchall()
        await cursor.close()
    return {str(row["name"]) for row in rows}


async def test_migrations_create_schema_and_set_version(database: Database) -> None:
    assert await _user_version(database) == LATEST_SCHEMA_VERSION

    tables = await _table_names(database)
    assert {"tasks", "instructions", "generations", "task_events"} <= tables

    async with database.read() as conn:
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'trg_tasks_prompt_immutable'"
        )
        trigger = await cursor.fetchone()
        await cursor.close()

    assert trigger is not None


async def test_connection_pragmas_are_applied(database: Database) -> None:
    async with database.read() as conn:
        cursor = await conn.execute("PRAGMA foreign_keys")
        foreign_keys = await cursor.fetchone()
        await cursor.close()

    assert foreign_keys is not None
    assert int(foreign_keys[0]) == 1
    assert database.journal_mode == "wal"


async def test_running_migrations_twice_is_safe(database: Database) -> None:
    before = await _table_names(database)

    version = await database.migrate()

    assert version == LATEST_SCHEMA_VERSION
    assert await _user_version(database) == LATEST_SCHEMA_VERSION
    assert await _table_names(database) == before


async def test_failed_migration_rolls_back_completely(tmp_path: Path) -> None:
    database = Database(tmp_path / "rollback.db")
    await database.connect()
    try:
        broken = (
            Migration(
                1,
                (
                    "CREATE TABLE should_rollback (id INTEGER PRIMARY KEY);",
                    "CREATE TABLE broken (",
                ),
            ),
        )

        with pytest.raises(aiosqlite.OperationalError):
            await database.migrate(migrations=broken)

        assert await _user_version(database) == 0
        assert "should_rollback" not in await _table_names(database)

        # A retry with the real schema must succeed after the failed attempt.
        assert await database.migrate() == LATEST_SCHEMA_VERSION
        assert "tasks" in await _table_names(database)
    finally:
        await database.close()


async def test_newer_database_schema_is_rejected(tmp_path: Path) -> None:
    database = Database(tmp_path / "future.db")
    await database.connect()
    try:
        async with database.transaction() as conn:
            await conn.execute("PRAGMA user_version = 99")

        with pytest.raises(MigrationError, match="newer than this code"):
            await database.migrate()
    finally:
        await database.close()


async def test_non_contiguous_migration_versions_are_rejected(
    database: Database,
) -> None:
    invalid = (Migration(1, ()), Migration(3, ()))

    with pytest.raises(MigrationError, match="contiguous"):
        await database.migrate(migrations=invalid)

    assert await _user_version(database) == LATEST_SCHEMA_VERSION
