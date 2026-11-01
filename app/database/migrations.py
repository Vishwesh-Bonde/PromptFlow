"""Transactional, retry-safe schema migrations.

SQLite DDL is transactional, so each migration runs inside one
``BEGIN IMMEDIATE`` transaction together with the ``PRAGMA user_version`` bump.
If any statement fails, the transaction is rolled back and the version is left
untouched. Running the migrator again is therefore always safe.

``executescript`` is intentionally not used: it commits any open transaction
before executing, which would break the all-or-nothing guarantee.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import aiosqlite

from app.core.errors import MigrationError


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    statements: tuple[str, ...]


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    """Split schema.sql into complete statements.

    ``sqlite3.complete_statement`` understands trigger bodies, so statements are
    accumulated line by line instead of naively splitting on semicolons.
    """
    sql = (Path(__file__).with_name("schema.sql")).read_text(encoding="utf-8")
    statements: list[str] = []
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            if statement:
                statements.append(statement)
            buffer = ""
    if buffer.strip():
        raise MigrationError("schema.sql contains an incomplete SQL statement")
    return tuple(statements)


SCHEMA_MIGRATIONS: tuple[Migration, ...] = (
    Migration(version=1, statements=_schema_statements()),
)
LATEST_SCHEMA_VERSION = SCHEMA_MIGRATIONS[-1].version


def _validate_migrations(migrations: tuple[Migration, ...]) -> None:
    if not migrations:
        raise MigrationError("At least one migration is required")
    versions = [migration.version for migration in migrations]
    if versions != list(range(1, len(versions) + 1)):
        raise MigrationError(
            "Migrations must be contiguous and start at version 1, "
            f"got versions {versions}"
        )


async def apply_migrations(
    conn: aiosqlite.Connection,
    migrations: tuple[Migration, ...] = SCHEMA_MIGRATIONS,
) -> int:
    """Apply pending migrations atomically and return the resulting version.

    The caller must not already be inside a transaction.
    """
    _validate_migrations(migrations)
    latest = migrations[-1].version

    await conn.execute("BEGIN IMMEDIATE")
    try:
        cursor = await conn.execute("PRAGMA user_version")
        row = await cursor.fetchone()
        await cursor.close()
        current = int(row[0]) if row else 0

        if current > latest:
            raise MigrationError(
                f"Database schema version {current} is newer than this code "
                f"(latest supported: {latest})"
            )

        for migration in migrations:
            if migration.version <= current:
                continue
            for statement in migration.statements:
                await conn.execute(statement)
            # user_version does not accept a bound parameter; the value comes
            # from the validated migration definition, never from user input.
            await conn.execute(f"PRAGMA user_version = {int(migration.version)}")
            current = migration.version

        await conn.commit()
        return current
    except BaseException:
        await conn.rollback()
        raise
