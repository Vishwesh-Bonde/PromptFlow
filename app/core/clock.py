"""UTC timestamp helpers.

All persisted timestamps are ISO-8601 strings in UTC with a trailing ``Z`` so
they sort lexicographically and are unambiguous when read from SQLite.
"""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")
