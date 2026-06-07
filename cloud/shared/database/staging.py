"""Runtime helpers for scanner staging tables."""

from __future__ import annotations

from pathlib import Path

_SQL_PATH = Path(__file__).resolve().parent / "sql" / "daily_scan_signals.sql"


def daily_scan_signals_ddl() -> str:
    """Return the canonical daily_scan_signals DDL + index SQL."""
    return _SQL_PATH.read_text()


def ensure_daily_scan_signals(cursor) -> None:
    """Create the staging table and index if they do not exist."""
    cursor.execute(daily_scan_signals_ddl())
