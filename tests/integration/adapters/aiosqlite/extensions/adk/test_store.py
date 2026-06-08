"""Integration tests for AioSQLite ADK session/event store.

The shared session/event CRUD lifecycle (create_tables, session round-trip, list/delete,
append/get events, get_events filtering) is covered by
tests/integration/adapters/contracts/test_adk_store_contract.py. This module keeps the
adapter-specific coverage (owner_id_column, storage-type fidelity, timestamp precision,
concurrency, event ordering/JSON details) that is not portable across the contract matrix.
"""

from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

pytestmark = pytest.mark.xdist_group("sqlite")


async def _build_store(tmp_path: Path) -> tuple[AiosqliteConfig, AiosqliteADKStore]:
    db_path = tmp_path / "test_adk_store.db"
    config = AiosqliteConfig(connection_config={"database": str(db_path)})
    store = AiosqliteADKStore(config)
    await store.create_tables()
    return config, store


async def test_aiosqlite_session_owner_column_is_created_when_configured(tmp_path: Path) -> None:
    """Owner-column DDL matches create_session's optional owner_id insert path."""
    db_path = tmp_path / "test_adk_owner.db"
    config = AiosqliteConfig(
        connection_config={"database": str(db_path)}, extension_config={"adk": {"owner_id_column": "owner_id TEXT"}}
    )
    store = AiosqliteADKStore(config)
    try:
        await store.create_tables()
        await store.create_session("session-owner", "app", "user", {}, owner_id="tenant-1")

        async with config.provide_connection() as conn:
            cursor = await conn.execute("SELECT owner_id FROM adk_sessions WHERE id = ?", ("session-owner",))
            row = await cursor.fetchone()

        assert row == ("tenant-1",)
    finally:
        await config.close_pool()
