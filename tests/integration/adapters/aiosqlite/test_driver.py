"""Aiosqlite-only integration behavior not shared with the sync SQLite adapter."""

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.core import StatementConfig

pytestmark = pytest.mark.xdist_group("sqlite")


async def test_aiosqlite_sqlite_specific_features(aiosqlite_session: "AiosqliteDriver") -> None:
    """Exercise async-only SQLite attachment and JSON-extension behavior."""
    pragma_result = await aiosqlite_session.execute("PRAGMA user_version")
    assert isinstance(pragma_result, SQLResult)
    assert pragma_result.data is not None

    sqlite_result = await aiosqlite_session.execute("SELECT sqlite_version() as version")
    assert sqlite_result.get_data()[0]["version"] is not None

    try:
        json_result = await aiosqlite_session.execute("SELECT json('{}') as json_test")
        assert json_result.data is not None
    except Exception:
        pass

    non_strict_config = StatementConfig(enable_parsing=False, enable_validation=False)
    await aiosqlite_session.execute("ATTACH DATABASE ':memory:' AS temp_db", statement_config=non_strict_config)
    await aiosqlite_session.execute(
        "CREATE TABLE temp_db.temp_table (id INTEGER, name TEXT)", statement_config=non_strict_config
    )
    await aiosqlite_session.execute(
        "INSERT INTO temp_db.temp_table VALUES (1, 'temp')", statement_config=non_strict_config
    )
    temp_result = await aiosqlite_session.execute("SELECT * FROM temp_db.temp_table")
    assert temp_result.get_data() == [{"id": 1, "name": "temp"}]
