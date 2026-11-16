# pyright: reportPrivateUsage=false
"""Unit tests for AsyncMy Litestar session store DDL configuration."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.asyncmy.litestar import AsyncmyStore

DEFAULT_ENGINE_CLAUSE = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"


def _mock_config(litestar_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"litestar": litestar_config or {}}
    return config


def test_asyncmy_litestar_store_default_ddl_unchanged() -> None:
    """Without table_options, the emitted DDL keeps today's exact ENGINE clause."""
    store = AsyncmyStore(_mock_config())

    ddl = store._table_ddl()

    assert ddl.strip().endswith(DEFAULT_ENGINE_CLAUSE)


def test_asyncmy_litestar_store_honors_table_options() -> None:
    """A configured table_options value is interpolated into the CREATE TABLE DDL."""
    store = AsyncmyStore(_mock_config({"table_options": "COMMENT='litestar-session'"}))

    ddl = store._table_ddl()

    assert f"{DEFAULT_ENGINE_CLAUSE} COMMENT='litestar-session'" in ddl


def test_asyncmy_litestar_store_drop_table_sql_omits_drop_index() -> None:
    """_drop_table_sql must emit only DROP TABLE IF EXISTS without preceding DROP INDEX."""
    store = AsyncmyStore(_mock_config())

    statements = store._drop_table_sql()

    assert len(statements) == 1
    assert statements[0] == "DROP TABLE IF EXISTS litestar_session"


async def test_asyncmy_litestar_store_set_uses_mariadb_compatible_upsert() -> None:
    """_set must use VALUES(col) in ON DUPLICATE KEY UPDATE for MySQL and MariaDB compatibility."""
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    conn.commit = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn

    store = AsyncmyStore(config)
    await store.set("session-1", b"payload", 300)

    assert cursor.execute.call_count == 1
    sql = cursor.execute.call_args_list[0].args[0]
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "data = VALUES(data)" in sql
    assert "expires_at = VALUES(expires_at)" in sql
    assert "AS new" not in sql


@pytest.mark.parametrize(
    ("renew_for", "expect_renew"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero"),
        pytest.param(-1, False, id="negative"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=500), True, id="subsecond-timedelta"),
        pytest.param(30, True, id="positive-int"),
    ],
)
async def test_asyncmy_litestar_store_get_renews_only_for_positive_duration(
    renew_for: "int | timedelta | None", expect_renew: bool
) -> None:
    """get renews expires_at only when renew_for is strictly positive."""
    future = datetime.now(UTC).replace(tzinfo=None) + timedelta(minutes=5)
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(b"payload", future))
    cursor.close = AsyncMock()
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    conn.commit = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn

    data = await AsyncmyStore(config).get("session-1", renew_for=renew_for)

    assert data == b"payload"
    if expect_renew:
        assert cursor.execute.await_count == 2
        assert "UPDATE litestar_session" in cursor.execute.await_args_list[1].args[0]
        conn.commit.assert_awaited_once()
    else:
        assert cursor.execute.await_count == 1
        conn.commit.assert_not_awaited()
