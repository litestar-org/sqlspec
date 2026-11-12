"""Snowflake Litestar session persistence contracts."""

from datetime import UTC, datetime, timedelta

import pytest

from sqlspec.adapters.snowflake.litestar import SnowflakeStore
from sqlspec.extensions.litestar.migrations.reconcile import downgrade, reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.utils.adapters import resolve_extension_store_class
from tests.unit.adapters.test_snowflake._store_helpers import store_config


def test_litestar_store_resolves_and_reconciles(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch, {"litestar": {"session_table": "custom_sessions"}})
    assert (
        resolve_extension_store_class(
            config, extension="litestar", store_attr="litestar_store_class", store_suffix="Store"
        )
        is SnowflakeStore
    )
    store = SnowflakeStore(config)
    assert store.get_companion_indexes() == []
    assert store.table_name == "custom_sessions"
    driver.data_dictionary.get_tables.return_value = []
    context = MigrationContext(config=config, dialect="snowflake", driver=driver)
    reconcile(context)
    ddl = driver.execute_script.call_args.args[0]
    assert "CREATE TABLE IF NOT EXISTS custom_sessions" in ddl
    assert "data BINARY NOT NULL" in ddl
    assert "expires_at TIMESTAMPTZ" in ddl or "expires_at TIMESTAMP_TZ" in ddl
    driver.commit.assert_not_called()
    driver.execute_script.reset_mock()
    downgrade(context)
    driver.execute_script.assert_called_once_with("DROP TABLE IF EXISTS custom_sessions")


def test_litestar_store_rejects_invalid_table(monkeypatch: pytest.MonkeyPatch) -> None:
    config, _ = store_config(monkeypatch, {"litestar": {"session_table": "bad;drop"}})
    with pytest.raises(ValueError, match="Invalid table name"):
        SnowflakeStore(config)


@pytest.mark.parametrize(
    "settings, creates_table, inspects_columns",
    [
        ({"create_schema": True}, True, True),
        ({"create_schema": False}, False, True),
        ({"manage_schema": False}, False, False),
    ],
)
async def test_litestar_create_table_honors_schema_lifecycle(
    monkeypatch: pytest.MonkeyPatch, settings: dict[str, bool], creates_table: bool, inspects_columns: bool
) -> None:
    config, driver = store_config(monkeypatch, {"litestar": {"session_table": "custom_sessions", **settings}})
    driver.data_dictionary.get_tables.return_value = [{"table_name": "custom_sessions"}]
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "session_id", "data_type": "VARCHAR", "is_nullable": "NO"},
        {"column_name": "data", "data_type": "BINARY", "is_nullable": "NO"},
        {"column_name": "expires_at", "data_type": "TIMESTAMP_TZ", "is_nullable": "YES"},
    ]

    await SnowflakeStore(config).create_table()

    if creates_table:
        driver.execute_script.assert_called_once_with(
            "CREATE TABLE IF NOT EXISTS custom_sessions "
            "(session_id VARCHAR(255) PRIMARY KEY, data BINARY NOT NULL, expires_at TIMESTAMP_TZ)"
        )
    else:
        driver.execute_script.assert_not_called()
    assert driver.data_dictionary.get_columns.called is inspects_columns
    driver.execute.assert_not_called()


async def test_litestar_create_table_defers_missing_schema_when_creation_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, driver = store_config(monkeypatch, {"litestar": {"create_schema": False}})
    driver.data_dictionary.get_tables.return_value = []

    await SnowflakeStore(config).create_table()

    driver.data_dictionary.get_tables.assert_called_once_with(driver)
    driver.data_dictionary.get_columns.assert_not_called()
    driver.execute_script.assert_not_called()
    driver.execute.assert_not_called()


async def test_litestar_set_uses_hex_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    await SnowflakeStore(config).set("sid-1", b"\x00\xab\xff", expires_in=120)
    sql, parameters = driver.execute.call_args.args
    assert "MERGE INTO litestar_session" in sql
    assert "TO_BINARY(?, 'HEX')" in sql
    assert parameters[:2] == ("sid-1", "00abff")
    assert 118 <= (parameters[2] - datetime.now(UTC)).total_seconds() <= 120
    driver.commit.assert_called_once_with()


@pytest.mark.parametrize("raw", [b"hi", bytearray(b"hi"), memoryview(b"hi"), "6869"])
async def test_litestar_get_decodes_binary_and_preserves_permanent_session(
    monkeypatch: pytest.MonkeyPatch, raw: object
) -> None:
    config, driver = store_config(monkeypatch)
    driver.select_one_or_none.return_value = {"DATA": raw, "EXPIRES_AT": None}
    assert await SnowflakeStore(config).get("sid", renew_for=60) == b"hi"
    driver.execute.assert_not_called()


async def test_litestar_get_renews_finite_session(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    driver.select_one_or_none.return_value = {
        "data": b"payload",
        "expires_at": datetime.now(UTC) + timedelta(seconds=60),
    }
    assert await SnowflakeStore(config).get("sid", renew_for=300) == b"payload"
    sql, parameters = driver.execute.call_args.args
    assert sql == "UPDATE litestar_session SET expires_at = ? WHERE session_id = ?"
    assert parameters[1] == "sid"
    assert 298 <= (parameters[0] - datetime.now(UTC)).total_seconds() <= 300


async def test_litestar_ttl_missing_permanent_expired_and_future(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeStore(config)
    assert await store.get("missing") is None
    assert await store.exists("missing") is False
    assert await store.expires_in("missing") is None
    driver.select_one_or_none.return_value = {"expires_at": None}
    assert await store.expires_in("permanent") is None
    driver.select_one_or_none.return_value = {"expires_at": datetime.now(UTC) - timedelta(seconds=5)}
    assert await store.expires_in("expired") == 0
    driver.select_one_or_none.return_value = {"EXPIRES_AT": (datetime.now(UTC) + timedelta(seconds=90)).isoformat()}
    assert await store.exists("active") is True
    assert await store.expires_in("active") in {89, 90}


async def test_litestar_delete_operations_commit_and_count(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    store = SnowflakeStore(config)
    await store.delete("sid")
    driver.execute.assert_called_with("DELETE FROM litestar_session WHERE session_id = ?", ("sid",))
    await store.delete_all()
    driver.execute.assert_called_with("DELETE FROM litestar_session")
    driver.execute.return_value.get_affected_count.return_value = 3
    assert await store.delete_expired() == 3
    driver.execute.assert_called_with("DELETE FROM litestar_session WHERE expires_at <= CURRENT_TIMESTAMP()")
    assert driver.commit.call_count == 3
