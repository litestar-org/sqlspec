"""Concrete ADK provider selection retains primary mutation consistency."""

import inspect
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk.store import adk_primary_read_scope
from sqlspec.utils.routing import RoutingContext


class _AsyncRows:
    def __aiter__(self) -> "_AsyncRows":
        return self

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


async def _invoke(function: Any, *args: Any, **kwargs: Any) -> Any:
    result = function(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


def _native_handle(adapter: str, asynchronous: bool) -> Any:
    connection = MagicMock()
    cursor = AsyncMock() if asynchronous else MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__aenter__.return_value = cursor
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = None
    cursor.rowcount = 0
    connection.test_cursor = cursor
    connection.cursor = (
        AsyncMock(return_value=cursor)
        if asynchronous and adapter in {"aiomysql", "mysqlconnector"}
        else MagicMock(return_value=cursor)
    )
    connection.__enter__.return_value = connection
    connection.__aenter__.return_value = connection
    connection.fetchall = AsyncMock(return_value=[]) if asynchronous else MagicMock(return_value=[])
    connection.fetchone = AsyncMock(return_value=None) if asynchronous else MagicMock(return_value=None)
    connection.commit = AsyncMock() if asynchronous else MagicMock()
    connection.rollback = AsyncMock() if asynchronous else MagicMock()
    result = MagicMock()
    result.rows_affected = 0
    result.get_affected_count.return_value = 0
    executed = result if adapter in {"db2", "snowflake"} else cursor if adapter == "duckdb" else "UPDATE 0"
    connection.execute = AsyncMock(return_value=executed) if asynchronous else MagicMock(return_value=executed)
    connection.data_dictionary.get_version = (
        AsyncMock(return_value=None) if asynchronous else MagicMock(return_value=None)
    )
    connection.fetchrow = AsyncMock(return_value=None)
    connection.fetchval = AsyncMock(return_value=None)
    if adapter == "psqlpy":
        result = MagicMock()
        result.result.return_value = []
        connection.fetch = AsyncMock(return_value=result)
    else:
        connection.fetch = AsyncMock(return_value=[])
    connection.select = AsyncMock(return_value=[]) if asynchronous else MagicMock(return_value=[])
    connection.select_one = AsyncMock(return_value=None) if asynchronous else MagicMock(return_value=None)
    connection.select_one_or_none = AsyncMock(return_value=None) if asynchronous else MagicMock(return_value=None)
    connection.execute_sql = AsyncMock(return_value=_AsyncRows()) if asynchronous else MagicMock(return_value=[])
    connection.query.return_value.result.return_value = []
    connection.query.return_value.num_dml_affected_rows = 0
    connection.is_in_transaction.return_value = False
    return connection


@pytest.mark.parametrize(
    ("adapter", "store_name", "config_name", "artifact"),
    [
        ("asyncpg", "AsyncpgADKStore", "AsyncpgConfig", False),
        ("asyncpg", "AsyncpgADKMemoryStore", "AsyncpgConfig", False),
        ("psycopg", "PsycopgAsyncADKStore", "PsycopgAsyncConfig", False),
        ("psycopg", "PsycopgSyncADKStore", "PsycopgSyncConfig", False),
        ("psycopg", "PsycopgAsyncADKMemoryStore", "PsycopgAsyncConfig", False),
        ("psycopg", "PsycopgSyncADKMemoryStore", "PsycopgSyncConfig", False),
        ("psqlpy", "PsqlpyADKStore", "PsqlpyConfig", False),
        ("psqlpy", "PsqlpyADKMemoryStore", "PsqlpyConfig", False),
        ("cockroach_asyncpg", "CockroachAsyncpgADKStore", "CockroachAsyncpgConfig", False),
        ("cockroach_asyncpg", "CockroachAsyncpgADKMemoryStore", "CockroachAsyncpgConfig", False),
        ("cockroach_psycopg", "CockroachPsycopgAsyncADKStore", "CockroachPsycopgAsyncConfig", False),
        ("cockroach_psycopg", "CockroachPsycopgSyncADKStore", "CockroachPsycopgSyncConfig", False),
        ("cockroach_psycopg", "CockroachPsycopgAsyncADKMemoryStore", "CockroachPsycopgAsyncConfig", False),
        ("cockroach_psycopg", "CockroachPsycopgSyncADKMemoryStore", "CockroachPsycopgSyncConfig", False),
        ("adbc", "AdbcADKStore", "AdbcConfig", False),
        ("adbc", "AdbcADKMemoryStore", "AdbcConfig", False),
        ("aiomysql", "AiomysqlADKStore", "AiomysqlConfig", False),
        ("aiomysql", "AiomysqlADKMemoryStore", "AiomysqlConfig", False),
        ("asyncmy", "AsyncmyADKStore", "AsyncmyConfig", False),
        ("asyncmy", "AsyncmyADKMemoryStore", "AsyncmyConfig", False),
        ("pymysql", "PyMysqlADKStore", "PyMysqlConfig", False),
        ("pymysql", "PyMysqlADKMemoryStore", "PyMysqlConfig", False),
        ("mssql_python", "MssqlPythonADKStore", "MssqlPythonConfig", False),
        ("mssql_python", "MssqlPythonADKMemoryStore", "MssqlPythonConfig", False),
        ("pymssql", "PymssqlADKStore", "PymssqlConfig", False),
        ("pymssql", "PymssqlADKMemoryStore", "PymssqlConfig", False),
        ("mysqlconnector", "MysqlConnectorAsyncADKStore", "MysqlConnectorAsyncConfig", False),
        ("mysqlconnector", "MysqlConnectorSyncADKStore", "MysqlConnectorSyncConfig", False),
        ("mysqlconnector", "MysqlConnectorAsyncADKMemoryStore", "MysqlConnectorAsyncConfig", False),
        ("mysqlconnector", "MysqlConnectorSyncADKMemoryStore", "MysqlConnectorSyncConfig", False),
        ("oracledb", "OracleAsyncADKStore", "OracleAsyncConfig", False),
        ("oracledb", "OracleSyncADKStore", "OracleSyncConfig", False),
        ("oracledb", "OracleAsyncADKMemoryStore", "OracleAsyncConfig", False),
        ("oracledb", "OracleSyncADKMemoryStore", "OracleSyncConfig", False),
        ("db2", "Db2SyncADKStore", "Db2SyncConfig", False),
        ("db2", "Db2AsyncADKStore", "Db2AsyncConfig", False),
        ("db2", "Db2SyncADKMemoryStore", "Db2SyncConfig", False),
        ("db2", "Db2AsyncADKMemoryStore", "Db2AsyncConfig", False),
        ("sqlanywhere", "SqlanywhereADKStore", "SqlanywhereConfig", False),
        ("sqlanywhere", "SqlanywhereADKMemoryStore", "SqlanywhereConfig", False),
        ("snowflake", "SnowflakeADKStore", "SnowflakeConfig", False),
        ("snowflake", "SnowflakeADKMemoryStore", "SnowflakeConfig", False),
        ("teradata", "TeradataADKStore", "TeradataConfig", False),
        ("teradata", "TeradataADKMemoryStore", "TeradataConfig", False),
        ("bigquery", "BigQueryADKStore", "BigQueryConfig", False),
        ("duckdb", "DuckDBADKStore", "DuckDBConfig", False),
        ("duckdb", "DuckDBADKMemoryStore", "DuckDBConfig", False),
        ("arrow_odbc", "ArrowOdbcADKStore", "ArrowOdbcConfig", False),
        ("arrow_odbc", "ArrowOdbcADKMemoryStore", "ArrowOdbcConfig", False),
        ("sybase", "SybaseADKStore", "SybaseConfig", False),
        ("sybase", "SybaseADKMemoryStore", "SybaseConfig", False),
        ("spanner", "SpannerSyncADKStore", "SpannerSyncConfig", False),
        ("spanner", "SpannerAsyncADKStore", "SpannerAsyncConfig", False),
        ("spanner", "SpannerSyncADKMemoryStore", "SpannerSyncConfig", False),
        ("spanner", "SpannerAsyncADKMemoryStore", "SpannerAsyncConfig", False),
        ("spanner", "SpannerSyncADKArtifactStore", "SpannerSyncConfig", True),
        ("spanner", "SpannerAsyncADKArtifactStore", "SpannerAsyncConfig", True),
    ],
)
async def test_adapter_store_offloads_only_pure_reads(
    monkeypatch: pytest.MonkeyPatch, adapter: str, store_name: str, config_name: str, artifact: bool
) -> None:
    module = pytest.importorskip(f"sqlspec.adapters.{adapter}.adk.{'artifact_store' if artifact else 'store'}")
    config_module = pytest.importorskip(f"sqlspec.adapters.{adapter}.config")
    config_type = getattr(config_module, config_name)
    store_type = getattr(module, store_name)
    options: dict[str, Any] = {"bind_key": "agents"}
    if adapter == "spanner":
        options["connection_config"] = {"project": "test", "instance_id": "test", "database_id": "test"}
    if adapter == "arrow_odbc":
        options["connection_config"] = {"driver": "ODBC Driver 18 for SQL Server", "server": "test", "database": "app"}
    if adapter == "bigquery":
        options["connection_config"] = {"project": "test"}
    writer = config_type(**options)
    reader = config_type(**options)
    asynchronous = writer.is_async
    calls: list[tuple[str, str]] = []
    handle = _native_handle(adapter, asynchronous)

    @contextmanager
    def sync_provider(config: Any, **kwargs: Any) -> Generator[Any, None, None]:
        label = "writer" if config is writer else "reader"
        calls.append((label, "enter"))
        try:
            yield handle
        finally:
            calls.append((label, "exit"))

    @asynccontextmanager
    async def async_provider(config: Any, **kwargs: Any) -> AsyncGenerator[Any, None]:
        with sync_provider(config, **kwargs) as connection:
            yield connection

    provider = async_provider if asynchronous else sync_provider
    monkeypatch.setattr(config_type, "provide_connection", provider)
    monkeypatch.setattr(config_type, "provide_session", provider)
    if adapter == "spanner":
        database = MagicMock()
        database.run_in_transaction = AsyncMock(return_value=0) if asynchronous else MagicMock(return_value=0)

        def get_database(config: Any) -> Any:
            calls.append(("writer" if config is writer else "reader", "database"))
            return database

        async def aget_database(config: Any) -> Any:
            return get_database(config)

        monkeypatch.setattr(config_type, "get_database", aget_database if asynchronous else get_database)
    store = store_type(writer, read_config=reader)
    assert store.config is writer
    assert store.read_config is reader
    memory = "MemoryStore" in store_name
    read_method = store.list_artifact_keys if artifact else store.search_entries if memory else store.list_sessions
    arguments = ("app", "user") if artifact or not memory else ("term", "app", "user")
    assert await _invoke(read_method, *arguments) == []
    assert calls[-2:] == [("reader", "enter"), ("reader", "exit")]
    if artifact:
        calls.clear()
        assert await _invoke(store.get_artifact, "app", "user", "artifact", version=None) is None
        assert calls[-2:] == [("reader", "enter"), ("reader", "exit")]
        calls.clear()
        assert await _invoke(store.list_artifact_versions, "app", "user", "artifact") == []
        assert calls[-2:] == [("reader", "enter"), ("reader", "exit")]
    elif not memory:
        calls.clear()
        assert await _invoke(store.get_session, "app", "user", "session") is None
        assert calls[-2:] == [("reader", "enter"), ("reader", "exit")]
        calls.clear()
        assert await _invoke(store.get_events, "app", "user", "session") == []
        assert calls[-2:] == [("reader", "enter"), ("reader", "exit")]
    calls.clear()
    with adk_primary_read_scope():
        assert await _invoke(read_method, *arguments) == []
    assert calls[-2:] == [("writer", "enter"), ("writer", "exit")]
    assert {label for label, action in calls} == {"writer"}
    calls.clear()
    with RoutingContext.primary("agents"):
        assert await _invoke(read_method, *arguments) == []
    assert calls[-2:] == [("writer", "enter"), ("writer", "exit")]
    assert {label for label, action in calls} == {"writer"}
    calls.clear()
    if artifact:
        await _invoke(store.get_next_version, "app", "user", "artifact")
    elif memory:
        await _invoke(store.delete_entries_by_session, "session")
    else:
        assert await _invoke(store.get_session, "app", "user", "session", renew_for=0) is None
    assert calls
    assert {label for label, action in calls} == {"writer"}

    if not memory and not artifact and adapter in {"psycopg", "aiomysql", "asyncmy", "mysqlconnector", "oracledb"}:
        calls.clear()
        with pytest.raises(RuntimeError, match="Failed to fetch created session"):
            await _invoke(store.create_session, "new-session", "app", "user", {})
        assert calls
        assert {label for label, action in calls} == {"writer"}
    calls.clear()
    writer.extension_config["adk"] = {"read_config_enabled": False}
    disabled = store_type(writer, read_config=reader)
    disabled_method = (
        disabled.list_artifact_keys if artifact else disabled.search_entries if memory else disabled.list_sessions
    )
    assert await _invoke(disabled_method, *arguments) == []
    assert calls[-2:] == [("writer", "enter"), ("writer", "exit")]
    assert {label for label, action in calls} == {"writer"}
    calls.clear()
    error = RuntimeError("reader unavailable")
    for operation in (
        handle.fetch,
        handle.fetchrow,
        handle.select,
        handle.execute_sql,
        handle.query,
        handle.execute,
        handle.test_cursor.execute,
    ):
        operation.side_effect = error
    with pytest.raises((RuntimeError, SQLSpecError), match="reader unavailable"):
        await _invoke(read_method, *arguments)
    assert calls[-2:] == [("reader", "enter"), ("reader", "exit")]
