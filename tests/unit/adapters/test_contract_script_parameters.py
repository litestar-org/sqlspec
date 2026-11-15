"""Per-statement script parameter binding across adapters."""

import importlib
import inspect
from enum import Enum
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec import SQL
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from tests.conftest import requires_interpreted

pytestmark = requires_interpreted

VALUE_V = "it's a\\b"


class Status(str, Enum):  # noqa: UP042
    ACTIVE = "active"


SCRIPT = "CREATE TABLE t(v TEXT); INSERT INTO t VALUES (:v); INSERT INTO t VALUES (:w)"


async def _run(result: Any) -> Any:
    return await result if inspect.isawaitable(result) else result


def _config(kind: str, tmp_path: Path) -> Any:
    database = str(tmp_path / f"{kind}.db")
    if kind == "sqlite":
        return SqliteConfig(connection_config={"database": database})
    if kind == "aiosqlite":
        return AiosqliteConfig(connection_config={"database": database})
    if kind == "duckdb":
        return DuckDBConfig(connection_config={"database": database})
    return AdbcConfig(connection_config={"driver_name": "adbc_driver_sqlite", "uri": database})


@pytest.mark.parametrize("kind", ["sqlite", "aiosqlite", "duckdb", "adbc-sqlite"])
async def test_script_parameters_bind_per_statement(kind: str, tmp_path: Path) -> None:
    config = _config(kind, tmp_path)
    try:
        if kind == "aiosqlite":
            async with config.provide_session() as session:
                await session.execute_script(SCRIPT, v=VALUE_V, w=Status.ACTIVE)
                rows = (await session.execute("SELECT v FROM t ORDER BY rowid")).get_data()
        else:
            with config.provide_session() as session:
                session.execute_script(SCRIPT, v=VALUE_V, w=Status.ACTIVE)
                rows = session.execute("SELECT v FROM t ORDER BY rowid").get_data()
    finally:
        await _run(config.close_pool())

    assert [row["v"] for row in rows] == [VALUE_V, "active"]


def _driver(adapter: str, class_name: str) -> "tuple[Any, Any]":
    driver_module = importlib.import_module(f"sqlspec.adapters.{adapter}.driver")
    core_module = importlib.import_module(f"sqlspec.adapters.{adapter}.core")
    config = core_module.default_statement_config
    driver = getattr(driver_module, class_name)(MagicMock(), statement_config=config)
    return driver, config


def _recording_cursor() -> MagicMock:
    cursor = MagicMock()
    cursor.description = None
    cursor.rowcount = 1
    return cursor


DRIVERS = [
    ("db2", "Db2SyncDriver"),
    ("pymssql", "PymssqlDriver"),
    ("sybase", "SybaseDriver"),
    ("sqlanywhere", "SqlanywhereDriver"),
    ("teradata", "TeradataDriver"),
    ("snowflake", "SnowflakeDriver"),
    ("oracledb", "OracleSyncDriver"),
]


def _executed_parameters(cursor: MagicMock) -> "list[Any]":
    executed: list[Any] = []
    for call in cursor.execute.call_args_list:
        params = call.args[1] if len(call.args) > 1 else call.kwargs.get("parameters")
        if isinstance(params, dict):
            params = list(params.values())
        executed.append(tuple(params) if isinstance(params, (list, tuple)) else params)
    return executed


@pytest.mark.parametrize(("adapter", "class_name"), DRIVERS, ids=[name for name, _ in DRIVERS])
def test_script_positional_parameters_slice_by_scanner(adapter: str, class_name: str) -> None:
    driver, config = _driver(adapter, class_name)
    cursor = _recording_cursor()
    statement = SQL(
        "INSERT INTO t VALUES ('?', ?); INSERT INTO t VALUES (?)", (1, 2), statement_config=config
    ).as_script()

    driver.dispatch_execute_script(cursor, statement)

    assert _executed_parameters(cursor) == [(1,), (2,)]


@pytest.mark.parametrize(
    ("adapter", "class_name"),
    [("db2", "Db2SyncDriver"), ("oracledb", "OracleSyncDriver"), ("snowflake", "SnowflakeDriver")],
    ids=["db2", "oracledb", "snowflake"],
)
def test_script_inlines_non_dml_pieces(adapter: str, class_name: str) -> None:
    driver, config = _driver(adapter, class_name)
    cursor = _recording_cursor()
    statement = SQL("SET x = :s; INSERT INTO t VALUES (:v)", statement_config=config, s="APP", v=1).as_script()

    driver.dispatch_execute_script(cursor, statement)

    first, second = cursor.execute.call_args_list
    assert "'APP'" in first.args[0]
    assert not (first.args[1:] and first.args[1])
    assert second.args[1] in ((1,), [1], {"v": 1}, {"1": 1})


def _bigquery_driver(monkeypatch: pytest.MonkeyPatch) -> "tuple[Any, MagicMock]":
    driver, _ = _driver("bigquery", "BigQueryDriver")
    captured = MagicMock()
    job = MagicMock()
    job.num_dml_affected_rows = 0
    captured.return_value = job
    monkeypatch.setattr(
        type(driver), "_run_query_job", lambda self, cursor, sql, parameters: captured(cursor, sql, parameters)
    )
    return driver, captured


def test_bigquery_script_binds_job_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    driver, run_job = _bigquery_driver(monkeypatch)
    config = driver.statement_config
    statement = SQL("INSERT INTO d.t VALUES (@a); SELECT @a", statement_config=config, a="x'y").as_script()

    driver.dispatch_execute_script(MagicMock(), statement)

    _, sql, parameters = run_job.call_args.args
    assert "x'y" not in sql
    assert parameters


def test_bigquery_script_inlines_when_non_dml_has_placeholders(monkeypatch: pytest.MonkeyPatch) -> None:
    driver, run_job = _bigquery_driver(monkeypatch)
    config = driver.statement_config
    statement = SQL(
        "CREATE TABLE d.t (c STRING DEFAULT @a); INSERT INTO d.t VALUES (@a)", statement_config=config, a="x'y"
    ).as_script()

    driver.dispatch_execute_script(MagicMock(), statement)

    _, sql, parameters = run_job.call_args.args
    assert "'x\\'y'" in sql
    assert not parameters


def test_spanner_script_binds_dml_and_batches_ddl(monkeypatch: pytest.MonkeyPatch) -> None:
    driver_module = importlib.import_module("sqlspec.adapters.spanner.driver")
    driver, config = _driver("spanner", "SpannerSyncDriver")
    ddl_batches: list[list[str]] = []
    monkeypatch.setattr(driver_module, "supports_write", lambda _cursor: True)
    monkeypatch.setattr(
        driver_module, "execute_ddl_sync", lambda _database, statements, timeout=None: ddl_batches.append(statements)
    )
    monkeypatch.setattr(type(driver), "_commit_before_ddl", lambda self: None)
    monkeypatch.setattr(type(driver), "_resolve_database", lambda self: None)
    cursor = MagicMock()
    statement = SQL(
        "INSERT INTO t (v) VALUES (@v); CREATE TABLE u (c STRING(10) DEFAULT ('x'))", statement_config=config, v="a'b"
    ).as_script()

    driver.dispatch_execute_script(cursor, statement)

    update_call = cursor.execute_update.call_args
    assert "a'b" not in update_call.args[0]
    assert update_call.kwargs["params"] == {"v": "a'b"}
    assert len(ddl_batches) == 1
