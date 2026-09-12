"""Native Cockroach storage SQL and fallback contracts."""

from importlib import import_module
from typing import Any

import pytest


@pytest.mark.parametrize("adapter", ["cockroach_asyncpg", "cockroach_psycopg"])
def test_native_storage_commands_bind_values(adapter: str) -> None:
    core = import_module("sqlspec.adapters." + adapter + ".core")
    uri = "s3://bucket/O'Reilly?secret=do-not-inline"
    sql, params = core.build_native_export("SELECT 7 AS id", [], uri, "csv", {"nullas": "'NULL'"})
    assert uri not in sql and "'NULL'" not in sql
    assert params == [uri, "'NULL'"]
    assert "EXPORT INTO CSV" in sql and "nullas" in sql
    sql, params = core.build_native_import('public."Odd Table"', uri, "csv", {"skip": 0, "nullif": "'NULL'"})
    assert '"public"."Odd Table"' in sql
    assert params == [uri, "0", "'NULL'"]
    assert uri not in sql and "'NULL'" not in sql


@pytest.mark.parametrize("adapter", ["cockroach_asyncpg", "cockroach_psycopg"])
def test_native_storage_rejects_empty_table(adapter: str) -> None:
    core = import_module("sqlspec.adapters." + adapter + ".core")
    with pytest.raises(ValueError, match="Table"):
        core.build_native_import("", "s3://bucket/file", "parquet", {})


@pytest.mark.parametrize("adapter", ["cockroach_asyncpg", "cockroach_psycopg"])
def test_native_storage_measured_metadata(adapter: str) -> None:
    core = import_module("sqlspec.adapters." + adapter + ".core")
    rows: list[dict[str, Any]] = [
        {"filename": "a.parquet", "rows": 2, "bytes": 12},
        {"filename": "b.parquet", "rows": 3, "bytes": 19},
    ]
    telemetry = core.native_export_telemetry(rows, "s3://bucket/prefix", "s3", "parquet")
    assert telemetry["rows_processed"] == 5
    assert telemetry["bytes_processed"] == 31
    assert telemetry["extra"]["files"] == ["a.parquet", "b.parquet"]
    imported = core.native_import_telemetry(
        [{"job_id": 42, "status": "succeeded", "rows": 5, "bytes": 900}], "target", "s3", "parquet"
    )
    assert imported["rows_processed"] == 5
    assert "bytes_processed" not in imported
    assert imported["extra"]["job_id"] == 42


@pytest.fixture(params=["asyncpg", "sync", "async"])
def native_driver(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Any:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
    from sqlspec.storage.pipeline import ResolvedStorageTarget

    cls = {"asyncpg": CockroachAsyncpgDriver, "sync": CockroachPsycopgSyncDriver, "async": CockroachPsycopgAsyncDriver}[
        request.param
    ]
    connection = MagicMock()
    connection.autocommit = True
    connection.info.transaction_status = 0
    connection.is_in_transaction.return_value = False
    driver = cls(connection, driver_features={"enable_native_storage": True})
    execute = MagicMock() if request.param == "sync" else AsyncMock()
    monkeypatch.setattr(cls, "_execute_native_storage", execute)
    pipeline = SimpleNamespace(
        resolve_destination=MagicMock(return_value=ResolvedStorageTarget("s3://bucket/O'Reilly", "s3"))
    )
    monkeypatch.setattr(cls, "_storage_pipeline", lambda self: pipeline)
    parent = cls.__bases__[0]
    fallback = MagicMock(return_value="inherited") if request.param == "sync" else AsyncMock(return_value="inherited")
    monkeypatch.setattr(parent, "select_to_storage", fallback)
    monkeypatch.setattr(parent, "load_from_storage", fallback)
    return SimpleNamespace(driver=driver, execute=execute, fallback=fallback, pipeline=pipeline, connection=connection)


async def _result(value: Any) -> Any:
    from inspect import isawaitable

    return await value if isawaitable(value) else value


async def test_native_driver_export_parameters_and_metadata(native_driver: Any) -> None:
    native_driver.execute.return_value = [{"filename": "a.csv", "rows": 1, "bytes": 19}]
    native_driver.driver.driver_features["native_storage_csv_options"] = {"nullas": "NULL"}
    result = await _result(
        native_driver.driver.select_to_storage(
            "SELECT :value::STRING AS label",
            "alias://remote/out",
            {"value": "O'Reilly"},
            format_hint="csv",
            partitioner={"x": 1},
            telemetry={"correlation_id": "original"},
        )
    )
    sql, values = native_driver.execute.call_args.args
    assert "O'Reilly" not in sql and "NULL" not in sql
    assert sorted(values) == sorted(["O'Reilly", "s3://bucket/O'Reilly", "NULL"])
    assert result.telemetry["extra"]["files"] == ["a.csv"]
    assert result.telemetry["extra"]["partitioner"] == {"x": 1}
    assert result.telemetry["extra"]["source"] == {"correlation_id": "original"}
    native_driver.execute.assert_called_once()
    native_driver.fallback.assert_not_called()


@pytest.mark.parametrize("reason", ["disabled", "local", "format", "transaction", "nonselect"])
async def test_native_export_fallback_before_sql(native_driver: Any, reason: str) -> None:
    from sqlspec.storage.pipeline import ResolvedStorageTarget

    if reason == "disabled":
        native_driver.driver.driver_features["enable_native_storage"] = False
    if reason == "local":
        native_driver.pipeline.resolve_destination.return_value = ResolvedStorageTarget("file:///tmp/file", "file")
    if reason == "transaction":
        native_driver.connection.info.transaction_status = 2
        native_driver.connection.is_in_transaction.return_value = True
    query = "DELETE FROM test" if reason == "nonselect" else "SELECT 1"
    result = await _result(
        native_driver.driver.select_to_storage(
            query, "alias://target/out", format_hint="json" if reason == "format" else "parquet"
        )
    )
    assert result == "inherited"
    native_driver.execute.assert_not_called()
    native_driver.fallback.assert_called_once()


@pytest.mark.parametrize(
    "file_format,options", [("parquet", {}), ("csv", {"skip": 0}), ("csv", {"skip": 1, "nullif": "NULL"})]
)
async def test_native_driver_import_options(native_driver: Any, file_format: str, options: dict[str, Any]) -> None:
    native_driver.driver.driver_features["native_storage_csv_options"] = options
    native_driver.execute.return_value = [{"job_id": 22, "status": "succeeded", "rows": 2}]
    result = await _result(
        native_driver.driver.load_from_storage(
            'public."Odd Table"', "alias://remote/file", file_format=file_format, partitioner={"p": 1}
        )
    )
    sql, values = native_driver.execute.call_args.args
    assert values == ["s3://bucket/O'Reilly", *[str(options[k]) for k in ("skip", "nullif") if k in options]]
    assert '"public"."Odd Table"' in sql
    assert result.telemetry["extra"]["job_id"] == 22
    assert result.telemetry["extra"]["partitioner"] == {"p": 1}
    native_driver.execute.assert_called_once()


@pytest.mark.parametrize("reason", ["disabled", "local", "format", "transaction", "overwrite", "csv"])
async def test_native_import_fallback_before_sql(native_driver: Any, reason: str) -> None:
    from sqlspec.storage.pipeline import ResolvedStorageTarget

    if reason == "disabled":
        native_driver.driver.driver_features["enable_native_storage"] = False
    if reason == "local":
        native_driver.pipeline.resolve_destination.return_value = ResolvedStorageTarget("file:///tmp/file", "file")
    if reason == "transaction":
        native_driver.connection.info.transaction_status = 2
        native_driver.connection.is_in_transaction.return_value = True
    result = await _result(
        native_driver.driver.load_from_storage(
            "target",
            "alias://source/file",
            file_format={"format": "json", "csv": "csv"}.get(reason, "parquet"),
            overwrite=reason == "overwrite",
        )
    )
    assert result == "inherited"
    native_driver.execute.assert_not_called()
    native_driver.fallback.assert_called_once()


@pytest.mark.parametrize("operation", ["export", "import"])
@pytest.mark.parametrize("failure", ["execute", "result"])
async def test_native_failures_never_replay(native_driver: Any, operation: str, failure: str) -> None:
    if failure == "execute":
        native_driver.execute.side_effect = RuntimeError("native failed")
    else:
        native_driver.execute.return_value = [{}]
    with pytest.raises((RuntimeError, KeyError)):
        value = (
            native_driver.driver.select_to_storage("SELECT 1", "s3://bucket/out")
            if operation == "export"
            else native_driver.driver.load_from_storage("target", "s3://bucket/in", file_format="parquet")
        )
        await _result(value)
    native_driver.execute.assert_called_once()
    native_driver.fallback.assert_not_called()


@pytest.mark.parametrize("adapter", ["cockroach_asyncpg", "cockroach_psycopg"])
def test_native_storage_failed_job_metadata(adapter: str) -> None:
    core = import_module("sqlspec.adapters." + adapter + ".core")
    with pytest.raises(ValueError, match="did not succeed"):
        core.native_import_telemetry([{"status": "failed"}], "target", "s3", "parquet")


async def test_native_driver_invalid_table_before_sql(native_driver: Any) -> None:
    with pytest.raises(ValueError, match="Table"):
        await _result(native_driver.driver.load_from_storage("", "s3://bucket/in", file_format="parquet"))
    native_driver.execute.assert_not_called()
    native_driver.fallback.assert_not_called()


@pytest.mark.parametrize("adapter", ["asyncpg", "sync", "async"])
async def test_native_database_failure_mapped_once(adapter: str) -> None:
    from unittest.mock import AsyncMock, MagicMock

    import asyncpg
    import psycopg

    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
    from sqlspec.exceptions import SQLSpecError

    connection = MagicMock()
    if adapter == "asyncpg":
        connection.fetch = AsyncMock(side_effect=asyncpg.PostgresError("native refused"))
        driver: Any = CockroachAsyncpgDriver(connection)
        execute = connection.fetch
    else:
        cursor = MagicMock()
        connection.cursor.return_value = cursor
        if adapter == "async":
            cursor.execute = AsyncMock(side_effect=psycopg.DatabaseError("native refused"))
            cursor.close = AsyncMock()
            driver = CockroachPsycopgAsyncDriver(connection)
        else:
            cursor.execute.side_effect = psycopg.DatabaseError("native refused")
            driver = CockroachPsycopgSyncDriver(connection)
        execute = cursor.execute
    with pytest.raises(SQLSpecError):
        await _result(driver._execute_native_storage("EXPORT INTO PARQUET placeholder FROM SELECT 1", ["uri"]))
    execute.assert_called_once()


async def test_native_export_after_compiled_cache_warmup(native_driver: Any) -> None:
    from sqlspec.core import SQL

    query = "SELECT :native_cached_value::INT8 AS value"
    warm = SQL(query, {"native_cached_value": 8}, statement_config=native_driver.driver.statement_config)
    native_driver.driver._compiled_sql(warm, warm.statement_config)
    native_driver.execute.return_value = [{"filename": "cached.parquet", "rows": 1, "bytes": 12}]
    result = await _result(native_driver.driver.select_to_storage(query, "s3://bucket/out", {"native_cached_value": 8}))
    assert result.telemetry["rows_processed"] == 1
    native_driver.execute.assert_called_once()
    native_driver.fallback.assert_not_called()


def test_native_psycopg_percent_literals_remain_literal() -> None:
    from sqlspec.adapters.cockroach_psycopg.core import build_native_export, build_native_import

    query, values = build_native_export("SELECT '100%'::STRING AS label", [], "s3://bucket/out", "parquet", {})
    assert "'100%%'" in query
    assert values == ["s3://bucket/out"]
    query, values = build_native_import('"100% table"', "s3://bucket/in", "parquet", {})
    assert '"100%% table"' in query
    assert values == ["s3://bucket/in"]


async def test_native_export_resolves_real_remote_and_local_aliases(
    native_driver: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    from sqlspec.storage import StorageRegistry, SyncStoragePipeline

    registry = StorageRegistry()
    registry.register_alias("remote", "s3://bucket/base", backend="fsspec", key="test", secret="test")
    registry.register_alias("local", str(tmp_path), backend="local")
    pipeline = SyncStoragePipeline(registry=registry)
    monkeypatch.setattr(type(native_driver.driver), "_storage_pipeline", lambda self: pipeline)
    native_driver.execute.return_value = [{"filename": "out.parquet", "rows": 1, "bytes": 10}]
    job = await _result(native_driver.driver.select_to_storage("SELECT 1", "alias://remote/prefix"))
    assert job.telemetry["destination"] == "s3://bucket/base/prefix"
    assert native_driver.execute.call_args.args[1] == ["s3://bucket/base/prefix"]
    assert await _result(native_driver.driver.select_to_storage("SELECT 1", "alias://local/out.parquet")) == "inherited"
    native_driver.execute.assert_called_once()
    native_driver.fallback.assert_called_once()


def test_native_benchmark_cli_omits_failure_credentials(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from unittest.mock import Mock

    from tools.scripts import bench_cockroach_storage

    monkeypatch.setattr("sys.argv", ["bench_cockroach_storage"])
    monkeypatch.setattr(bench_cockroach_storage, "run_benchmark", Mock(side_effect=RuntimeError("secret-access-key")))
    with pytest.raises(SystemExit) as exc:
        bench_cockroach_storage.main()
    assert exc.value.code == 1
    output = capsys.readouterr()
    assert "RuntimeError" in output.err
    assert "secret-access-key" not in output.err


@pytest.mark.parametrize("query", ["SELECT 1 -- trailing comment", "SELECT ';' AS value -- trailing comment"])
async def test_native_export_preserves_comments_and_literal_delimiters(native_driver: Any, query: str) -> None:
    native_driver.execute.return_value = [{"filename": "out.parquet", "rows": 1, "bytes": 10}]
    await _result(native_driver.driver.select_to_storage(query, "s3://bucket/out"))
    command = native_driver.execute.call_args.args[0]
    assert command.endswith("\n)")
    assert "-- trailing comment" in command
    assert "; -- trailing" not in command


async def test_native_export_raw_script_falls_back(native_driver: Any) -> None:
    assert await _result(native_driver.driver.select_to_storage("SELECT 1; SELECT 2", "s3://bucket/out")) == "inherited"
    native_driver.execute.assert_not_called()


@pytest.mark.parametrize("adapter", ["cockroach_asyncpg", "cockroach_psycopg"])
def test_native_export_delimiter_normalization(adapter: str) -> None:
    core = import_module("sqlspec.adapters." + adapter + ".core")
    assert core.normalize_native_export_query("SELECT ';' AS value; -- tail") == "SELECT ';' AS value -- tail"
    assert core.normalize_native_export_query("SELECT 1; SELECT 2") is None
