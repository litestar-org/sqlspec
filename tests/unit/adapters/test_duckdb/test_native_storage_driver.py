"""Execute the native transfer statements with a counted in-memory connection."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import duckdb
import pytest

from sqlspec.adapters.duckdb._typing import DuckDBConnection
from sqlspec.adapters.duckdb.driver import DuckDBDriver
from sqlspec.exceptions import SQLSpecError
from sqlspec.storage import ResolvedStorageTarget

_FEATURES = {"storage_capabilities": {"arrow_export_enabled": True, "arrow_import_enabled": True}}


def test_native_export_keeps_builder_and_statement_filter_parameters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sqlspec import sql
    from sqlspec.core import LimitOffsetFilter

    path = str(tmp_path / "filtered.parquet")
    monkeypatch.setattr(
        "sqlspec.adapters.duckdb.driver._resolve_native_storage_target",
        lambda *args, **kwargs: ResolvedStorageTarget(path, "s3"),
    )
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE dataset AS SELECT i AS id FROM range(5) t(i)")
        driver = DuckDBDriver(connection, driver_features=_FEATURES)
        query = sql.select("id").from_("dataset").where("id > :minimum", minimum=0).order_by("id")
        job = driver.select_to_storage(query, "s3://bucket/filtered.parquet", LimitOffsetFilter(limit=2, offset=1))
        assert job.telemetry["rows_processed"] == 2
        assert connection.execute("SELECT * FROM read_parquet(?)", [path]).fetchall() == [(2,), (3,)]


@pytest.mark.parametrize("empty", [False, True])
def test_native_transfer_executes_one_statement_each(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, empty: bool
) -> None:
    path = str(tmp_path / "data.parquet")
    monkeypatch.setattr(
        "sqlspec.adapters.duckdb.driver._resolve_native_storage_target",
        lambda *args, **kwargs: ResolvedStorageTarget(path, "s3"),
    )
    with duckdb.connect() as connection:
        statements: list[str] = []

        def execute(sql: str, parameters: Any = None) -> Any:
            statements.append(sql)
            return connection.execute(sql, parameters)

        proxy = cast("DuckDBConnection", SimpleNamespace(execute=execute, fetchone=connection.fetchone))
        driver = DuckDBDriver(proxy, driver_features=_FEATURES)
        query = "SELECT :value AS value WHERE :include"
        exported = driver.select_to_storage(
            query,
            "alias://objects/data.parquet",
            {"value": "001", "include": not empty},
            partitioner={"label": "retained"},
        )
        assert len(statements) == 1
        assert exported.telemetry["rows_processed"] == int(not empty)
        assert exported.telemetry["extra"]["partitioner"] == {"label": "retained"}
        connection.execute("CREATE TABLE target(value VARCHAR)")
        imported = driver.load_from_storage("target", "alias://objects/data.parquet", file_format="parquet")
        assert len(statements) == 2
        assert imported.telemetry["rows_processed"] == int(not empty)
        assert connection.execute("SELECT * FROM target").fetchall() == ([] if empty else [("001",)])


@pytest.mark.parametrize("operation", ["read", "write"])
def test_native_failure_is_not_retried_through_arrow(monkeypatch: pytest.MonkeyPatch, operation: str) -> None:
    monkeypatch.setattr(
        "sqlspec.adapters.duckdb.driver._resolve_native_storage_target",
        lambda *args, **kwargs: ResolvedStorageTarget("s3://bucket/object.parquet", "s3"),
    )
    attempts: list[str] = []

    def execute(sql: str, parameters: Any = None) -> Any:
        attempts.append(sql)
        raise duckdb.IOException("storage authentication failed")

    def reject_arrow(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("native execution failure retried through Arrow")

    proxy = cast("DuckDBConnection", SimpleNamespace(execute=execute))
    driver = DuckDBDriver(proxy, driver_features=_FEATURES)
    monkeypatch.setattr(DuckDBDriver, "select_to_arrow", reject_arrow)
    monkeypatch.setattr(DuckDBDriver, "load_from_arrow", reject_arrow)
    with pytest.raises(SQLSpecError, match="authentication failed"):
        if operation == "write":
            driver.select_to_storage("SELECT 1", "s3://bucket/object.parquet")
        else:
            driver.load_from_storage("target", "s3://bucket/object.parquet", file_format="parquet")
    assert len(attempts) == 1


def test_custom_pipeline_and_arrow_schema_keep_existing_export(tmp_path: Path) -> None:
    import pyarrow as pa

    from sqlspec.storage import SyncStoragePipeline

    class CustomPipeline(SyncStoragePipeline):
        def __init__(self) -> None:
            super().__init__(storage_options={"write_options": {"delimiter": ";"}})

    class CustomDriver(DuckDBDriver):
        storage_pipeline_factory = CustomPipeline

    with duckdb.connect() as connection:
        driver = CustomDriver(connection, driver_features=_FEATURES)
        destination = tmp_path / "data.csv"
        driver.select_to_storage(
            "SELECT 1 AS a, 2 AS b",
            destination,
            format_hint="csv",
            arrow_schema=pa.schema([("a", pa.int32()), ("b", pa.int32())]),
        )
        assert destination.read_text() == '"a";"b"\n1;2\n'


def test_csv_import_preserves_arrow_inference(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "inference.csv"
    path.write_text('"123",code,date,empty,null\n1,001,2024-01-01,"",\n2,002,2024-01-02,"x",NA\n')

    def reject_native(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("CSV inference cannot safely select the native reader")

    monkeypatch.setattr("sqlspec.adapters.duckdb.driver._resolve_native_storage_target", reject_native)
    with duckdb.connect() as connection:
        connection.execute(
            'CREATE TABLE target("123" VARCHAR, code VARCHAR, date VARCHAR, empty VARCHAR, null_value VARCHAR)'
        )
        driver = DuckDBDriver(connection, driver_features=_FEATURES)
        driver.load_from_storage("target", path, file_format="csv")
        assert connection.execute("SELECT * FROM target").fetchall() == [
            ("1", "1", "2024-01-01", "", None),
            ("2", "2", "2024-01-02", "x", None),
        ]
