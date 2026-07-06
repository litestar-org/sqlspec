"""Unit tests for DuckDB Arrow streaming paths."""

from collections.abc import Iterator
from contextlib import contextmanager
from uuid import UUID

import pyarrow as pa
import pytest

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.driver import DuckDBDriver
from sqlspec.core.result import DMLResult
from sqlspec.exceptions import MissingDependencyError


@contextmanager
def _seed_driver() -> Iterator[DuckDBDriver]:
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute_script("""
            CREATE OR REPLACE TABLE arrow_streaming (id INTEGER, name VARCHAR);
            INSERT INTO arrow_streaming VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta'), (5, 'epsilon');
        """)
        yield driver


def test_select_to_arrow_reader_honors_batch_size() -> None:
    with _seed_driver() as driver:
        result = driver.select_to_arrow(
            "SELECT id, name FROM arrow_streaming ORDER BY id", return_format="reader", batch_size=2
        )

    assert result.rows_affected == -1
    assert isinstance(result.data, pa.RecordBatchReader)
    batches = list(result.data)
    assert [batch.num_rows for batch in batches] == [2, 2, 1]


def test_select_to_arrow_batches_uses_reader_batch_boundaries() -> None:
    with _seed_driver() as driver:
        result = driver.select_to_arrow(
            "SELECT id, name FROM arrow_streaming ORDER BY id", return_format="batches", batch_size=2
        )

    batches = result.get_data()
    assert [batch.num_rows for batch in batches] == [2, 2, 1]
    assert result.rows_affected == 5


def test_load_from_arrow_registers_record_batch_reader() -> None:
    with _seed_driver() as driver:
        driver.execute("CREATE OR REPLACE TABLE reader_target (id INTEGER, name VARCHAR)")
        table = pa.table({"id": [10, 11], "name": ["ten", "eleven"]})
        reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=1))

        job = driver.load_from_arrow("reader_target", reader)

        assert job.telemetry["rows_processed"] == 2
        result = driver.execute("SELECT id, name FROM reader_target ORDER BY id")
        assert result.get_data() == [{"id": 10, "name": "ten"}, {"id": 11, "name": "eleven"}]


def test_execute_many_uses_bulk_insert_fast_path() -> None:
    with _seed_driver() as driver:
        driver.execute("CREATE OR REPLACE TABLE bulk_target (id INTEGER, name VARCHAR)")

        result = driver.execute_many(
            "INSERT INTO bulk_target (id, name) VALUES (?, ?)", [(1, "one"), (2, "two"), (3, "three")]
        )

        assert isinstance(result, DMLResult)
        assert result.operation_type == "INSERT"
        assert result.rows_affected == 3
        assert driver.execute("SELECT id, name FROM bulk_target ORDER BY id").get_data() == [
            {"id": 1, "name": "one"},
            {"id": 2, "name": "two"},
            {"id": 3, "name": "three"},
        ]


def test_select_arrow_path_restores_uuid_columns_only() -> None:
    uuid_value = "550e8400-e29b-41d4-a716-446655440000"
    with _seed_driver() as driver:
        driver.execute("CREATE OR REPLACE TABLE uuid_target (id UUID, text_id VARCHAR)")
        driver.execute("INSERT INTO uuid_target VALUES (?, ?)", uuid_value, uuid_value)

        row = driver.select_one("SELECT id, text_id FROM uuid_target")

    assert isinstance(row["id"], UUID)
    assert str(row["id"]) == uuid_value
    assert row["text_id"] == uuid_value
    assert isinstance(row["text_id"], str)


def test_missing_pyarrow_raises_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    with _seed_driver() as driver:

        def raise_missing_pyarrow() -> None:
            raise MissingDependencyError("pyarrow")

        monkeypatch.setattr("sqlspec.adapters.duckdb.driver.ensure_pyarrow", raise_missing_pyarrow)

        with pytest.raises(MissingDependencyError):
            driver.select_to_arrow("SELECT id FROM arrow_streaming", return_format="reader")
