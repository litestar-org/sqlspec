"""Unit tests for BigQuery Arrow streaming export paths."""

from collections.abc import Iterator
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.driver import SyncDriverAdapterBase

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery._typing import BigQueryConnection


class _FakeRowIterator:
    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.arrow_iterable_calls: list[object | None] = []

    def to_arrow_iterable(self, *, bqstorage_client: object | None = None) -> Iterator[pa.RecordBatch]:
        self.arrow_iterable_calls.append(bqstorage_client)
        return iter(self.table.to_batches(max_chunksize=2))


class _FakeQueryJob:
    statement_type = "SELECT"
    schema: list[object] = []

    def __init__(self, table: pa.Table) -> None:
        self.row_iterator = _FakeRowIterator(table)
        self.result_calls: list[dict[str, object]] = []

    def result(self, **kwargs: object) -> _FakeRowIterator:
        self.result_calls.append(kwargs)
        return self.row_iterator

    def to_arrow(self) -> pa.Table:
        msg = "reader formats should use RowIterator.to_arrow_iterable(), not QueryJob.to_arrow()"
        raise AssertionError(msg)


class _FakeBigQueryConnection:
    def __init__(self, job: _FakeQueryJob) -> None:
        self.job = job
        self._connection = SimpleNamespace(API_BASE_URL="https://bigquery.googleapis.com")
        self.query_calls: list[tuple[str, dict[str, object]]] = []

    def query(self, sql: str, **kwargs: object) -> _FakeQueryJob:
        self.query_calls.append((sql, kwargs))
        return self.job

    def _ensure_bqstorage_client(self) -> object:
        raise RuntimeError("storage client unavailable")


def test_select_to_arrow_reader_uses_row_iterator_even_without_storage_api(monkeypatch: pytest.MonkeyPatch) -> None:
    table = pa.table({"x": [1, 2, 3]})
    job = _FakeQueryJob(table)
    connection = _FakeBigQueryConnection(job)
    driver = BigQueryDriver(cast("BigQueryConnection", connection))

    monkeypatch.setattr("sqlspec.adapters.bigquery.driver.storage_api_available", lambda: False)

    def fallback(*_args: object, **_kwargs: object) -> object:
        msg = "reader formats should not fall back to row conversion"
        raise AssertionError(msg)

    monkeypatch.setattr(SyncDriverAdapterBase, "select_to_arrow", fallback)

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="reader", batch_size=2)

    assert result.rows_affected == -1
    assert result.get_data().read_all().to_pydict() == {"x": [1, 2, 3]}
    assert job.result_calls[0]["page_size"] == 2
    assert job.row_iterator.arrow_iterable_calls == [None]


def test_select_to_arrow_table_uses_conversion_path_for_local_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    table = pa.table({"x": [1, 2, 3]})
    job = _FakeQueryJob(table)
    connection = _FakeBigQueryConnection(job)
    connection._connection = SimpleNamespace(API_BASE_URL="http://127.0.0.1:9050")
    driver = BigQueryDriver(cast("BigQueryConnection", connection))
    fallback_result = object()
    fallback_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    monkeypatch.setattr("sqlspec.adapters.bigquery.driver.storage_api_available", lambda: True)

    def fallback(*args: object, **kwargs: object) -> object:
        fallback_calls.append((args, kwargs))
        return fallback_result

    monkeypatch.setattr(SyncDriverAdapterBase, "select_to_arrow", fallback)

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="table")

    assert result is fallback_result
    assert fallback_calls
    assert job.result_calls == []
