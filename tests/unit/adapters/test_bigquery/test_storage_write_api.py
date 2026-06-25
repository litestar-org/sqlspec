"""BigQuery opt-in Storage Write API Arrow transport for load_from_arrow."""

from typing import Any, cast

import pyarrow as pa
import pytest

from sqlspec.adapters.bigquery.driver import BigQueryDriver

CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": ["fixed"],
}


class _ParquetJob:
    def __init__(self) -> None:
        self.statement_type = "LOAD"
        self.job_id = "job_1"
        self.started = None
        self.ended = None
        self._properties: dict[str, Any] = {}

    def result(self, **_kwargs: Any) -> "_ParquetJob":
        return self


class _Connection:
    def __init__(self) -> None:
        self.project = "proj"
        self._credentials = None
        self.load_file_calls: list[Any] = []
        self.load_job = _ParquetJob()

    def load_table_from_file(self, file_obj: Any, destination: Any, **kwargs: Any) -> _ParquetJob:
        self.load_file_calls.append((destination, kwargs))
        return self.load_job


class _Error:
    code = 0


class _AppendResponse:
    error = _Error()


class _WriteStream:
    def __init__(self, name: str) -> None:
        self.name = name


class _CommitResponse:
    stream_errors: list[Any] = []


class _FakeWriteClient:
    instances: list["_FakeWriteClient"] = []

    def __init__(self, **_kwargs: Any) -> None:
        self.create_calls: list[Any] = []
        self.append_request_batches: list[list[Any]] = []
        self.finalize_calls: list[str] = []
        self.commit_calls: list[Any] = []
        _FakeWriteClient.instances.append(self)

    def create_write_stream(self, *, parent: str, write_stream: Any) -> _WriteStream:
        self.create_calls.append((parent, write_stream))
        return _WriteStream(f"{parent}/streams/s1")

    def append_rows(self, requests: Any) -> list[_AppendResponse]:
        self.append_request_batches.append(list(requests))
        return [_AppendResponse()]

    def finalize_write_stream(self, *, name: str) -> None:
        self.finalize_calls.append(name)

    def batch_commit_write_streams(self, *, request: Any) -> _CommitResponse:
        self.commit_calls.append((request.parent, list(request.write_streams)))
        return _CommitResponse()


@pytest.fixture(autouse=True)
def _reset_fake() -> None:
    _FakeWriteClient.instances.clear()


def _patch_write_client(monkeypatch: pytest.MonkeyPatch) -> None:
    import google.cloud.bigquery_storage_v1 as bqs

    monkeypatch.setattr(bqs, "BigQueryWriteClient", _FakeWriteClient)


def test_storage_write_api_orchestration(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_write_client(monkeypatch)
    connection = _Connection()
    driver = BigQueryDriver(
        cast("Any", connection),
        driver_features={"enable_storage_write_api": True, "storage_capabilities": CAPABILITIES},
    )

    job = driver.load_from_arrow("dataset.table", pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))

    assert connection.load_file_calls == []
    client = _FakeWriteClient.instances[0]
    assert client.create_calls
    parent, _stream = client.create_calls[0]
    assert parent == "projects/proj/datasets/dataset/tables/table"
    assert client.append_request_batches and client.append_request_batches[0]
    assert client.append_request_batches[0][0].write_stream == "projects/proj/datasets/dataset/tables/table/streams/s1"
    assert client.finalize_calls == ["projects/proj/datasets/dataset/tables/table/streams/s1"]
    assert client.commit_calls
    assert job.telemetry["rows_processed"] == 3


def test_storage_write_api_overwrite_uses_parquet(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_write_client(monkeypatch)
    connection = _Connection()
    driver = BigQueryDriver(
        cast("Any", connection),
        driver_features={"enable_storage_write_api": True, "storage_capabilities": CAPABILITIES},
    )

    driver.load_from_arrow("dataset.table", pa.table({"id": [1]}), overwrite=True)

    assert connection.load_file_calls
    assert _FakeWriteClient.instances == []


def test_feature_off_uses_parquet(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_write_client(monkeypatch)
    connection = _Connection()
    driver = BigQueryDriver(cast("Any", connection), driver_features={"storage_capabilities": CAPABILITIES})

    driver.load_from_arrow("dataset.table", pa.table({"id": [1]}))

    assert connection.load_file_calls
    assert _FakeWriteClient.instances == []


def test_import_failure_falls_back_to_parquet(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _Connection()
    driver = BigQueryDriver(
        cast("Any", connection),
        driver_features={"enable_storage_write_api": True, "storage_capabilities": CAPABILITIES},
    )

    def _raise(_self: Any, _table: str, _arrow: Any) -> Any:
        raise ImportError("bigquery_storage not available")

    monkeypatch.setattr(BigQueryDriver, "_load_arrow_via_storage_write_api", _raise)

    driver.load_from_arrow("dataset.table", pa.table({"id": [1]}))

    assert connection.load_file_calls
