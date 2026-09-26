"""BigQuery opt-in Storage Write API Arrow transport for load_from_arrow."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pytest
from google.cloud.bigquery_storage_v1 import types

import sqlspec.adapters.bigquery.config as bigquery_config
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.bigquery.core import build_arrow_write_stream_payload
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.exceptions import StorageOperationFailedError

CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
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
        driver_features={
            "enable_storage_write_api": True,
            "storage_write_stream_type": "PENDING",
            "storage_capabilities": CAPABILITIES,
        },
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


def test_storage_write_api_uses_committed_stream(monkeypatch: pytest.MonkeyPatch) -> None:
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
    parent, stream = client.create_calls[0]
    assert parent == "projects/proj/datasets/dataset/tables/table"
    assert stream.type_ == types.WriteStream.Type.COMMITTED
    assert client.append_request_batches and client.append_request_batches[0]
    assert client.finalize_calls == []
    assert client.commit_calls == []
    assert job.telemetry["rows_processed"] == 3


def test_storage_write_api_accepts_backtick_quoted_full_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_write_client(monkeypatch)
    connection = _Connection()
    driver = BigQueryDriver(
        cast("Any", connection),
        driver_features={"enable_storage_write_api": True, "storage_capabilities": CAPABILITIES},
    )

    driver.load_from_arrow("`project-1.dataset.table_name`", pa.table({"id": [1]}))

    parent, _stream = _FakeWriteClient.instances[0].create_calls[0]
    assert parent == "projects/project-1/datasets/dataset/tables/table_name"


def test_storage_write_api_rejects_overqualified_table_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_write_client(monkeypatch)
    connection = _Connection()
    driver = BigQueryDriver(
        cast("Any", connection),
        driver_features={"enable_storage_write_api": True, "storage_capabilities": CAPABILITIES},
    )

    with pytest.raises(StorageOperationFailedError, match="dataset-qualified table"):
        driver.load_from_arrow("too.many.path.parts", pa.table({"id": [1]}))


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


def test_storage_write_payload_splits_before_request_limit() -> None:
    table = pa.table({"payload": ["x" * 100 for _ in range(30)]})

    requests = build_arrow_write_stream_payload("streams/s1", table, types, max_request_bytes=1_200)

    assert len(requests) > 1
    assert requests[0].write_stream == "streams/s1"
    assert requests[0].arrow_rows.writer_schema.serialized_schema
    assert all(request.arrow_rows.rows.serialized_record_batch for request in requests)
    assert all(len(request.arrow_rows.rows.serialized_record_batch) <= 1_200 for request in requests)


def test_storage_write_client_is_created_once_per_config() -> None:
    """The write client is expensive; two ingests must share one."""
    built: list[dict[str, Any]] = []

    class _WriteClient:
        def __init__(self, **kwargs: Any) -> None:
            built.append(kwargs)

    module = SimpleNamespace(BigQueryWriteClient=_WriteClient)
    config = BigQueryConfig(connection_config={"project": "p", "dataset_id": "d"})
    with patch.object(bigquery_config, "BigQueryStorageWriteModule", module):
        first = config.provide_storage_write_client(cast("Any", SimpleNamespace(_credentials=None)))
        second = config.provide_storage_write_client(cast("Any", SimpleNamespace(_credentials=None)))

    assert first is second
    assert len(built) == 1


def test_storage_write_client_receives_client_options() -> None:
    """Client options configured on the connection must reach the write client."""
    built: list[dict[str, Any]] = []

    class _WriteClient:
        def __init__(self, **kwargs: Any) -> None:
            built.append(kwargs)

    options = object()
    module = SimpleNamespace(BigQueryWriteClient=_WriteClient)
    config = BigQueryConfig(connection_config={"project": "p", "client_options": cast("Any", options)})
    with patch.object(bigquery_config, "BigQueryStorageWriteModule", module):
        config.provide_storage_write_client(cast("Any", SimpleNamespace(_credentials=None)))

    assert built[0]["client_options"] is options


def test_close_pool_closes_only_clients_sqlspec_created() -> None:
    """A caller-supplied client keeps its own lifetime."""
    closed: list[str] = []

    class _WriteClient:
        def __init__(self, **_kwargs: Any) -> None:
            self.transport = SimpleNamespace(close=lambda: closed.append("write"))

    module = SimpleNamespace(BigQueryWriteClient=_WriteClient)
    owned = BigQueryConfig(connection_config={"project": "p"})
    owned._connection_instance = cast("Any", SimpleNamespace(close=lambda: closed.append("owned")))
    with patch.object(bigquery_config, "BigQueryStorageWriteModule", module):
        owned.provide_storage_write_client(cast("Any", SimpleNamespace(_credentials=None)))
    owned.close_pool()

    assert closed == ["write", "owned"]

    supplied_client = SimpleNamespace(close=lambda: closed.append("supplied"))
    supplied = BigQueryConfig(connection_config={"project": "p"}, connection_instance=cast("Any", supplied_client))
    supplied.close_pool()

    assert "supplied" not in closed
