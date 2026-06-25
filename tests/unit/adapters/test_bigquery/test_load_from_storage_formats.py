"""BigQuery load_from_storage honors all natively loadable formats."""

from typing import Any, cast

import pytest

from sqlspec.adapters.bigquery.core import _map_bigquery_source_format, build_load_job_config
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.exceptions import StorageCapabilityError


class _RecordingJob:
    def __init__(self) -> None:
        self.statement_type = "LOAD"
        self.output_rows = 0
        self.job_id = "job_123"
        self.started = None
        self.ended = None
        self._properties: dict[str, Any] = {}
        self.result_calls: list[dict[str, Any]] = []

    def result(self, **kwargs: Any) -> "_RecordingJob":
        self.result_calls.append(kwargs)
        return self


class _RecordingConnection:
    def __init__(self) -> None:
        self.load_uri_calls: list[tuple[Any, Any, dict[str, Any]]] = []
        self.load_job = _RecordingJob()

    def load_table_from_uri(self, source_uris: Any, destination: Any, **kwargs: Any) -> _RecordingJob:
        self.load_uri_calls.append((source_uris, destination, kwargs))
        return self.load_job


def test_map_source_format_supports_all_loadable_formats() -> None:
    assert _map_bigquery_source_format("parquet") == "PARQUET"
    assert _map_bigquery_source_format("json") == "NEWLINE_DELIMITED_JSON"
    assert _map_bigquery_source_format("jsonl") == "NEWLINE_DELIMITED_JSON"
    assert _map_bigquery_source_format("csv") == "CSV"
    assert _map_bigquery_source_format("avro") == "AVRO"
    assert _map_bigquery_source_format("orc") == "ORC"


def test_map_source_format_rejects_arrow_ipc() -> None:
    with pytest.raises(StorageCapabilityError):
        _map_bigquery_source_format("arrow-ipc")


@pytest.mark.parametrize(
    ("file_format", "expected"),
    [
        ("parquet", "PARQUET"),
        ("jsonl", "NEWLINE_DELIMITED_JSON"),
        ("json", "NEWLINE_DELIMITED_JSON"),
        ("csv", "CSV"),
        ("avro", "AVRO"),
        ("orc", "ORC"),
    ],
)
def test_load_from_storage_routes_each_format(file_format: str, expected: str) -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast("Any", connection))

    driver.load_from_storage("dataset.table", f"gs://bucket/object.{file_format}", file_format=cast("Any", file_format))

    job_config = connection.load_uri_calls[0][2]["job_config"]
    assert job_config.source_format == expected


def test_load_from_storage_rejects_arrow_ipc() -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast("Any", connection))

    with pytest.raises(StorageCapabilityError):
        driver.load_from_storage("dataset.table", "gs://bucket/object.arrow", file_format=cast("Any", "arrow-ipc"))


def test_build_load_job_config_csv_source_format() -> None:
    assert build_load_job_config("csv", overwrite=False).source_format == "CSV"
