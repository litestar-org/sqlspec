"""Unit tests for BigQuery job-control behavior."""

from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery.enums import QueryApiMethod, TimestampPrecision

from sqlspec.adapters.bigquery.core import build_load_job_config, run_query_job, try_bulk_insert
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.utils.serializers import to_json

CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": ["fixed"],
}


class _RecordingJob:
    def __init__(
        self,
        rows: list[dict[str, object]] | None = None,
        *,
        statement_type: str = "SELECT",
        schema: list[SimpleNamespace] | None = None,
        num_dml_affected_rows: int | None = None,
        job_id: str = "job_123",
    ) -> None:
        self.rows = rows or []
        self.statement_type = statement_type
        self.schema = schema
        self.num_dml_affected_rows = num_dml_affected_rows
        self.job_id = job_id
        self.labels: dict[str, str] = {}
        self.started = None
        self.ended = None
        self._properties: dict[str, Any] = {}
        self.result_calls: list[dict[str, Any]] = []

    def result(self, **kwargs: Any) -> list[dict[str, object]]:
        self.result_calls.append(kwargs)
        return self.rows


class _RecordingExtractJob:
    def __init__(self) -> None:
        self.result_calls: list[dict[str, Any]] = []
        self.job_id = "extract_123"

    def result(self, **kwargs: Any) -> None:
        self.result_calls.append(kwargs)


class _RecordingRowIterator:
    def __init__(
        self,
        rows: list[dict[str, object]] | None = None,
        *,
        schema: list[SimpleNamespace] | None = None,
        num_dml_affected_rows: int | None = None,
    ) -> None:
        self.rows = rows or []
        self.schema = schema
        self.num_dml_affected_rows = num_dml_affected_rows

    def __iter__(self) -> Any:
        return iter(self.rows)


class _RecordingConnection:
    def __init__(self) -> None:
        self.query_calls: list[tuple[str, dict[str, Any]]] = []
        self.query_and_wait_calls: list[tuple[str, dict[str, Any]]] = []
        self.load_file_calls: list[tuple[Any, Any, dict[str, Any]]] = []
        self.load_uri_calls: list[tuple[Any, Any, dict[str, Any]]] = []
        self.extract_calls: list[tuple[Any, Any, dict[str, Any]]] = []
        self.query_job = _RecordingJob()
        self.row_iterator = _RecordingRowIterator()
        self.load_job = _RecordingJob(statement_type="LOAD", schema=None)
        self.extract_job = _RecordingExtractJob()

    def query(self, sql: str, **kwargs: Any) -> _RecordingJob:
        self.query_calls.append((sql, kwargs))
        return self.query_job

    def query_and_wait(self, sql: str, **kwargs: Any) -> _RecordingRowIterator:
        self.query_and_wait_calls.append((sql, kwargs))
        return self.row_iterator

    def load_table_from_file(self, file_obj: Any, destination: Any, **kwargs: Any) -> _RecordingJob:
        self.load_file_calls.append((file_obj, destination, kwargs))
        return self.load_job

    def load_table_from_uri(self, source_uris: Any, destination: Any, **kwargs: Any) -> _RecordingJob:
        self.load_uri_calls.append((source_uris, destination, kwargs))
        return self.load_job

    def extract_table(self, source: Any, destination_uris: Any, **kwargs: Any) -> _RecordingExtractJob:
        self.extract_calls.append((source, destination_uris, kwargs))
        return self.extract_job


def _schema(*names: str) -> list[SimpleNamespace]:
    return [SimpleNamespace(name=name) for name in names]


def test_run_query_job_passes_job_id_prefix_only_without_job_id() -> None:
    connection = _RecordingConnection()

    run_query_job(
        cast(Any, connection),
        "SELECT @name",
        {"name": "alpha"},
        default_job_config=None,
        job_config=None,
        json_serializer=to_json,
        retry=None,
        timeout=3.0,
        job_retry=None,
        api_method=QueryApiMethod.INSERT,
        timestamp_precision=TimestampPrecision.MICROSECOND,
        job_id_prefix="prefix-",
    )

    sql, kwargs = connection.query_calls[0]
    assert sql == "SELECT @name"
    assert kwargs["api_method"] == QueryApiMethod.INSERT
    assert kwargs["timestamp_precision"] == TimestampPrecision.MICROSECOND
    assert kwargs["job_id_prefix"] == "prefix-"
    assert "job_id" not in kwargs


def test_query_and_wait_used_when_enabled() -> None:
    connection = _RecordingConnection()
    connection.row_iterator = _RecordingRowIterator(rows=[{"v": 1}], schema=_schema("v"))
    driver = BigQueryDriver(cast(Any, connection), driver_features={"use_query_and_wait": True})

    result = driver.execute("SELECT 1 AS v")

    assert connection.query_calls == []
    assert connection.query_and_wait_calls[0][0] == "SELECT 1 AS v"
    assert connection.query_and_wait_calls[0][1]["api_timeout"] == driver._job_request_timeout()
    assert connection.query_and_wait_calls[0][1]["wait_timeout"] == driver._job_request_timeout()
    assert result.get_data()[0]["v"] == 1


def test_query_and_wait_dml_rowcount() -> None:
    connection = _RecordingConnection()
    connection.row_iterator = _RecordingRowIterator(num_dml_affected_rows=3)
    driver = BigQueryDriver(cast(Any, connection), driver_features={"use_query_and_wait": True})

    result = driver.execute("UPDATE t SET v = 1")

    assert connection.query_calls == []
    assert result.rows_affected == 3


def test_query_and_wait_default_off() -> None:
    connection = _RecordingConnection()
    connection.query_job = _RecordingJob(rows=[{"v": 1}], schema=_schema("v"))
    connection.row_iterator = _RecordingRowIterator(rows=[{"v": 1}], schema=_schema("v"))
    driver = BigQueryDriver(cast(Any, connection))

    result = driver.execute("SELECT 1 AS v")

    assert connection.query_calls
    assert connection.query_and_wait_calls == []
    assert result.get_data()[0]["v"] == 1


def test_try_bulk_insert_bounds_result_timeout() -> None:
    connection = _RecordingConnection()
    connection.load_job = _RecordingJob(statement_type="LOAD")

    rowcount = try_bulk_insert(
        cast(Any, connection),
        "INSERT INTO dataset.table (id) VALUES (@id)",
        [{"id": 1}],
        result_timeout=5.0,
    )

    assert rowcount == 1
    assert connection.load_file_calls[0][2]["timeout"] == 5.0
    assert connection.load_job.result_calls[0]["timeout"] == 5.0


def test_load_from_arrow_bounds_result_timeout() -> None:
    connection = _RecordingConnection()
    connection.load_job = _RecordingJob(statement_type="LOAD")
    driver = BigQueryDriver(
        cast(Any, connection), driver_features={"job_result_timeout": 5.0, "storage_capabilities": CAPABILITIES}
    )

    result = driver.load_from_arrow("dataset.table", pa.table({"id": [1]}))

    assert result.telemetry["rows_processed"] == 0
    assert connection.load_file_calls[0][2]["timeout"] == driver._job_request_timeout()
    assert connection.load_job.result_calls[0]["timeout"] == driver._job_request_timeout()


def test_load_from_storage_forwards_retry_and_bounds_result_timeout() -> None:
    connection = _RecordingConnection()
    connection.load_job = _RecordingJob(statement_type="LOAD")
    driver = BigQueryDriver(cast(Any, connection), driver_features={"job_result_timeout": 5.0})

    result = driver.load_from_storage(
        "dataset.table",
        "gs://bucket/object.parquet",
        file_format="parquet",
    )

    assert result.telemetry["rows_processed"] == 0
    assert connection.load_uri_calls[0][2]["retry"] is driver._job_retry
    assert connection.load_uri_calls[0][2]["timeout"] == driver._job_request_timeout()
    assert connection.load_job.result_calls[0]["timeout"] == driver._job_request_timeout()


def test_load_job_config_fill_from_default_preserves_defaults() -> None:
    default_job_config = LoadJobConfig(labels={"source": "default"})

    filled_job_config = build_load_job_config("parquet", overwrite=False)._fill_from_default(default_job_config)

    assert filled_job_config.source_format == "PARQUET"
    assert filled_job_config.write_disposition == "WRITE_APPEND"
    assert filled_job_config.labels == {"source": "default"}


def test_export_table_to_storage_forwards_job_controls() -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast(Any, connection))

    job = driver.export_table_to_storage("dataset.table", "gs://bucket/object.csv")

    assert job is connection.extract_job
    assert connection.extract_calls[0][0] == "dataset.table"
    assert connection.extract_calls[0][1] == "gs://bucket/object.csv"
    assert connection.extract_calls[0][2]["retry"] is driver._job_retry
    assert connection.extract_calls[0][2]["timeout"] == driver._job_request_timeout()
    assert connection.extract_job.result_calls[0]["timeout"] == driver._job_request_timeout()
