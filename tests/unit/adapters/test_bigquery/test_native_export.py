"""Local SDK contracts for native exports; no cloud jobs are submitted."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import Forbidden
from google.cloud.bigquery import QueryJobConfig

from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.exceptions import ImproperConfigurationError, PermissionDeniedError
from sqlspec.storage import SyncStoragePipeline
from tests.unit.adapters.test_bigquery.test_job_controls import CAPABILITIES, _RecordingConnection


@pytest.mark.parametrize("uri", ["gs://bucket/report.csv", "gcs://bucket/report.csv"])
def test_native_export_binds_parameters_and_preserves_job_controls(uri: str, monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _RecordingConnection()
    default = QueryJobConfig(labels={"owner": "export"}, maximum_bytes_billed=123)
    driver = BigQueryDriver(
        cast(Any, connection),
        driver_features={
            "storage_capabilities": CAPABILITIES,
            "default_query_job_config": default,
            "request_timeout": 4,
            "job_result_timeout": 7,
        },
    )
    arrow = Mock(side_effect=AssertionError("native export must not encode Arrow"))
    monkeypatch.setattr(BigQueryDriver, "select_to_arrow", arrow)
    result = driver.select_to_storage("SELECT :name AS name", uri, {"name": "O'Reilly"}, format_hint="csv")
    assert len(connection.query_calls) == 1
    sql, options = connection.query_calls[0]
    assert sql.startswith("EXPORT DATA OPTIONS (")
    assert "uri='gs://bucket/report-*.csv'" in sql
    assert "format='CSV'" in sql and "header=true" in sql and "overwrite=true" in sql
    assert "O'Reilly" not in sql
    assert "@name" in sql
    assert options["job_config"].query_parameters[0].value == "O'Reilly"
    assert options["job_config"].labels == default.labels
    assert options["job_config"].maximum_bytes_billed == 123
    assert options["timeout"] == 4
    assert connection.query_job.result_calls[0]["timeout"] == 7
    assert result.telemetry["destination"] == "gs://bucket/report-*.csv"
    assert "bytes_processed" not in result.telemetry
    assert "rows_processed" not in result.telemetry
    arrow.assert_not_called()


@pytest.mark.parametrize(
    "uri", ["s3://bucket/report.parquet", "azure://account.blob.core.windows.net/container/report.parquet"]
)
def test_native_export_uses_explicit_provider_connection(uri: str) -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(
        cast(Any, connection),
        driver_features={
            "storage_capabilities": CAPABILITIES,
            "native_export_connection": "my-project.us.connection_1",
        },
    )
    driver.select_to_storage("SELECT 1", uri)
    assert connection.query_calls[0][0].startswith("EXPORT DATA WITH CONNECTION `my-project.us.connection_1`")


@pytest.mark.parametrize("connection_name", ["bad", "a.b.c.d", "a.b.`x`", "a.b.x;SELECT 1"])
def test_invalid_export_connection_fails_before_query(connection_name: str) -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(
        cast(Any, connection),
        driver_features={"storage_capabilities": CAPABILITIES, "native_export_connection": connection_name},
    )
    with pytest.raises(ImproperConfigurationError):
        driver.select_to_storage("SELECT 1", "s3://bucket/out.parquet")
    assert not connection.query_calls


def test_native_failure_never_redispatches(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _RecordingConnection()
    monkeypatch.setattr(connection.query_job, "result", Mock(side_effect=RuntimeError("job failed")))
    arrow = Mock(side_effect=AssertionError("must not fall back"))
    monkeypatch.setattr(BigQueryDriver, "select_to_arrow", arrow)
    driver = BigQueryDriver(cast(Any, connection), driver_features={"storage_capabilities": CAPABILITIES})
    with pytest.raises(RuntimeError, match="job failed"):
        driver.select_to_storage("SELECT 1", "gs://bucket/out.parquet")
    assert len(connection.query_calls) == 1
    arrow.assert_not_called()


def test_native_api_failure_keeps_exception_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _RecordingConnection()
    query = Mock(side_effect=Forbidden("denied"))  # type: ignore[no-untyped-call]
    monkeypatch.setattr(connection, "query", query)
    arrow = Mock()
    monkeypatch.setattr(BigQueryDriver, "select_to_arrow", arrow)
    driver = BigQueryDriver(cast(Any, connection), driver_features={"storage_capabilities": CAPABILITIES})
    with pytest.raises(PermissionDeniedError):
        driver.select_to_storage("SELECT 1", "gs://bucket/out.parquet")
    query.assert_called_once()
    arrow.assert_not_called()


@pytest.mark.parametrize(
    ("format_hint", "expected"), [(None, "PARQUET"), ("parquet", "PARQUET"), ("json", "JSON"), ("jsonl", "JSON")]
)
def test_native_format_is_independent_of_suffix(format_hint: Any, expected: str) -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast(Any, connection), driver_features={"storage_capabilities": CAPABILITIES})
    driver.select_to_storage("SELECT 1", "gs://bucket/out.csv", format_hint=format_hint)
    sql = connection.query_calls[0][0]
    assert f"format='{expected}'" in sql
    assert "out-*.csv" in sql
    assert "header=" not in sql


@pytest.mark.parametrize(
    ("destination", "features", "format_hint"),
    [
        ("gs://bucket/out.parquet", {"enable_native_storage": False}, "parquet"),
        ("s3://bucket/out.parquet", {}, "parquet"),
        ("azure://account.blob.core.windows.net/container/out.parquet", {}, "parquet"),
        ("az://container/out.parquet", {}, "parquet"),
        ("file:///tmp/out.parquet", {}, "parquet"),
        ("gs://bucket/out.arrow", {}, "arrow-ipc"),
    ],
)
def test_ineligible_export_uses_client_before_submission(
    destination: str, features: dict[str, Any], format_hint: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast(Any, connection), driver_features={"storage_capabilities": CAPABILITIES, **features})
    arrow = Mock()
    monkeypatch.setattr(BigQueryDriver, "select_to_arrow", arrow)
    writer = Mock(return_value={"destination": destination, "rows_processed": 2})
    monkeypatch.setattr(BigQueryDriver, "_write_storage_result", writer)
    result = driver.select_to_storage("SELECT :x", destination, {"x": 2}, format_hint=format_hint)
    assert result.telemetry["rows_processed"] == 2
    assert not connection.query_calls
    arrow.assert_called_once()
    writer.assert_called_once()


def test_alias_export_resolves_once_without_storage_read(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _RecordingConnection()
    resolver = Mock(return_value=SimpleNamespace(uri="gs://bucket/prefix/result.parquet"))
    monkeypatch.setattr(SyncStoragePipeline, "resolve_destination", resolver)
    driver = BigQueryDriver(cast(Any, connection), driver_features={"storage_capabilities": CAPABILITIES})
    result = driver.select_to_storage("SELECT 1", "alias://exports/result.parquet", partitioner={"strategy": "fixed"})
    resolver.assert_called_once_with("alias://exports/result.parquet")
    assert len(connection.query_calls) == 1
    assert result.telemetry["destination"] == "gs://bucket/prefix/result-*.parquet"


@pytest.mark.parametrize("custom_pipeline", [False, True])
def test_emulator_or_custom_pipeline_preserves_client_path(
    custom_pipeline: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = _RecordingConnection()
    if custom_pipeline:
        monkeypatch.setattr(BigQueryDriver, "storage_pipeline_factory", SyncStoragePipeline)
    else:
        cast(Any, connection)._connection = SimpleNamespace(API_BASE_URL="http://localhost:9050")
    arrow = Mock()
    monkeypatch.setattr(BigQueryDriver, "select_to_arrow", arrow)
    monkeypatch.setattr(BigQueryDriver, "_write_storage_result", Mock(return_value={"destination": "gs://bucket/out"}))
    driver = BigQueryDriver(cast(Any, connection), driver_features={"storage_capabilities": CAPABILITIES})
    driver.select_to_storage("SELECT 1", "gs://bucket/out")
    arrow.assert_called_once()
    assert not connection.query_calls
