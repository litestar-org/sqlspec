"""Unit tests for BigQuery core performance helpers."""

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

from google.cloud.bigquery import QueryJobConfig

from sqlspec.adapters.bigquery.core import (
    _COPY_JOB_FIELDS,
    build_profile,
    build_retry,
    collect_rows,
    copy_job_config,
    driver_profile,
    resolve_column_names,
    run_query_job,
)
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.adapters.bigquery.type_converter import BigQueryOutputConverter
from sqlspec.core import ParameterStyle
from sqlspec.utils.serializers import to_json


class _RecordingConnection:
    def __init__(self) -> None:
        self.job = object()
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def query(self, sql: str, **kwargs: Any) -> object:
        self.queries.append((sql, kwargs))
        return self.job


def _schema_field(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


def test_resolve_column_names_reuses_cached_schema() -> None:
    schema = [_schema_field("id"), _schema_field("name")]
    cache: dict[int, tuple[object, list[str]]] = {}
    first = resolve_column_names(schema, cache)
    second = resolve_column_names(schema, cache)
    assert first == ["id", "name"]
    assert second is first
    assert len(cache) == 1


def test_resolve_column_names_distinguishes_schema_identity() -> None:
    schema_one = [_schema_field("id")]
    schema_two = [_schema_field("id")]
    cache: dict[int, tuple[object, list[str]]] = {}
    first = resolve_column_names(schema_one, cache)
    second = resolve_column_names(schema_two, cache)
    assert first == second == ["id"]
    assert first is not second
    assert len(cache) == 2


def test_collect_rows_uses_precomputed_column_names() -> None:
    rows = [{"id": 1}]
    schema = [_schema_field("ignored")]
    (data, column_names) = collect_rows(rows, schema, column_names=["id"])
    assert data is rows
    assert column_names == ["id"]


def test_collect_rows_uses_cache_when_column_names_not_precomputed() -> None:
    rows = [{"id": 1, "name": "x"}]
    schema = [_schema_field("id"), _schema_field("name")]
    cache: dict[int, tuple[object, list[str]]] = {}
    (data_one, column_names_one) = collect_rows(rows, schema, column_name_cache=cache)
    (data_two, column_names_two) = collect_rows(rows, schema, column_name_cache=cache)
    assert data_one is rows
    assert data_two is rows
    assert column_names_one == ["id", "name"]
    assert column_names_two is column_names_one
    assert len(cache) == 1


def test_build_profile_does_not_advertise_qmark() -> None:
    """BigQuery rejects positional params, so QMARK must not be advertised."""
    profile = build_profile()
    assert ParameterStyle.QMARK not in profile.supported_styles


def test_module_level_driver_profile_does_not_advertise_qmark() -> None:
    """Module-level driver_profile should match the supported BigQuery style set."""
    assert ParameterStyle.QMARK not in driver_profile.supported_styles


def test_build_profile_named_at_is_only_supported_style() -> None:
    """NAMED_AT is the only supported BigQuery parameter style."""
    profile = build_profile()
    assert profile.supported_styles == frozenset({ParameterStyle.NAMED_AT})


def test_copy_job_config_copies_set_fields() -> None:
    source = QueryJobConfig()
    source.use_query_cache = True
    source.maximum_bytes_billed = 1000000
    target = QueryJobConfig()
    copy_job_config(source, target)
    assert target.use_query_cache is True
    assert target.maximum_bytes_billed == 1000000


def test_copy_job_config_does_not_overwrite_with_none() -> None:
    source = QueryJobConfig()
    target = QueryJobConfig()
    target.use_query_cache = False
    copy_job_config(source, target)
    assert target.use_query_cache is False


def test_copy_job_fields_exclude_caller_owned_and_none_sensitive_fields() -> None:
    assert "query_parameters" not in _COPY_JOB_FIELDS
    assert "table_definitions" not in _COPY_JOB_FIELDS


def test_copy_job_fields_use_real_reservation_attribute() -> None:
    assert "reservation" in _COPY_JOB_FIELDS
    assert "reservation_id" not in _COPY_JOB_FIELDS


def test_copy_job_attribute_helper_removed() -> None:
    import sqlspec.adapters.bigquery.core as bigquery_core

    assert not hasattr(bigquery_core, "_should_copy_job_attribute")


def test_run_query_job_passes_query_start_retry_timeout_and_job_retry() -> None:
    connection = _RecordingConnection()
    retry = build_retry(1.0)
    job_retry = build_retry(2.0)
    job = run_query_job(
        cast(Any, connection),
        "SELECT @name",
        {"name": "alpha"},
        default_job_config=None,
        job_config=None,
        json_serializer=to_json,
        retry=retry,
        timeout=3.0,
        job_retry=job_retry,
    )

    assert job is connection.job
    sql, kwargs = connection.queries[0]
    assert sql == "SELECT @name"
    assert kwargs["retry"] is retry
    assert kwargs["timeout"] == 3.0
    assert kwargs["job_retry"] is job_retry

    job_config = kwargs["job_config"]
    assert isinstance(job_config, QueryJobConfig)
    assert job_config.query_parameters is not None
    assert len(job_config.query_parameters) == 1
    assert job_config.query_parameters[0].name == "name"


def test_bigquery_driver_applies_result_timeout_to_query_start_request() -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(
        cast(Any, connection), driver_features={"job_result_timeout": 3.0, "job_retry_deadline": 1.0}
    )
    job = driver._run_query_job(cast(Any, connection), "SELECT @name", {"name": "alpha"})  # type: ignore[protected-access]

    assert job is connection.job
    _, kwargs = connection.queries[0]
    assert kwargs["retry"] is kwargs["job_retry"]
    assert kwargs["timeout"] == 3.0


def test_bigquery_driver_does_not_pass_polling_sentinel_as_query_start_timeout() -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast(Any, connection))
    driver._run_query_job(cast(Any, connection), "SELECT 1", None)  # type: ignore[protected-access]

    _, kwargs = connection.queries[0]
    assert "timeout" not in kwargs


def test_type_converter_bigquery_output_converter_is_final() -> None:
    assert getattr(BigQueryOutputConverter, "__final__", False) is True


def test_type_converter_bigquery_output_converter_instantiates() -> None:
    converter = BigQueryOutputConverter()
    assert isinstance(converter, BigQueryOutputConverter)


def test_type_converter_bigquery_output_converter_converts_uuid_string() -> None:
    converter = BigQueryOutputConverter(enable_uuid_conversion=True)
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    result = converter.convert(uuid_str)
    assert isinstance(result, UUID)
    assert str(result) == uuid_str


def test_type_converter_bigquery_output_converter_convert_detected_fallback() -> None:
    converter = BigQueryOutputConverter()
    result = converter._convert_detected("hello", "unknown_type")
    assert result == "hello"
