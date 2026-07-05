"""Unit tests for BigQuery core performance helpers."""

import importlib
from collections.abc import Iterable
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

import pytest

from google.cloud.bigquery import QueryJobConfig

from sqlspec.adapters.bigquery.core import (
    _COPY_JOB_FIELDS,
    DEFAULT_REQUEST_TIMEOUT,
    BigQueryStreamSource,
    build_profile,
    build_retry,
    collect_rows,
    copy_job_config,
    driver_profile,
    resolve_column_names,
    run_query_job,
)
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.core import ParameterStyle
from sqlspec.utils.serializers import to_json


class _RecordingConnection:
    def __init__(self) -> None:
        self.job = object()
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def query(self, sql: str, **kwargs: Any) -> object:
        self.queries.append((sql, kwargs))
        return self.job


class _RecordingSelectJob:
    statement_type = "SELECT"
    schema = [SimpleNamespace(name="id")]

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.result_calls: list[dict[str, Any]] = []

    def result(self, **kwargs: Any) -> list[dict[str, object]]:
        self.result_calls.append(kwargs)
        return self.rows


class _RecordingDmlJob:
    statement_type = "INSERT"

    def __init__(self, *, num_dml_affected_rows: int = 1) -> None:
        self.num_dml_affected_rows = num_dml_affected_rows
        self.result_calls: list[dict[str, Any]] = []

    def result(self, **kwargs: Any) -> None:
        self.result_calls.append(kwargs)
        msg = "DML job.result() should not be called in this test"
        raise AssertionError(msg)


class _RecordingScriptJob:
    statement_type = "SCRIPT"

    def __init__(self, *, num_dml_affected_rows: int = 1) -> None:
        self.num_dml_affected_rows = num_dml_affected_rows
        self.result_calls: list[dict[str, Any]] = []

    def result(self, **kwargs: Any) -> None:
        self.result_calls.append(kwargs)


class _RecordingRow:
    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def items(self) -> Iterable[tuple[str, object]]:
        return self._values.items()


def _schema_field(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


class _RecordingStreamJob:
    def __init__(self, pages: Iterable[Iterable[_RecordingRow]] | None = None) -> None:
        self.result_kwargs: dict[str, Any] = {}
        self._pages = iter(pages or ())

    def result(self, **kwargs: Any) -> SimpleNamespace:
        self.result_kwargs = kwargs
        return SimpleNamespace(pages=self._pages)


class _RecordingStreamDriver:
    def __init__(self, api_base_url: str, pages: Iterable[Iterable[_RecordingRow]] | None = None) -> None:
        self.connection = SimpleNamespace(_connection=SimpleNamespace(API_BASE_URL=api_base_url))
        self._job_retry: Any = build_retry(2.0)
        self._job_retry_deadline = 2.0
        self._job_result_timeout: Any = 3.0
        self.job = _RecordingStreamJob(pages)

    def _run_query_job(self, connection: Any, sql: str, parameters: Any) -> Any:
        return self.job

    def handle_database_exceptions(self) -> Any:
        return nullcontext()

    def _check_pending_exception(self, exc_handler: Any) -> None:
        return None


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


def test_bigquery_driver_defaults_to_finite_query_start_timeout() -> None:
    """A missing timeout would reach the transport as None and wait on the socket forever."""
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast(Any, connection))
    driver._run_query_job(cast(Any, connection), "SELECT 1", None)  # type: ignore[protected-access]

    _, kwargs = connection.queries[0]
    assert kwargs["timeout"] == DEFAULT_REQUEST_TIMEOUT


def test_bigquery_driver_request_timeout_feature_overrides_result_timeout() -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast(Any, connection), driver_features={"job_result_timeout": 30.0, "request_timeout": 7.5})
    driver._run_query_job(cast(Any, connection), "SELECT 1", None)  # type: ignore[protected-access]

    _, kwargs = connection.queries[0]
    assert kwargs["timeout"] == 7.5


def test_bigquery_driver_zero_job_retry_deadline_disables_retries() -> None:
    """job_retry=None is the only opt-out of the client's 600s jobs.insert retry wrapper."""
    connection = _RecordingConnection()
    driver = BigQueryDriver(cast(Any, connection), driver_features={"job_retry_deadline": 0.0})
    driver._run_query_job(cast(Any, connection), "SELECT 1", None)  # type: ignore[protected-access]

    _, kwargs = connection.queries[0]
    assert kwargs["retry"] is None
    assert kwargs["job_retry"] is None


def test_bigquery_driver_select_result_passes_job_result_kwargs() -> None:
    connection = _RecordingConnection()
    connection.job = _RecordingSelectJob([{"id": 1}])
    driver = BigQueryDriver(
        cast(Any, connection),
        driver_features={"query_page_size": 17, "query_max_results": 11, "job_result_timeout": 3.0},
    )

    result = driver.dispatch_execute(cast(Any, connection), driver.prepare_statement("SELECT 1"))

    assert result.is_select_result is True
    assert connection.job.result_calls[0] == {
        "page_size": 17,
        "max_results": 11,
        "job_retry": driver._job_retry,
        "timeout": 3.0,
    }


def test_bigquery_driver_minimal_select_result_omits_paging_kwargs() -> None:
    connection = _RecordingConnection()
    connection.job = _RecordingSelectJob([{"id": 1}])
    driver = BigQueryDriver(cast(Any, connection), driver_features={"job_result_timeout": 3.0})

    result = driver.dispatch_execute(cast(Any, connection), driver.prepare_statement("SELECT 1"))

    assert result.is_select_result is True
    assert connection.job.result_calls[0] == {"job_retry": driver._job_retry, "timeout": 3.0}


def test_bigquery_driver_dml_and_script_do_not_pass_job_result_kwargs() -> None:
    connection = _RecordingConnection()
    driver = BigQueryDriver(
        cast(Any, connection),
        driver_features={"query_page_size": 17, "query_max_results": 11, "job_result_timeout": 3.0},
    )

    connection.job = _RecordingDmlJob(num_dml_affected_rows=1)
    dml_result = driver.dispatch_execute(
        cast(Any, connection), driver.prepare_statement("INSERT INTO t (id) VALUES (1)")
    )

    assert dml_result.is_select_result is False
    assert connection.job.result_calls == []

    connection.job = _RecordingScriptJob(num_dml_affected_rows=1)
    script_result = driver.dispatch_execute_script(
        cast(Any, connection), driver.prepare_statement("INSERT INTO t (id) VALUES (1);")
    )

    assert script_result.is_script_result is True
    assert connection.job.result_calls[0] == {"job_retry": driver._job_retry, "timeout": 3.0}


def test_stream_source_local_endpoint_uses_single_page_and_bounded_retry() -> None:
    driver = _RecordingStreamDriver("http://127.0.0.1:9050")
    source = BigQueryStreamSource(cast(Any, driver), "SELECT 1", None, 100)
    source.start()

    kwargs = driver.job.result_kwargs
    assert kwargs["page_size"] is None
    assert kwargs["retry"].timeout == driver._job_retry_deadline
    assert kwargs["job_retry"] is driver._job_retry
    assert kwargs["timeout"] == 3.0


def test_stream_source_disabled_retries_pass_none_page_retry() -> None:
    driver = _RecordingStreamDriver("http://127.0.0.1:9050")
    driver._job_retry = None
    driver._job_retry_deadline = 0.0
    source = BigQueryStreamSource(cast(Any, driver), "SELECT 1", None, 100)
    source.start()

    kwargs = driver.job.result_kwargs
    assert kwargs["retry"] is None
    assert kwargs["job_retry"] is None


def test_stream_source_remote_endpoint_passes_chunk_page_size() -> None:
    driver = _RecordingStreamDriver("https://bigquery.googleapis.com")
    source = BigQueryStreamSource(cast(Any, driver), "SELECT 1", None, 100)
    source.start()

    kwargs = driver.job.result_kwargs
    assert kwargs["page_size"] == 100
    assert kwargs["retry"].timeout == driver._job_retry_deadline


def test_stream_source_skips_empty_pages_before_rows() -> None:
    driver = _RecordingStreamDriver("https://bigquery.googleapis.com", pages=((), (_RecordingRow({"value": 1}),)))
    source = BigQueryStreamSource(cast(Any, driver), "SELECT 1", None, 100)
    source.start()

    assert source.fetch_chunk() == [{"value": 1}]
    assert source.fetch_chunk() == []


def test_bigquery_type_converter_module_is_gone() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("sqlspec.adapters.bigquery.type_converter")
