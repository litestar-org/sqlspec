"""Unit tests for BigQuery core performance helpers."""

import importlib
from collections.abc import ItemsView, Iterable, Iterator, Mapping
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import patch

import pytest
from google.api_core.exceptions import RetryError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.cloud.exceptions import Forbidden, GoogleCloudError

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.bigquery._typing import BigQueryCursor, BigQuerySessionContext
from sqlspec.adapters.bigquery.adk.store import BigQueryADKStore
from sqlspec.adapters.bigquery.core import (
    _COPY_JOB_FIELDS,
    DEFAULT_REQUEST_TIMEOUT,
    BigQueryStreamSource,
    build_dml_rowcount,
    build_profile,
    build_retry,
    collect_rows,
    copy_job_config,
    create_mapped_exception,
    create_parameters,
    driver_profile,
    resolve_column_names,
    run_query_job,
    try_bulk_insert,
)
from sqlspec.adapters.bigquery.data_dictionary import BigQueryDataDictionary
from sqlspec.adapters.bigquery.driver import BigQueryDriver, BigQueryExceptionHandler
from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore
from sqlspec.adapters.bigquery.litestar.store import BigQueryStore
from sqlspec.core import ParameterStyle, TypedParameter
from sqlspec.exceptions import (
    ImproperConfigurationError,
    NotFoundError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable


class _RecordingConnection:
    def __init__(self) -> None:
        self.default_query_job_config: QueryJobConfig | None = None
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


class _RecordingRow(Mapping[str, object]):
    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, key: str) -> object:
        return self._values[key]

    def items(self) -> ItemsView[str, object]:
        return self._values.items()


class _MappingRow(_RecordingRow):
    def items(self) -> ItemsView[str, object]:
        msg = "fetch_chunk should use dict(row), not row.items()"
        raise AssertionError(msg)


def _schema_field(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


class _RecordingStreamJob:
    def __init__(self, pages: Iterable[Iterable[Mapping[str, object]]] | None = None) -> None:
        self.result_kwargs: dict[str, Any] = {}
        self._pages = iter(pages or ())

    def result(self, **kwargs: Any) -> SimpleNamespace:
        self.result_kwargs = kwargs
        return SimpleNamespace(pages=self._pages)


class _RecordingStreamDriver:
    def __init__(self, api_base_url: str, pages: Iterable[Iterable[Mapping[str, object]]] | None = None) -> None:
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


def test_stream_source_converts_rows_without_items_copying() -> None:
    driver = _RecordingStreamDriver("https://bigquery.googleapis.com", pages=((_MappingRow({"value": 1}),),))
    source = BigQueryStreamSource(cast(Any, driver), "SELECT 1", None, 100)
    source.start()

    assert source.fetch_chunk() == [{"value": 1}]


def test_bigquery_type_converter_module_is_gone() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("sqlspec.adapters.bigquery.type_converter")


class _RecordingInteractiveConnection:
    """Mock BigQuery connection tracking query and query_and_wait calls."""

    def __init__(self, row_iterator: Any = None) -> None:
        self.row_iterator = row_iterator
        self.query_and_wait_calls: list[tuple[str, dict[str, Any]]] = []
        self.query_calls: list[tuple[str, dict[str, Any]]] = []
        self.job = object()

    def query_and_wait(self, sql: str, **kwargs: Any) -> Any:
        self.query_and_wait_calls.append((sql, kwargs))
        return self.row_iterator

    def query(self, sql: str, **kwargs: Any) -> Any:
        self.query_calls.append((sql, kwargs))
        return self.job


def test_bigquery_driver_defaults_to_query_and_wait_for_interactive_select() -> None:
    """Verify BigQueryDriver defaults to query_and_wait for interactive SELECT statements."""

    class _MockRowIterator:
        schema = [SimpleNamespace(name="id")]
        total_rows = 1

        def __iter__(self) -> Iterator[dict[str, Any]]:
            return iter([{"id": 1}])

    row_iterator = _MockRowIterator()
    connection = _RecordingInteractiveConnection(row_iterator=row_iterator)
    driver = BigQueryDriver(cast(Any, connection))

    result = driver.dispatch_execute(cast(Any, connection), driver.prepare_statement("SELECT 1 AS id"))

    assert len(connection.query_and_wait_calls) == 1
    assert len(connection.query_calls) == 0
    assert result.is_select_result is True
    assert result.selected_data == [{"id": 1}]


def test_destination_table_bypasses_query_and_wait() -> None:
    """Verify queries with destination tables bypass query_and_wait and route to query."""
    row_iterator = SimpleNamespace(schema=[SimpleNamespace(name="id")], total_rows=1)
    connection = _RecordingInteractiveConnection(row_iterator=row_iterator)
    connection.job = _RecordingSelectJob([{"id": 1}])

    job_config = QueryJobConfig()
    job_config.destination = "project.dataset.table"
    driver = BigQueryDriver(cast(Any, connection), driver_features={"default_query_job_config": job_config})

    stmt = driver.prepare_statement("SELECT 1 AS id")
    result = driver.dispatch_execute(cast(Any, connection), stmt)

    assert len(connection.query_and_wait_calls) == 0
    assert len(connection.query_calls) == 1
    assert result.is_select_result is True


def test_batch_priority_bypasses_query_and_wait() -> None:
    """Verify queries with BATCH priority bypass query_and_wait and route to query."""
    row_iterator = SimpleNamespace(schema=[SimpleNamespace(name="id")], total_rows=1)
    connection = _RecordingInteractiveConnection(row_iterator=row_iterator)
    connection.job = _RecordingSelectJob([{"id": 1}])

    job_config = QueryJobConfig()
    job_config.priority = "BATCH"
    driver = BigQueryDriver(cast(Any, connection), driver_features={"default_query_job_config": job_config})

    stmt = driver.prepare_statement("SELECT 1 AS id")
    driver.dispatch_execute(cast(Any, connection), stmt)

    assert len(connection.query_and_wait_calls) == 0
    assert len(connection.query_calls) == 1


def test_script_execution_runs_as_single_unsplit_job() -> None:
    """Verify multi-statement script runs as a single un-split query job."""
    connection = _RecordingConnection()
    script_job = SimpleNamespace(
        statement_type="SCRIPT",
        num_dml_affected_rows=5,
        statistics=None,
        _properties={"statistics": {"query": {"scriptStatistics": {"executionPath": [1, 2, 3, 4]}}}},
        result=lambda **kwargs: None,
    )
    connection.job = script_job
    driver = BigQueryDriver(cast(Any, connection))

    script_sql = "BEGIN DECLARE x INT64; SET x = 1; SELECT x; END;"
    result = driver.dispatch_execute_script(cast(Any, connection), driver.prepare_statement(script_sql))

    assert len(connection.queries) == 1
    executed_sql, _ = connection.queries[0]
    assert executed_sql.strip() == script_sql.strip()
    assert result.is_script_result is True
    assert result.statement_count == 1
    assert result.successful_statements == 1


def test_script_execution_counts_multiple_statements() -> None:
    """Verify dispatch_execute_script reports the split statement count for multi-statement scripts."""
    connection = _RecordingConnection()
    script_job = SimpleNamespace(
        statement_type="SCRIPT", num_dml_affected_rows=3, statistics=None, result=lambda **kwargs: None
    )
    connection.job = script_job
    driver = BigQueryDriver(cast(Any, connection))

    script_sql = "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3);"
    result = driver.dispatch_execute_script(cast(Any, connection), driver.prepare_statement(script_sql))

    assert result.is_script_result is True
    assert result.statement_count == 3
    assert result.successful_statements == 3


def test_script_preserves_bound_parameters() -> None:
    """Verify scripts retain native parameter binding without reparsing."""
    connection = _RecordingConnection()
    script_job = SimpleNamespace(
        statement_type="SCRIPT", num_dml_affected_rows=1, statistics=None, result=lambda **kwargs: None
    )
    connection.job = script_job
    driver = BigQueryDriver(cast(Any, connection))

    script_sql = "INSERT INTO t (id, name) VALUES (@id, @name);"
    stmt = driver.prepare_statement(script_sql, ({"id": 42, "name": "alice"},))
    driver.dispatch_execute_script(cast(Any, connection), stmt)

    assert len(connection.queries) == 1
    executed_sql, kwargs = connection.queries[0]
    assert "@id" in executed_sql
    assert "@name" in executed_sql
    assert {p.name: p.value for p in kwargs["job_config"].query_parameters} == {"id": 42, "name": "alice"}


def test_result_timeout_uses_query_job() -> None:
    connection = _RecordingInteractiveConnection()
    connection.job = _RecordingSelectJob([{"id": 1}])
    driver = BigQueryDriver(cast("Any", connection), driver_features={"job_result_timeout": 5.0})

    driver.execute("SELECT 1 AS id")

    assert len(connection.query_calls) == 1
    assert connection.job.result_calls[0]["timeout"] == 5.0


@pytest.mark.parametrize(
    ("declared", "expected"), [(int, "INT64"), (bool, "BOOL"), (float, "FLOAT64"), (str, "STRING")]
)
def test_declared_empty_array_preserves_element_type(declared: type, expected: str) -> None:
    from sqlspec.adapters.bigquery.core import create_parameters
    from sqlspec.core import TypedParameter

    parameter = create_parameters({"values": TypedParameter([], declared)})[0]
    assert parameter.to_api_repr()["parameterType"] == {"type": "ARRAY", "arrayType": {"type": expected}}


def test_native_default_job_config_guides_fast_path() -> None:
    from sqlspec.core import SQL

    connection = _RecordingConnection()
    native_defaults = QueryJobConfig(priority="BATCH")
    connection.default_query_job_config = native_defaults
    driver = BigQueryDriver(cast("Any", connection))
    assert driver._default_query_job_config is native_defaults
    assert driver._can_use_query_and_wait(SQL("SELECT 1"), "SELECT 1") is False


def test_exception_mapping_permission_denied_unauthenticated_and_timeout() -> None:
    """Verify BigQuery exception mapping: 403 -> PermissionDeniedError, timeout -> QueryTimeoutError, 503 -> OperationalError."""
    forbidden = cast("Any", Forbidden)("Access Denied", errors=[{"reason": "accessDenied"}])
    timeout_err = _cloud_error("Query timed out", code=504, errors=[{"reason": "timeout"}])
    server_err = _cloud_error("Service unavailable", code=503, errors=[{"reason": "backendError"}])
    syntax_err = _cloud_error("Syntax error at [1:5]", code=400, errors=[{"reason": "invalidQuery"}])
    conflict_err = _cloud_error("Already exists", code=409, errors=[{"reason": "duplicate"}])
    not_found_err = _cloud_error("Not found", code=404, errors=[{"reason": "notFound"}])

    assert isinstance(create_mapped_exception(forbidden), PermissionDeniedError)
    assert isinstance(create_mapped_exception(timeout_err), QueryTimeoutError)
    assert isinstance(create_mapped_exception(server_err), OperationalError)
    assert isinstance(create_mapped_exception(syntax_err), SQLParsingError)
    assert isinstance(create_mapped_exception(conflict_err), UniqueViolationError)
    assert isinstance(create_mapped_exception(not_found_err), NotFoundError)


def test_adk_store_maps_google_cloud_errors_and_returns_dml_rowcount(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify BigQueryADKStore._run_query maps GoogleCloudError and delete helpers return affected rows."""
    config = BigQueryConfig(
        connection_config={"project": "p", "dataset_id": "ds"}, extension_config={"adk": {"enable_memory": False}}
    )
    store = BigQueryADKStore(config)

    class _Job:
        num_dml_affected_rows = 4

        def result(self) -> list[dict[str, Any]]:
            return []

    class _Client:
        def query(self, _sql: str, **_kwargs: Any) -> _Job:
            return _Job()

    monkeypatch.setattr(config, "create_connection", _Client)
    deleted = store.delete_expired_events(datetime.now(UTC))
    assert deleted == 4

    class _FailingClient:
        def query(self, _sql: str, **_kwargs: Any) -> Any:
            raise cast("Any", Forbidden)("Permission denied", errors=[{"reason": "accessDenied"}])

    monkeypatch.setattr(config, "create_connection", _FailingClient)
    with pytest.raises(PermissionDeniedError):
        store.delete_expired_events(datetime.now(UTC))


def test_adk_store_json_param_avoids_double_encoding_and_infers_owner_id_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _json_param passes parsed JSON value to ScalarQueryParameter and infers INT64 owner_id."""
    custom_serialized: list[Any] = []

    def custom_to_json(val: Any) -> str:
        custom_serialized.append(val)
        return '{"ok":true}'

    config = BigQueryConfig(
        connection_config={"project": "p", "dataset_id": "ds"},
        driver_features={"json_serializer": custom_to_json},
        extension_config={"adk": {"enable_memory": False, "owner_id_column": "owner_id INT64"}},
    )
    store = BigQueryADKStore(config)
    param = store._json_param("state", {"ok": True})
    assert param.type_ == "JSON"
    assert param.value == {"ok": True}
    assert custom_serialized == [{"ok": True}]

    captured_params: list[Any] = []

    def capture_query(_self: BigQueryADKStore, _sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        captured_params.extend(parameters or [])
        return []

    monkeypatch.setattr(BigQueryADKStore, "_run_query", capture_query)
    store.create_session("s1", "app", "u1", {"a": 1}, owner_id=99)
    owner_param = next(p for p in captured_params if p.name == "owner_id")
    assert owner_param.type_ == "INT64"
    assert owner_param.value == 99


def test_bigquery_driver_session_transactions_and_dry_run() -> None:
    """Verify BigQueryDriver begin/commit/rollback session propagation and dry_run bytes processed."""
    queries: list[tuple[str, Any]] = []

    class _Job:
        statement_type = "SCRIPT"
        schema: list[Any] = []
        total_bytes_processed = 2048
        session_info = SimpleNamespace(session_id="bq-session-123")

        def result(self, **_kwargs: Any) -> list[Any]:
            return []

    class _Conn:
        def query(self, sql: str, *, job_config: Any = None, **_kwargs: Any) -> _Job:
            queries.append((sql, job_config))
            return _Job()

    driver = BigQueryDriver(cast("Any", _Conn()))
    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    assert driver._session_id == "bq-session-123"
    assert queries[-1][0] == "BEGIN TRANSACTION"
    assert queries[-1][1].create_session is True

    driver.commit()
    assert driver._connection_in_transaction() is False
    assert queries[-1][0] == "COMMIT TRANSACTION"
    conn_props = queries[-1][1].connection_properties
    assert any(p.key == "session_id" and p.value == "bq-session-123" for p in conn_props)

    estimated = driver.dry_run("SELECT 1")
    assert estimated == 2048
    assert queries[-1][1].dry_run is True
    assert queries[-1][1].use_query_cache is False


def test_select_to_arrow_reader_handles_empty_batches_and_local_emulator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify select_to_arrow('reader') does not re-execute on empty batches and checks native_only on emulator."""
    query_calls = 0

    class _EmptyRowIterator:
        schema = [SimpleNamespace(name="id")]

        def to_arrow_iterable(self, *, bqstorage_client: Any = None) -> Any:
            del bqstorage_client
            return iter(())

    class _Job:
        statement_type = "SELECT"
        schema = [SimpleNamespace(name="id")]

        def result(self, **_kwargs: Any) -> _EmptyRowIterator:
            return _EmptyRowIterator()

    class _Conn:
        _connection = SimpleNamespace(API_BASE_URL="https://bigquery.googleapis.com")

        def query(self, _sql: str, **_kwargs: Any) -> _Job:
            nonlocal query_calls
            query_calls += 1
            return _Job()

    driver = BigQueryDriver(cast("Any", _Conn()))
    result = driver.select_to_arrow("SELECT id FROM t WHERE 1=0", return_format="reader")
    assert query_calls == 1
    assert result.get_data().read_all().num_rows == 0
    assert result.get_data().schema.names == ["id"]

    local_conn = _Conn()
    local_conn._connection = SimpleNamespace(API_BASE_URL="http://127.0.0.1:9050")
    local_driver = BigQueryDriver(cast("Any", local_conn))
    monkeypatch.setattr("sqlspec.adapters.bigquery.driver.storage_api_available", lambda: False)
    with pytest.raises(ImproperConfigurationError):
        local_driver.select_to_arrow("SELECT 1", return_format="reader", native_only=True)


def test_parameter_creation_resolves_typed_empty_arrays_and_json_dicts() -> None:
    """Verify create_parameters handles TypedParameter([], list[int]) and JSON dicts."""
    params = {
        "empty_int_list": TypedParameter([], list[int]),
        "null_int_list": TypedParameter([None], list[int]),
        "nested": {"outer": {"inner": 42}, "tags": ["a", "b"]},
    }
    bq_params = create_parameters(params, json_serializer=to_json)
    by_name = {p.name: p for p in bq_params}

    assert isinstance(by_name["empty_int_list"], bigquery.ArrayQueryParameter)
    assert by_name["empty_int_list"].array_type == "INT64"
    assert isinstance(by_name["null_int_list"], bigquery.ArrayQueryParameter)
    assert by_name["null_int_list"].array_type == "INT64"
    assert isinstance(by_name["nested"], bigquery.ScalarQueryParameter)
    assert by_name["nested"].type_ == "JSON"


def test_default_dataset_qualifies_unqualified_tables_in_bulk_insert_and_stores() -> None:
    """Verify default_dataset qualifies tables across bulk insert, data dictionary, events, and Litestar."""
    loaded_tables: list[str] = []

    class _LoadJob:
        output_rows = 2

        def result(self, **_kwargs: Any) -> None:
            return None

    class _Conn:
        def load_table_from_file(self, _buf: Any, table_id: str, **_kwargs: Any) -> _LoadJob:
            loaded_tables.append(table_id)
            return _LoadJob()

    rowcount = try_bulk_insert(
        cast("Any", _Conn()),
        "INSERT INTO users (id, name) VALUES (@p1, @p2)",
        [{"p1": 1, "p2": "a"}, {"p1": 2, "p2": "b"}],
        None,
        allow_parse=True,
        default_dataset="my_proj.my_ds",
    )
    assert rowcount == 2
    assert loaded_tables == ["my_proj.my_ds.users"]

    config = BigQueryConfig(
        connection_config={"project": "my_proj", "dataset_id": "my_ds"},
        extension_config={"litestar": {"require_partition_filter": True}},
    )
    events_store = BigQueryEventQueueStore(config)
    assert events_store.table_name == "my_ds.sqlspec_event_queue"
    assert "my_ds.sqlspec_event_queue" in events_store.create_statements()[0]

    litestar_store = BigQueryStore(config)
    assert litestar_store.table_name == "my_ds.litestar_session"
    executed_sql: list[str] = []

    class _FakeDriver:
        def execute(self, sql: str, **_kwargs: Any) -> Any:
            executed_sql.append(sql)
            return SimpleNamespace(get_affected_count=lambda: 1)

    cast("Any", config).provide_session = lambda *_args, **_kwargs: nullcontext(_FakeDriver())
    litestar_store._delete_expired()
    assert "my_ds.litestar_session" in executed_sql[0]
    assert "1970-01-01" in executed_sql[0]

    dd = BigQueryDataDictionary()
    dd_queries: list[tuple[str, dict[str, Any]]] = []

    class _DDDriver:
        _default_query_job_config = SimpleNamespace(default_dataset="my_proj.my_ds")

        def select(self, query: str, **kwargs: Any) -> list[Any]:
            dd_queries.append((query, kwargs))
            return []

    dd.get_tables(cast("Any", _DDDriver()))
    assert dd_queries[0][1]["schema_name"] == "my_ds"
    assert "`my_proj.my_ds.INFORMATION_SCHEMA.TABLES`" in dd_queries[0][0]


def test_bigquery_stream_source_closes_and_cancels_active_job() -> None:
    """Verify BigQueryStreamSource.close cancels active query job and BigQueryCursor cleans up safely."""
    cancelled: list[bool] = []

    class _Job:
        state = "RUNNING"
        schema = [SimpleNamespace(name="x")]

        def cancel(self) -> None:
            cancelled.append(True)

        def result(self, **_kwargs: Any) -> Any:
            return SimpleNamespace(pages=iter([[{"x": 1}]]))

    class _Driver:
        connection = SimpleNamespace()
        _job_retry = None
        _job_retry_deadline = 0.0
        _job_result_timeout = None

        def handle_database_exceptions(self) -> Any:
            return nullcontext()

        def _check_pending_exception(self, _handler: Any) -> None:
            return None

        def _run_query_job(self, *_args: Any) -> _Job:
            return _Job()

    source = BigQueryStreamSource(cast("Any", _Driver()), "SELECT 1 AS x", None, 10)
    source.start()
    chunk = source.fetch_chunk()
    assert chunk == [{"x": 1}]
    source.close()
    assert cancelled == [True]

    cursor = BigQueryCursor(cast("Any", SimpleNamespace()))
    cursor.job = cast("Any", _Job())
    with cursor:
        pass
    assert cursor.job is None
    assert len(cancelled) == 2


def test_bigquery_session_context_rolls_back_on_exception_when_in_transaction() -> None:
    """Verify BigQuerySessionContext.__exit__ rolls back active transaction on exception."""
    rolled_back: list[bool] = []
    released: list[bool] = []

    class _Driver:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def rollback(self) -> None:
            rolled_back.append(True)

        def _connection_in_transaction(self) -> bool:
            return True

    ctx = BigQuerySessionContext(
        acquire_connection=lambda: SimpleNamespace(),
        release_connection=lambda *_args, **_kwargs: released.append(True),
        statement_config=BigQueryDriver(cast("Any", SimpleNamespace())).statement_config,
        driver_features={},
        prepare_driver=lambda d: d,
    )
    with (
        pytest.raises(RuntimeError, match="boom"),
        patch("sqlspec.adapters.bigquery._typing._get_driver_type", return_value=_Driver),
        ctx,
    ):
        raise RuntimeError("boom")

    assert rolled_back == [True]
    assert released == [True]


def test_build_dml_rowcount_preserves_zero_affected_rows() -> None:
    """Verify build_dml_rowcount returns 0 when num_dml_affected_rows is 0 instead of falling back."""
    job_zero = SimpleNamespace(num_dml_affected_rows=0)
    job_none = SimpleNamespace(num_dml_affected_rows=None, total_rows=0)
    assert build_dml_rowcount(job_zero, 5) == 0
    assert build_dml_rowcount(job_none, 5) == 5


def _cloud_error(message: str, *, code: int, errors: list[dict[str, str]] | None = None) -> GoogleCloudError:
    err: Any = cast("Any", GoogleCloudError)(message)
    err.code = code
    err._errors = errors or []
    return cast("GoogleCloudError", err)


def test_retry_error_mapped_to_query_timeout() -> None:
    """RetryError maps to QueryTimeoutError in BigQuery create_mapped_exception."""
    err = cast("Callable[[str, Exception | None], RetryError]", RetryError)("Deadline exceeded waiting for query", None)
    mapped = create_mapped_exception(err)
    assert isinstance(mapped, QueryTimeoutError)


def test_exception_handler_catches_google_api_error() -> None:
    """BigQueryExceptionHandler catches GoogleAPIError and maps it."""
    handler = BigQueryExceptionHandler()
    err = cast("Callable[[str, Exception | None], RetryError]", RetryError)("Deadline exceeded", None)
    with handler:
        raise err
    assert isinstance(handler.pending_exception, QueryTimeoutError)
