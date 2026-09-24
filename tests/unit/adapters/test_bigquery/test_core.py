"""Unit tests for BigQuery core performance helpers."""

import importlib
from collections.abc import ItemsView, Iterable, Iterator, Mapping
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

import pytest
from google.cloud.bigquery import ArrayQueryParameter, QueryJobConfig, StructQueryParameter

from sqlspec.adapters.bigquery.config import BigQueryConfig, BigQueryConnectionParams
from sqlspec.adapters.bigquery.core import (
    _COPY_JOB_FIELDS,
    DEFAULT_REQUEST_TIMEOUT,
    BigQueryStreamSource,
    build_profile,
    build_retry,
    collect_rows,
    copy_job_config,
    create_parameters,
    driver_profile,
    resolve_column_names,
    run_query_job,
)
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.core import ParameterStyle, TypedParameter
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


def test_adk_store_uses_query_and_wait() -> None:
    """Verify BigQueryADKStore._run_query invokes client.query_and_wait."""
    from sqlspec.adapters.bigquery.adk import BigQueryADKStore

    class _MockRowIterator:
        schema = [SimpleNamespace(name="id")]

        def __iter__(self) -> Iterator[dict[str, Any]]:
            return iter([{"id": "session_1"}])

    class _MockClient:
        def __init__(self) -> None:
            self.query_and_wait_calls: list[str] = []

        def query_and_wait(self, sql: str, **kwargs: Any) -> Any:
            self.query_and_wait_calls.append(sql)
            return _MockRowIterator()

    mock_client = _MockClient()
    config = BigQueryConfig(
        connection_config={"project": "test-project", "dataset_id": "test_dataset"},
        driver_features={"_connection_factory": lambda: mock_client},
    )
    store = BigQueryADKStore(config)
    store._config = cast(Any, SimpleNamespace(create_connection=lambda: mock_client))

    result = store._run_query("SELECT id FROM test_table")
    assert len(mock_client.query_and_wait_calls) == 1
    assert result == [{"id": "session_1"}]


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
    assert result.statement_count == 4
    assert result.successful_statements == 4


def test_script_parameter_literal_inlining() -> None:
    """Verify multi-statement script inlines literal parameters."""
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
    assert "42" in executed_sql
    assert "'alice'" in executed_sql
    assert kwargs["job_config"].query_parameters == []


def test_session_id_propagation_across_queries() -> None:
    """Verify session_id from create_session is propagated in connection_properties."""
    connection = _RecordingConnection()
    session_job = SimpleNamespace(
        statement_type="SELECT",
        session_info=SimpleNamespace(session_id="session_12345"),
        result=lambda **kwargs: [],
        schema=[],
    )
    connection.job = session_job
    driver = BigQueryDriver(cast(Any, connection), driver_features={"use_query_and_wait": False})

    driver.begin()
    assert driver.session_id == "session_12345"

    subsequent_job = SimpleNamespace(
        statement_type="SELECT",
        session_info=SimpleNamespace(session_id="session_12345"),
        result=lambda **kwargs: [{"id": 1}],
        schema=[SimpleNamespace(name="id")],
    )
    connection.job = subsequent_job
    driver.dispatch_execute(cast(Any, connection), driver.prepare_statement("SELECT 1"))

    assert len(connection.queries) == 2
    _, kwargs = connection.queries[1]
    job_config = kwargs["job_config"]
    assert any(
        getattr(prop, "key", None) == "session_id" and getattr(prop, "value", None) == "session_12345"
        for prop in job_config.connection_properties
    )


def test_multi_statement_transaction_lifecycle() -> None:
    """Verify begin, commit, and rollback manage transaction state and session properties."""
    connection = _RecordingConnection()
    session_job = SimpleNamespace(
        statement_type="SCRIPT",
        session_info=SimpleNamespace(session_id="txn_session_99"),
        result=lambda **kwargs: [],
        schema=[],
    )
    connection.job = session_job
    driver = BigQueryDriver(cast(Any, connection))

    assert driver.session_id is None
    driver.begin()
    assert driver._connection_in_transaction() is True
    assert driver.session_id == "txn_session_99"

    driver.commit()
    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    driver.rollback()
    assert driver._connection_in_transaction() is False

    assert len(connection.queries) == 4
    assert connection.queries[0][0] == "BEGIN TRANSACTION;"
    assert connection.queries[1][0] == "COMMIT TRANSACTION;"
    assert connection.queries[2][0] == "BEGIN TRANSACTION;"
    assert connection.queries[3][0] == "ROLLBACK TRANSACTION;"


def test_driver_dry_run_returns_cost_and_schema() -> None:
    """Verify driver.dry_run executes with dry_run=True and extracts metrics without calling result()."""
    dry_run_job = SimpleNamespace(
        total_bytes_processed=10 * (1024**4),
        schema=[
            SimpleNamespace(name="user_id", field_type="INT64", mode="REQUIRED"),
            SimpleNamespace(name="email", field_type="STRING", mode="NULLABLE"),
        ],
        referenced_tables=[SimpleNamespace(project="my-proj", dataset_id="analytics", table_id="users")],
        statement_type="SELECT",
    )
    connection = _RecordingConnection()
    connection.job = dry_run_job
    driver = BigQueryDriver(cast(Any, connection))

    res = driver.dry_run("SELECT user_id, email FROM analytics.users")

    assert res["total_bytes_processed"] == 10 * (1024**4)
    assert res["estimated_cost_usd"] == 62.5
    assert len(res["schema"]) == 2
    assert res["schema"][0] == {"name": "user_id", "field_type": "INT64", "mode": "REQUIRED"}
    assert res["referenced_tables"] == ["my-proj.analytics.users"]
    assert res["statement_type"] == "SELECT"

    _, kwargs = connection.queries[0]
    assert kwargs["job_config"].dry_run is True
    assert kwargs["job_config"].use_query_cache is False


def test_governance_config_passed_to_job_config() -> None:
    """Verify governance parameters copy into default_query_job_config."""
    params = BigQueryConnectionParams(
        project="test-proj",
        labels={"team": "data-platform", "env": "prod"},
        priority="BATCH",
        reservation="projects/test-proj/reservations/prod-res",
        max_slots=500,
    )
    config = BigQueryConfig(connection_config=params)
    assert config.default_query_job_config is not None
    assert config.default_query_job_config.labels == {"team": "data-platform", "env": "prod"}
    assert config.default_query_job_config.priority == "BATCH"
    assert getattr(config.default_query_job_config, "reservation", None) == "projects/test-proj/reservations/prod-res"
    assert getattr(config.default_query_job_config, "max_slots", None) == 500


def test_parameter_struct_and_empty_array_typed_parameter() -> None:
    """Verify StructQueryParameter from dict and ArrayQueryParameter element type from TypedParameter."""
    struct_param = TypedParameter({"city": "Seattle", "zip": 98101}, dict)
    params = create_parameters({"address": struct_param}, to_json)

    assert len(params) == 1
    assert isinstance(params[0], StructQueryParameter)
    assert params[0].name == "address"

    empty_array_param = TypedParameter([], list[int])
    arr_params = create_parameters({"ids": empty_array_param}, to_json)

    assert len(arr_params) == 1
    assert isinstance(arr_params[0], ArrayQueryParameter)
    assert arr_params[0].name == "ids"
    assert arr_params[0].array_type == "INT64"
    assert arr_params[0].values == []
