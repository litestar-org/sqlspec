"""Unit tests for observability helpers."""

import builtins
import hashlib
import sys
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Literal, cast

from sqlspec import ObservabilityConfig, ObservabilityRuntime, RedactionConfig, SQLSpec, StatementObserver
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import LifecycleConfig
from sqlspec.core import SQL, ArrowResult, StatementConfig
from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase
from sqlspec.observability import LifecycleDispatcher, compute_sql_hash, get_trace_context, resolve_db_system
from sqlspec.storage import StorageTelemetry
from sqlspec.storage.pipeline import (
    record_storage_diagnostic_event,
    reset_storage_bridge_events,
    reset_storage_bridge_metrics,
)
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata
from sqlspec.utils.correlation import CorrelationContext
from tests.conftest import requires_interpreted


class _NoOpExceptionHandler:
    """No-op exception handler for testing.

    Implements the SyncExceptionHandler protocol but never maps exceptions.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "_NoOpExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        return False


def _lifecycle_config(hooks: dict[str, list[Any]]) -> "LifecycleConfig":
    return cast("LifecycleConfig", hooks)


class _FakeSpan:
    def __init__(self, name: str, attributes: dict[str, Any]) -> None:
        self.name = name
        self.attributes = attributes
        self.closed = False
        self.exception: Exception | None = None

    def end(self) -> None:
        self.closed = True

    def record_exception(self, error: Exception) -> None:
        self.exception = error

    def set_attribute(self, name: str, value: Any) -> None:
        self.attributes[name] = value


class _FakeSpanManager:
    def __init__(self) -> None:
        self.is_enabled = True
        self.started: list[_FakeSpan] = []
        self.finished: list[_FakeSpan] = []

    def start_span(self, name: str, attributes: dict[str, Any]) -> _FakeSpan:
        correlation = attributes.get("correlation_id")
        if correlation is not None:
            attributes.setdefault("sqlspec.correlation_id", correlation)
        span = _FakeSpan(name, dict(attributes))
        self.started.append(span)
        return span

    def start_query_span(self, **attributes: Any) -> _FakeSpan:
        return self.start_span("sqlspec.query", attributes)

    def end_span(self, span: _FakeSpan | None, error: Exception | None = None) -> None:
        if span is None:
            return
        if error is not None:
            span.record_exception(error)
        span.end()
        self.finished.append(span)


class _ArrowResultStub:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any, Any, Any]] = []

    def write_to_storage_sync(
        self, destination: Any, *, format_hint: Any = None, storage_options: Any = None, pipeline: Any = None
    ) -> dict[str, Any]:
        self.calls.append((destination, format_hint, storage_options, pipeline))
        return {
            "destination": str(destination),
            "backend": "local",
            "bytes_processed": 1,
            "rows_processed": 1,
            "format": format_hint or "jsonl",
        }


class _FakeSyncPipeline:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any]] = []

    def read_arrow(self, source: Any, *, file_format: str, storage_options: Any = None) -> tuple[str, dict[str, Any]]:
        _ = storage_options
        self.calls.append((source, file_format))
        return (
            "table",
            {
                "destination": str(source),
                "backend": "s3",
                "bytes_processed": 10,
                "rows_processed": 5,
                "format": file_format,
            },
        )


class _DummyDictionary(SyncDataDictionaryBase):
    def get_version(self, driver: "_DummyDriver") -> None:
        _ = driver

    def get_feature_flag(self, driver: "_DummyDriver", feature: str) -> bool:
        _ = driver, feature
        return False

    def get_optimal_type(self, driver: "_DummyDriver", type_category: str) -> str:
        _ = driver, type_category
        return "TEXT"

    def get_tables(self, driver: "_DummyDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        _ = (driver, schema)
        return []

    def get_columns(
        self, driver: "_DummyDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        _ = (driver, table, schema)
        return []

    def get_indexes(
        self, driver: "_DummyDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        _ = (driver, table, schema)
        return []

    def get_foreign_keys(
        self, driver: "_DummyDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        _ = (driver, table, schema)
        return []


class _DummyCursor:
    rowcount = 1


class _DummyDriver(SyncDriverAdapterBase):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._dictionary = _DummyDictionary()
        super().__init__(*args, **kwargs)

    @property
    def data_dictionary(self) -> SyncDataDictionaryBase:
        return self._dictionary

    def with_cursor(self, connection: Any):
        @contextmanager
        def _cursor() -> Any:
            yield _DummyCursor()

        return _cursor()

    def handle_database_exceptions(self) -> "_NoOpExceptionHandler":
        return _NoOpExceptionHandler()

    def begin(self) -> None:  # pragma: no cover - unused in tests
        return None

    def rollback(self) -> None:  # pragma: no cover - unused in tests
        return None

    def commit(self) -> None:  # pragma: no cover - unused in tests
        return None

    def dispatch_special_handling(self, cursor: Any, statement: SQL):
        _ = cursor, statement

    def dispatch_execute(self, cursor: Any, statement: SQL):
        _ = cursor, statement
        return self.create_execution_result(
            cursor_result=None,
            rowcount_override=1,
            special_data={},
            selected_data=None,
            column_names=None,
            data_row_count=None,
            statement_count=None,
            successful_statements=None,
            is_script_result=False,
            is_select_result=False,
            is_many_result=False,
        )

    def dispatch_execute_many(self, cursor: Any, statement: SQL):
        _ = cursor, statement
        return self.create_execution_result(
            cursor_result=None,
            rowcount_override=1,
            special_data={},
            selected_data=None,
            column_names=None,
            data_row_count=None,
            statement_count=None,
            successful_statements=None,
            is_script_result=False,
            is_select_result=False,
            is_many_result=True,
        )


def test_observability_config_merge_combines_hooks_and_observers() -> None:
    """Merged configs should merge lifecycle hooks and observers."""

    base = ObservabilityConfig(lifecycle=_lifecycle_config({"on_query_start": [lambda ctx: ctx]}), print_sql=False)
    observer_called = []

    def observer(_event: Any) -> None:
        observer_called.append(True)

    override = ObservabilityConfig(
        lifecycle=_lifecycle_config({"on_query_start": [lambda ctx: ctx]}),
        print_sql=True,
        statement_observers=(observer,),
    )

    merged = ObservabilityConfig.merge(base, override)

    assert merged.print_sql is True
    assert merged.statement_observers is not None
    assert len(merged.statement_observers) == 1
    dispatcher = LifecycleDispatcher(cast("dict[str, Iterable[Any]]", merged.lifecycle))
    assert getattr(dispatcher, "has_query_start") is True
    dispatcher.emit_query_start({"foo": "bar"})
    assert observer_called == []  # observers run via runtime, dispatcher unaffected


def test_lifecycle_dispatcher_guard_attributes_always_accessible() -> None:
    """All guard attributes should be accessible even with no hooks (mypyc compatibility)."""

    dispatcher = LifecycleDispatcher(None)
    assert dispatcher.has_pool_create is False
    assert dispatcher.has_pool_destroy is False
    assert dispatcher.has_connection_create is False
    assert dispatcher.has_connection_destroy is False
    assert dispatcher.has_session_start is False
    assert dispatcher.has_session_end is False
    assert dispatcher.has_query_start is False
    assert dispatcher.has_query_complete is False
    assert dispatcher.has_error is False

    dispatcher_with_hooks = LifecycleDispatcher(cast("dict[str, Iterable[Any]]", {"on_query_start": [lambda ctx: ctx]}))
    assert dispatcher_with_hooks.has_query_start is True
    assert dispatcher_with_hooks.has_pool_create is False


def test_lifecycle_dispatcher_counts_events() -> None:
    """Lifecycle dispatcher should count emitted events for diagnostics."""

    dispatcher = LifecycleDispatcher(
        cast("dict[str, Iterable[Any]]", {"on_query_start": [lambda ctx: ctx], "on_query_complete": [lambda ctx: ctx]})
    )
    dispatcher.emit_query_start({})
    dispatcher.emit_query_complete({})
    dispatcher.emit_query_complete({})
    snapshot = dispatcher.snapshot(prefix="test-config")
    assert snapshot["test-config.lifecycle.query_start"] == 1
    assert snapshot["test-config.lifecycle.query_complete"] == 2


def test_runtime_statement_event_redaction() -> None:
    """Runtime should redact SQL and parameters before notifying observers."""

    observed: list[dict[str, Any]] = []

    def observer(event: Any) -> None:
        observed.append(event.as_dict())

    config = ObservabilityConfig(
        redaction=RedactionConfig(mask_literals=True, mask_parameters=True),
        statement_observers=(cast(StatementObserver, observer),),
    )
    runtime = ObservabilityRuntime(config, bind_key="primary", config_name="TestConfig")

    runtime.emit_statement_event(
        sql="select * from users where email='secret'",
        parameters={"email": "secret@example.com"},
        driver="DummyDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        storage_backend=None,
    )

    assert observed, "Observer should capture at least one event"
    event = observed[0]
    assert "'***'" in event["sql"]
    assert event["parameters"] == {"email": "***"}


def test_runtime_emits_pool_events_with_context() -> None:
    """Emit helpers should forward base context to lifecycle hooks."""

    captured: list[dict[str, Any]] = []

    def hook(context: dict[str, Any]) -> None:
        captured.append(context)

    runtime = ObservabilityRuntime(
        ObservabilityConfig(lifecycle=_lifecycle_config({"on_pool_create": [hook], "on_pool_destroy": [hook]})),
        bind_key="primary",
        config_name="TestConfig",
    )

    runtime.emit_pool_create("pool-obj")
    runtime.emit_pool_destroy("pool-obj")

    assert len(captured) == 2
    assert captured[0]["config"] == "TestConfig"
    assert captured[0]["bind_key"] == "primary"
    assert captured[1]["config"] == "TestConfig"


def test_lifecycle_spans_emit_even_without_hooks() -> None:
    """Lifecycle emissions should still create spans when no hooks exist."""

    runtime = ObservabilityRuntime(ObservabilityConfig(), bind_key="primary", config_name="DummyAdapter")
    fake_manager = _FakeSpanManager()
    runtime.span_manager = cast(Any, fake_manager)

    runtime.emit_connection_create(object())
    runtime.emit_connection_destroy(object())

    span_names = [span.name for span in fake_manager.finished]
    assert "sqlspec.lifecycle.connection.create" in span_names
    assert "sqlspec.lifecycle.connection.destroy" in span_names


@requires_interpreted
def test_driver_dispatch_records_query_span() -> None:
    """Driver dispatch should start and finish query spans."""

    span_manager = _FakeSpanManager()
    runtime = ObservabilityRuntime(ObservabilityConfig(), config_name="DummyAdapter")
    runtime.span_manager = cast(Any, span_manager)

    statement_config = StatementConfig()
    driver = _DummyDriver(connection=object(), statement_config=statement_config, observability=runtime)
    statement = SQL("SELECT 1", statement_config=statement_config)

    with CorrelationContext.context("query-correlation"):
        driver.dispatch_statement_execution(statement, driver.connection)

    assert span_manager.started, "Query span should start"
    assert span_manager.finished, "Query span should finish"
    assert span_manager.started[0].name == "sqlspec.query"
    assert span_manager.started[0].attributes["adapter"] == "DummyAdapter"
    assert span_manager.started[0].attributes["sqlspec.correlation_id"] == "query-correlation"
    assert span_manager.finished[0].closed is True


def test_runtime_query_span_omits_sql_unless_print_sql_enabled() -> None:
    """Query spans should only include SQL when print_sql is enabled."""

    span_manager = _FakeSpanManager()
    runtime = ObservabilityRuntime(ObservabilityConfig(print_sql=False), config_name="DummyAdapter")
    runtime.span_manager = cast(Any, span_manager)

    runtime.start_query_span("SELECT 1", "SELECT", "DummyDriver")

    assert span_manager.started[0].attributes["sql"] == ""
    assert span_manager.started[0].attributes["connection_info"]["sqlspec.statement.hash"]
    assert span_manager.started[0].attributes["connection_info"]["sqlspec.statement.length"] == len("SELECT 1")

    span_manager_enabled = _FakeSpanManager()
    runtime_enabled = ObservabilityRuntime(ObservabilityConfig(print_sql=True), config_name="DummyAdapter")
    runtime_enabled.span_manager = cast(Any, span_manager_enabled)

    runtime_enabled.start_query_span("SELECT 1", "SELECT", "DummyDriver")

    assert span_manager_enabled.started[0].attributes["sql"] == "SELECT 1"


def test_storage_span_records_telemetry_attributes() -> None:
    """Storage spans should capture telemetry attributes when ending."""

    runtime = ObservabilityRuntime(ObservabilityConfig(), config_name="TestConfig")
    span_manager = _FakeSpanManager()
    runtime.span_manager = cast(Any, span_manager)
    span = runtime.start_storage_span("write", destination="alias://foo", format_label="parquet")
    telemetry: StorageTelemetry = {
        "destination": "alias://foo",
        "backend": "s3",
        "bytes_processed": 1024,
        "rows_processed": 8,
        "format": "parquet",
        "duration_s": 0.5,
    }
    runtime.end_storage_span(span, telemetry=telemetry)

    assert span_manager.finished, "Storage span should finish"
    assert span_manager.finished[0].attributes["sqlspec.storage.backend"] == "s3"


@requires_interpreted
def test_write_storage_helper_emits_span() -> None:
    """Storage driver helper should wrap sync writes with spans."""

    runtime = ObservabilityRuntime(ObservabilityConfig(), config_name="DummyAdapter")
    span_manager = _FakeSpanManager()
    runtime.span_manager = cast(Any, span_manager)
    statement_config = StatementConfig()
    driver = _DummyDriver(connection=object(), statement_config=statement_config, observability=runtime)
    result_stub = _ArrowResultStub()

    with CorrelationContext.context("test-correlation"):
        telemetry = driver._write_result_to_storage_sync(  # pyright: ignore[reportPrivateUsage]
            cast(ArrowResult, result_stub), "alias://bucket/object"
        )

    assert telemetry["backend"] == "local"
    assert telemetry["correlation_id"] == "test-correlation"
    assert any(span.name == "sqlspec.storage.write" for span in span_manager.finished)


@requires_interpreted
def test_read_storage_helper_emits_span() -> None:
    """Reading from storage via helper should emit spans and return telemetry."""

    runtime = ObservabilityRuntime(ObservabilityConfig(), config_name="DummyAdapter")
    span_manager = _FakeSpanManager()
    runtime.span_manager = cast(Any, span_manager)
    statement_config = StatementConfig()
    driver = _DummyDriver(connection=object(), statement_config=statement_config, observability=runtime)
    pipeline = _FakeSyncPipeline()
    driver.storage_pipeline_factory = lambda: pipeline  # type: ignore[misc,assignment]

    with CorrelationContext.context("read-correlation"):
        _table, telemetry = driver._read_arrow_from_storage_sync(  # pyright: ignore[reportPrivateUsage]
            "alias://bucket/data", file_format="parquet"
        )

    assert telemetry["backend"] == "s3"
    assert telemetry["correlation_id"] == "read-correlation"
    assert pipeline.calls, "Pipeline should be invoked"
    assert any(span.name == "sqlspec.storage.read" for span in span_manager.finished)


def test_telemetry_snapshot_includes_recent_storage_jobs() -> None:
    """Telemetry snapshot should surface recent storage jobs with correlation metadata."""

    reset_storage_bridge_metrics()
    reset_storage_bridge_events()

    spec = SQLSpec()
    spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    record_storage_diagnostic_event({
        "destination": "alias://bucket/path",
        "backend": "s3",
        "bytes_processed": 512,
        "rows_processed": 8,
        "config": "SqliteConfig",
        "bind_key": "default",
        "correlation_id": "diag-test",
    })

    snapshot = spec.telemetry_snapshot()
    recent_jobs = snapshot.get("storage_bridge.recent_jobs")
    assert recent_jobs, "Recent storage jobs should be included in diagnostics"
    assert recent_jobs[0]["correlation_id"] == "diag-test"


def test_telemetry_snapshot_includes_loader_metrics(tmp_path: "Path") -> None:
    """Telemetry snapshot should expose loader metric counters after a load."""

    sql_path = tmp_path / "queries.sql"
    sql_path.write_text("-- name: example\nSELECT 1;\n", encoding="utf-8")

    spec = SQLSpec()
    spec.load_sql_files(sql_path)

    snapshot = spec.telemetry_snapshot()
    assert "SQLFileLoader.loader.load.invocations" in snapshot
    assert snapshot["SQLFileLoader.loader.files.loaded"] >= 1


@requires_interpreted
def test_disabled_runtime_avoids_lifecycle_counters() -> None:
    """Drivers should skip lifecycle hooks entirely when none are registered."""

    runtime = ObservabilityRuntime()
    statement_config = StatementConfig()
    driver = _DummyDriver(connection=object(), statement_config=statement_config, observability=runtime)
    statement = SQL("SELECT 1", statement_config=statement_config)

    driver.dispatch_statement_execution(statement, driver.connection)

    snapshot = runtime.lifecycle_snapshot()
    assert all(value == 0 for value in snapshot.values())


@requires_interpreted
def test_runtime_with_lifecycle_hooks_records_counters() -> None:
    """Lifecycle counters should increment when hooks are configured."""

    captured: list[dict[str, Any]] = []

    def hook(ctx: dict[str, Any]) -> None:
        captured.append(ctx)

    runtime = ObservabilityRuntime(
        ObservabilityConfig(lifecycle=_lifecycle_config({"on_query_start": [hook]})), config_name="DummyConfig"
    )
    statement_config = StatementConfig()
    driver = _DummyDriver(connection=object(), statement_config=statement_config, observability=runtime)
    statement = SQL("SELECT 1", statement_config=statement_config)

    driver.dispatch_statement_execution(statement, driver.connection)

    snapshot = runtime.lifecycle_snapshot()
    assert snapshot["DummyConfig.lifecycle.query_start"] == 1
    assert captured, "Hook should have been invoked"


def test_resolve_db_system_asyncpg() -> None:
    assert resolve_db_system("AsyncpgDriver") == "postgresql"


def test_resolve_db_system_sqlite() -> None:
    assert resolve_db_system("SqliteDriver") == "sqlite"
    assert resolve_db_system("AiosqliteDriver") == "sqlite"


def test_resolve_db_system_unknown() -> None:
    assert resolve_db_system("UnknownDriver") == "other_sql"


def test_compute_sql_hash() -> None:
    sql = "SELECT 1"
    expected = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]
    assert compute_sql_hash(sql) == expected


def test_get_trace_context_without_otel(monkeypatch) -> None:
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "opentelemetry":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert get_trace_context() == (None, None)


def test_get_trace_context_with_otel(monkeypatch) -> None:
    opentelemetry_module = ModuleType("opentelemetry")
    trace_module = ModuleType("opentelemetry.trace")

    class FakeSpanContext:
        is_valid = True
        trace_id = int("0" * 31 + "1", 16)
        span_id = int("0" * 15 + "2", 16)

    class FakeSpan:
        def is_recording(self) -> bool:
            return True

        def get_span_context(self) -> "FakeSpanContext":
            return FakeSpanContext()

    def get_current_span() -> "FakeSpan":
        return FakeSpan()

    setattr(trace_module, "get_current_span", get_current_span)
    setattr(opentelemetry_module, "trace", trace_module)

    monkeypatch.setitem(sys.modules, "opentelemetry", opentelemetry_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace_module)

    trace_id, span_id = get_trace_context()
    assert trace_id == "00000000000000000000000000000001"
    assert span_id == "0000000000000002"
