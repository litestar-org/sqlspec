"""Unit tests for observability helpers."""

import asyncio
import builtins
import hashlib
import logging
import sys
import zlib
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Literal, Self, cast
from unittest.mock import patch

import pytest

from sqlspec import ObservabilityConfig, ObservabilityRuntime, RedactionConfig, SQLSpec, StatementObserver
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import LifecycleConfig
from sqlspec.core import SQL, ArrowResult, StatementConfig
from sqlspec.data_dictionary import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata
from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase
from sqlspec.observability import (
    GCPLogFormatter,
    LifecycleDispatcher,
    SamplingConfig,
    compute_sql_hash,
    get_trace_context,
    resolve_db_system,
)
from sqlspec.observability._diagnostics import collect_diagnostics
from sqlspec.observability._observer import _maybe_truncate_parameters
from sqlspec.storage import StorageTelemetry
from sqlspec.storage.pipeline import (
    record_storage_diagnostic_event,
    reset_storage_bridge_events,
    reset_storage_bridge_metrics,
)
from sqlspec.utils.correlation import CorrelationContext
from tests.conftest import requires_interpreted

_LIFECYCLE_EVENTS = (
    "on_pool_create",
    "on_pool_destroying",
    "on_pool_destroy",
    "on_connection_create",
    "on_connection_destroy",
    "on_session_start",
    "on_session_end",
    "on_query_start",
    "on_query_complete",
    "on_error",
)


class _NoOpExceptionHandler:
    """No-op exception handler for testing.

    Implements the SyncExceptionHandler protocol but never maps exceptions.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> Self:
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
    assert dispatcher.has_pool_destroying is False
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


def test_lifecycle_dispatcher_register_hook_flips_guard_and_runs() -> None:
    """register_hook adds a callback at runtime and flips its guard flag."""

    dispatcher = LifecycleDispatcher(None)
    assert dispatcher.has_pool_destroying is False

    invoked: list[dict[str, Any]] = []

    def _hook(ctx: dict[str, Any]) -> None:
        invoked.append(ctx)

    dispatcher.register_hook("on_pool_destroying", _hook)
    assert dispatcher.has_pool_destroying is True
    dispatcher.emit_pool_destroying_sync({"pool": "x"})
    assert invoked == [{"pool": "x"}]


def test_lifecycle_dispatcher_async_emit_awaits_coroutines() -> None:
    """emit_pool_destroying_async awaits coroutine returns from hooks."""

    dispatcher = LifecycleDispatcher(None)
    seen: list[str] = []

    async def _async_hook(ctx: dict[str, Any]) -> None:
        await asyncio.sleep(0)
        seen.append(cast(str, ctx.get("pool")))

    def _sync_hook(ctx: dict[str, Any]) -> None:
        seen.append("sync:" + cast(str, ctx.get("pool")))

    dispatcher.register_hook("on_pool_destroying", _async_hook)
    dispatcher.register_hook("on_pool_destroying", _sync_hook)

    asyncio.run(dispatcher.emit_pool_destroying_async({"pool": "p"}))
    assert seen == ["p", "sync:p"]


@pytest.mark.parametrize("event_name", _LIFECYCLE_EVENTS)
def test_lifecycle_dispatcher_async_emit_awaits_all_events(event_name: str) -> None:
    dispatcher = LifecycleDispatcher(None)
    seen: list[str] = []

    async def _hook(ctx: dict[str, Any]) -> None:
        await asyncio.sleep(0)
        seen.append(cast(str, ctx["event"]))

    dispatcher.register_hook(cast(Any, event_name), _hook)
    method = getattr(dispatcher, f"emit_{event_name.removeprefix('on_')}_async")
    asyncio.run(method({"event": event_name}))

    assert seen == [event_name]


@pytest.mark.parametrize("event_name", _LIFECYCLE_EVENTS)
def test_lifecycle_dispatcher_legacy_emit_aliases_stay_sync(event_name: str) -> None:
    dispatcher = LifecycleDispatcher(None)
    seen: list[str] = []

    def _hook(ctx: dict[str, Any]) -> None:
        seen.append(cast(str, ctx["event"]))

    dispatcher.register_hook(cast(Any, event_name), _hook)
    method = getattr(dispatcher, f"emit_{event_name.removeprefix('on_')}")
    method({"event": event_name})

    assert seen == [event_name]


@pytest.mark.parametrize("event_name", _LIFECYCLE_EVENTS)
def test_observability_runtime_async_emit_awaits_all_events(event_name: str) -> None:
    runtime = ObservabilityRuntime(config_name="test")
    seen: list[str] = []

    async def _hook(_ctx: dict[str, Any]) -> None:
        await asyncio.sleep(0)
        seen.append(event_name)

    runtime.register_lifecycle_hook(event_name, _hook)
    method = getattr(runtime, f"emit_{event_name.removeprefix('on_')}_async")
    if event_name == "on_error":
        asyncio.run(method(RuntimeError("boom")))
    elif event_name.startswith("on_query_"):
        asyncio.run(method(sql="SELECT 1"))
    else:
        asyncio.run(method(object()))

    assert seen == [event_name]


def test_observability_runtime_async_lifecycle_span_closes_after_hook() -> None:
    runtime = ObservabilityRuntime(config_name="test")
    span_manager = _FakeSpanManager()
    runtime.span_manager = span_manager
    states: list[bool] = []

    async def _hook(_ctx: dict[str, Any]) -> None:
        states.append(bool(span_manager.finished))

    runtime.register_lifecycle_hook("on_connection_create", _hook)
    asyncio.run(runtime.emit_connection_create_async(object()))

    assert states == [False]
    assert len(span_manager.finished) == 1


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


@requires_interpreted
def test_driver_dispatch_computes_single_sql_hash_for_span_and_observer() -> None:
    observed: list[dict[str, Any]] = []

    def observer(event: Any) -> None:
        observed.append(event.as_dict())

    span_manager = _FakeSpanManager()
    runtime = ObservabilityRuntime(
        ObservabilityConfig(print_sql=True, statement_observers=(cast(StatementObserver, observer),)),
        config_name="DummyAdapter",
    )
    runtime.span_manager = cast(Any, span_manager)
    statement_config = StatementConfig()
    driver = _DummyDriver(connection=object(), statement_config=statement_config, observability=runtime)
    statement = SQL("SELECT 1", statement_config=statement_config)

    with patch("sqlspec.observability._runtime.compute_sql_hash", return_value="precomputed") as hash_mock:
        driver.dispatch_statement_execution(statement, driver.connection)

    assert hash_mock.call_count == 1
    assert span_manager.started[0].attributes["connection_info"]["sqlspec.statement.hash"] == "precomputed"
    assert observed[0]["sql_hash"] == "precomputed"


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
        telemetry = driver._write_storage_result(  # pyright: ignore[reportPrivateUsage]
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
        _table, telemetry = driver._read_storage_arrow(  # pyright: ignore[reportPrivateUsage]
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


def test_resolve_db_system_is_lru_cached() -> None:
    resolve_db_system.cache_clear()

    assert resolve_db_system("AsyncpgDriver") == "postgresql"
    assert resolve_db_system("AsyncpgDriver") == "postgresql"

    assert resolve_db_system.cache_info().hits == 1


def test_compute_sql_hash() -> None:
    sql = "SELECT 1"
    expected = hashlib.sha256(sql.encode("utf-8"), usedforsecurity=False).hexdigest()[:16]
    assert compute_sql_hash(sql) == expected


def test_compute_sql_hash_returns_16_char_hex_string() -> None:
    first_hash = compute_sql_hash("SELECT 1")
    second_hash = compute_sql_hash("SELECT 2")

    assert len(first_hash) == 16
    assert first_hash != second_hash
    assert first_hash.islower()
    assert all(character in "0123456789abcdef" for character in first_hash)


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


def test_parameter_allow_list_preserves_scalar_and_sequence_values() -> None:
    """Verify parameter_allow_list preserves allowed scalar and sequence values while masking others."""
    observed: list[dict[str, Any]] = []
    config = ObservabilityConfig(
        redaction=RedactionConfig(mask_parameters=True, parameter_allow_list=("tenant_id", "ids")),
        statement_observers=(lambda event: observed.append(event.as_dict()),),
    )
    runtime = ObservabilityRuntime(config)
    runtime.emit_statement_event(
        sql="SELECT * FROM items WHERE tenant_id = :tenant_id",
        parameters={"tenant_id": "acme", "ids": [1, 2, 3], "secret": "xyz"},
        driver="SqliteDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        storage_backend=None,
    )
    assert observed[0]["parameters"] == {"tenant_id": "acme", "ids": [1, 2, 3], "secret": "***"}


def test_lifecycle_hooks_redact_sql_and_parameters() -> None:
    """Verify on_query_start, on_query_complete, and on_error redact SQL literals and parameters."""
    captured: list[dict[str, Any]] = []
    config = ObservabilityConfig(
        redaction=RedactionConfig(mask_literals=True, mask_parameters=True, parameter_allow_list=("tenant_id",)),
        lifecycle=_lifecycle_config({
            "on_query_start": [captured.append],
            "on_query_complete": [captured.append],
            "on_error": [captured.append],
        }),
    )
    runtime = ObservabilityRuntime(config)
    runtime.emit_query_start_sync(sql="SELECT 'secret'", parameters={"tenant_id": "acme", "pw": "hunter2"})
    runtime.emit_query_complete_sync(sql="SELECT 'secret'", parameters={"tenant_id": "acme", "pw": "hunter2"})
    runtime.emit_error_sync(
        RuntimeError("fail"), sql="SELECT 'secret'", parameters={"tenant_id": "acme", "pw": "hunter2"}
    )
    assert len(captured) == 3
    for ctx in captured:
        assert ctx["sql"] == "SELECT '***'"
        assert ctx["parameters"] == {"tenant_id": "acme", "pw": "***"}


def test_sql_literal_redaction_handles_backslash_and_dollar_quotes() -> None:
    """Verify _redact_sql masks backslash-escaped quotes and dollar-quoted strings."""
    observed: list[dict[str, Any]] = []
    config = ObservabilityConfig(
        redaction=RedactionConfig(mask_literals=True),
        statement_observers=(lambda event: observed.append(event.as_dict()),),
    )
    runtime = ObservabilityRuntime(config)
    runtime.emit_statement_event(
        sql=r"SELECT 'admin\'s secret', $$top_secret$$, $tag$tagged_secret$tag$",
        parameters=None,
        driver="AsyncpgDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        storage_backend=None,
    )
    redacted_sql = observed[0]["sql"]
    assert "secret" not in redacted_sql
    assert redacted_sql == "SELECT '***', '***', '***'"


def test_precomputed_sql_hash_hashes_sanitized_sql_when_mask_literals_enabled() -> None:
    """Verify precomputed_sql_hash is recomputed on redacted SQL when mask_literals is active."""
    observed: list[dict[str, Any]] = []
    config = ObservabilityConfig(
        redaction=RedactionConfig(mask_literals=True),
        statement_observers=(lambda event: observed.append(event.as_dict()),),
    )
    runtime = ObservabilityRuntime(config)
    raw_sql = "SELECT * FROM users WHERE ssn = '123-45-6789'"
    raw_hash = compute_sql_hash(raw_sql)
    expected_redacted_hash = compute_sql_hash("SELECT * FROM users WHERE ssn = '***'")
    runtime.emit_statement_event(
        sql=raw_sql,
        parameters=None,
        driver="SqliteDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        storage_backend=None,
        precomputed_sql_hash=raw_hash,
    )
    assert observed[0]["sql_hash"] == expected_redacted_hash


def test_deterministic_sampling_uses_crc32_across_processes() -> None:
    """Verify deterministic sampling uses zlib.crc32 rather than process-seeded hash()."""
    cid = "cross-process-cid-99"
    expected_bucket = zlib.crc32(cid.encode("utf-8")) % SamplingConfig.HASH_MODULUS
    rate_above = (expected_bucket + 1) / SamplingConfig.HASH_MODULUS
    rate_below = expected_bucket / SamplingConfig.HASH_MODULUS
    assert SamplingConfig(sample_rate=rate_above, deterministic=True).should_sample(correlation_id=cid) is True
    assert SamplingConfig(sample_rate=rate_below, deterministic=True).should_sample(correlation_id=cid) is False


def test_statement_observer_exception_is_isolated() -> None:
    """Verify exceptions raised by a StatementObserver do not abort subsequent observers or callers."""
    observed: list[str] = []

    def bad_observer(_event: Any) -> None:
        msg = "observer boom"
        raise RuntimeError(msg)

    def good_observer(event: Any) -> None:
        observed.append(event.sql)

    config = ObservabilityConfig(statement_observers=(bad_observer, good_observer))
    runtime = ObservabilityRuntime(config)
    runtime.emit_statement_event(
        sql="SELECT 1",
        parameters=None,
        driver="SqliteDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        storage_backend=None,
    )
    assert observed == ["SELECT 1"]


def test_register_lifecycle_hook_updates_is_idle() -> None:
    """Verify register_lifecycle_hook updates is_idle from True to False."""
    runtime = ObservabilityRuntime()
    assert runtime.is_idle is True
    runtime.register_lifecycle_hook("on_query_start", lambda _ctx: None)
    assert runtime.is_idle is False


def test_span_manager_attaches_context_and_resolves_db_system(monkeypatch) -> None:
    """Verify SpanManager attaches active span context and resolves db.system from driver fallback."""
    assert resolve_db_system("CockroachAsyncpgConfig") == "cockroachdb"
    assert resolve_db_system("PymssqlDriver") == "mssql"
    assert resolve_db_system("MariaDBConfig") == "mariadb"

    observed: list[dict[str, Any]] = []
    runtime = ObservabilityRuntime(
        ObservabilityConfig(statement_observers=(lambda e: observed.append(e.as_dict()),)), config_name="SQLSpecConfig"
    )
    runtime.emit_statement_event(
        sql="SELECT 1",
        parameters=None,
        driver="AsyncpgDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        storage_backend=None,
    )
    assert observed[0]["db_system"] == "postgresql"


def test_observability_config_diagnostics_and_logging(caplog) -> None:
    """Verify sampling merge, diagnostics prefixing, hook validation, and GCP log formatting."""
    base = ObservabilityConfig(sampling=SamplingConfig(sample_rate=0.5, force_sample_slow_queries_ms=500.0))
    override_partial = ObservabilityConfig(sampling=SamplingConfig(sample_rate=0.2))
    merged_partial = ObservabilityConfig.merge(base, override_partial)
    assert merged_partial.sampling is not None
    assert merged_partial.sampling.force_sample_slow_queries_ms == 500.0

    override_disable = ObservabilityConfig(sampling=SamplingConfig(sample_rate=0.2, force_sample_slow_queries_ms=None))
    merged_disable = ObservabilityConfig.merge(base, override_disable)
    assert merged_disable.sampling is not None
    assert merged_disable.sampling.force_sample_slow_queries_ms is None

    diag = collect_diagnostics([("db1", {"lifecycle.query_start": 2}), ("db2", {"lifecycle.query_start": 3})])
    assert diag["db1.lifecycle.query_start"] == 2.0
    assert diag["db2.lifecycle.query_start"] == 3.0

    dispatcher = LifecycleDispatcher()
    with pytest.raises(ValueError, match="Unsupported lifecycle event"):
        dispatcher.register_hook(cast(Any, "on_invalid_event"), lambda _ctx: None)

    caplog.set_level(logging.INFO, logger="sqlspec.sql")
    runtime_gcp = ObservabilityRuntime(
        ObservabilityConfig(print_sql=True, cloud_formatter=GCPLogFormatter(project_id="my-gcp-proj"))
    )
    runtime_gcp.emit_statement_event(
        sql="SELECT 1",
        parameters=None,
        driver="SqliteDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        storage_backend=None,
        started_at=12.34,
        transaction_state="in_transaction",
        prepared_statement=True,
    )
    sql_records = [r for r in caplog.records if r.name == "sqlspec.sql"]
    assert sql_records
    last_record = sql_records[-1]
    assert last_record.__dict__.get("severity") == "INFO"
    assert last_record.__dict__.get("sqlspec.transaction_state") == "in_transaction"
    assert last_record.__dict__.get("sqlspec.prepared_statement") is True
    assert cast(float, last_record.__dict__.get("started_at")) >= 1_000_000_000.0

    span_manager = _FakeSpanManager()
    runtime_gcp.span_manager = cast(Any, span_manager)
    runtime_gcp.emit_pool_destroying_sync(object())
    assert any(s.name == "sqlspec.lifecycle.pool.destroying" for s in span_manager.finished)

    storage_span = runtime_gcp.start_storage_span(
        "write", destination="s3://user:secret_pass@bucket/path/file.parquet?X-Amz-Signature=secret"
    )
    assert storage_span.attributes["sqlspec.storage.destination"] == "s3://bucket/path/file.parquet"

    orig_telemetry: StorageTelemetry = {
        "destination": "s3://user:secret@bucket/key?sig=123",
        "backend": "s3",
        "bytes_processed": 10,
        "rows_processed": 1,
    }
    annotated = runtime_gcp.annotate_storage_telemetry(orig_telemetry)
    assert annotated is not orig_telemetry
    assert "config" not in orig_telemetry

    truncated_batch, was_truncated = _maybe_truncate_parameters([[1, 2, 3, 4], [5, 6, 7, 8]], max_items=2)
    assert was_truncated is True
    assert truncated_batch == [[1, 2], [5, 6]]
