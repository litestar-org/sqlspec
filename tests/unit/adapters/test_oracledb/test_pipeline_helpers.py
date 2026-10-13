# pyright: reportCallIssue=false
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import oracledb
import pytest

from sqlspec import SQL, StatementStack
from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection
from sqlspec.adapters.oracledb.core import (
    _coerce_sync_row_values,  # pyright: ignore[reportPrivateUsage]
    build_insert_statement,
    build_pipeline_stack_result,
    create_mapped_exception,
    default_statement_config,
    normalize_execute_many_parameters_async,
    normalize_execute_many_parameters_sync,
)
from sqlspec.adapters.oracledb.data_dictionary import OracledbAsyncDataDictionary
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.adapters.oracledb.events._hub import OracleAsyncAQHub, OracleSyncAQHub
from sqlspec.data_dictionary import VersionInfo
from sqlspec.driver import StackExecutionObserver
from sqlspec.exceptions import DatabaseConnectionError, DataError, OperationalError, SQLParsingError

pytest.importorskip("oracledb")


class _StubAsyncConnection:
    """Minimal async connection stub for OracleAsyncDriver tests."""

    def __init__(self) -> None:
        self.in_transaction = False


class _PipelineConnection:
    """Connection stub exposing the python-oracledb pipeline API."""

    def __init__(self, *, thin: bool = True) -> None:
        self.in_transaction = False
        self.thin = thin

    def run_pipeline(self, *_args: object, **_kwargs: object) -> list[object]:
        return []


class _AsyncPipelineConnection:
    """Async connection stub exposing the python-oracledb async pipeline API."""

    def __init__(self, *, thin: bool = True) -> None:
        self.in_transaction = False
        self.thin = thin

    async def run_pipeline(self, *_args: object, **_kwargs: object) -> list[object]:
        return []


class _StubPipelineResult:
    """Pipeline result stub for driver helper tests."""

    def __init__(
        self,
        *,
        rows: list[tuple[Any, ...]] | None = None,
        columns: list[Any] | None = None,
        warning: Any | None = None,
        error: Exception | None = None,
        rowcount: int | None = None,
    ) -> None:
        self.rows = rows
        self.columns = columns
        self.warning = warning
        self.error = error
        self.rowcount = rowcount
        self.return_value = None


class _StubObserver:
    """Observer stub capturing recorded errors."""

    def __init__(self) -> None:
        self.errors: list[Exception] = []

    def record_operation_error(self, error: Exception) -> None:
        self.errors.append(error)


class _StubColumn:
    """Simple column metadata stub."""

    def __init__(self, name: str) -> None:
        self.name = name


def _make_driver() -> OracleAsyncDriver:
    connection = cast("OracleAsyncConnection", _StubAsyncConnection())
    return OracleAsyncDriver(connection=connection, statement_config=default_statement_config, driver_features={})


def test_stack_native_blocker_detects_arrow() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute_arrow("SELECT * FROM dual")
    assert driver._stack_native_blocker(stack) == "arrow_operation"


def test_stack_native_blocker_detects_script() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute_script("BEGIN NULL; END;")
    assert driver._stack_native_blocker(stack) == "script_operation"


def test_stack_native_blocker_allows_standard_operations() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute("SELECT 1 FROM dual")
    assert driver._stack_native_blocker(stack) is None


def test_pipeline_result_to_stack_result_uses_rowcount_attr() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute("SELECT 1 FROM dual")
    compiled = driver._prepare_pipeline_operation(stack.operations[0])
    pipeline_result = _StubPipelineResult(rows=[(1,)], columns=[_StubColumn("VALUE")], warning="warn", rowcount=7)
    stack_result = build_pipeline_stack_result(
        compiled.statement,
        compiled.method,
        compiled.returns_rows,
        compiled.parameters,
        pipeline_result,
        driver.driver_features,
    )
    assert stack_result.rows_affected == 7
    assert stack_result.warning == "warn"
    result = stack_result.result
    assert result is not None
    assert result.metadata is not None
    assert result.metadata["pipeline_operation"] == "execute"


def test_pipeline_result_execute_many_rowcount_fallback() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute_many("INSERT INTO demo VALUES (:1)", [(1,), (2,)])
    compiled = driver._prepare_pipeline_operation(stack.operations[0])
    pipeline_result = _StubPipelineResult()
    stack_result = build_pipeline_stack_result(
        compiled.statement,
        compiled.method,
        compiled.returns_rows,
        compiled.parameters,
        pipeline_result,
        driver.driver_features,
    )
    assert stack_result.rows_affected == 2


def test_build_stack_results_records_errors() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute("SELECT 1 FROM dual")
    compiled = driver._prepare_pipeline_operation(stack.operations[0])
    observer_stub = _StubObserver()
    observer = cast(StackExecutionObserver, observer_stub)
    results = driver._build_stack_results_from_pipeline(
        (compiled,), (_StubPipelineResult(error=RuntimeError("boom")),), True, observer
    )
    assert results[0].error is not None
    assert len(observer_stub.errors) == 1


pytest.importorskip("oracledb")


def _make_sync_pipeline_driver() -> OracleSyncDriver:
    return OracleSyncDriver(
        connection=cast("OracleSyncConnection", _PipelineConnection()),
        statement_config=default_statement_config,
        driver_features={},
    )


def _make_async_pipeline_driver(*, thin: bool = True) -> OracleAsyncDriver:
    return OracleAsyncDriver(
        connection=cast("OracleAsyncConnection", _AsyncPipelineConnection(thin=thin)),
        statement_config=default_statement_config,
        driver_features={},
    )


def _patch_async_server_version(monkeypatch: pytest.MonkeyPatch, major: int) -> None:
    """Route the async pipeline gate's data-dictionary version lookup to a stub."""

    async def fake_get_version(_self: OracledbAsyncDataDictionary, _driver: OracleAsyncDriver) -> VersionInfo:
        return VersionInfo(major, 0, 0)

    monkeypatch.setattr(OracledbAsyncDataDictionary, "get_version", fake_get_version)


def test_sync_pipeline_gate_requires_async_thin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sync Oracle connections must not claim native pipeline support."""
    driver = _make_sync_pipeline_driver()
    monkeypatch.setattr(OracleSyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))

    assert driver._pipeline_native_supported() is False
    assert driver._pipeline_support_reason == "asyncio_thin_required"


async def test_async_pipeline_gate_requires_thin_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async Oracle pipeline support is Thin-mode only."""
    driver = _make_async_pipeline_driver(thin=False)

    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))
    _patch_async_server_version(monkeypatch, 26)

    assert await driver._pipeline_native_supported() is False
    assert driver._pipeline_support_reason == "thin_mode_required"


async def test_async_pipeline_gate_accepts_23ai(monkeypatch: pytest.MonkeyPatch) -> None:
    """Oracle documents 23ai as the pipelining requirement, so 23 is accepted."""
    driver = _make_async_pipeline_driver(thin=True)

    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))
    _patch_async_server_version(monkeypatch, 23)

    assert await driver._pipeline_native_supported() is True
    assert driver._pipeline_support_reason is None


async def test_async_pipeline_gate_rejects_pre_23ai(monkeypatch: pytest.MonkeyPatch) -> None:
    """Databases below the policy threshold still decline native pipelining."""
    driver = _make_async_pipeline_driver(thin=True)

    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))
    _patch_async_server_version(monkeypatch, 21)

    assert await driver._pipeline_native_supported() is False
    assert driver._pipeline_support_reason == "database_version"


async def test_async_pipeline_gate_accepts_thin_26ai(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async Thin connections on Oracle 26ai can use native pipeline execution."""
    driver = _make_async_pipeline_driver(thin=True)

    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))
    _patch_async_server_version(monkeypatch, 26)

    assert await driver._pipeline_native_supported() is True
    assert driver._pipeline_support_reason is None


def test_normalize_execute_many_normalize_execute_many_parameters_sync_tuple_to_list() -> None:
    parameters = ({"x": 1}, {"x": 2})
    result = normalize_execute_many_parameters_sync(parameters)
    assert result == [{"x": 1}, {"x": 2}]
    assert isinstance(result, list)


def test_normalize_execute_many_normalize_execute_many_parameters_async_tuple_to_list() -> None:
    parameters = ({"x": 1}, {"x": 2})
    result = normalize_execute_many_parameters_async(parameters)
    assert result == [{"x": 1}, {"x": 2}]
    assert isinstance(result, list)


@pytest.mark.parametrize(
    "normalizer", [normalize_execute_many_parameters_sync, normalize_execute_many_parameters_async]
)
def test_normalize_execute_many_normalize_execute_many_parameters_passes_empty_through(
    normalizer: Callable[[object], object],
) -> None:
    assert normalizer([]) == []


def test_dml_returning_into_avoids_fetchall_and_uses_pipeline_add_execute() -> None:
    """DML RETURNING INTO with cursor.description=None must not call fetchall or pipeline.add_fetchall."""
    driver = _make_sync_pipeline_driver()
    stmt = SQL(
        "INSERT INTO items (name) VALUES (:name) RETURNING id INTO :out_id",
        {"name": "a", "out_id": 1},
        statement_config=default_statement_config,
    )

    cursor = MagicMock()
    cursor.description = None
    cursor.rowcount = 1
    cursor.fetchall.side_effect = oracledb.InterfaceError("DPY-1003")

    exec_result = driver.dispatch_execute(cursor, stmt)
    assert exec_result.rowcount_override == 1
    cursor.fetchall.assert_not_called()

    stack = StatementStack().push_execute("INSERT INTO items (name) VALUES (:name) RETURNING id INTO :out_id")
    compiled = driver._prepare_pipeline_operation(stack.operations[0])
    assert compiled.returns_rows is False

    pipeline = MagicMock()
    driver._add_pipeline_operation(pipeline, compiled)
    pipeline.add_execute.assert_called_once()
    pipeline.add_fetchall.assert_not_called()


async def test_aq_hubs_reset_session_on_database_error_and_sync_pool_hook() -> None:
    """AQ hubs reset cached session/queues on OracleDatabaseError and _pool_destroying_hook is synchronous."""
    sync_config = MagicMock()
    sync_hub = OracleSyncAQHub(
        sync_config, queue_name_template="AQ_{channel}", visibility=1, default_visibility=1, wait_ceiling=5
    )
    fake_queue = MagicMock()
    fake_queue.deqone.side_effect = oracledb.DatabaseError("DPY-4011: connection closed")
    sync_hub._queues["ch1"] = fake_queue
    sync_hub._session_driver = MagicMock()
    sync_hub._session_cm = MagicMock()

    assert sync_hub.dequeue("ch1", 0) is None
    assert sync_hub._queues == {}
    assert sync_hub._session_driver is None

    async_config = MagicMock()
    async_hub = OracleAsyncAQHub(
        async_config, queue_name_template="AQ_{channel}", visibility=1, default_visibility=1, wait_ceiling=5
    )
    fake_async_queue = MagicMock()
    fake_async_queue.deqone = AsyncMock(side_effect=oracledb.DatabaseError("DPY-4011"))
    async_hub._queues["ch1"] = fake_async_queue
    async_hub._session_driver = MagicMock()
    async_hub._session_cm = MagicMock()

    assert await async_hub.dequeue("ch1", 0) is None
    assert async_hub._queues == {}
    assert async_hub._session_driver is None

    async_hub._queues["ch1"] = fake_async_queue
    hook_ret = async_hub._pool_destroying_hook({})
    assert hook_ret is None
    assert async_hub._queues == {}


def test_build_insert_statement_quotes_identifiers_and_sync_driver_has_no_native_pipeline_exec() -> None:
    """build_insert_statement quotes reserved identifiers and OracleSyncDriver omits _execute_stack_native."""
    sql = build_insert_statement("my_schema.my_table", ["id", "date", "level"])
    assert sql == 'INSERT INTO my_schema.my_table (id, "date", "level") VALUES (:1, :2, :3)'
    assert "_execute_stack_native" not in OracleSyncDriver.__dict__


def test_create_mapped_exception_maps_ora_and_dpy_codes_and_lob_errors_propagate() -> None:
    """Map ORA-00942, ORA-12899, ORA-03113, DPY-* and propagate LOB read errors."""
    err_942 = MagicMock()
    err_942.code = 942
    err_942.full_code = "ORA-00942"
    assert isinstance(create_mapped_exception(oracledb.DatabaseError(err_942)), (OperationalError, SQLParsingError))

    err_12899 = MagicMock()
    err_12899.code = 12899
    err_12899.full_code = "ORA-12899"
    assert isinstance(create_mapped_exception(oracledb.DatabaseError(err_12899)), DataError)

    err_3113 = MagicMock()
    err_3113.code = 3113
    err_3113.full_code = "ORA-03113"
    assert isinstance(create_mapped_exception(oracledb.DatabaseError(err_3113)), DatabaseConnectionError)

    err_dpy = MagicMock()
    err_dpy.code = 0
    err_dpy.full_code = "DPY-4011"
    assert isinstance(create_mapped_exception(oracledb.OperationalError(err_dpy)), DatabaseConnectionError)

    broken_lob = MagicMock()
    broken_lob.read.side_effect = oracledb.DatabaseError("ORA-01555: snapshot too old")
    with pytest.raises(oracledb.DatabaseError):
        _coerce_sync_row_values((broken_lob,))


def test_dispatch_execute_script_filters_named_binds_per_statement() -> None:
    """Multi-statement script only binds variables referenced in each individual statement."""
    driver = _make_sync_pipeline_driver()
    cursor = MagicMock()
    stmt = SQL(
        "INSERT INTO t1 VALUES (:a); INSERT INTO t2 VALUES (:b);",
        {"a": 1, "b": 2},
        statement_config=default_statement_config,
    )

    driver.dispatch_execute_script(cursor, stmt)

    assert cursor.execute.call_count == 2
    assert cursor.execute.call_args_list[0].args[1] == {"a": 1}
    assert cursor.execute.call_args_list[1].args[1] == {"b": 2}
