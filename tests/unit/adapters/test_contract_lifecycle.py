"""Unit tests for the driver lifecycle (pooling / connection-hook) contract wiring."""

import ast
import inspect
import pkgutil
from contextlib import suppress
from importlib import import_module
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import sqlspec.adapters
from sqlspec.core import StatementConfig as SQLSpecStatementConfig
from sqlspec.driver._common import _DETACHED_CONNECTION
from sqlspec.exceptions import ImproperConfigurationError
from tests.integration.adapters._shared._cases import DRIVER_CASES, get_driver_case

_SESSION_CONTEXTS = (
    ("sqlspec.adapters.sqlite._typing", "SqliteSessionContext"),
    ("sqlspec.adapters.duckdb._typing", "DuckDBSessionContext"),
    ("sqlspec.adapters.pymysql._typing", "PyMysqlSessionContext"),
    ("sqlspec.adapters.pymssql._typing", "PymssqlSessionContext"),
    ("sqlspec.adapters.mssql_python._typing", "MssqlPythonSessionContext"),
    ("sqlspec.adapters.adbc._typing", "AdbcSessionContext"),
    ("sqlspec.adapters.arrow_odbc._typing", "ArrowOdbcSessionContext"),
    ("sqlspec.adapters.bigquery._typing", "BigQuerySessionContext"),
    ("sqlspec.adapters.db2._typing", "Db2SyncSessionContext"),
    ("sqlspec.adapters.teradata._typing", "TeradataSessionContext"),
)

POOLING_CASES = (
    "sqlite-sync",
    "duckdb-sync",
    "aiosqlite-async",
    "psycopg-sync",
    "cockroach-psycopg-sync",
    "asyncpg-async",
    "psqlpy-async",
    "psycopg-async",
    "cockroach-asyncpg-async",
    "cockroach-psycopg-async",
    "mysqlconnector-sync",
    "pymysql-sync",
    "aiomysql-async",
    "asyncmy-async",
    "oracledb-sync",
    "oracledb-async",
    "db2-sync",
    "db2-async",
)
CONNECTION_HOOK_CASES = (*POOLING_CASES, "mysqlconnector-async", "bigquery-sync")


@pytest.mark.parametrize("case_id", POOLING_CASES)
def test_pooling_case_declares_factory(case_id: str) -> None:
    """A case that opts into the pooling contract must provide a config factory fixture."""
    case = get_driver_case(case_id)
    assert case.supports_pooling
    assert case.config_factory_fixture is not None


@pytest.mark.parametrize("case_id", CONNECTION_HOOK_CASES)
def test_connection_hook_case_declares_factory(case_id: str) -> None:
    """A case that opts into the connection-hook contract must provide a config factory fixture."""
    case = get_driver_case(case_id)
    assert case.supports_connection_hook
    assert case.config_factory_fixture is not None


def test_lifecycle_flags_require_config_factory() -> None:
    """No case may declare a config-factory-driven feature without a config factory (no untestable claim)."""
    for case in DRIVER_CASES:
        needs_factory = (
            case.supports_pooling
            or case.supports_connection_hook
            or case.supports_lowercase_columns
            or case.supports_uuid_feature
            or case.supports_custom_json_serializer
            or case.supports_custom_type_adapters
        )
        if needs_factory:
            assert case.config_factory_fixture is not None, (
                f"{case.id} declares a config-factory feature without a config_factory_fixture"
            )


def test_session_contexts_forward_exception_info_to_release() -> None:
    """Every adapter session context must hand its exception triple to connection release."""
    offenders: list[str] = []
    for module_name, context_name in _SESSION_CONTEXTS:
        context_class = getattr(import_module(module_name), context_name)
        recorded: list[dict[str, Any]] = []
        context = context_class(
            acquire_connection=lambda: object(),
            release_connection=lambda _conn, **kwargs: recorded.append(kwargs),
            statement_config=SQLSpecStatementConfig(),
            driver_features={},
            prepare_driver=lambda driver: driver,
        )
        with suppress(RuntimeError):
            with context:
                raise RuntimeError
        if not recorded or recorded[0].get("exc_type") is not RuntimeError:
            offenders.append(f"{module_name}.{context_name}")

    assert offenders == []


_DETACH_SESSION_CONTEXTS: tuple[tuple[str, str, bool], ...] = (
    ("sqlspec.adapters.asyncpg._typing", "AsyncpgSessionContext", True),
    ("sqlspec.adapters.psycopg._typing", "PsycopgSyncSessionContext", False),
    ("sqlspec.adapters.psycopg._typing", "PsycopgAsyncSessionContext", True),
    ("sqlspec.adapters.psqlpy._typing", "PsqlpySessionContext", True),
    ("sqlspec.adapters.cockroach_asyncpg._typing", "CockroachAsyncpgSessionContext", True),
    ("sqlspec.adapters.cockroach_psycopg._typing", "CockroachPsycopgSyncSessionContext", False),
    ("sqlspec.adapters.cockroach_psycopg._typing", "CockroachPsycopgAsyncSessionContext", True),
    ("sqlspec.adapters.sqlite._typing", "SqliteSessionContext", False),
    ("sqlspec.adapters.aiosqlite._typing", "AiosqliteSessionContext", True),
    ("sqlspec.adapters.pymysql._typing", "PyMysqlSessionContext", False),
    ("sqlspec.adapters.aiomysql._typing", "AiomysqlSessionContext", True),
    ("sqlspec.adapters.mysqlconnector._typing", "MysqlConnectorSyncSessionContext", False),
    ("sqlspec.adapters.mysqlconnector._typing", "MysqlConnectorAsyncSessionContext", True),
    ("sqlspec.adapters.asyncmy.config", "AsyncmySessionContext", True),
    ("sqlspec.adapters.mssql_python._typing", "MssqlPythonSessionContext", False),
    ("sqlspec.adapters.pymssql._typing", "PymssqlSessionContext", False),
    ("sqlspec.adapters.sybase._typing", "SybaseSessionContext", False),
    ("sqlspec.adapters.sqlanywhere._typing", "SqlanywhereSessionContext", False),
    ("sqlspec.adapters.oracledb._typing", "OracleSyncSessionContext", False),
    ("sqlspec.adapters.oracledb._typing", "OracleAsyncSessionContext", True),
    ("sqlspec.adapters.db2._typing", "Db2SyncSessionContext", False),
    ("sqlspec.adapters.db2._typing", "Db2AsyncSessionContext", True),
    ("sqlspec.adapters.bigquery._typing", "BigQuerySessionContext", False),
    ("sqlspec.adapters.spanner._typing", "SpannerSyncSessionContext", False),
    ("sqlspec.adapters.spanner._typing", "SpannerAsyncSessionContext", True),
    ("sqlspec.adapters.snowflake._typing", "SnowflakeSessionContext", False),
    ("sqlspec.adapters.teradata._typing", "TeradataSessionContext", False),
    ("sqlspec.adapters.duckdb._typing", "DuckDBSessionContext", False),
    ("sqlspec.adapters.adbc._typing", "AdbcSessionContext", False),
    ("sqlspec.adapters.arrow_odbc._typing", "ArrowOdbcSessionContext", False),
)

_SYNC_DETACH_SESSION_CONTEXTS = tuple(
    (module_name, context_name) for module_name, context_name, is_async in _DETACH_SESSION_CONTEXTS if not is_async
)
_ASYNC_DETACH_SESSION_CONTEXTS = tuple(
    (module_name, context_name) for module_name, context_name, is_async in _DETACH_SESSION_CONTEXTS if is_async
)
_SYNC_ENTER_FAILURE_CONTEXTS = (
    ("sqlspec.adapters.mssql_python._typing", "MssqlPythonSessionContext"),
    ("sqlspec.adapters.pymssql._typing", "PymssqlSessionContext"),
    ("sqlspec.adapters.oracledb._typing", "OracleSyncSessionContext"),
    ("sqlspec.adapters.db2._typing", "Db2SyncSessionContext"),
    ("sqlspec.adapters.snowflake._typing", "SnowflakeSessionContext"),
    ("sqlspec.adapters.teradata._typing", "TeradataSessionContext"),
    ("sqlspec.adapters.adbc._typing", "AdbcSessionContext"),
)
_ASYNC_ENTER_FAILURE_CONTEXTS = (
    ("sqlspec.adapters.oracledb._typing", "OracleAsyncSessionContext"),
    ("sqlspec.adapters.db2._typing", "Db2AsyncSessionContext"),
)


class _SessionBodyError(Exception):
    pass


def _make_stub_connection(*, is_async: bool) -> Any:
    stub = AsyncMock() if is_async else MagicMock()
    stub.in_transaction = False
    stub.is_in_transaction = MagicMock(return_value=False)
    stub.get_transaction_status = MagicMock(return_value=False)
    stub.autocommit = True
    stub.info = MagicMock(transaction_status=0)
    stub.adbc_get_info = MagicMock(return_value={"vendor_name": "sqlite", "driver_name": "sqlite"})
    return stub


def _discover_module_session_contexts(module_name: str, module_path: Path) -> set[tuple[str, str]]:
    """Discover SessionContext classes declared in an adapter module, falling back to AST when optional SDKs are absent."""
    try:
        module = import_module(module_name)
    except ModuleNotFoundError:
        tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
        return {
            (module_name, node.name)
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name.endswith("SessionContext")
        }
    return {
        (module_name, cls_name)
        for cls_name, cls_obj in inspect.getmembers(module, inspect.isclass)
        if cls_name.endswith("SessionContext") and cls_obj.__module__ == module_name
    }


def test_detach_contract_covers_every_session_context() -> None:
    """Every adapter SessionContext class across all 24 adapters is registered in _DETACH_SESSION_CONTEXTS."""
    adapters_root = Path(sqlspec.adapters.__file__).resolve().parent
    candidate_modules = [
        *(
            (f"sqlspec.adapters.{pkg.name}._typing", adapters_root / pkg.name / "_typing.py")
            for pkg in pkgutil.iter_modules(sqlspec.adapters.__path__)
            if (adapters_root / pkg.name / "_typing.py").is_file()
        ),
        ("sqlspec.adapters.asyncmy.config", adapters_root / "asyncmy" / "config.py"),
    ]
    discovered = {
        pair
        for mod_name, mod_path in candidate_modules
        for pair in _discover_module_session_contexts(mod_name, mod_path)
    }
    registered = {(module_name, context_name) for module_name, context_name, _ in _DETACH_SESSION_CONTEXTS}

    assert len(registered) == 30
    assert discovered == registered


@pytest.mark.parametrize(("module_name", "context_name"), _SYNC_DETACH_SESSION_CONTEXTS)
@pytest.mark.parametrize("body_raises", [False, True], ids=["clean-exit", "body-exception"])
def test_sync_session_context_detaches_driver_after_release(
    module_name: str, context_name: str, body_raises: bool
) -> None:
    """Sync adapter session contexts release the live connection first and detach the driver on exit."""
    context_class = getattr(import_module(module_name), context_name)
    stub_conn = _make_stub_connection(is_async=False)
    release_events: list[tuple[bool, bool, Any]] = []
    holder: list[Any] = []

    def release_connection(conn: Any, **kwargs: Any) -> None:
        assert conn is not _DETACHED_CONNECTION
        drv = holder[0]
        release_events.append((conn is stub_conn, drv.connection is stub_conn, kwargs.get("exc_type")))

    context = context_class(
        acquire_connection=lambda: stub_conn,
        release_connection=release_connection,
        statement_config=SQLSpecStatementConfig(),
        driver_features={},
        prepare_driver=lambda driver: driver,
    )
    if body_raises:
        with pytest.raises(_SessionBodyError), context as driver:
            holder.append(driver)
            assert driver.connection is stub_conn
            raise _SessionBodyError
    else:
        with context as driver:
            holder.append(driver)
            assert driver.connection is stub_conn

    assert release_events == [(True, True, _SessionBodyError if body_raises else None)]
    assert driver.connection is _DETACHED_CONNECTION
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        driver.execute("SELECT 1")


@pytest.mark.parametrize(("module_name", "context_name"), _ASYNC_DETACH_SESSION_CONTEXTS)
@pytest.mark.parametrize("body_raises", [False, True], ids=["clean-exit", "body-exception"])
async def test_async_session_context_detaches_driver_after_release(
    module_name: str, context_name: str, body_raises: bool
) -> None:
    """Async adapter session contexts release the live connection first and detach the driver on exit."""
    context_class = getattr(import_module(module_name), context_name)
    stub_conn = _make_stub_connection(is_async=True)
    release_events: list[tuple[bool, bool, Any]] = []
    holder: list[Any] = []

    async def acquire_connection() -> Any:
        return stub_conn

    async def release_connection(conn: Any, **kwargs: Any) -> None:
        assert conn is not _DETACHED_CONNECTION
        drv = holder[0]
        release_events.append((conn is stub_conn, drv.connection is stub_conn, kwargs.get("exc_type")))

    context = context_class(
        acquire_connection=acquire_connection,
        release_connection=release_connection,
        statement_config=SQLSpecStatementConfig(),
        driver_features={},
        prepare_driver=lambda driver: driver,
    )
    driver: Any = None
    if body_raises:
        with pytest.raises(_SessionBodyError):
            async with context as driver:
                holder.append(driver)
                assert driver.connection is stub_conn
                raise _SessionBodyError
    else:
        async with context as driver:
            holder.append(driver)
            assert driver.connection is stub_conn

    assert release_events == [(True, True, _SessionBodyError if body_raises else None)]
    assert driver.connection is _DETACHED_CONNECTION
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        await driver.execute("SELECT 1")


@pytest.mark.parametrize(("module_name", "context_name"), _SYNC_ENTER_FAILURE_CONTEXTS)
def test_sync_session_context_enter_failure_detaches_driver(module_name: str, context_name: str) -> None:
    """Sync session contexts with guarded __enter__ setup release first and detach constructed drivers on failure."""
    context_class = getattr(import_module(module_name), context_name)
    stub_conn = _make_stub_connection(is_async=False)
    release_events: list[tuple[bool, bool, Any]] = []
    holder: list[Any] = []

    def prepare_driver(driver: Any) -> Any:
        holder.append(driver)
        raise _SessionBodyError

    def release_connection(conn: Any, **kwargs: Any) -> None:
        assert conn is not _DETACHED_CONNECTION
        drv = holder[0]
        release_events.append((conn is stub_conn, drv.connection is stub_conn, kwargs.get("exc_type")))

    context = context_class(
        acquire_connection=lambda: stub_conn,
        release_connection=release_connection,
        statement_config=SQLSpecStatementConfig(),
        driver_features={},
        prepare_driver=prepare_driver,
    )
    with pytest.raises(_SessionBodyError), context:
        pytest.fail("prepare_driver failure must abort __enter__")

    assert release_events == [(True, True, _SessionBodyError)]
    assert holder[0].connection is _DETACHED_CONNECTION


@pytest.mark.parametrize(("module_name", "context_name"), _ASYNC_ENTER_FAILURE_CONTEXTS)
async def test_async_session_context_enter_failure_detaches_driver(module_name: str, context_name: str) -> None:
    """Async session contexts with guarded __aenter__ setup release first and detach constructed drivers on failure."""
    context_class = getattr(import_module(module_name), context_name)
    stub_conn = _make_stub_connection(is_async=True)
    release_events: list[tuple[bool, bool, Any]] = []
    holder: list[Any] = []

    async def acquire_connection() -> Any:
        return stub_conn

    def prepare_driver(driver: Any) -> Any:
        holder.append(driver)
        raise _SessionBodyError

    async def release_connection(conn: Any, **kwargs: Any) -> None:
        assert conn is not _DETACHED_CONNECTION
        drv = holder[0]
        release_events.append((conn is stub_conn, drv.connection is stub_conn, kwargs.get("exc_type")))

    context = context_class(
        acquire_connection=acquire_connection,
        release_connection=release_connection,
        statement_config=SQLSpecStatementConfig(),
        driver_features={},
        prepare_driver=prepare_driver,
    )
    with pytest.raises(_SessionBodyError):
        async with context:
            pytest.fail("prepare_driver failure must abort __aenter__")

    assert release_events == [(True, True, _SessionBodyError)]
    assert holder[0].connection is _DETACHED_CONNECTION
