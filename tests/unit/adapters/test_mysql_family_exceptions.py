"""Unit tests for MySQL-family exception mapping helpers."""

import importlib
from typing import Any

import pytest

from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    ReadOnlyTransactionError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)

MODULE_PATHS = (
    "sqlspec.adapters.aiomysql.core",
    "sqlspec.adapters.asyncmy.core",
    "sqlspec.adapters.mysqlconnector.core",
    "sqlspec.adapters.pymysql.core",
)


class FakeMysqlError(Exception):
    """Minimal MySQL-driver-like error for mapper tests."""

    def __init__(self, *args: Any, errno: int | None = None, sqlstate: str | None = None) -> None:
        super().__init__(*args)
        self.errno = errno
        self.sqlstate = sqlstate


def _make_error(code: int | None, sqlstate: str | None = None) -> FakeMysqlError:
    if code is None:
        return FakeMysqlError("synthetic mysql error", sqlstate=sqlstate)
    return FakeMysqlError(code, "synthetic mysql error", errno=code, sqlstate=sqlstate)


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_mysql_exception_dispatch_tables_are_adapter_local(module_path: str) -> None:
    core = importlib.import_module(module_path)

    assert core.__dict__["_MYSQL_CONSTRAINT_ERROR_DISPATCH"][1062][0] is UniqueViolationError
    assert core.__dict__["_MYSQL_ACCESS_ERROR_DISPATCH"][1045][0] is PermissionDeniedError
    assert core.__dict__["_MYSQL_TRANSACTION_ERROR_DISPATCH"][1213][0] is DeadlockError
    assert core.__dict__["_MYSQL_CONNECTION_ERROR_DISPATCH"][2013][0] is ConnectionTimeoutError


@pytest.mark.parametrize("module_path", MODULE_PATHS)
@pytest.mark.parametrize(
    ("code", "expected_type"),
    (
        (1062, UniqueViolationError),
        (1452, ForeignKeyViolationError),
        (1048, NotNullViolationError),
        (3819, CheckViolationError),
        (1045, PermissionDeniedError),
        (1213, DeadlockError),
        (1205, QueryTimeoutError),
        (2013, ConnectionTimeoutError),
        (2002, DatabaseConnectionError),
        (1064, SQLParsingError),
    ),
)
def test_mysql_create_mapped_exception_uses_error_code_dispatch(
    module_path: str, code: int, expected_type: type[Exception]
) -> None:
    core = importlib.import_module(module_path)
    error = _make_error(code)

    result = core.create_mapped_exception(error)

    assert isinstance(result, expected_type)
    assert result.__cause__ is error


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_mysql_create_mapped_exception_preserves_legacy_dispatch_priority(module_path: str) -> None:
    core = importlib.import_module(module_path)

    integrity_result = core.create_mapped_exception(_make_error(1045, sqlstate="23000"))
    unique_result = core.create_mapped_exception(_make_error(1062, sqlstate="28000"))

    assert isinstance(integrity_result, IntegrityError)
    assert isinstance(unique_result, UniqueViolationError)


@pytest.mark.parametrize("module_path", MODULE_PATHS)
@pytest.mark.parametrize("code", [1061, 1091])
def test_mysql_create_mapped_exception_maps_ddl_codes_to_operational_error(module_path: str, code: int) -> None:
    core = importlib.import_module(module_path)

    result = core.create_mapped_exception(_make_error(code, sqlstate="42000"))

    assert isinstance(result, OperationalError)


HANDLER_CASES = (
    ("sqlspec.adapters.aiomysql.driver", "AiomysqlExceptionHandler", "AiomysqlPymysqlError"),
    ("sqlspec.adapters.asyncmy.driver", "AsyncmyExceptionHandler", "AsyncmyError"),
    ("sqlspec.adapters.mysqlconnector.driver", "MysqlConnectorSyncExceptionHandler", "MysqlConnectorError"),
    ("sqlspec.adapters.pymysql.driver", "PyMysqlExceptionHandler", "PyMysqlMySQLError"),
)


@pytest.mark.parametrize(("module_path", "handler_name", "error_name"), HANDLER_CASES)
def test_mysql_handler_records_mapped_ddl_error(module_path: str, handler_name: str, error_name: str) -> None:
    module = importlib.import_module(module_path)
    native_error_cls = getattr(module, error_name)
    if not isinstance(native_error_cls, type):
        pytest.skip(f"{error_name} driver is not installed")
    error = (
        native_error_cls(msg="Duplicate key name 'idx'", errno=1061)
        if error_name == "MysqlConnectorError"
        else native_error_cls(1061, "Duplicate key name 'idx'")
    )
    handler = getattr(module, handler_name)()

    suppressed = handler._handle_exception(type(error), error)

    assert suppressed is True
    assert isinstance(handler.pending_exception, OperationalError)


@pytest.mark.parametrize("module_path", MODULE_PATHS)
@pytest.mark.parametrize(
    ("code", "sqlstate", "message", "expected"),
    [
        (1792, None, "read-only transaction", ReadOnlyTransactionError),
        (1836, None, "read-only server", ReadOnlyTransactionError),
        (1290, None, "The MySQL server is running with --read-only", ReadOnlyTransactionError),
        (1290, None, "The MySQL server is running with --super_read_only", ReadOnlyTransactionError),
        (1290, None, "The MySQL server is running with --secure-file-priv", SQLSpecError),
        (None, "25006", "read-only transaction", ReadOnlyTransactionError),
    ],
)
def test_mysql_readonly_error_qualification(
    module_path: str, code: int | None, sqlstate: str | None, message: str, expected: type[Exception]
) -> None:
    error = FakeMysqlError(code, message, errno=code, sqlstate=sqlstate)
    result = importlib.import_module(module_path).create_mapped_exception(error)
    if code == 1290 and "secure-file-priv" in message and module_path == "sqlspec.adapters.asyncmy.core":
        expected = PermissionDeniedError
    assert type(result) is expected
    assert result.__cause__ is error


@pytest.mark.parametrize(
    ("adapter", "driver_name", "asynchronous"),
    [
        ("asyncmy", "AsyncmyDriver", True),
        ("aiomysql", "AiomysqlDriver", True),
        ("pymysql", "PyMysqlDriver", False),
        ("mysqlconnector", "MysqlConnectorSyncDriver", False),
        ("mysqlconnector", "MysqlConnectorAsyncDriver", True),
    ],
)
@pytest.mark.parametrize("mariadb", [False, True])
@pytest.mark.parametrize(("position", "reached"), [(0, True), (1, False), (-1, False), (None, False)])
async def test_mysql_gtid_wait_runs_parameterized_native_query(
    adapter: str, driver_name: str, asynchronous: bool, mariadb: bool, position: int | None, reached: bool
) -> None:
    from unittest.mock import AsyncMock, MagicMock

    cursor = MagicMock()
    cursor.description = [("position", 3, None, None, None, None, None)]
    cursor.rowcount = 1
    cursor.lastrowid = None
    cursor.__enter__.return_value = cursor
    if asynchronous:
        cursor.execute = AsyncMock()
        cursor.fetchall = AsyncMock(return_value=[(position,)])
        cursor.close = AsyncMock()
    else:
        cursor.fetchall.return_value = [(position,)]
    connection = MagicMock()
    if asynchronous and adapter != "asyncmy":
        connection.cursor = AsyncMock(return_value=cursor)
    else:
        connection.cursor.return_value = cursor
    driver_type = getattr(importlib.import_module(f"sqlspec.adapters.{adapter}.driver"), driver_name)
    driver = driver_type(connection=connection)
    pending = driver.wait_for_gtid_set("0-1-10", timeout=0.5, mariadb=mariadb)
    result = await pending if asynchronous else pending
    assert result is reached
    sql, parameters = cursor.execute.call_args.args
    assert ("MASTER_GTID_WAIT" if mariadb else "WAIT_FOR_EXECUTED_GTID_SET") in sql
    assert tuple(parameters) == ("0-1-10", 0.5)
    cursor.close.assert_called_once()


@pytest.mark.parametrize("module_path", MODULE_PATHS)
@pytest.mark.parametrize(
    ("gtid", "timeout"),
    [("", 1.0), (" ", 1.0), ("0-1-10", -1.0), ("0-1-10", True), ("0-1-10", float("inf")), ("0-1-10", float("nan"))],
)
def test_mysql_gtid_wait_refuses_unbounded_or_empty_inputs(module_path: str, gtid: str, timeout: float) -> None:
    core = importlib.import_module(module_path)
    with pytest.raises(ValueError, match="GTID wait"):
        core.build_gtid_wait_query(gtid, timeout, mariadb=False)
