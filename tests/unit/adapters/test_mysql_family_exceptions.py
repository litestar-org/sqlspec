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
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
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
def test_mysql_create_mapped_exception_suppresses_expected_migration_codes(module_path: str) -> None:
    core = importlib.import_module(module_path)

    assert core.create_mapped_exception(_make_error(1091)) is True
