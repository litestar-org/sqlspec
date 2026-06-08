"""Unit tests for aiosqlite exception mapping parity with sqlite."""

import sqlite3

from sqlspec.adapters.aiosqlite.core import create_mapped_exception
from sqlspec.exceptions import DeadlockError, PermissionDeniedError, QueryTimeoutError


class _SqliteError(sqlite3.OperationalError):
    def __init__(self, message: str, code: int | None = None, name: str | None = None) -> None:
        super().__init__(message)
        self.sqlite_errorcode = code
        self.sqlite_errorname = name


def test_busy_error_code_maps_to_deadlock() -> None:
    err = _SqliteError("database is locked", 5, "SQLITE_BUSY")
    result = create_mapped_exception(err)
    assert isinstance(result, DeadlockError)
    assert result.__cause__ is err


def test_busy_error_name_maps_to_deadlock() -> None:
    result = create_mapped_exception(_SqliteError("database is busy", None, "SQLITE_BUSY"))
    assert isinstance(result, DeadlockError)


def test_locked_error_code_maps_to_deadlock() -> None:
    err = _SqliteError("database table is locked", 6, "SQLITE_LOCKED")
    result = create_mapped_exception(err)
    assert isinstance(result, DeadlockError)
    assert result.__cause__ is err


def test_locked_error_name_maps_to_deadlock() -> None:
    result = create_mapped_exception(_SqliteError("table is locked", None, "SQLITE_LOCKED"))
    assert isinstance(result, DeadlockError)


def test_locked_text_heuristic_maps_to_deadlock() -> None:
    result = create_mapped_exception(sqlite3.OperationalError("database locked"))
    assert isinstance(result, DeadlockError)


def test_busy_text_heuristic_maps_to_deadlock() -> None:
    result = create_mapped_exception(sqlite3.OperationalError("database is busy, please retry"))
    assert isinstance(result, DeadlockError)


def test_interrupt_error_code_maps_to_query_timeout() -> None:
    err = _SqliteError("interrupted", 9, "SQLITE_INTERRUPT")
    result = create_mapped_exception(err)
    assert isinstance(result, QueryTimeoutError)
    assert result.__cause__ is err


def test_interrupt_error_name_maps_to_query_timeout() -> None:
    result = create_mapped_exception(_SqliteError("query was interrupted", None, "SQLITE_INTERRUPT"))
    assert isinstance(result, QueryTimeoutError)


def test_interrupt_text_heuristic_maps_to_query_timeout() -> None:
    result = create_mapped_exception(sqlite3.OperationalError("query was interrupted by application"))
    assert isinstance(result, QueryTimeoutError)


def test_perm_error_code_maps_to_permission_denied() -> None:
    err = _SqliteError("access permission denied", 3, "SQLITE_PERM")
    result = create_mapped_exception(err)
    assert isinstance(result, PermissionDeniedError)
    assert result.__cause__ is err


def test_perm_error_name_maps_to_permission_denied() -> None:
    result = create_mapped_exception(_SqliteError("access permission denied", None, "SQLITE_PERM"))
    assert isinstance(result, PermissionDeniedError)


def test_readonly_error_code_maps_to_permission_denied() -> None:
    err = _SqliteError("attempt to write a readonly database", 8, "SQLITE_READONLY")
    result = create_mapped_exception(err)
    assert isinstance(result, PermissionDeniedError)
    assert result.__cause__ is err


def test_readonly_error_name_maps_to_permission_denied() -> None:
    result = create_mapped_exception(_SqliteError("readonly database", None, "SQLITE_READONLY"))
    assert isinstance(result, PermissionDeniedError)


def test_readonly_text_heuristic_maps_to_permission_denied() -> None:
    result = create_mapped_exception(sqlite3.OperationalError("attempt to write a readonly database"))
    assert isinstance(result, PermissionDeniedError)


def test_permission_denied_text_heuristic_maps_to_permission_denied() -> None:
    result = create_mapped_exception(sqlite3.OperationalError("permission denied: cannot open /etc/passwd"))
    assert isinstance(result, PermissionDeniedError)
