"""Unit tests for SQLite exception mapping."""

import sqlite3

from sqlspec.adapters.sqlite.core import create_mapped_exception
from sqlspec.exceptions import DeadlockError, OperationCancelledError, PermissionDeniedError, UniqueViolationError


class _SqliteError(sqlite3.OperationalError):
    def __init__(self, message: str, code: int | None = None, name: str | None = None) -> None:
        super().__init__(message)
        self.sqlite_errorcode = code
        self.sqlite_errorname = name


class _SqliteIntegrityError(sqlite3.IntegrityError):
    def __init__(self, message: str, code: int | None = None, name: str | None = None) -> None:
        super().__init__(message)
        self.sqlite_errorcode = code
        self.sqlite_errorname = name


def test_primary_key_code_maps_to_unique_violation() -> None:
    err = _SqliteIntegrityError("UNIQUE constraint failed: t.id", 1555, None)
    result = create_mapped_exception(err)
    assert isinstance(result, UniqueViolationError)
    assert result.__cause__ is err


def test_primary_key_error_name_maps_to_unique_violation() -> None:
    err = _SqliteIntegrityError("primary key rejected", None, "SQLITE_CONSTRAINT_PRIMARYKEY")
    result = create_mapped_exception(err)
    assert isinstance(result, UniqueViolationError)
    assert result.__cause__ is err


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


def test_interrupt_error_code_maps_to_operation_cancelled() -> None:
    err = _SqliteError("interrupted", 9, "SQLITE_INTERRUPT")
    result = create_mapped_exception(err)
    assert isinstance(result, OperationCancelledError)
    assert result.__cause__ is err


def test_perm_error_code_maps_to_permission_denied() -> None:
    err = _SqliteError("access permission denied", 3, "SQLITE_PERM")
    result = create_mapped_exception(err)
    assert isinstance(result, PermissionDeniedError)
    assert result.__cause__ is err


def test_readonly_error_code_maps_to_permission_denied() -> None:
    err = _SqliteError("attempt to write a readonly database", 8, "SQLITE_READONLY")
    result = create_mapped_exception(err)
    assert isinstance(result, PermissionDeniedError)
    assert result.__cause__ is err
