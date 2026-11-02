"""Unit tests for SQLite exception mapping."""

import sqlite3

import pytest

from sqlspec.adapters.sqlite.core import create_mapped_exception
from sqlspec.exceptions import (
    CheckViolationError,
    DataError,
    DeadlockError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    UniqueViolationError,
)


class _SqliteError(sqlite3.OperationalError):
    def __init__(self, message: str, code: int | None = None, name: str | None = None) -> None:
        super().__init__(message)
        setattr(self, "sqlite_errorcode", code)
        setattr(self, "sqlite_errorname", name)


class _SqliteIntegrityError(sqlite3.IntegrityError):
    def __init__(self, message: str, code: int | None = None, name: str | None = None) -> None:
        super().__init__(message)
        setattr(self, "sqlite_errorcode", code)
        setattr(self, "sqlite_errorname", name)


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


@pytest.mark.parametrize("table", ["busy", "locked", "interrupt", "readonly"])
@pytest.mark.parametrize(("code", "name"), [(1555, None), (None, "SQLITE_CONSTRAINT_PRIMARYKEY")])
def test_primary_key_identity_takes_precedence_over_message(table: str, code: int | None, name: str | None) -> None:
    error = _SqliteIntegrityError(f"UNIQUE constraint failed: {table}.id", code, name)
    mapped = create_mapped_exception(error)
    assert isinstance(mapped, UniqueViolationError)
    assert mapped.__cause__ is error


def test_not_null_error_code_maps_to_not_null_violation() -> None:
    err = _SqliteIntegrityError("NOT NULL constraint failed: t.val", 1299, None)
    result = create_mapped_exception(err)
    assert isinstance(result, NotNullViolationError)
    assert result.__cause__ is err


def test_check_error_code_maps_to_check_violation() -> None:
    err = _SqliteIntegrityError("CHECK constraint failed: val > 0", 275, None)
    result = create_mapped_exception(err)
    assert isinstance(result, CheckViolationError)
    assert result.__cause__ is err


def test_trigger_abort_does_not_map_to_not_null_violation() -> None:
    err = _SqliteIntegrityError("trigger aborted", 1811, None)
    result = create_mapped_exception(err)
    assert not isinstance(result, NotNullViolationError)
    assert isinstance(result, IntegrityError)
    assert result.__cause__ is err


def test_no_such_column_with_locked_name_maps_to_operational_error() -> None:
    err = sqlite3.OperationalError("no such column: is_locked")
    result = create_mapped_exception(err)
    assert isinstance(result, OperationalError)
    assert not isinstance(result, DeadlockError)
    assert result.__cause__ is err


def test_programming_error_maps_to_data_error() -> None:
    err = sqlite3.ProgrammingError("cannot operate on a closed database")
    result = create_mapped_exception(err)
    assert isinstance(result, DataError)
    assert result.__cause__ is err


def test_interface_error_maps_to_operational_error() -> None:
    err = sqlite3.InterfaceError("error binding parameter")
    result = create_mapped_exception(err)
    assert isinstance(result, OperationalError)
    assert result.__cause__ is err


def test_misuse_maps_to_data_error() -> None:
    err = _SqliteError("bad call", 21, "SQLITE_MISUSE")
    result = create_mapped_exception(err)
    assert isinstance(result, DataError)
    assert result.__cause__ is err
