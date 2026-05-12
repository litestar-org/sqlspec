"""Tests for DuckDB exception mapping via create_mapped_exception."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("duckdb", reason="DuckDB adapter requires duckdb package")

import duckdb

from sqlspec.adapters.duckdb.core import create_mapped_exception
from sqlspec.exceptions import (
    CheckViolationError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)


def _make_native(name: str, message: str) -> tuple[type[BaseException], BaseException]:
    cls = getattr(duckdb, name, None)
    if cls is None or not isinstance(cls, type) or not issubclass(cls, BaseException):
        pytest.skip(f"duckdb.{name} not available in this duckdb build")
    instance: BaseException = cls(message)
    return cls, instance


@pytest.mark.parametrize(
    ("native_name", "message", "expected_class"),
    [
        ("ConstraintException", "Duplicate key value violates unique constraint", UniqueViolationError),
        ("ConstraintException", "duplicate key", UniqueViolationError),
        ("ConstraintException", "violates foreign key constraint on insert", ForeignKeyViolationError),
        ("ConstraintException", "NOT NULL constraint failed", NotNullViolationError),
        ("ConstraintException", "null value in column not allowed", NotNullViolationError),
        ("ConstraintException", "check constraint failed", CheckViolationError),
        ("ConstraintException", "violated check condition", CheckViolationError),
        ("ConstraintException", "some other constraint failure", IntegrityError),
        ("CatalogException", "table users does not exist", NotFoundError),
        ("ParserException", "syntax error at line 1", SQLParsingError),
        ("BinderException", "column not found", SQLParsingError),
        ("PermissionException", "access denied to resource", PermissionDeniedError),
        ("InterruptException", "query interrupted", QueryTimeoutError),
        ("IOException", "disk read failure", OperationalError),
        ("ConversionException", "could not convert string", DataError),
    ],
)
def test_create_mapped_exception_native_dispatch(
    native_name: str, message: str, expected_class: type[SQLSpecError]
) -> None:
    """Native DuckDB exception types must map to the expected SQLSpec error class."""
    exc_type, error = _make_native(native_name, message)
    mapped = create_mapped_exception(exc_type, error)
    assert isinstance(mapped, expected_class), (
        f"{native_name}({message!r}) should map to {expected_class.__name__}, got {type(mapped).__name__}"
    )
    assert mapped.__cause__ is error
    assert "DuckDB" in str(mapped)


def test_create_mapped_exception_fallback_unknown_returns_sqlspec_error() -> None:
    """An unrecognized exception type returns the SQLSpecError fallback."""

    class _SomeOtherError(Exception):
        pass

    error: BaseException = _SomeOtherError("totally unrelated")
    mapped = create_mapped_exception(_SomeOtherError, error)
    assert type(mapped) is SQLSpecError
    assert mapped.__cause__ is error


def test_create_mapped_exception_substring_fallback_permission_message() -> None:
    """When type doesn't match but message indicates permission issue, map accordingly."""

    class _Generic(Exception):
        pass

    error: BaseException = _Generic("permission denied for table users")
    mapped = create_mapped_exception(_Generic, error)
    assert isinstance(mapped, PermissionDeniedError)


def test_create_mapped_exception_substring_fallback_interrupt_message() -> None:
    """When type doesn't match but message indicates cancellation, map accordingly."""

    class _Generic(Exception):
        pass

    error: BaseException = _Generic("statement was canceled by user")
    mapped = create_mapped_exception(_Generic, error)
    assert isinstance(mapped, QueryTimeoutError)


def test_create_mapped_exception_substring_fallback_type_mismatch_message() -> None:
    """When type doesn't match but message indicates type mismatch, map accordingly."""

    class _Generic(Exception):
        pass

    error: BaseException = _Generic("type mismatch in cast")
    mapped = create_mapped_exception(_Generic, error)
    assert isinstance(mapped, DataError)


def test_create_mapped_exception_subclass_dispatch() -> None:
    """Subclasses of mapped types resolve via the MRO walk."""
    catalog_cls = getattr(duckdb, "CatalogException", None)
    if catalog_cls is None:
        pytest.skip("duckdb.CatalogException not available")

    class _CustomCatalogError(catalog_cls):  # type: ignore[misc, valid-type]
        pass

    err: Any = _CustomCatalogError("derived missing-table error")
    mapped = create_mapped_exception(_CustomCatalogError, err)
    assert isinstance(mapped, NotFoundError)


def test_create_mapped_exception_repeated_calls_consistent() -> None:
    """Repeated calls for the same type return the same mapping class (cache safety)."""
    exc_type, error = _make_native("CatalogException", "missing table x")
    first = create_mapped_exception(exc_type, error)
    second = create_mapped_exception(exc_type, error)
    assert type(first) is type(second)
    assert isinstance(first, NotFoundError)
    assert isinstance(second, NotFoundError)
