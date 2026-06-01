"""Regression tests for wrap_exceptions."""

import inspect

import pytest

from sqlspec.exceptions import RepositoryError, SQLSpecError, wrap_exceptions


def test_wrap_exceptions_suppresses_single_type() -> None:
    """suppress=<type> silently swallows matching exceptions."""
    with wrap_exceptions(suppress=ValueError):
        raise ValueError("suppressed")


def test_wrap_exceptions_suppresses_tuple_of_types() -> None:
    """suppress=(<type>, ...) silently swallows matching exceptions."""
    with wrap_exceptions(suppress=(ValueError, TypeError)):
        raise TypeError("suppressed")


def test_wrap_exceptions_wraps_unmatched_suppressed_type() -> None:
    """Non-matching exceptions are still wrapped."""
    with pytest.raises(RepositoryError):
        with wrap_exceptions(suppress=ValueError):
            raise RuntimeError("not suppressed")


def test_wrap_exceptions_sqlspec_error_passes_through() -> None:
    """SQLSpecError is reraised when it is not explicitly suppressed."""
    original = SQLSpecError("already mapped")

    with pytest.raises(SQLSpecError) as exc_info:
        with wrap_exceptions():
            raise original

    assert exc_info.value is original


def test_wrap_exceptions_does_not_guard_suppress_classinfo_shape() -> None:
    """wrap_exceptions should delegate classinfo handling directly to isinstance."""
    source = inspect.getsource(wrap_exceptions)

    assert "isinstance(suppress, type)" not in source
    assert "isinstance(suppress, tuple)" not in source
    assert "isinstance(exc, suppress)" in source
