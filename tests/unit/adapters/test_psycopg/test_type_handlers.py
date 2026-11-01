"""Unit tests for Psycopg pgvector type handlers."""

from unittest.mock import AsyncMock, MagicMock

import pytest


def test_register_pgvector_sync_with_pgvector_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_sync with pgvector installed."""
    import sqlspec.adapters.psycopg.type_converter as type_converter
    from sqlspec.adapters.psycopg.type_converter import register_pgvector_sync

    mock_connection = MagicMock()
    mock_pgvector = MagicMock()
    monkeypatch.setattr(type_converter, "_pgvector_psycopg", mock_pgvector)

    register_pgvector_sync(mock_connection)

    mock_pgvector.register_vector.assert_called_once_with(mock_connection)


def test_register_pgvector_sync_without_pgvector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_sync gracefully handles pgvector not installed."""
    import sqlspec.adapters.psycopg.type_converter

    monkeypatch.setattr(sqlspec.adapters.psycopg.type_converter, "_pgvector_psycopg", None)

    from sqlspec.adapters.psycopg.type_converter import register_pgvector_sync

    mock_connection = MagicMock(spec=[])
    register_pgvector_sync(mock_connection)

    assert len(mock_connection.method_calls) == 0


async def test_register_pgvector_async_with_pgvector_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_async with pgvector installed."""
    import sqlspec.adapters.psycopg.type_converter as type_converter
    from sqlspec.adapters.psycopg.type_converter import register_pgvector_async

    mock_connection = AsyncMock()
    mock_pgvector = MagicMock()
    mock_pgvector.register_vector_async = AsyncMock()
    monkeypatch.setattr(type_converter, "_pgvector_psycopg", mock_pgvector)

    await register_pgvector_async(mock_connection)

    mock_pgvector.register_vector_async.assert_called_once_with(mock_connection)


async def test_register_pgvector_async_without_pgvector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_async gracefully handles pgvector not installed."""
    import sqlspec.adapters.psycopg.type_converter

    monkeypatch.setattr(sqlspec.adapters.psycopg.type_converter, "_pgvector_psycopg", None)

    from sqlspec.adapters.psycopg.type_converter import register_pgvector_async

    mock_connection = AsyncMock(spec=[])
    await register_pgvector_async(mock_connection)

    assert len(mock_connection.method_calls) == 0


def test_register_pgvector_sync_handles_registration_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_sync handles registration failures gracefully."""
    import sqlspec.adapters.psycopg.type_converter as type_converter
    from sqlspec.adapters.psycopg.type_converter import register_pgvector_sync

    mock_connection = MagicMock()
    mock_pgvector = MagicMock()
    mock_pgvector.register_vector.side_effect = Exception("Registration failed")
    monkeypatch.setattr(type_converter, "_pgvector_psycopg", mock_pgvector)

    register_pgvector_sync(mock_connection)


async def test_register_pgvector_async_handles_registration_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_async handles registration failures gracefully."""
    import sqlspec.adapters.psycopg.type_converter as type_converter
    from sqlspec.adapters.psycopg.type_converter import register_pgvector_async

    mock_connection = AsyncMock()
    mock_pgvector = MagicMock()
    mock_pgvector.register_vector_async = AsyncMock(side_effect=Exception("Registration failed"))
    monkeypatch.setattr(type_converter, "_pgvector_psycopg", mock_pgvector)

    await register_pgvector_async(mock_connection)


def test_pgvector_module_reference_is_cached() -> None:
    import sqlspec.adapters.psycopg.type_converter as type_converter

    assert not hasattr(type_converter, "importlib")
    assert hasattr(type_converter, "_pgvector_psycopg")


def test_operational_error_with_none_sqlstate_maps_to_database_connection_error() -> None:
    """OperationalError with sqlstate=None maps to DatabaseConnectionError."""
    import psycopg

    from sqlspec.adapters.psycopg.core import create_mapped_exception
    from sqlspec.exceptions import DatabaseConnectionError

    err = psycopg.OperationalError("connection failed")
    mapped = create_mapped_exception(err)
    assert type(mapped) is DatabaseConnectionError


def test_lock_not_available_maps_to_query_timeout_error() -> None:
    """pg_errors.LockNotAvailable (55P03) maps to QueryTimeoutError."""
    from psycopg import errors as pg_errors

    from sqlspec.adapters.psycopg.core import create_mapped_exception
    from sqlspec.exceptions import QueryTimeoutError

    err = pg_errors.LockNotAvailable("could not obtain lock on relation")
    mapped = create_mapped_exception(err)
    assert type(mapped) is QueryTimeoutError


def test_sqlstate_class_25_maps_to_transaction_error() -> None:
    """Class 25 SQLSTATE maps to TransactionError."""
    from psycopg import OperationalError

    from sqlspec.adapters.psycopg.core import create_mapped_exception
    from sqlspec.exceptions import TransactionError

    err = OperationalError("active sql transaction")
    err.sqlstate = "25000"
    mapped = create_mapped_exception(err)
    assert type(mapped) is TransactionError


def test_exclusion_violation_23p01_preserves_sqlstate() -> None:
    """ExclusionViolation (23P01) preserves its exact SQLSTATE code in error message."""
    from psycopg import errors as pg_errors

    from sqlspec.adapters.psycopg.core import create_mapped_exception
    from sqlspec.exceptions import IntegrityError

    err = pg_errors.ExclusionViolation("conflicting key value violates exclusion constraint")
    err.sqlstate = "23P01"
    mapped = create_mapped_exception(err)
    assert isinstance(mapped, IntegrityError)
    assert "[23P01]" in str(mapped)
