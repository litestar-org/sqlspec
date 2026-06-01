"""Unit tests for Psycopg pgvector type handlers."""

from unittest.mock import AsyncMock, MagicMock

import pytest


def test_register_pgvector_sync_with_pgvector_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_sync with pgvector installed."""
    import sqlspec.adapters.psycopg.type_converter as type_converter
    from sqlspec.adapters.psycopg.type_converter import register_pgvector_sync

    mock_connection = MagicMock()
    mock_pgvector = MagicMock()
    monkeypatch.setattr(type_converter, "PGVECTOR_INSTALLED", True)
    monkeypatch.setattr(type_converter, "_pgvector_psycopg", mock_pgvector)

    register_pgvector_sync(mock_connection)

    mock_pgvector.register_vector.assert_called_once_with(mock_connection)


def test_register_pgvector_sync_without_pgvector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_sync gracefully handles pgvector not installed."""
    import sqlspec.adapters.psycopg.type_converter

    monkeypatch.setattr(sqlspec.adapters.psycopg.type_converter, "PGVECTOR_INSTALLED", False)

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
    monkeypatch.setattr(type_converter, "PGVECTOR_INSTALLED", True)
    monkeypatch.setattr(type_converter, "_pgvector_psycopg", mock_pgvector)

    await register_pgvector_async(mock_connection)

    mock_pgvector.register_vector_async.assert_called_once_with(mock_connection)


async def test_register_pgvector_async_without_pgvector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_async gracefully handles pgvector not installed."""
    import sqlspec.adapters.psycopg.type_converter

    monkeypatch.setattr(sqlspec.adapters.psycopg.type_converter, "PGVECTOR_INSTALLED", False)

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
    monkeypatch.setattr(type_converter, "PGVECTOR_INSTALLED", True)
    monkeypatch.setattr(type_converter, "_pgvector_psycopg", mock_pgvector)

    register_pgvector_sync(mock_connection)


async def test_register_pgvector_async_handles_registration_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_async handles registration failures gracefully."""
    import sqlspec.adapters.psycopg.type_converter as type_converter
    from sqlspec.adapters.psycopg.type_converter import register_pgvector_async

    mock_connection = AsyncMock()
    mock_pgvector = MagicMock()
    mock_pgvector.register_vector_async = AsyncMock(side_effect=Exception("Registration failed"))
    monkeypatch.setattr(type_converter, "PGVECTOR_INSTALLED", True)
    monkeypatch.setattr(type_converter, "_pgvector_psycopg", mock_pgvector)

    await register_pgvector_async(mock_connection)


def test_pgvector_module_reference_is_cached() -> None:
    import sqlspec.adapters.psycopg.type_converter as type_converter

    assert not hasattr(type_converter, "importlib")
    assert hasattr(type_converter, "_pgvector_psycopg")
