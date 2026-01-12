"""Unit tests for Psycopg pgvector type handlers."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.typing import PGVECTOR_INSTALLED


def test_register_pgvector_sync_with_pgvector_installed() -> None:
    """Test register_pgvector_sync with pgvector installed."""
    if not PGVECTOR_INSTALLED:
        pytest.skip("pgvector not installed")

    from sqlspec.adapters.psycopg.type_converter import register_pgvector_sync

    mock_connection = MagicMock()
    # Mock the pgvector registration to avoid warnings with mock connections
    with patch("sqlspec.adapters.psycopg.type_converter.importlib.import_module") as mock_import:
        mock_pgvector = MagicMock()
        mock_import.return_value = mock_pgvector
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


async def test_register_pgvector_async_with_pgvector_installed() -> None:
    """Test register_pgvector_async with pgvector installed."""
    if not PGVECTOR_INSTALLED:
        pytest.skip("pgvector not installed")

    from sqlspec.adapters.psycopg.type_converter import register_pgvector_async

    mock_connection = AsyncMock()
    # Mock the pgvector registration to avoid warnings with mock connections
    with patch("sqlspec.adapters.psycopg.type_converter.importlib.import_module") as mock_import:
        mock_pgvector = MagicMock()
        mock_pgvector.register_vector_async = AsyncMock()
        mock_import.return_value = mock_pgvector
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


def test_register_pgvector_sync_handles_registration_failure() -> None:
    """Test register_pgvector_sync handles registration failures gracefully."""
    if not PGVECTOR_INSTALLED:
        pytest.skip("pgvector not installed")

    from sqlspec.adapters.psycopg.type_converter import register_pgvector_sync

    mock_connection = MagicMock()
    # Mock the pgvector module to raise an exception
    with patch("sqlspec.adapters.psycopg.type_converter.importlib.import_module") as mock_import:
        mock_pgvector = MagicMock()
        mock_pgvector.register_vector.side_effect = Exception("Registration failed")
        mock_import.return_value = mock_pgvector
        # Should not raise - errors are logged and swallowed
        register_pgvector_sync(mock_connection)


async def test_register_pgvector_async_handles_registration_failure() -> None:
    """Test register_pgvector_async handles registration failures gracefully."""
    if not PGVECTOR_INSTALLED:
        pytest.skip("pgvector not installed")

    from sqlspec.adapters.psycopg.type_converter import register_pgvector_async

    mock_connection = AsyncMock()
    # Mock the pgvector module to raise an exception
    with patch("sqlspec.adapters.psycopg.type_converter.importlib.import_module") as mock_import:
        mock_pgvector = MagicMock()
        mock_pgvector.register_vector_async = AsyncMock(side_effect=Exception("Registration failed"))
        mock_import.return_value = mock_pgvector
        # Should not raise - errors are logged and swallowed
        await register_pgvector_async(mock_connection)
