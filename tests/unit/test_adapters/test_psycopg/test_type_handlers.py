"""Unit tests for Psycopg pgvector type handlers."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec._typing import PGVECTOR_INSTALLED


def test_register_pgvector_sync_with_pgvector_installed() -> None:
    """Test register_pgvector_sync with pgvector installed."""
    if not PGVECTOR_INSTALLED:
        pytest.skip("pgvector not installed")

    from sqlspec.adapters.psycopg._type_converter import register_pgvector_sync

    mock_connection = MagicMock()
    register_pgvector_sync(mock_connection)


def test_register_pgvector_sync_without_pgvector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_sync gracefully handles pgvector not installed."""
    import sqlspec.adapters.psycopg._type_converter

    monkeypatch.setattr(sqlspec.adapters.psycopg._type_converter, "PGVECTOR_INSTALLED", False)

    from sqlspec.adapters.psycopg._type_converter import register_pgvector_sync

    mock_connection = MagicMock(spec=[])
    register_pgvector_sync(mock_connection)

    assert len(mock_connection.method_calls) == 0


async def test_register_pgvector_async_with_pgvector_installed() -> None:
    """Test register_pgvector_async with pgvector installed."""
    if not PGVECTOR_INSTALLED:
        pytest.skip("pgvector not installed")

    from sqlspec.adapters.psycopg._type_converter import register_pgvector_async

    mock_connection = AsyncMock()
    await register_pgvector_async(mock_connection)


async def test_register_pgvector_async_without_pgvector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_pgvector_async gracefully handles pgvector not installed."""
    import sqlspec.adapters.psycopg._type_converter

    monkeypatch.setattr(sqlspec.adapters.psycopg._type_converter, "PGVECTOR_INSTALLED", False)

    from sqlspec.adapters.psycopg._type_converter import register_pgvector_async

    mock_connection = AsyncMock(spec=[])
    await register_pgvector_async(mock_connection)

    assert len(mock_connection.method_calls) == 0


def test_register_pgvector_sync_handles_registration_failure() -> None:
    """Test register_pgvector_sync handles registration failures gracefully."""
    if not PGVECTOR_INSTALLED:
        pytest.skip("pgvector not installed")

    from sqlspec.adapters.psycopg._type_converter import register_pgvector_sync

    mock_connection = MagicMock()
    mock_connection.side_effect = Exception("Registration failed")

    register_pgvector_sync(mock_connection)


async def test_register_pgvector_async_handles_registration_failure() -> None:
    """Test register_pgvector_async handles registration failures gracefully."""
    if not PGVECTOR_INSTALLED:
        pytest.skip("pgvector not installed")

    from sqlspec.adapters.psycopg._type_converter import register_pgvector_async

    mock_connection = AsyncMock()
    mock_connection.side_effect = Exception("Registration failed")

    await register_pgvector_async(mock_connection)
