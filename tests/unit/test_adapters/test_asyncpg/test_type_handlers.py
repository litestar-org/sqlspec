"""Unit tests for asyncpg type handlers."""

from unittest.mock import AsyncMock, MagicMock, patch

from sqlspec.adapters.asyncpg.config import register_json_codecs, register_pgvector_support


async def test_register_json_codecs_success() -> None:
    """Test successful JSON codec registration."""
    connection = AsyncMock()
    encoder = MagicMock()
    decoder = MagicMock()

    await register_json_codecs(connection, encoder, decoder)

    assert connection.set_type_codec.call_count == 2

    json_call = connection.set_type_codec.call_args_list[0]
    assert json_call.args == ("json",)
    assert json_call.kwargs == {"encoder": encoder, "decoder": decoder, "schema": "pg_catalog"}

    jsonb_call = connection.set_type_codec.call_args_list[1]
    assert jsonb_call.args == ("jsonb",)
    assert jsonb_call.kwargs == {"encoder": encoder, "decoder": decoder, "schema": "pg_catalog"}


async def test_register_json_codecs_handles_exception() -> None:
    """Test that JSON codec registration handles exceptions gracefully."""
    connection = AsyncMock()
    connection.set_type_codec.side_effect = Exception("Database error")
    encoder = MagicMock()
    decoder = MagicMock()

    await register_json_codecs(connection, encoder, decoder)

    connection.set_type_codec.assert_called_once()


@patch("sqlspec.adapters.asyncpg.config.PGVECTOR_INSTALLED", False)
async def test_register_pgvector_support_not_installed() -> None:
    """Test pgvector registration when library not installed."""
    connection = AsyncMock()

    await register_pgvector_support(connection)

    connection.assert_not_called()


@patch("sqlspec.adapters.asyncpg.config.PGVECTOR_INSTALLED", True)
async def test_register_pgvector_support_success() -> None:
    """Test successful pgvector registration."""
    connection = AsyncMock()

    with patch("pgvector.asyncpg.register_vector", new_callable=AsyncMock) as mock_register:
        await register_pgvector_support(connection)
        mock_register.assert_called_once_with(connection)


@patch("sqlspec.adapters.asyncpg.config.PGVECTOR_INSTALLED", True)
async def test_register_pgvector_support_handles_exception() -> None:
    """Test that pgvector registration handles exceptions gracefully."""
    connection = AsyncMock()

    with patch("pgvector.asyncpg.register_vector", new_callable=AsyncMock) as mock_register:
        mock_register.side_effect = Exception("Registration error")
        await register_pgvector_support(connection)
