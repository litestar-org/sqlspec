"""Unit tests for asyncpg type handlers."""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from sqlspec.adapters.asyncpg.config import register_json_codecs, register_pgvector_support
from sqlspec.adapters.asyncpg.core import create_mapped_exception
from sqlspec.exceptions import (
    DatabaseConnectionError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json


@pytest.mark.anyio
async def test_register_json_codecs_success() -> None:
    """Test successful JSON codec registration."""
    connection = AsyncMock()
    encoder = to_json
    decoder = from_json

    await register_json_codecs(connection, encoder, decoder)

    assert connection.set_type_codec.call_count == 2

    json_call = connection.set_type_codec.call_args_list[0]
    assert json_call.args == ("json",)
    assert json_call.kwargs == {
        "encoder": json_call.kwargs["encoder"],
        "decoder": json_call.kwargs["decoder"],
        "schema": "pg_catalog",
        "format": "binary",
    }

    jsonb_call = connection.set_type_codec.call_args_list[1]
    assert jsonb_call.args == ("jsonb",)
    assert jsonb_call.kwargs == {
        "encoder": jsonb_call.kwargs["encoder"],
        "decoder": jsonb_call.kwargs["decoder"],
        "schema": "pg_catalog",
        "format": "binary",
    }

    assert json_call.kwargs["encoder"]({"value": 1}) == b'{"value":1}'
    assert json_call.kwargs["encoder"]("console") == b'"console"'
    assert json_call.kwargs["encoder"]("123") == b'"123"'
    assert json_call.kwargs["encoder"]("true") == b'"true"'
    assert json_call.kwargs["encoder"]("null") == b'"null"'
    assert json_call.kwargs["encoder"]('"text"') == b'"\\"text\\""'
    assert json_call.kwargs["encoder"]('{"pre":"serialized"}') == b'"{\\"pre\\":\\"serialized\\"}"'
    assert json_call.kwargs["encoder"](b'\x01{"value":1}') == b'{"value":1}'
    assert json_call.kwargs["decoder"](b'{"value":1}') == {"value": 1}
    assert jsonb_call.kwargs["encoder"]({"value": 1}) == b'\x01{"value":1}'
    assert jsonb_call.kwargs["encoder"]("console") == b'\x01"console"'
    assert jsonb_call.kwargs["encoder"]("123") == b'\x01"123"'
    assert jsonb_call.kwargs["encoder"]("true") == b'\x01"true"'
    assert jsonb_call.kwargs["encoder"]("null") == b'\x01"null"'
    assert jsonb_call.kwargs["encoder"]('"text"') == b'\x01"\\"text\\""'
    assert jsonb_call.kwargs["encoder"]('{"pre":"serialized"}') == b'\x01"{\\"pre\\":\\"serialized\\"}"'
    assert jsonb_call.kwargs["encoder"](b'\x01{"value":1}') == b'\x01{"value":1}'
    assert jsonb_call.kwargs["decoder"](b'\x01{"value":1}') == {"value": 1}


@pytest.mark.anyio
@pytest.mark.parametrize("scalar", ["", "123", "-45.6", "true", "false", "null", "[1, 2]", '{"a": 1}'])
async def test_register_json_codecs_invokes_custom_serializer_for_string_scalars(scalar: str) -> None:
    """Plain Python strings serialize as JSON string scalars and invoke custom serializers."""
    connection = AsyncMock()
    seen: list[object] = []

    def custom_encoder(val: object) -> str:
        seen.append(val)
        return to_json(val)

    await register_json_codecs(connection, custom_encoder, from_json)
    json_encoder = connection.set_type_codec.call_args_list[0].kwargs["encoder"]
    json_decoder = connection.set_type_codec.call_args_list[0].kwargs["decoder"]
    jsonb_encoder = connection.set_type_codec.call_args_list[1].kwargs["encoder"]
    jsonb_decoder = connection.set_type_codec.call_args_list[1].kwargs["decoder"]

    encoded_json = json_encoder(scalar)
    encoded_jsonb = jsonb_encoder(scalar)
    assert encoded_json == to_json(scalar).encode("utf-8")
    assert encoded_jsonb == b"\x01" + to_json(scalar).encode("utf-8")
    assert json_decoder(encoded_json) == scalar
    assert jsonb_decoder(encoded_jsonb) == scalar
    assert seen == [scalar, scalar]


@pytest.mark.anyio
async def test_asyncpg_connection_error_mapping() -> None:
    """ConnectionDoesNotExistError and CannotConnectNowError map to DatabaseConnectionError."""
    exc1 = asyncpg.exceptions.ConnectionDoesNotExistError("connection lost")
    mapped1 = create_mapped_exception(exc1)
    assert isinstance(mapped1, DatabaseConnectionError)

    exc2 = asyncpg.exceptions.CannotConnectNowError("server starting up")
    mapped2 = create_mapped_exception(exc2)
    assert isinstance(mapped2, DatabaseConnectionError)


@pytest.mark.anyio
async def test_register_json_codecs_handles_exception() -> None:
    """Test that JSON codec registration handles exceptions gracefully."""
    connection = AsyncMock()
    connection.set_type_codec.side_effect = Exception("Database error")
    encoder = MagicMock()
    decoder = MagicMock()

    await register_json_codecs(connection, encoder, decoder)

    connection.set_type_codec.assert_called_once()


@patch("sqlspec.adapters.asyncpg.config.PGVECTOR_INSTALLED", False)
@pytest.mark.anyio
async def test_register_pgvector_support_not_installed() -> None:
    """Test pgvector registration when library not installed."""
    connection = AsyncMock()

    await register_pgvector_support(connection)

    connection.assert_not_called()


@patch("sqlspec.adapters.asyncpg.config.PGVECTOR_INSTALLED", True)
@pytest.mark.anyio
async def test_register_pgvector_support_success() -> None:
    """Test successful pgvector registration."""
    connection = AsyncMock()

    with patch("pgvector.asyncpg.register_vector", new_callable=AsyncMock) as mock_register:
        await register_pgvector_support(connection)
        mock_register.assert_called_once_with(connection)


@patch("sqlspec.adapters.asyncpg.config.PGVECTOR_INSTALLED", True)
@pytest.mark.anyio
async def test_register_pgvector_support_handles_exception() -> None:
    """Test that pgvector registration handles exceptions gracefully."""
    connection = AsyncMock()

    with patch("pgvector.asyncpg.register_vector", new_callable=AsyncMock) as mock_register:
        mock_register.side_effect = Exception("Registration error")
        await register_pgvector_support(connection)


def test_create_mapped_exception_uses_exact_exception_dispatch() -> None:
    error = asyncpg.exceptions.UniqueViolationError("duplicate key")

    result = create_mapped_exception(error)

    assert isinstance(result, UniqueViolationError)
    assert result.__cause__ is error


def test_create_mapped_exception_uses_registered_permission_dispatch() -> None:
    error = asyncpg.exceptions.InvalidPasswordError("bad password")

    result = create_mapped_exception(error)

    assert isinstance(result, PermissionDeniedError)
    assert result.__cause__ is error


def test_create_mapped_exception_distinguishes_user_cancel_from_statement_timeout() -> None:
    cancelled = create_mapped_exception(
        asyncpg.exceptions.QueryCanceledError("canceling statement due to user request")
    )
    timed_out = create_mapped_exception(
        asyncpg.exceptions.QueryCanceledError("canceling statement due to statement timeout")
    )

    assert isinstance(cancelled, OperationCancelledError)
    assert isinstance(timed_out, QueryTimeoutError)
