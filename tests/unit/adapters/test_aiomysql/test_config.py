"""aiomysql configuration tests covering statement config builders."""

from unittest.mock import AsyncMock, MagicMock

import aiomysql  # pyright: ignore
import pytest

from sqlspec.adapters.aiomysql._typing import AiomysqlCursor, AiomysqlRawCursor
from sqlspec.adapters.aiomysql.config import AiomysqlConfig
from sqlspec.adapters.aiomysql.core import build_statement_config


def test_build_default_statement_config_custom_serializers() -> None:
    """Custom serializers should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    def deserializer(_: str) -> object:
        return {"value": "deserialized"}

    statement_config = build_statement_config(json_serializer=serializer, json_deserializer=deserializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.json_deserializer is deserializer


def test_aiomysql_config_applies_driver_feature_serializers() -> None:
    """Driver features should mutate the aiomysql statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    def deserializer(_: str) -> object:
        return {"feature": True}

    config = AiomysqlConfig(driver_features={"json_serializer": serializer, "json_deserializer": deserializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.json_deserializer is deserializer


def test_aiomysql_signature_namespace_exposes_pool_type() -> None:
    """DI signature namespace must expose AiomysqlPool so Litestar route handlers can type-hint the pool.

    Parity check against asyncmy, which registers AsyncmyPool for the same reason.
    """
    namespace = AiomysqlConfig().get_signature_namespace()
    assert "AiomysqlPool" in namespace, "AiomysqlPool missing from DI namespace (parity gap vs asyncmy)"
    assert namespace["AiomysqlPool"] is aiomysql.Pool


@pytest.mark.asyncio
async def test_aiomysql_cursor_omits_class_arg_when_unset() -> None:
    """AiomysqlCursor with cursor_class=None must call conn.cursor() without args.

    aiomysql's Connection.cursor(*cursors) rejects None via issubclass(None, Cursor),
    so passing None-as-positional is a runtime TypeError. This test locks in the
    omit-when-unset behavior.
    """
    raw_cursor = MagicMock()
    raw_cursor.close = AsyncMock()
    connection = MagicMock()
    connection.cursor = AsyncMock(return_value=raw_cursor)

    async with AiomysqlCursor(connection) as cursor:
        assert cursor is raw_cursor

    connection.cursor.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_aiomysql_cursor_forwards_class_arg_when_set() -> None:
    """AiomysqlCursor with cursor_class=AiomysqlRawCursor must forward it.

    First-party store code passes the tuple cursor class explicitly so that a
    user-configured cursor_class=DictCursor on AiomysqlConfig doesn't break
    positional row access in ADK/Litestar/Events stores.
    """
    raw_cursor = MagicMock()
    raw_cursor.close = AsyncMock()
    connection = MagicMock()
    connection.cursor = AsyncMock(return_value=raw_cursor)

    async with AiomysqlCursor(connection, cursor_class=AiomysqlRawCursor) as cursor:
        assert cursor is raw_cursor

    connection.cursor.assert_awaited_once_with(AiomysqlRawCursor)
