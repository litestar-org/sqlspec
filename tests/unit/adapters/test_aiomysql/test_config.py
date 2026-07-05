"""aiomysql configuration tests covering statement config builders."""

from ssl import PROTOCOL_TLS_CLIENT, SSLContext
from unittest.mock import AsyncMock, MagicMock

import aiomysql  # pyright: ignore
import pytest

from sqlspec.adapters.aiomysql._typing import AiomysqlCursor, AiomysqlDictCursor, AiomysqlRawCursor
from sqlspec.adapters.aiomysql.config import AiomysqlConfig
from sqlspec.adapters.aiomysql.core import build_statement_config
from sqlspec.exceptions import ImproperConfigurationError


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


def test_aiomysql_connection_kwargs_normalize_cursor_alias_and_omit_pool_only_keys() -> None:
    """Connection kwargs should use upstream names and exclude pool-only settings."""
    config = AiomysqlConfig(
        connection_config={
            "cursor_class": AiomysqlDictCursor,
            "minsize": 2,
            "maxsize": 8,
            "pool_recycle": 30,
            "enable_local_infile": True,
            "allow_local_infile": True,
        }
    )

    connect_kwargs = config._connection_kwargs()  # pyright: ignore[reportPrivateUsage]

    assert connect_kwargs["cursorclass"] is AiomysqlDictCursor
    assert "cursor_class" not in connect_kwargs
    assert "minsize" not in connect_kwargs
    assert "maxsize" not in connect_kwargs
    assert "pool_recycle" not in connect_kwargs
    assert connect_kwargs["local_infile"] is True
    assert "enable_local_infile" not in connect_kwargs
    assert "allow_local_infile" not in connect_kwargs


def test_aiomysql_local_infile_requires_explicit_security_gate() -> None:
    """LOAD DATA LOCAL INFILE should require a separate consent gate."""
    with pytest.raises(ImproperConfigurationError, match="allow_local_infile=True"):
        AiomysqlConfig(connection_config={"local_infile": True})

    config = AiomysqlConfig(connection_config={"allow_local_infile": True, "local_infile": True})
    connect_kwargs = config._connection_kwargs()  # pyright: ignore[reportPrivateUsage]

    assert connect_kwargs["local_infile"] is True
    assert "allow_local_infile" not in connect_kwargs


def test_aiomysql_connection_kwargs_default_local_infile_disabled() -> None:
    """LOAD DATA LOCAL INFILE should stay disabled unless explicitly enabled."""
    config = AiomysqlConfig()

    connect_kwargs = config._connection_kwargs()  # pyright: ignore[reportPrivateUsage]

    assert connect_kwargs["local_infile"] is False


def test_aiomysql_connection_kwargs_prefers_canonical_cursorclass() -> None:
    """The upstream cursorclass key should win over the compatibility alias."""
    config = AiomysqlConfig(connection_config={"cursorclass": AiomysqlRawCursor, "cursor_class": AiomysqlDictCursor})

    connect_kwargs = config._connection_kwargs()  # pyright: ignore[reportPrivateUsage]

    assert connect_kwargs["cursorclass"] is AiomysqlRawCursor
    assert "cursor_class" not in connect_kwargs


def test_aiomysql_connection_kwargs_maps_safe_pymysql_aliases() -> None:
    """PyMySQL-compatible database and password aliases should map to aiomysql names."""
    config = AiomysqlConfig(connection_config={"database": "app", "passwd": "secret"})

    connect_kwargs = config._connection_kwargs()  # pyright: ignore[reportPrivateUsage]

    assert connect_kwargs["db"] == "app"
    assert connect_kwargs["password"] == "secret"
    assert "database" not in connect_kwargs
    assert "passwd" not in connect_kwargs


def test_aiomysql_connection_kwargs_forwards_supported_current_options() -> None:
    """Current supported aiomysql connection options should stay typed and routed."""
    conv = {1: int}
    ssl_context = SSLContext(PROTOCOL_TLS_CLIENT)
    config = AiomysqlConfig(
        connection_config={
            "charset": "utf8mb4",
            "use_unicode": False,
            "conv": conv,
            "ssl": ssl_context,
            "client_flag": 123,
            "auth_plugin": "mysql_clear_password",
            "program_name": "sqlspec",
            "server_public_key": "public-key",
            "autocommit": None,
            "connect_timeout": 5.5,
        }
    )

    connect_kwargs = config._connection_kwargs()  # pyright: ignore[reportPrivateUsage]

    assert connect_kwargs["charset"] == "utf8mb4"
    assert connect_kwargs["use_unicode"] is False
    assert connect_kwargs["conv"] is conv
    assert connect_kwargs["ssl"] is ssl_context
    assert connect_kwargs["client_flag"] == 123
    assert connect_kwargs["auth_plugin"] == "mysql_clear_password"
    assert connect_kwargs["program_name"] == "sqlspec"
    assert connect_kwargs["server_public_key"] == "public-key"
    assert connect_kwargs["autocommit"] is None
    assert connect_kwargs["connect_timeout"] == 5.5


@pytest.mark.anyio
async def test_aiomysql_create_pool_forwards_sanitized_pool_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool creation should receive normalized connection kwargs plus pool-only settings."""
    seen_kwargs: dict[str, object] = {}

    async def fake_create_pool(**kwargs: object) -> object:
        seen_kwargs.update(kwargs)
        return MagicMock()

    monkeypatch.setattr(aiomysql, "create_pool", fake_create_pool)
    config = AiomysqlConfig(
        connection_config={
            "cursor_class": AiomysqlDictCursor,
            "enable_local_infile": True,
            "allow_local_infile": True,
            "minsize": 2,
            "maxsize": 8,
            "pool_recycle": 30,
        }
    )

    await config._create_pool()  # pyright: ignore[reportPrivateUsage]

    assert seen_kwargs["cursorclass"] is AiomysqlDictCursor
    assert seen_kwargs["local_infile"] is True
    assert seen_kwargs["minsize"] == 2
    assert seen_kwargs["maxsize"] == 8
    assert seen_kwargs["pool_recycle"] == 30
    assert "cursor_class" not in seen_kwargs
    assert "enable_local_infile" not in seen_kwargs


@pytest.mark.anyio
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


@pytest.mark.anyio
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
