"""AsyncPG database configuration with direct field-based configuration."""

import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from asyncpg import Record
from asyncpg import create_pool as asyncpg_create_pool
from typing_extensions import NotRequired, Unpack

from sqlspec.adapters.asyncpg.driver import AsyncpgConnection, AsyncpgDriver
from sqlspec.config import AsyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop

    from asyncpg.pool import Pool
    from sqlglot.dialects.dialect import DialectType


__all__ = ("CONNECTION_FIELDS", "POOL_FIELDS", "AsyncpgConfig")

logger = logging.getLogger("sqlspec")


class AsyncpgConnectionParams(TypedDict, total=False):
    """TypedDict for AsyncPG connection parameters."""

    dsn: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    ssl: NotRequired[Any]  # Can be bool, SSLContext, or specific string
    passfile: NotRequired[str]
    direct_tls: NotRequired[bool]
    connect_timeout: NotRequired[float]
    command_timeout: NotRequired[float]
    statement_cache_size: NotRequired[int]
    max_cached_statement_lifetime: NotRequired[int]
    max_cacheable_statement_size: NotRequired[int]
    server_settings: NotRequired[dict[str, str]]


class AsyncpgPoolParams(AsyncpgConnectionParams, total=False):
    """TypedDict for AsyncPG pool parameters, inheriting connection parameters."""

    min_size: NotRequired[int]
    max_size: NotRequired[int]
    max_queries: NotRequired[int]
    max_inactive_connection_lifetime: NotRequired[float]
    setup: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    init: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    loop: NotRequired["AbstractEventLoop"]
    connection_class: NotRequired[type["AsyncpgConnection"]]
    record_class: NotRequired[type[Record]]


class DriverParameters(AsyncpgPoolParams, total=False):
    """TypedDict for additional parameters that can be passed to AsyncPG."""

    statement_config: NotRequired[SQLConfig]
    default_row_type: NotRequired[type[DictRow]]
    json_serializer: NotRequired[Callable[[Any], str]]
    json_deserializer: NotRequired[Callable[[str], Any]]
    pool_instance: NotRequired["Pool[Record]"]
    extras: NotRequired[dict[str, Any]]


CONNECTION_FIELDS = {
    "dsn",
    "host",
    "port",
    "user",
    "password",
    "database",
    "ssl",
    "passfile",
    "direct_tls",
    "connect_timeout",
    "command_timeout",
    "statement_cache_size",
    "max_cached_statement_lifetime",
    "max_cacheable_statement_size",
    "server_settings",
}
POOL_FIELDS = CONNECTION_FIELDS.union(
    {
        "min_size",
        "max_size",
        "max_queries",
        "max_inactive_connection_lifetime",
        "setup",
        "init",
        "loop",
        "connection_class",
        "record_class",
    }
)


class AsyncpgConfig(AsyncDatabaseConfig[AsyncpgConnection, "Pool[Record]", AsyncpgDriver]):
    """Configuration for AsyncPG database connections using TypedDict."""

    __slots__ = (
        "_dialect",
        "command_timeout",
        "connect_timeout",
        "connection_class",
        "database",
        "default_row_type",
        "direct_tls",
        "dsn",
        "extras",
        "host",
        "init",
        "json_deserializer",
        "json_serializer",
        "loop",
        "max_cacheable_statement_size",
        "max_cached_statement_lifetime",
        "max_inactive_connection_lifetime",
        "max_queries",
        "max_size",
        "min_size",
        "passfile",
        "password",
        "pool_instance",
        "port",
        "record_class",
        "server_settings",
        "setup",
        "ssl",
        "statement_cache_size",
        "statement_config",
        "user",
    )

    driver_type: type[AsyncpgDriver] = AsyncpgDriver
    connection_type: type[AsyncpgConnection] = type(AsyncpgConnection)  # type: ignore[assignment]
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("numeric",)
    preferred_parameter_style: ClassVar[str] = "numeric"

    def __init__(self, **kwargs: "Unpack[DriverParameters]") -> None:
        """Initialize AsyncPG configuration."""
        # Known fields that are part of the config
        known_fields = {
            "dsn",
            "host",
            "port",
            "user",
            "password",
            "database",
            "ssl",
            "passfile",
            "direct_tls",
            "connect_timeout",
            "command_timeout",
            "statement_cache_size",
            "max_cached_statement_lifetime",
            "max_cacheable_statement_size",
            "server_settings",
            "min_size",
            "max_size",
            "max_queries",
            "max_inactive_connection_lifetime",
            "setup",
            "init",
            "loop",
            "connection_class",
            "record_class",
            "extras",
            "statement_config",
            "default_row_type",
            "json_serializer",
            "json_deserializer",
            "pool_instance",
        }

        self.dsn = kwargs.get("dsn")
        self.host = kwargs.get("host")
        self.port = kwargs.get("port")
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")
        self.database = kwargs.get("database")
        self.ssl = kwargs.get("ssl")
        self.passfile = kwargs.get("passfile")
        self.direct_tls = kwargs.get("direct_tls")
        self.connect_timeout = kwargs.get("connect_timeout")
        self.command_timeout = kwargs.get("command_timeout")
        self.statement_cache_size = kwargs.get("statement_cache_size")
        self.max_cached_statement_lifetime = kwargs.get("max_cached_statement_lifetime")
        self.max_cacheable_statement_size = kwargs.get("max_cacheable_statement_size")
        self.server_settings = kwargs.get("server_settings")
        self.min_size = kwargs.get("min_size")
        self.max_size = kwargs.get("max_size")
        self.max_queries = kwargs.get("max_queries")
        self.max_inactive_connection_lifetime = kwargs.get("max_inactive_connection_lifetime")
        self.setup = kwargs.get("setup")
        self.init = kwargs.get("init")
        self.loop = kwargs.get("loop")
        self.connection_class = kwargs.get("connection_class")
        self.record_class = kwargs.get("record_class")

        # Collect unknown parameters into extras
        provided_extras = kwargs.get("extras", {})
        unknown_params = {k: v for k, v in kwargs.items() if k not in known_fields}
        self.extras = {**provided_extras, **unknown_params}

        self.statement_config = (
            SQLConfig() if kwargs.get("statement_config") is None else kwargs.get("statement_config")
        )
        self.default_row_type = kwargs.get("default_row_type", dict[str, Any])
        self.json_serializer = kwargs.get("json_serializer", to_json)
        self.json_deserializer = kwargs.get("json_deserializer", from_json)
        pool_instance_from_kwargs = kwargs.get("pool_instance")
        self._dialect: DialectType = None

        super().__init__()

        # Set pool_instance after super().__init__() to ensure it's not overridden
        if pool_instance_from_kwargs is not None:
            self.pool_instance = pool_instance_from_kwargs

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for asyncpg.connect().

        This method filters out pool-specific parameters that are not valid for asyncpg.connect().
        """
        # Gather non-None connection parameters
        config = {
            field: getattr(self, field)
            for field in CONNECTION_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Add connection-specific extras (not pool-specific ones)
        config.update(self.extras)

        return config

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the full pool configuration as a dict for asyncpg.create_pool().

        Returns:
            A dictionary containing all pool configuration parameters.
        """
        # All AsyncPG parameter names (connection + pool)
        config = {
            field: getattr(self, field)
            for field in POOL_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras parameters
        config.update(self.extras)

        return config

    async def _create_pool(self) -> "Pool[Record]":
        """Create the actual async connection pool."""
        pool_args = self.pool_config_dict
        return await asyncpg_create_pool(**pool_args)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> AsyncpgConnection:
        """Create a single async connection (not from pool).

        Returns:
            An AsyncPG connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self._create_pool()
        return await self.pool_instance.acquire()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncpgConnection, None]:
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncPG connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self._create_pool()
        connection = None
        try:
            connection = await self.pool_instance.acquire()
            yield connection
        finally:
            if connection is not None:
                await self.pool_instance.release(connection)

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncpgDriver, None]:
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncpgDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config is not None and statement_config.allowed_parameter_styles is None:
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            yield self.driver_type(connection=connection, config=statement_config)

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool[Record]":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for AsyncPG types.

        This provides all AsyncPG-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """
        # Get base types from parent
        namespace = super().get_signature_namespace()

        # Add AsyncPG-specific types
        try:
            from asyncpg import Connection, Record
            from asyncpg.connection import ConnectionMeta
            from asyncpg.pool import Pool, PoolConnectionProxy, PoolConnectionProxyMeta

            namespace.update(
                {
                    "Connection": Connection,
                    "Pool": Pool,
                    "PoolConnectionProxy": PoolConnectionProxy,
                    "PoolConnectionProxyMeta": PoolConnectionProxyMeta,
                    "ConnectionMeta": ConnectionMeta,
                    "Record": Record,
                    "AsyncpgConnection": type(AsyncpgConnection),  # The Union type alias
                }
            )
        except ImportError:
            logger.warning("Failed to import AsyncPG types for signature namespace")

        return namespace
