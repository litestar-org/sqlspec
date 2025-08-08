"""AsyncPG database configuration with direct field-based configuration."""

import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

from asyncpg import Connection, Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.connection import ConnectionMeta
from asyncpg.pool import Pool, PoolConnectionProxy, PoolConnectionProxyMeta
from typing_extensions import NotRequired

from sqlspec.adapters.asyncpg._types import AsyncpgConnection
from sqlspec.adapters.asyncpg.driver import AsyncpgCursor, AsyncpgDriver, asyncpg_statement_config
from sqlspec.config import AsyncDatabaseConfig
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop
    from collections.abc import AsyncGenerator, Awaitable

    from sqlspec.core.statement import StatementConfig


__all__ = ("AsyncpgConfig", "AsyncpgConnectionConfig", "AsyncpgDriverFeatures", "AsyncpgPoolConfig")

logger = logging.getLogger("sqlspec")


class AsyncpgConnectionConfig(TypedDict, total=False):
    """TypedDict for AsyncPG connection parameters."""

    dsn: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    ssl: NotRequired[Any]
    passfile: NotRequired[str]
    direct_tls: NotRequired[bool]
    connect_timeout: NotRequired[float]
    command_timeout: NotRequired[float]
    statement_cache_size: NotRequired[int]
    max_cached_statement_lifetime: NotRequired[int]
    max_cacheable_statement_size: NotRequired[int]
    server_settings: NotRequired[dict[str, str]]


class AsyncpgPoolConfig(AsyncpgConnectionConfig, total=False):
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
    extra: NotRequired[dict[str, Any]]


class AsyncpgDriverFeatures(TypedDict, total=False):
    """TypedDict for AsyncPG driver features configuration."""

    json_serializer: NotRequired[Callable[[Any], str]]
    json_deserializer: NotRequired[Callable[[str], Any]]


class AsyncpgConfig(AsyncDatabaseConfig[AsyncpgConnection, "Pool[Record]", AsyncpgDriver]):
    """Configuration for AsyncPG database connections using TypedDict."""

    driver_type: "ClassVar[type[AsyncpgDriver]]" = AsyncpgDriver
    connection_type: "ClassVar[type[AsyncpgConnection]]" = type(AsyncpgConnection)  # type: ignore[assignment]

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[AsyncpgPoolConfig, dict[str, Any]]]" = None,
        pool_instance: "Optional[Pool[Record]]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[Union[AsyncpgDriverFeatures, dict[str, Any]]]" = None,
    ) -> None:
        """Initialize AsyncPG configuration.

        Args:
            pool_config: Pool configuration parameters (TypedDict or dict)
            pool_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: Driver features configuration (TypedDict or dict)
        """
        features_dict: dict[str, Any] = dict(driver_features) if driver_features else {}

        if "json_serializer" not in features_dict:
            features_dict["json_serializer"] = to_json
        if "json_deserializer" not in features_dict:
            features_dict["json_deserializer"] = from_json
        super().__init__(
            pool_config=dict(pool_config) if pool_config else {},
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or asyncpg_statement_config,
            driver_features=features_dict,
        )

    def _get_pool_config_dict(self) -> "dict[str, Any]":
        """Get pool configuration as plain dict for external library.

        Returns:
            Dictionary with pool parameters, filtering out None values.
        """
        config: dict[str, Any] = dict(self.pool_config)
        extras = config.pop("extra", {})
        config.update(extras)
        return {k: v for k, v in config.items() if v is not None}

    async def _create_pool(self) -> "Pool[Record]":
        """Create the actual async connection pool."""
        config = self._get_pool_config_dict()

        if "init" not in config:
            config["init"] = self._init_pgvector_connection

        return await asyncpg_create_pool(**config)

    async def _init_pgvector_connection(self, connection: "AsyncpgConnection") -> None:
        """Initialize pgvector support for asyncpg connections."""
        try:
            import pgvector.asyncpg

            await pgvector.asyncpg.register_vector(connection)
        except ImportError:
            pass
        except Exception as e:
            logger.debug("Failed to register pgvector for asyncpg: %s", e)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> "AsyncpgConnection":
        """Create a single async connection from the pool.

        Returns:
            An AsyncPG connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self._create_pool()
        return await self.pool_instance.acquire()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[AsyncpgConnection, None]":
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
    async def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "AsyncGenerator[AsyncpgDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncpgDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            final_statement_config = statement_config or self.statement_config or asyncpg_statement_config
            yield self.driver_type(connection=connection, statement_config=final_statement_config)

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

        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "Connection": Connection,
                "Pool": Pool,
                "PoolConnectionProxy": PoolConnectionProxy,
                "PoolConnectionProxyMeta": PoolConnectionProxyMeta,
                "ConnectionMeta": ConnectionMeta,
                "Record": Record,
                "AsyncpgConnection": AsyncpgConnection,  # type: ignore[dict-item]
                "AsyncpgCursor": AsyncpgCursor,
            }
        )
        return namespace
