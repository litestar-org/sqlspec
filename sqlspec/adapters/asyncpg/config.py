"""AsyncPG database configuration with direct field-based configuration."""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

from asyncpg import Connection, Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.connection import ConnectionMeta
from asyncpg.pool import Pool, PoolConnectionProxy, PoolConnectionProxyMeta
from typing_extensions import NotRequired

from sqlspec.adapters.asyncpg.driver import AsyncpgConnection, AsyncpgDriver
from sqlspec.adapters.asyncpg.pipeline_steps import postgres_copy_pipeline_step
from sqlspec.config import AsyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop
    from collections.abc import AsyncGenerator, Awaitable, Callable


__all__ = ("AsyncpgConfig", "AsyncpgConnectionConfig", "AsyncpgPoolConfig")

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


class AsyncpgConfig(AsyncDatabaseConfig[AsyncpgConnection, "Pool[Record]", AsyncpgDriver]):
    """Configuration for AsyncPG database connections using TypedDict."""

    driver_type: "ClassVar[type[AsyncpgDriver]]" = AsyncpgDriver
    connection_type: "ClassVar[type[AsyncpgConnection]]" = type(AsyncpgConnection)  # type: ignore[assignment]
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ("numeric",)
    default_parameter_style: "ClassVar[str]" = "numeric"

    def __init__(
        self,
        *,
        pool_instance: "Optional[Pool[Record]]" = None,
        pool_config: "Optional[Union[AsyncpgPoolConfig, dict[str, Any]]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
        json_serializer: "Optional[Callable[[Any], str]]" = None,
        json_deserializer: "Optional[Callable[[str], Any]]" = None,
    ) -> None:
        """Initialize AsyncPG configuration.

        Args:
            pool_config: Pool configuration parameters (TypedDict or dict)
            pool_instance: Existing pool instance to use
            statement_config: SQL statement configuration
            json_serializer: JSON serialization function
            json_deserializer: JSON deserialization function
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements
        """
        # Store the pool config as a dict
        self.pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        self.statement_config = statement_config or SQLConfig()
        self.json_serializer = json_serializer or to_json
        self.json_deserializer = json_deserializer or from_json
        super().__init__(
            pool_instance=pool_instance,
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
        )

    def _get_pool_config_dict(self) -> "dict[str, Any]":
        """Get pool configuration as plain dict for external library.

        Returns:
            Dictionary with pool parameters, filtering out None values.
        """
        config: "dict[str, Any]" = dict(self.pool_config)  # noqa: UP037
        # Extract extra parameters if they exist
        extras = config.pop("extra", {})
        config.update(extras)
        # Filter out None values since external libraries may not handle them
        return {k: v for k, v in config.items() if v is not None}

    async def _create_pool(self) -> "Pool[Record]":
        """Create the actual async connection pool."""
        config = self._get_pool_config_dict()
        return await asyncpg_create_pool(**config)

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
    async def provide_session(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[AsyncpgDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncpgDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            statement_config = self.statement_config
            if statement_config is not None and statement_config.allowed_parameter_styles is None:
                statement_config = statement_config.replace(
                    allowed_parameter_styles=self.supported_parameter_styles,
                    default_parameter_style=self.default_parameter_style,
                )
            custom_pipeline_steps = [postgres_copy_pipeline_step]
            if statement_config.custom_pipeline_steps:
                custom_pipeline_steps.extend(statement_config.custom_pipeline_steps)
            statement_config = statement_config.replace(custom_pipeline_steps=custom_pipeline_steps)
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
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "Connection": Connection,
                "Pool": Pool,
                "PoolConnectionProxy": PoolConnectionProxy,
                "PoolConnectionProxyMeta": PoolConnectionProxyMeta,
                "ConnectionMeta": ConnectionMeta,
                "Record": Record,
                "AsyncpgConnection": type(AsyncpgConnection),
            }
        )
        return namespace
