"""Aiosqlite database configuration."""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
from sqlspec.adapters.aiosqlite.driver import AiosqliteCursor, AiosqliteDriver, aiosqlite_statement_config
from sqlspec.adapters.aiosqlite.pool import (
    AiosqliteConnectionPool,
    AiosqliteConnectTimeoutError,
    AiosqlitePoolClosedError,
    AiosqlitePoolConnection,
)
from sqlspec.config import AsyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlspec.core.statement import StatementConfig

__all__ = ("AiosqliteConfig", "AiosqliteConnectionParams", "AiosqlitePoolParams")

logger = logging.getLogger(__name__)


class AiosqliteConnectionParams(TypedDict, total=False):
    """TypedDict for aiosqlite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


class AiosqlitePoolParams(AiosqliteConnectionParams, total=False):
    """TypedDict for aiosqlite pool parameters, inheriting connection parameters."""

    pool_size: NotRequired[int]
    connect_timeout: NotRequired[float]
    idle_timeout: NotRequired[float]
    operation_timeout: NotRequired[float]
    extra: NotRequired[dict[str, Any]]


class AiosqliteConfig(AsyncDatabaseConfig["AiosqliteConnection", AiosqliteConnectionPool, AiosqliteDriver]):
    """Database configuration for AioSQLite engine."""

    driver_type: "ClassVar[type[AiosqliteDriver]]" = AiosqliteDriver
    connection_type: "ClassVar[type[AiosqliteConnection]]" = AiosqliteConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[AiosqlitePoolParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[AiosqliteConnectionPool]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        """Initialize AioSQLite configuration.

        Args:
            pool_config: Pool configuration parameters (TypedDict or dict)
            pool_instance: Optional pre-configured connection pool instance.
            migration_config: Optional migration configuration.
            statement_config: Optional statement configuration.
            driver_features: Optional driver feature configuration.
        """
        config_dict = dict(pool_config) if pool_config else {}

        if "database" not in config_dict or config_dict["database"] == ":memory:":
            config_dict["database"] = "file::memory:?cache=shared"
            config_dict["uri"] = True

        super().__init__(
            pool_config=config_dict,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or aiosqlite_statement_config,
            driver_features=driver_features or {},
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

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation.

        Returns:
            Dictionary with connection parameters for creating connections.
        """

        excluded_keys = {
            "pool_size",
            "connect_timeout",
            "idle_timeout",
            "operation_timeout",
            "extra",
            "pool_min_size",
            "pool_max_size",
            "pool_timeout",
            "pool_recycle_seconds",
        }
        return {k: v for k, v in self.pool_config.items() if k not in excluded_keys}

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[AiosqliteConnection, None]":
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An aiosqlite connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self._create_pool()
        async with self.pool_instance.get_connection() as connection:
            yield connection

    @asynccontextmanager
    async def provide_session(
        self, *_args: Any, statement_config: "Optional[StatementConfig]" = None, **_kwargs: Any
    ) -> "AsyncGenerator[AiosqliteDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Yields:
            An AiosqliteDriver instance.
        """
        async with self.provide_connection(*_args, **_kwargs) as connection:
            yield self.driver_type(connection=connection, statement_config=statement_config or self.statement_config)

    async def _create_pool(self) -> AiosqliteConnectionPool:
        """Create the connection pool instance.

        Returns:
            AiosqliteConnectionPool: The connection pool instance.
        """
        config = self._get_pool_config_dict()
        pool_size = config.pop("pool_size", 5)
        connect_timeout = config.pop("connect_timeout", 30.0)
        idle_timeout = config.pop("idle_timeout", 24 * 60 * 60)
        operation_timeout = config.pop("operation_timeout", 10.0)

        return AiosqliteConnectionPool(
            connection_parameters=self._get_connection_config_dict(),
            pool_size=pool_size,
            connect_timeout=connect_timeout,
            idle_timeout=idle_timeout,
            operation_timeout=operation_timeout,
        )

    async def close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance and not self.pool_instance.is_closed:
            await self.pool_instance.close()

    async def create_connection(self) -> "AiosqliteConnection":
        """Create a single async connection from the pool.

        Returns:
            An aiosqlite connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self._create_pool()
        pool_connection = await self.pool_instance.acquire()
        return pool_connection.connection

    async def provide_pool(self) -> AiosqliteConnectionPool:
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for aiosqlite types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "AiosqliteConnection": AiosqliteConnection,
                "AiosqliteConnectionPool": AiosqliteConnectionPool,
                "AiosqliteConnectTimeoutError": AiosqliteConnectTimeoutError,
                "AiosqliteCursor": AiosqliteCursor,
                "AiosqlitePoolClosedError": AiosqlitePoolClosedError,
                "AiosqlitePoolConnection": AiosqlitePoolConnection,
            }
        )
        return namespace

    async def _close_pool(self) -> None:
        """Close the connection pool."""
        await self.close_pool()
