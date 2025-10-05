"""Asyncmy database configuration."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

import asyncmy
from asyncmy.cursors import Cursor, DictCursor  # pyright: ignore
from asyncmy.pool import Pool as AsyncmyPool  # pyright: ignore
from typing_extensions import NotRequired

from sqlspec.adapters.asyncmy._types import AsyncmyConnection
from sqlspec.adapters.asyncmy.driver import AsyncmyCursor, AsyncmyDriver, asyncmy_statement_config
from sqlspec.config import AsyncDatabaseConfig

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor, DictCursor  # pyright: ignore
    from asyncmy.pool import Pool  # pyright: ignore

    from sqlspec.core.statement import StatementConfig


__all__ = ("AsyncmyConfig", "AsyncmyConnectionParams", "AsyncmyPoolParams")

logger = logging.getLogger(__name__)


class AsyncmyConnectionParams(TypedDict, total=False):
    """Asyncmy connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    connect_timeout: NotRequired[int]
    read_default_file: NotRequired[str]
    read_default_group: NotRequired[str]
    autocommit: NotRequired[bool]
    local_infile: NotRequired[bool]
    ssl: NotRequired[Any]
    sql_mode: NotRequired[str]
    init_command: NotRequired[str]
    cursor_class: NotRequired[type["Cursor"] | type["DictCursor"]]
    extra: NotRequired[dict[str, Any]]


class AsyncmyPoolParams(AsyncmyConnectionParams, total=False):
    """Asyncmy pool parameters."""

    minsize: NotRequired[int]
    maxsize: NotRequired[int]
    echo: NotRequired[bool]
    pool_recycle: NotRequired[int]


class AsyncmyConfig(AsyncDatabaseConfig[AsyncmyConnection, "AsyncmyPool", AsyncmyDriver]):  # pyright: ignore
    """Configuration for Asyncmy database connections."""

    driver_type: ClassVar[type[AsyncmyDriver]] = AsyncmyDriver
    connection_type: "ClassVar[type[AsyncmyConnection]]" = AsyncmyConnection  # pyright: ignore

    def __init__(
        self,
        *,
        pool_config: "AsyncmyPoolParams | dict[str, Any] | None" = None,
        pool_instance: "AsyncmyPool | None" = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any]] | None" = None,
    ) -> None:
        """Initialize Asyncmy configuration.

        Args:
            pool_config: Pool configuration parameters
            pool_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: Driver feature configuration
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
        """
        processed_pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in processed_pool_config:
            extras = processed_pool_config.pop("extra")
            processed_pool_config.update(extras)

        if "host" not in processed_pool_config:
            processed_pool_config["host"] = "localhost"
        if "port" not in processed_pool_config:
            processed_pool_config["port"] = 3306

        if statement_config is None:
            statement_config = asyncmy_statement_config

        super().__init__(
            pool_config=processed_pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features or {},
            bind_key=bind_key,
            extension_config=extension_config,
        )

    async def _create_pool(self) -> "AsyncmyPool":  # pyright: ignore
        """Create the actual async connection pool."""
        return await asyncmy.create_pool(**dict(self.pool_config))  # pyright: ignore

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> AsyncmyConnection:  # pyright: ignore
        """Create a single async connection (not from pool).

        Returns:
            An Asyncmy connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return await self.pool_instance.acquire()  # pyright: ignore

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncmyConnection, None]:  # pyright: ignore
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Asyncmy connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        async with self.pool_instance.acquire() as connection:  # pyright: ignore
            yield connection

    @asynccontextmanager
    async def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> AsyncGenerator[AsyncmyDriver, None]:
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncmyDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            final_statement_config = statement_config or asyncmy_statement_config
            yield self.driver_type(connection=connection, statement_config=final_statement_config)

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool":  # pyright: ignore
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for Asyncmy types.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update(
            {"AsyncmyConnection": AsyncmyConnection, "AsyncmyPool": AsyncmyPool, "AsyncmyCursor": AsyncmyCursor}
        )
        return namespace
