"""Asyncmy database configuration with direct field-based configuration."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

import asyncmy
from asyncmy.cursors import Cursor, DictCursor
from asyncmy.pool import Pool as AsyncmyPool
from typing_extensions import NotRequired

from sqlspec.adapters.asyncmy._types import AsyncmyConnection
from sqlspec.adapters.asyncmy.driver import AsyncmyCursor, AsyncmyDriver, asyncmy_statement_config
from sqlspec.config import AsyncDatabaseConfig

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor, DictCursor
    from asyncmy.pool import Pool

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
    cursor_class: NotRequired[Union[type["Cursor"], type["DictCursor"]]]
    extra: NotRequired[dict[str, Any]]


class AsyncmyPoolParams(AsyncmyConnectionParams, total=False):
    """Asyncmy pool parameters."""

    minsize: NotRequired[int]
    maxsize: NotRequired[int]
    echo: NotRequired[bool]
    pool_recycle: NotRequired[int]


class AsyncmyConfig(AsyncDatabaseConfig[AsyncmyConnection, "Pool", AsyncmyDriver]):  # pyright: ignore
    """Configuration for Asyncmy database connections with direct field-based configuration."""

    driver_type: ClassVar[type[AsyncmyDriver]] = AsyncmyDriver
    connection_type: "ClassVar[type[AsyncmyConnection]]" = AsyncmyConnection  # pyright: ignore

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[AsyncmyPoolParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[Pool]" = None,
        migration_config: Optional[dict[str, Any]] = None,
        statement_config: "Optional[StatementConfig]" = None,
    ) -> None:
        """Initialize Asyncmy configuration.

        Args:
            pool_config: Pool configuration parameters
            pool_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Statement configuration override
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
            driver_features={},
        )

    async def _create_pool(self) -> "Pool":  # pyright: ignore
        """Create the actual async connection pool."""
        return await asyncmy.create_pool(**dict(self.pool_config))

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

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
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
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

        This provides all Asyncmy-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update(
            {"AsyncmyConnection": AsyncmyConnection, "AsyncmyPool": AsyncmyPool, "AsyncmyCursor": AsyncmyCursor}
        )
        return namespace
