"""Aiosqlite database configuration using TypedDict for better maintainability."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict

import aiosqlite
from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite.driver import AiosqliteConnection, AiosqliteDriver
from sqlspec.config import AsyncDatabaseConfig, InstrumentationConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from typing import Literal


__all__ = ("AiosqliteConfig", "AiosqliteConnectionConfig")


class AiosqliteConnectionConfig(TypedDict, total=False):
    """Aiosqlite connection configuration as TypedDict.

    Basic connection parameters for aiosqlite.connect().
    Based on aiosqlite documentation.
    """

    database: NotRequired[str]
    """The path to the database file to be opened. Pass ":memory:" to open a connection to a database that resides in RAM instead of on disk."""

    timeout: NotRequired[float]
    """How many seconds the connection should wait before raising an OperationalError when a table is locked."""

    detect_types: NotRequired[int]
    """Control whether and how data types are detected. It can be 0 (default) or a combination of PARSE_DECLTYPES and PARSE_COLNAMES."""

    isolation_level: NotRequired["Optional[Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']]"]
    """The isolation_level of the connection. This can be None for autocommit mode or one of "DEFERRED", "IMMEDIATE" or "EXCLUSIVE"."""

    check_same_thread: NotRequired[bool]
    """If True (default), ProgrammingError is raised if the database connection is used by a thread other than the one that created it."""

    cached_statements: NotRequired[int]
    """The number of statements that SQLite will cache for this connection. The default is 128."""

    uri: NotRequired[bool]
    """If set to True, database is interpreted as a URI with supported options."""


class AiosqliteConfig(AsyncDatabaseConfig[AiosqliteConnection, None, AiosqliteDriver]):
    """Configuration for Aiosqlite database connections using TypedDict.

    Note: Aiosqlite doesn't support connection pooling, so pool_instance is always None.
    """

    __is_async__: ClassVar[bool] = True
    __supports_connection_pooling__: ClassVar[bool] = False

    def __init__(
        self,
        connection_config: Optional[AiosqliteConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,  # type: ignore[assignment]
    ) -> None:
        """Initialize Aiosqlite configuration.

        Args:
            connection_config: Aiosqlite connection parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
        """
        self.connection_config = connection_config or {}
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type

        super().__init__(
            instrumentation=instrumentation or InstrumentationConfig(),
        )

    @property
    def connection_type(self) -> type[AiosqliteConnection]:  # type: ignore[override]
        """Return the connection type."""
        return AiosqliteConnection

    @property
    def driver_type(self) -> type[AiosqliteDriver]:  # type: ignore[override]
        """Return the driver type."""
        return AiosqliteDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict."""
        config = {k: v for k, v in self.connection_config.items() if v is not Empty}

        # Set default database to :memory: if not specified
        if "database" not in config:
            config["database"] = ":memory:"

        return config

    async def _create_pool_impl(self) -> None:
        """Aiosqlite doesn't support pooling."""
        return

    async def _close_pool_impl(self) -> None:
        """Aiosqlite doesn't support pooling."""

    async def create_connection(self) -> AiosqliteConnection:
        """Create a single async connection.

        Returns:
            An Aiosqlite connection instance.
        """
        async with instrument_operation_async(self, "aiosqlite_create_connection", "database"):
            try:
                config = self.connection_config_dict
                return await aiosqlite.connect(**config)
            except Exception as e:
                msg = f"Could not configure the Aiosqlite connection. Error: {e!s}"
                raise ImproperConfigurationError(msg) from e

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AiosqliteConnection, None]:
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Aiosqlite connection instance.
        """
        connection = await self.create_connection()
        try:
            yield connection
        finally:
            await connection.close()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AiosqliteDriver, None]:
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AiosqliteDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(
                connection=connection,
                config=self.statement_config,
                instrumentation_config=self.instrumentation,
            )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """Aiosqlite doesn't support pooling.

        Returns:
            None (no pool support).
        """
        return
