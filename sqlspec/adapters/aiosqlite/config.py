# ruff: noqa: PLR6301
"""Aiosqlite database configuration with direct field-based configuration."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional

import aiosqlite

from sqlspec.adapters.aiosqlite.driver import AiosqliteConnection, AiosqliteDriver
from sqlspec.config import AsyncDatabaseConfig, InstrumentationConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from typing import Literal


__all__ = ("CONNECTION_FIELDS", "AiosqliteConfig")

logger = logging.getLogger(__name__)

CONNECTION_FIELDS = frozenset(
    {"database", "timeout", "detect_types", "isolation_level", "check_same_thread", "cached_statements", "uri"}
)


class AiosqliteConfig(AsyncDatabaseConfig[AiosqliteConnection, None, AiosqliteDriver]):
    """Configuration for Aiosqlite database connections with direct field-based configuration.

    Note: Aiosqlite doesn't support connection pooling, so pool_instance is always None.
    """

    __slots__ = (
        "cached_statements",
        "check_same_thread",
        "database",
        "default_row_type",
        "detect_types",
        "extras",
        "isolation_level",
        "statement_config",
        "timeout",
        "uri",
    )

    is_async: ClassVar[bool] = True
    supports_connection_pooling: ClassVar[bool] = False

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[AiosqliteDriver]] = AiosqliteDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    """AIOSQLite supports ? (qmark) and :name (named_colon) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "qmark"
    """AIOSQLite's native parameter style is ? (qmark)."""

    def __init__(
        self,
        database: str = ":memory:",
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        # Connection parameters
        timeout: Optional[float] = None,
        detect_types: Optional[int] = None,
        isolation_level: Optional["Optional[Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']]"] = None,
        check_same_thread: Optional[bool] = None,
        cached_statements: Optional[int] = None,
        uri: Optional[bool] = None,
        # User-defined extras
        extras: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Aiosqlite configuration.

        Args:
            database: The path to the database file to be opened. Pass ":memory:" for in-memory database
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            timeout: How many seconds the connection should wait before raising an OperationalError when a table is locked
            detect_types: Control whether and how data types are detected. It can be 0 (default) or a combination of PARSE_DECLTYPES and PARSE_COLNAMES
            isolation_level: The isolation_level of the connection. This can be None for autocommit mode or one of "DEFERRED", "IMMEDIATE" or "EXCLUSIVE"
            check_same_thread: If True (default), ProgrammingError is raised if the database connection is used by a thread other than the one that created it
            cached_statements: The number of statements that SQLite will cache for this connection. The default is 128
            uri: If set to True, database is interpreted as a URI with supported options
            extras: Additional connection parameters not explicitly defined
            **kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.database = database
        self.timeout = timeout
        self.detect_types = detect_types
        self.isolation_level = isolation_level
        self.check_same_thread = check_same_thread
        self.cached_statements = cached_statements
        self.uri = uri

        # Handle extras and additional kwargs
        self.extras = extras or {}
        self.extras.update(kwargs)

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type

        super().__init__(instrumentation=instrumentation or InstrumentationConfig())

    @property
    def connection_type(self) -> type[AiosqliteConnection]:  # type: ignore[override]
        """Return the connection type."""
        return AiosqliteConnection

    @property
    def driver_type(self) -> type[AiosqliteDriver]:  # type: ignore[override]
        """Return the driver type."""
        return AiosqliteDriver

    @classmethod
    def from_connection_config(
        cls,
        connection_config: dict[str, Any],
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> "AiosqliteConfig":
        """Create config from old-style connection_config dict for backward compatibility.

        Args:
            connection_config: Dictionary with connection parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results

        Returns:
            AiosqliteConfig instance
        """
        # Extract required database parameter
        database = connection_config.get("database")
        if database is None:
            msg = "'database' parameter is required"
            raise ImproperConfigurationError(msg)

        # Create config with all parameters
        return cls(
            database=database,
            statement_config=statement_config,
            instrumentation=instrumentation,
            default_row_type=default_row_type,
            **{
                k: v for k, v in connection_config.items() if k != "database"
            },  # Other connection parameters go to direct fields or extras
        )

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for aiosqlite.connect()."""
        # Gather non-None connection parameters
        config = {
            field: getattr(self, field)
            for field in CONNECTION_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras parameters
        config.update(self.extras)

        return config

    async def _create_pool(self) -> None:
        """Aiosqlite doesn't support pooling."""
        return

    async def _close_pool(self) -> None:
        """Aiosqlite doesn't support pooling."""
        return

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
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            yield self.driver_type(
                connection=connection, config=statement_config, instrumentation_config=self.instrumentation
            )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """Aiosqlite doesn't support pooling."""
        return
