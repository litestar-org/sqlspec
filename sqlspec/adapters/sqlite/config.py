"""SQLite database configuration using TypedDict for better maintainability."""

import logging
import sqlite3
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

from typing_extensions import NotRequired, Required

from sqlspec.adapters.sqlite.driver import SqliteConnection, SqliteDriver
from sqlspec.config import InstrumentationConfig, NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Generator

logger = logging.getLogger(__name__)

__all__ = ("SqliteConfig", "SqliteConnectionConfig", "sqlite3")


class SqliteConnectionConfig(TypedDict, total=False):
    """SQLite connection configuration as TypedDict.

    All parameters for sqlite3.connect() except database which is required.
    """

    database: Required[str]
    """Path to the SQLite database file. Use ':memory:' for in-memory database."""

    timeout: NotRequired[float]
    """Connection timeout in seconds."""

    detect_types: NotRequired[int]
    """Type detection flags (sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)."""

    isolation_level: NotRequired[Union[str, None]]
    """Transaction isolation level."""

    check_same_thread: NotRequired[bool]
    """Whether to check that connection is used on same thread."""

    factory: NotRequired[type[SqliteConnection]]
    """Custom Connection class factory."""

    cached_statements: NotRequired[int]
    """Number of statements to cache."""

    uri: NotRequired[bool]
    """Whether to interpret database as URI."""


class SqliteConfig(NoPoolSyncConfig[SqliteConnection, SqliteDriver]):
    """Configuration for SQLite database connections using TypedDict."""

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[SqliteDriver]] = SqliteDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    """SQLite supports ? (qmark) and :name (named_colon) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "qmark"
    """SQLite's native parameter style is ? (qmark)."""

    def __init__(
        self,
        connection_config: SqliteConnectionConfig,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            connection_config: SQLite connection parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
        """
        self.connection_config = connection_config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        super().__init__(
            instrumentation=instrumentation or InstrumentationConfig(),
        )

    @property
    def connection_type(self) -> type[SqliteConnection]:  # type: ignore[override]
        """Return the connection type."""
        return SqliteConnection

    @property
    def driver_type(self) -> type[SqliteDriver]:  # type: ignore[override]
        """Return the driver type."""
        return SqliteDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict."""
        return {k: v for k, v in self.connection_config.items() if v is not Empty}

    def create_connection(self) -> SqliteConnection:
        """Create and return a SQLite connection."""
        import sqlite3

        # Extract database separately since it's required
        config = self.connection_config_dict

        if self.instrumentation.log_pool_operations:
            logger.info("Creating SQLite connection", extra={"adapter": "sqlite", "database": config.get("database")})

        try:
            connection = sqlite3.connect(**config)

            # Configure row factory for dictionary-like access
            connection.row_factory = sqlite3.Row

            if self.instrumentation.log_pool_operations:
                logger.info("SQLite connection created successfully", extra={"adapter": "sqlite"})

        except Exception as e:
            logger.exception("Failed to create SQLite connection", extra={"adapter": "sqlite", "error": str(e)})
            raise
        return connection  # type: ignore[no-any-return]

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[SqliteConnection, None, None]":
        """Provide a SQLite connection context manager.

        Args:
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments

        Yields:
            SqliteConnection: A SQLite connection

        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            if self.instrumentation.log_pool_operations:
                logger.debug("Closing SQLite connection", extra={"adapter": "sqlite"})
            try:
                connection.close()
            except Exception as e:
                logger.exception("Failed to close SQLite connection", extra={"adapter": "sqlite", "error": str(e)})
                raise

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[SqliteDriver, None, None]":
        """Provide a SQLite driver session context manager.

        Args:
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments

        Yields:
            SqliteDriver: A SQLite driver
        """
        with self.provide_connection(*args, **kwargs) as connection:
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                from dataclasses import replace

                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            yield self.driver_type(
                connection=connection,
                config=statement_config,
                instrumentation_config=self.instrumentation,
            )
