"""SQLite database configuration with direct field-based configuration."""

import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from litestar.exceptions import ImproperlyConfiguredException

from sqlspec.adapters.sqlite.driver import SqliteConnection, SqliteDriver
from sqlspec.config import InstrumentationConfig, NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    from collections.abc import Generator

logger = logging.getLogger(__name__)

CONNECTION_FIELDS = frozenset(
    {
        "database",
        "timeout",
        "detect_types",
        "isolation_level",
        "check_same_thread",
        "factory",
        "cached_statements",
        "uri",
    }
)

__all__ = ("CONNECTION_FIELDS", "SqliteConfig", "sqlite3")


class SqliteConfig(NoPoolSyncConfig[SqliteConnection, SqliteDriver]):
    """Configuration for SQLite database connections with direct field-based configuration."""

    __slots__ = (
        "cached_statements",
        "check_same_thread",
        "database",
        "default_row_type",
        "detect_types",
        "extras",
        "factory",
        "isolation_level",
        "statement_config",
        "timeout",
        "uri",
    )

    is_async: ClassVar[bool] = False
    supports_connection_pooling: ClassVar[bool] = False

    driver_type: type[SqliteDriver] = SqliteDriver
    connection_type: type[SqliteConnection] = SqliteConnection
    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    """SQLite supports ? (qmark) and :name (named_colon) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "qmark"
    """SQLite's native parameter style is ? (qmark)."""

    def __init__(
        self,
        database: str = ":memory:",
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        # SQLite connection parameters
        timeout: Optional[float] = None,
        detect_types: Optional[int] = None,
        isolation_level: Optional[Union[str, None]] = None,
        check_same_thread: Optional[bool] = None,
        factory: Optional[type[SqliteConnection]] = None,
        cached_statements: Optional[int] = None,
        uri: Optional[bool] = None,
        # User-defined extras
        extras: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            database: Path to the SQLite database file. Use ':memory:' for in-memory database.
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            timeout: Connection timeout in seconds
            detect_types: Type detection flags (sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            isolation_level: Transaction isolation level
            check_same_thread: Whether to check that connection is used on same thread
            factory: Custom Connection class factory
            cached_statements: Number of statements to cache
            uri: Whether to interpret database as URI
            extras: Additional connection parameters not explicitly defined
            **kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.database = database
        self.timeout = timeout
        self.detect_types = detect_types
        self.isolation_level = isolation_level
        self.check_same_thread = check_same_thread
        self.factory = factory
        self.cached_statements = cached_statements
        self.uri = uri

        # Handle extras and additional kwargs
        self.extras = extras or {}
        self.extras.update(kwargs)

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        super().__init__(instrumentation=instrumentation or InstrumentationConfig())

    @classmethod
    def from_connection_config(
        cls,
        connection_config: dict[str, Any],
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> "SqliteConfig":
        """Create config from old-style connection_config dict for backward compatibility.

        Args:
            connection_config: Dictionary with connection parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results

        Returns:
            SqliteConfig instance
        """
        # Extract database parameter (required)
        if "database" not in connection_config:
            msg = "database parameter is required"
            raise ImproperlyConfiguredException(msg)

        # Extract known parameters
        database = connection_config.pop("database")

        # Create config with all parameters
        return cls(
            database=database,
            statement_config=statement_config,
            instrumentation=instrumentation,
            default_row_type=default_row_type,
            **connection_config,  # All other parameters go to extras
        )

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
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            yield self.driver_type(
                connection=connection, config=statement_config, instrumentation_config=self.instrumentation
            )
