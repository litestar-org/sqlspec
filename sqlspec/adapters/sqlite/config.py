"""SQLite database configuration with direct field-based configuration."""

import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from sqlspec.adapters.sqlite.driver import SqliteConnection, SqliteDriver
from sqlspec.config import NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlglot.dialects.dialect import DialectType

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
        "_dialect",
        "cached_statements",
        "check_same_thread",
        "database",
        "default_row_type",
        "detect_types",
        "extras",
        "factory",
        "isolation_level",
        "pool_instance",
        "statement_config",
        "timeout",
        "uri",
    )

    driver_type: type[SqliteDriver] = SqliteDriver
    connection_type: type[SqliteConnection] = SqliteConnection
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    preferred_parameter_style: ClassVar[str] = "qmark"

    def __init__(
        self,
        database: str = ":memory:",
        statement_config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        # SQLite connection parameters
        timeout: Optional[float] = None,
        detect_types: Optional[int] = None,
        isolation_level: Optional[Union[str, None]] = None,
        check_same_thread: Optional[bool] = None,
        factory: Optional[type[SqliteConnection]] = None,
        cached_statements: Optional[int] = None,
        uri: Optional[bool] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            database: Path to the SQLite database file. Use ':memory:' for in-memory database.
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            timeout: Connection timeout in seconds
            detect_types: Type detection flags (sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            isolation_level: Transaction isolation level
            check_same_thread: Whether to check that connection is used on same thread
            factory: Custom Connection class factory
            cached_statements: Number of statements to cache
            uri: Whether to interpret database as URI
            **kwargs: Additional parameters (stored in extras)
        """
        # Validate required parameters
        if database is None:
            msg = "database parameter cannot be None"
            raise TypeError(msg)

        # Store connection parameters as instance attributes
        self.database = database
        self.timeout = timeout
        self.detect_types = detect_types
        self.isolation_level = isolation_level
        self.check_same_thread = check_same_thread
        self.factory = factory
        self.cached_statements = cached_statements
        self.uri = uri

        self.extras = kwargs or {}

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self._dialect: DialectType = None
        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return a dictionary of connection parameters for SQLite."""
        config = {
            "database": self.database,
            "timeout": self.timeout,
            "detect_types": self.detect_types,
            "isolation_level": self.isolation_level,
            "check_same_thread": self.check_same_thread,
            "factory": self.factory,
            "cached_statements": self.cached_statements,
            "uri": self.uri,
        }
        # Filter out None values since sqlite3.connect doesn't accept them
        return {k: v for k, v in config.items() if v is not None}

    def create_connection(self) -> SqliteConnection:
        """Create and return a SQLite connection."""
        connection = sqlite3.connect(**self.connection_config_dict)
        connection.row_factory = sqlite3.Row
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
            connection.close()

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
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            yield self.driver_type(connection=connection, config=statement_config)
