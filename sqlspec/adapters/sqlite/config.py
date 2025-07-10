"""SQLite database configuration with direct field-based configuration."""

import logging
import sqlite3
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite.driver import SqliteConnection, SqliteDriver
from sqlspec.config import NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    from collections.abc import Generator


logger = logging.getLogger(__name__)


class SqliteConnectionParams(TypedDict, total=False):
    """SQLite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    factory: NotRequired[Optional[type[SqliteConnection]]]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


__all__ = ("SqliteConfig", "SqliteConnectionParams", "sqlite3")


class SqliteConfig(NoPoolSyncConfig[SqliteConnection, SqliteDriver]):
    """Configuration for SQLite database connections with direct field-based configuration."""

    driver_type: ClassVar[type[SqliteDriver]] = SqliteDriver
    connection_type: ClassVar[type[SqliteConnection]] = SqliteConnection
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    default_parameter_style: ClassVar[str] = "qmark"

    def __init__(
        self,
        *,
        connection_config: "Optional[Union[SqliteConnectionParams, dict[str, Any]]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            connection_config: Connection configuration parameters as TypedDict
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements
        """
        # Store the connection config and extract/merge extras
        self.connection_config: dict[str, Any] = (
            dict(connection_config) if connection_config else {"database": ":memory:"}
        )
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra")
            self.connection_config.update(extras)

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        super().__init__(
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
        )

    def _get_connection_config_dict(self) -> dict[str, Any]:
        """Get connection configuration as plain dict for external library.

        Returns:
            Dictionary with connection parameters, filtering out None values.
        """
        config: dict[str, Any] = dict(self.connection_config)
        # Remove extra key if it exists (it should already be merged)
        config.pop("extra", None)
        # Filter out None values since sqlite3.connect doesn't accept them
        return {k: v for k, v in config.items() if v is not None}

    def create_connection(self) -> SqliteConnection:
        """Create and return a SQLite connection."""
        config = self._get_connection_config_dict()
        connection = sqlite3.connect(**config)
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
            # Inject parameter style info if not already set
            if statement_config.allowed_parameter_styles is None:
                statement_config = statement_config.replace(
                    allowed_parameter_styles=self.supported_parameter_styles,
                    default_parameter_style=self.default_parameter_style,
                )
            yield self.driver_type(connection=connection, config=statement_config)
