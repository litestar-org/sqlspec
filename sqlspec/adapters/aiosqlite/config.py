"""Aiosqlite database configuration with direct field-based configuration."""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

import aiosqlite
from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite.driver import AiosqliteConnection, AiosqliteDriver
from sqlspec.config import NoPoolAsyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

__all__ = ("AiosqliteConfig", "AiosqliteConnectionParams")

logger = logging.getLogger(__name__)


class AiosqliteConnectionParams(TypedDict, total=False):
    """aiosqlite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class AiosqliteConfig(NoPoolAsyncConfig[AiosqliteConnection, AiosqliteDriver]):
    """Configuration for Aiosqlite database connections with direct field-based configuration.

    Note: Aiosqlite doesn't support connection pooling, so pool_instance is always None.
    """

    driver_type: ClassVar[type[AiosqliteDriver]] = AiosqliteDriver
    connection_type: ClassVar[type[AiosqliteConnection]] = AiosqliteConnection
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    default_parameter_style: ClassVar[str] = "qmark"

    def __init__(
        self,
        connection_config: "Optional[Union[AiosqliteConnectionParams, dict[str, Any]]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
    ) -> None:
        """Initialize Aiosqlite configuration.

        Args:
            connection_config: Connection configuration parameters (TypedDict or dict)
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements

        Example:
            >>> config = AiosqliteConfig(
            ...     connection_config={
            ...         "database": "test.db",
            ...         "timeout": 30.0,
            ...         "detect_types": 0,
            ...         "isolation_level": "DEFERRED",
            ...     }
            ... )
        """
        if connection_config is None:
            connection_config = {"database": ":memory:"}
        self.connection_config: dict[str, Any] = dict(connection_config)
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra")
            self.connection_config.update(extras)

        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        super().__init__(
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
        )

    async def create_connection(self) -> "AiosqliteConnection":
        """Create a single async connection.

        Returns:
            An Aiosqlite connection instance.
        """
        try:
            # Use connection config directly (extras already merged)
            return await aiosqlite.connect(**self.connection_config)
        except Exception as e:
            msg = f"Could not configure the Aiosqlite connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[AiosqliteConnection, None]":
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
    async def provide_session(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[AiosqliteDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AiosqliteDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            statement_config = self.statement_config
            # Inject parameter style info if not already set
            if statement_config.allowed_parameter_styles is None:
                statement_config = statement_config.replace(
                    allowed_parameter_styles=self.supported_parameter_styles,
                    default_parameter_style=self.default_parameter_style,
                )
            yield self.driver_type(connection=connection, config=statement_config)

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for Aiosqlite types.

        This provides all Aiosqlite-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({"AiosqliteConnection": AiosqliteConnection})
        return namespace
