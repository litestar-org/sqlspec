"""OracleDB database configuration with direct field-based configuration."""

import contextlib
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union, cast

import oracledb
from typing_extensions import NotRequired

from sqlspec.adapters.oracledb._types import (
    OracleAsyncConnection,
    OracleAsyncConnectionPool,
    OracleSyncConnection,
    OracleSyncConnectionPool,
)
from sqlspec.adapters.oracledb.driver import (
    OracleAsyncCursor,
    OracleAsyncDriver,
    OracleSyncCursor,
    OracleSyncDriver,
    oracledb_statement_config,
)
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator

    from oracledb import AuthMode

    from sqlspec.core.statement import StatementConfig


__all__ = ("OracleAsyncConfig", "OracleConnectionParams", "OraclePoolParams", "OracleSyncConfig")

logger = logging.getLogger(__name__)


class OracleConnectionParams(TypedDict, total=False):
    """OracleDB connection parameters."""

    dsn: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    service_name: NotRequired[str]
    sid: NotRequired[str]
    wallet_location: NotRequired[str]
    wallet_password: NotRequired[str]
    config_dir: NotRequired[str]
    tcp_connect_timeout: NotRequired[float]
    retry_count: NotRequired[int]
    retry_delay: NotRequired[int]
    mode: NotRequired["AuthMode"]
    events: NotRequired[bool]
    edition: NotRequired[str]


class OraclePoolParams(OracleConnectionParams, total=False):
    """OracleDB pool parameters."""

    min: NotRequired[int]
    max: NotRequired[int]
    increment: NotRequired[int]
    threaded: NotRequired[bool]
    getmode: NotRequired[Any]
    homogeneous: NotRequired[bool]
    timeout: NotRequired[int]
    wait_timeout: NotRequired[int]
    max_lifetime_session: NotRequired[int]
    session_callback: NotRequired["Callable[..., Any]"]
    max_sessions_per_shard: NotRequired[int]
    soda_metadata_cache: NotRequired[bool]
    ping_interval: NotRequired[int]
    extra: NotRequired[dict[str, Any]]


class OracleSyncConfig(SyncDatabaseConfig[OracleSyncConnection, "OracleSyncConnectionPool", OracleSyncDriver]):
    """Configuration for Oracle synchronous database connections."""

    __slots__ = ()

    driver_type: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver
    connection_type: "ClassVar[type[OracleSyncConnection]]" = OracleSyncConnection
    migration_tracker_type: "ClassVar[type[OracleSyncMigrationTracker]]" = OracleSyncMigrationTracker

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[OraclePoolParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[OracleSyncConnectionPool]" = None,
        migration_config: Optional[dict[str, Any]] = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        """Initialize Oracle synchronous configuration.

        Args:
            pool_config: Pool configuration parameters
            pool_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
        """

        processed_pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in processed_pool_config:
            extras = processed_pool_config.pop("extra")
            processed_pool_config.update(extras)
        statement_config = statement_config or oracledb_statement_config
        super().__init__(
            pool_config=processed_pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features or {},
        )

    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create the actual connection pool."""

        return oracledb.create_pool(**dict(self.pool_config))

    def _close_pool(self) -> None:
        """Close the actual connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> "OracleSyncConnection":
        """Create a single connection (not from pool).

        Returns:
            An Oracle Connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        return self.pool_instance.acquire()

    @contextlib.contextmanager
    def provide_connection(self) -> "Generator[OracleSyncConnection, None, None]":
        """Provide a connection context manager.

        Yields:
            An Oracle Connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        conn = self.pool_instance.acquire()
        try:
            yield conn
        finally:
            self.pool_instance.release(conn)

    @contextlib.contextmanager
    def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "Generator[OracleSyncDriver, None, None]":
        """Provide a driver session context manager.

        Args:
            *args: Positional arguments (unused).
            statement_config: Optional statement configuration override.
            **kwargs: Keyword arguments (unused).

        Yields:
            An OracleSyncDriver instance.
        """
        _ = (args, kwargs)  # Mark as intentionally unused
        with self.provide_connection() as conn:
            yield self.driver_type(connection=conn, statement_config=statement_config or self.statement_config)

    def provide_pool(self) -> "OracleSyncConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for OracleDB types.

        Provides OracleDB-specific types for Litestar framework recognition.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "OracleSyncConnection": OracleSyncConnection,
                "OracleAsyncConnection": OracleAsyncConnection,
                "OracleSyncConnectionPool": OracleSyncConnectionPool,
                "OracleAsyncConnectionPool": OracleAsyncConnectionPool,
                "OracleSyncCursor": OracleSyncCursor,
            }
        )
        return namespace


class OracleAsyncConfig(AsyncDatabaseConfig[OracleAsyncConnection, "OracleAsyncConnectionPool", OracleAsyncDriver]):
    """Configuration for Oracle asynchronous database connections."""

    __slots__ = ()

    connection_type: "ClassVar[type[OracleAsyncConnection]]" = OracleAsyncConnection
    driver_type: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver
    migration_tracker_type: "ClassVar[type[OracleAsyncMigrationTracker]]" = OracleAsyncMigrationTracker

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[OraclePoolParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[OracleAsyncConnectionPool]" = None,
        migration_config: Optional[dict[str, Any]] = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        """Initialize Oracle asynchronous configuration.

        Args:
            pool_config: Pool configuration parameters
            pool_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
        """

        processed_pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in processed_pool_config:
            extras = processed_pool_config.pop("extra")
            processed_pool_config.update(extras)

        super().__init__(
            pool_config=processed_pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or oracledb_statement_config,
            driver_features=driver_features or {},
        )

    async def _create_pool(self) -> "OracleAsyncConnectionPool":
        """Create the actual async connection pool."""

        return oracledb.create_pool_async(**dict(self.pool_config))

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> OracleAsyncConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Oracle AsyncConnection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return cast("OracleAsyncConnection", await self.pool_instance.acquire())

    @asynccontextmanager
    async def provide_connection(self) -> "AsyncGenerator[OracleAsyncConnection, None]":
        """Provide an async connection context manager.

        Yields:
            An Oracle AsyncConnection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        conn = await self.pool_instance.acquire()
        try:
            yield conn
        finally:
            await self.pool_instance.release(conn)

    @asynccontextmanager
    async def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "AsyncGenerator[OracleAsyncDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Positional arguments (unused).
            statement_config: Optional statement configuration override.
            **kwargs: Keyword arguments (unused).

        Yields:
            An OracleAsyncDriver instance.
        """
        _ = (args, kwargs)  # Mark as intentionally unused
        async with self.provide_connection() as conn:
            yield self.driver_type(connection=conn, statement_config=statement_config or self.statement_config)

    async def provide_pool(self) -> "OracleAsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for OracleDB async types.

        Provides OracleDB async-specific types for Litestar framework recognition.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "OracleSyncConnection": OracleSyncConnection,
                "OracleAsyncConnection": OracleAsyncConnection,
                "OracleSyncConnectionPool": OracleSyncConnectionPool,
                "OracleAsyncConnectionPool": OracleAsyncConnectionPool,
                "OracleSyncCursor": OracleSyncCursor,
                "OracleAsyncCursor": OracleAsyncCursor,
            }
        )
        return namespace
