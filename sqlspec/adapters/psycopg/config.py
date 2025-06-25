"""Psycopg database configuration with direct field-based configuration."""

import contextlib
import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, cast

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from sqlspec.adapters.psycopg.driver import (
    PsycopgAsyncConnection,
    PsycopgAsyncDriver,
    PsycopgSyncConnection,
    PsycopgSyncDriver,
)
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator

    from psycopg import Connection
    from sqlglot.dialects.dialect import DialectType

logger = logging.getLogger("sqlspec.adapters.psycopg")

CONNECTION_FIELDS = frozenset(
    {
        "conninfo",
        "host",
        "port",
        "user",
        "password",
        "dbname",
        "connect_timeout",
        "options",
        "application_name",
        "sslmode",
        "sslcert",
        "sslkey",
        "sslrootcert",
        "autocommit",
    }
)

POOL_FIELDS = CONNECTION_FIELDS.union(
    {
        "min_size",
        "max_size",
        "name",
        "timeout",
        "max_waiting",
        "max_lifetime",
        "max_idle",
        "reconnect_timeout",
        "num_workers",
        "configure",
        "kwargs",
    }
)

__all__ = ("CONNECTION_FIELDS", "POOL_FIELDS", "PsycopgAsyncConfig", "PsycopgSyncConfig")


class PsycopgSyncConfig(SyncDatabaseConfig[PsycopgSyncConnection, ConnectionPool, PsycopgSyncDriver]):
    """Configuration for Psycopg synchronous database connections with direct field-based configuration."""

    __slots__ = (
        "_dialect",
        "application_name",
        "autocommit",
        "configure",
        "connect_timeout",
        "conninfo",
        "dbname",
        "default_row_type",
        "extras",
        "host",
        "kwargs",
        "max_idle",
        "max_lifetime",
        "max_size",
        "max_waiting",
        "min_size",
        "name",
        "num_workers",
        "options",
        "password",
        "pool_instance",
        "port",
        "reconnect_timeout",
        "sslcert",
        "sslkey",
        "sslmode",
        "sslrootcert",
        "statement_config",
        "timeout",
        "user",
    )

    is_async: ClassVar[bool] = False
    supports_connection_pooling: ClassVar[bool] = True

    # Driver class reference for dialect resolution
    driver_type: type[PsycopgSyncDriver] = PsycopgSyncDriver
    connection_type: type[PsycopgSyncConnection] = PsycopgSyncConnection
    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("pyformat_positional", "pyformat_named")
    """Psycopg supports %s (positional) and %(name)s (named) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "pyformat_positional"
    """Psycopg's native parameter style is %s (pyformat positional)."""

    def __init__(
        self,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        # Connection parameters
        conninfo: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        connect_timeout: Optional[float] = None,
        options: Optional[str] = None,
        application_name: Optional[str] = None,
        sslmode: Optional[str] = None,
        sslcert: Optional[str] = None,
        sslkey: Optional[str] = None,
        sslrootcert: Optional[str] = None,
        autocommit: Optional[bool] = None,
        # Pool parameters
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        name: Optional[str] = None,
        timeout: Optional[float] = None,
        max_waiting: Optional[int] = None,
        max_lifetime: Optional[float] = None,
        max_idle: Optional[float] = None,
        reconnect_timeout: Optional[float] = None,
        num_workers: Optional[int] = None,
        configure: Optional["Callable[[Connection[Any]], None]"] = None,
        kwargs: Optional[dict[str, Any]] = None,
        # User-defined extras
        extras: Optional[dict[str, Any]] = None,
        **additional_kwargs: Any,
    ) -> None:
        """Initialize Psycopg synchronous configuration.

        Args:
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            conninfo: Connection string in libpq format
            host: Database server host
            port: Database server port
            user: Database user
            password: Database password
            dbname: Database name
            connect_timeout: Connection timeout in seconds
            options: Command-line options to send to the server
            application_name: Application name for logging and statistics
            sslmode: SSL mode (disable, prefer, require, etc.)
            sslcert: SSL client certificate file
            sslkey: SSL client private key file
            sslrootcert: SSL root certificate file
            autocommit: Enable autocommit mode
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
            name: Name of the connection pool
            timeout: Timeout for acquiring connections
            max_waiting: Maximum number of waiting clients
            max_lifetime: Maximum connection lifetime
            max_idle: Maximum idle time for connections
            reconnect_timeout: Time between reconnection attempts
            num_workers: Number of background workers
            configure: Callback to configure new connections
            kwargs: Additional connection parameters
            extras: Additional connection parameters not explicitly defined
            **additional_kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.conninfo = conninfo
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.connect_timeout = connect_timeout
        self.options = options
        self.application_name = application_name
        self.sslmode = sslmode
        self.sslcert = sslcert
        self.sslkey = sslkey
        self.sslrootcert = sslrootcert
        self.autocommit = autocommit

        # Store pool parameters as instance attributes
        self.min_size = min_size
        self.max_size = max_size
        self.name = name
        self.timeout = timeout
        self.max_waiting = max_waiting
        self.max_lifetime = max_lifetime
        self.max_idle = max_idle
        self.reconnect_timeout = reconnect_timeout
        self.num_workers = num_workers
        self.configure = configure
        self.kwargs = kwargs or {}

        # Handle extras and additional kwargs
        self.extras = extras or {}
        self.extras.update(additional_kwargs)

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self._dialect: DialectType = None

        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for psycopg operations.

        Returns only connection-specific parameters.
        """
        # Gather non-None parameters from connection fields only
        config = {
            field: getattr(self, field)
            for field in CONNECTION_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras and kwargs
        config.update(self.extras)
        if self.kwargs:
            config.update(self.kwargs)

        # Set DictRow as the row factory
        config["row_factory"] = dict_row

        return config

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict for psycopg pool operations.

        Returns all configuration parameters including connection and pool-specific parameters.
        """
        # Gather non-None parameters from all fields (connection + pool)
        config = {
            field: getattr(self, field)
            for field in POOL_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras and kwargs
        config.update(self.extras)
        if self.kwargs:
            config.update(self.kwargs)

        # Set DictRow as the row factory
        config["row_factory"] = dict_row

        return config

    def _create_pool(self) -> "ConnectionPool":
        """Create the actual connection pool."""
        logger.info("Creating Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            # Get all config (creates a new dict)
            all_config = self.pool_config_dict.copy()

            # Separate pool-specific parameters that ConnectionPool accepts directly
            pool_params = {
                "min_size": all_config.pop("min_size", 4),
                "max_size": all_config.pop("max_size", None),
                "name": all_config.pop("name", None),
                "timeout": all_config.pop("timeout", 30.0),
                "max_waiting": all_config.pop("max_waiting", 0),
                "max_lifetime": all_config.pop("max_lifetime", 3600.0),
                "max_idle": all_config.pop("max_idle", 600.0),
                "reconnect_timeout": all_config.pop("reconnect_timeout", 300.0),
                "num_workers": all_config.pop("num_workers", 3),
            }

            # Create a configure callback to set row_factory
            def configure_connection(conn: "PsycopgSyncConnection") -> None:
                # Set DictRow as the row factory
                conn.row_factory = dict_row

            pool_params["configure"] = all_config.pop("configure", configure_connection)

            # Remove None values from pool_params
            pool_params = {k: v for k, v in pool_params.items() if v is not None}

            # Handle conninfo vs individual connection parameters
            conninfo = all_config.pop("conninfo", None)
            if conninfo:
                # If conninfo is provided, use it directly
                # Don't pass kwargs when using conninfo string
                pool = ConnectionPool(conninfo, **pool_params)
            else:
                # Otherwise, pass connection parameters via kwargs
                # Remove any non-connection parameters
                # row_factory is already popped out earlier
                all_config.pop("row_factory", None)
                # Remove pool-specific settings that may have been left
                all_config.pop("kwargs", None)
                pool = ConnectionPool("", kwargs=all_config, **pool_params)

            logger.info("Psycopg connection pool created successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception("Failed to create Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)})
            raise
        return pool

    def _close_pool(self) -> None:
        """Close the actual connection pool."""
        if not self.pool_instance:
            return

        logger.info("Closing Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            self.pool_instance.close()
            logger.info("Psycopg connection pool closed successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception("Failed to close Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)})
            raise

    def create_connection(self) -> "PsycopgSyncConnection":
        """Create a single connection (not from pool).

        Returns:
            A psycopg Connection instance configured with DictRow.
        """
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        return cast("PsycopgSyncConnection", self.pool_instance.getconn())  # pyright: ignore

    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[PsycopgSyncConnection, None, None]":
        """Provide a connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psycopg Connection instance.
        """
        if self.pool_instance:
            with self.pool_instance.connection() as conn:
                yield conn  # type: ignore[misc]
        else:
            conn = self.create_connection()  # type: ignore[assignment]
            try:
                yield conn  # type: ignore[misc]
            finally:
                conn.close()

    @contextlib.contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[PsycopgSyncDriver, None, None]":
        """Provide a driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsycopgSyncDriver instance.
        """
        with self.provide_connection(*args, **kwargs) as conn:
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            driver = self.driver_type(connection=conn, config=statement_config)
            yield driver

    def provide_pool(self, *args: Any, **kwargs: Any) -> "ConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = self.create_pool()
        return self.pool_instance


class PsycopgAsyncConfig(AsyncDatabaseConfig[PsycopgAsyncConnection, AsyncConnectionPool, PsycopgAsyncDriver]):
    """Configuration for Psycopg asynchronous database connections with direct field-based configuration."""

    __slots__ = (
        "_dialect",
        "application_name",
        "autocommit",
        "configure",
        "connect_timeout",
        "conninfo",
        "dbname",
        "default_row_type",
        "extras",
        "host",
        "kwargs",
        "max_idle",
        "max_lifetime",
        "max_size",
        "max_waiting",
        "min_size",
        "name",
        "num_workers",
        "options",
        "password",
        "pool_instance",
        "port",
        "reconnect_timeout",
        "sslcert",
        "sslkey",
        "sslmode",
        "sslrootcert",
        "statement_config",
        "timeout",
        "user",
    )

    is_async: ClassVar[bool] = True
    supports_connection_pooling: ClassVar[bool] = True

    # Driver class reference for dialect resolution
    driver_type: type[PsycopgAsyncDriver] = PsycopgAsyncDriver
    connection_type: type[PsycopgAsyncConnection] = PsycopgAsyncConnection

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("pyformat_positional", "pyformat_named")
    """Psycopg supports %s (pyformat_positional) and %(name)s (pyformat_named) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "pyformat_positional"
    """Psycopg's preferred parameter style is %s (pyformat_positional)."""

    def __init__(
        self,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        # Connection parameters
        conninfo: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        connect_timeout: Optional[float] = None,
        options: Optional[str] = None,
        application_name: Optional[str] = None,
        sslmode: Optional[str] = None,
        sslcert: Optional[str] = None,
        sslkey: Optional[str] = None,
        sslrootcert: Optional[str] = None,
        autocommit: Optional[bool] = None,
        # Pool parameters
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        name: Optional[str] = None,
        timeout: Optional[float] = None,
        max_waiting: Optional[int] = None,
        max_lifetime: Optional[float] = None,
        max_idle: Optional[float] = None,
        reconnect_timeout: Optional[float] = None,
        num_workers: Optional[int] = None,
        configure: Optional["Callable[[Connection[Any]], None]"] = None,
        kwargs: Optional[dict[str, Any]] = None,
        # User-defined extras
        extras: Optional[dict[str, Any]] = None,
        **additional_kwargs: Any,
    ) -> None:
        """Initialize Psycopg asynchronous configuration.

        Args:
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            conninfo: Connection string in libpq format
            host: Database server host
            port: Database server port
            user: Database user
            password: Database password
            dbname: Database name
            connect_timeout: Connection timeout in seconds
            options: Command-line options to send to the server
            application_name: Application name for logging and statistics
            sslmode: SSL mode (disable, prefer, require, etc.)
            sslcert: SSL client certificate file
            sslkey: SSL client private key file
            sslrootcert: SSL root certificate file
            autocommit: Enable autocommit mode
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
            name: Name of the connection pool
            timeout: Timeout for acquiring connections
            max_waiting: Maximum number of waiting clients
            max_lifetime: Maximum connection lifetime
            max_idle: Maximum idle time for connections
            reconnect_timeout: Time between reconnection attempts
            num_workers: Number of background workers
            configure: Callback to configure new connections
            kwargs: Additional connection parameters
            extras: Additional connection parameters not explicitly defined
            **additional_kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.conninfo = conninfo
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.connect_timeout = connect_timeout
        self.options = options
        self.application_name = application_name
        self.sslmode = sslmode
        self.sslcert = sslcert
        self.sslkey = sslkey
        self.sslrootcert = sslrootcert
        self.autocommit = autocommit

        # Store pool parameters as instance attributes
        self.min_size = min_size
        self.max_size = max_size
        self.name = name
        self.timeout = timeout
        self.max_waiting = max_waiting
        self.max_lifetime = max_lifetime
        self.max_idle = max_idle
        self.reconnect_timeout = reconnect_timeout
        self.num_workers = num_workers
        self.configure = configure
        self.kwargs = kwargs or {}

        # Handle extras and additional kwargs
        self.extras = extras or {}
        self.extras.update(additional_kwargs)

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self._dialect: DialectType = None

        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for psycopg operations.

        Returns only connection-specific parameters.
        """
        # Gather non-None parameters from connection fields only
        config = {
            field: getattr(self, field)
            for field in CONNECTION_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras and kwargs
        config.update(self.extras)
        if self.kwargs:
            config.update(self.kwargs)

        # Set DictRow as the row factory
        config["row_factory"] = dict_row

        return config

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict for psycopg pool operations.

        Returns all configuration parameters including connection and pool-specific parameters.
        """
        # Gather non-None parameters from all fields (connection + pool)
        config = {
            field: getattr(self, field)
            for field in POOL_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras and kwargs
        config.update(self.extras)
        if self.kwargs:
            config.update(self.kwargs)

        # Set DictRow as the row factory
        config["row_factory"] = dict_row

        return config

    async def _create_pool(self) -> "AsyncConnectionPool":
        """Create the actual async connection pool."""
        logger.info("Creating async Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            # Get all config (creates a new dict)
            all_config = self.pool_config_dict.copy()

            # Separate pool-specific parameters that AsyncConnectionPool accepts directly
            pool_params = {
                "min_size": all_config.pop("min_size", 4),
                "max_size": all_config.pop("max_size", None),
                "name": all_config.pop("name", None),
                "timeout": all_config.pop("timeout", 30.0),
                "max_waiting": all_config.pop("max_waiting", 0),
                "max_lifetime": all_config.pop("max_lifetime", 3600.0),
                "max_idle": all_config.pop("max_idle", 600.0),
                "reconnect_timeout": all_config.pop("reconnect_timeout", 300.0),
                "num_workers": all_config.pop("num_workers", 3),
            }

            # Create a configure callback to set row_factory
            async def configure_connection(conn: "PsycopgAsyncConnection") -> None:
                # Set DictRow as the row factory
                conn.row_factory = dict_row

            pool_params["configure"] = all_config.pop("configure", configure_connection)

            # Remove None values from pool_params
            pool_params = {k: v for k, v in pool_params.items() if v is not None}

            # Handle conninfo vs individual connection parameters
            conninfo = all_config.pop("conninfo", None)
            if conninfo:
                # If conninfo is provided, use it directly
                # Don't pass kwargs when using conninfo string
                pool = AsyncConnectionPool(conninfo, **pool_params)
            else:
                # Otherwise, pass connection parameters via kwargs
                # Remove any non-connection parameters
                # row_factory is already popped out earlier
                all_config.pop("row_factory", None)
                # Remove pool-specific settings that may have been left
                all_config.pop("kwargs", None)
                pool = AsyncConnectionPool("", kwargs=all_config, **pool_params)

            await pool.open()
            logger.info("Async Psycopg connection pool created successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception(
                "Failed to create async Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)}
            )
            raise
        return pool

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.pool_instance:
            return

        logger.info("Closing async Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            await self.pool_instance.close()
            logger.info("Async Psycopg connection pool closed successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception(
                "Failed to close async Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)}
            )
            raise

    async def create_connection(self) -> "PsycopgAsyncConnection":  # pyright: ignore
        """Create a single async connection (not from pool).

        Returns:
            A psycopg AsyncConnection instance configured with DictRow.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return cast("PsycopgAsyncConnection", await self.pool_instance.getconn())  # pyright: ignore

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[PsycopgAsyncConnection, None]":  # pyright: ignore
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psycopg AsyncConnection instance.
        """
        if self.pool_instance:
            async with self.pool_instance.connection() as conn:
                yield conn  # type: ignore[misc]
        else:
            conn = await self.create_connection()  # type: ignore[assignment]
            try:
                yield conn  # type: ignore[misc]
            finally:
                await conn.close()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[PsycopgAsyncDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsycopgAsyncDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as conn:
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            driver = self.driver_type(connection=conn, config=statement_config)
            yield driver

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance
