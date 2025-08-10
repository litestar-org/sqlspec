"""Psycopg database configuration with direct field-based configuration."""

import contextlib
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union, cast

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from typing_extensions import NotRequired

from sqlspec.adapters.psycopg._types import PsycopgAsyncConnection, PsycopgSyncConnection
from sqlspec.adapters.psycopg.driver import (
    PsycopgAsyncCursor,
    PsycopgAsyncDriver,
    PsycopgSyncCursor,
    PsycopgSyncDriver,
    psycopg_statement_config,
)
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator

    from sqlspec.core.statement import StatementConfig


logger = logging.getLogger("sqlspec.adapters.psycopg")


class PsycopgConnectionParams(TypedDict, total=False):
    """Psycopg connection parameters."""

    conninfo: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    user: NotRequired[str]
    password: NotRequired[str]
    dbname: NotRequired[str]
    connect_timeout: NotRequired[int]
    options: NotRequired[str]
    application_name: NotRequired[str]
    sslmode: NotRequired[str]
    sslcert: NotRequired[str]
    sslkey: NotRequired[str]
    sslrootcert: NotRequired[str]
    autocommit: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class PsycopgPoolParams(PsycopgConnectionParams, total=False):
    """Psycopg pool parameters."""

    min_size: NotRequired[int]
    max_size: NotRequired[int]
    name: NotRequired[str]
    timeout: NotRequired[float]
    max_waiting: NotRequired[int]
    max_lifetime: NotRequired[float]
    max_idle: NotRequired[float]
    reconnect_timeout: NotRequired[float]
    num_workers: NotRequired[int]
    configure: NotRequired["Callable[..., Any]"]
    kwargs: NotRequired[dict[str, Any]]


__all__ = (
    "PsycopgAsyncConfig",
    "PsycopgAsyncCursor",
    "PsycopgConnectionParams",
    "PsycopgPoolParams",
    "PsycopgSyncConfig",
    "PsycopgSyncCursor",
)


class PsycopgSyncConfig(SyncDatabaseConfig[PsycopgSyncConnection, ConnectionPool, PsycopgSyncDriver]):
    """Configuration for Psycopg synchronous database connections with direct field-based configuration."""

    driver_type: "ClassVar[type[PsycopgSyncDriver]]" = PsycopgSyncDriver
    connection_type: "ClassVar[type[PsycopgSyncConnection]]" = PsycopgSyncConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[PsycopgPoolParams, dict[str, Any]]]" = None,
        pool_instance: Optional["ConnectionPool"] = None,
        statement_config: "Optional[StatementConfig]" = None,
        migration_config: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize Psycopg synchronous configuration.

        Args:
            pool_config: Pool configuration parameters (TypedDict or dict)
            pool_instance: Existing pool instance to use
            statement_config: Default SQL statement configuration
            migration_config: Migration configuration

        """
        processed_pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in processed_pool_config:
            extras = processed_pool_config.pop("extra")
            processed_pool_config.update(extras)

        super().__init__(
            pool_config=processed_pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or psycopg_statement_config,
        )

    def _create_pool(self) -> "ConnectionPool":
        """Create the actual connection pool."""
        logger.info("Creating Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            all_config = dict(self.pool_config)

            pool_parameters = {
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

            autocommit_setting = all_config.get("autocommit")

            def configure_connection(conn: "PsycopgSyncConnection") -> None:
                conn.row_factory = dict_row
                if autocommit_setting is not None:
                    conn.autocommit = autocommit_setting

                try:
                    import pgvector.psycopg

                    pgvector.psycopg.register_vector(conn)
                    logger.debug("pgvector registered successfully for psycopg sync connection")
                except ImportError:
                    pass
                except Exception as e:
                    logger.debug("Failed to register pgvector for psycopg sync: %s", e)

            pool_parameters["configure"] = all_config.pop("configure", configure_connection)

            pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

            conninfo = all_config.pop("conninfo", None)
            if conninfo:
                pool = ConnectionPool(conninfo, open=True, **pool_parameters)
            else:
                kwargs = all_config.pop("kwargs", {})
                all_config.update(kwargs)
                pool = ConnectionPool("", kwargs=all_config, open=True, **pool_parameters)

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
            if hasattr(self.pool_instance, "_closed"):
                self.pool_instance._closed = True

            self.pool_instance.close()
            logger.info("Psycopg connection pool closed successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception("Failed to close Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)})
            raise
        finally:
            self.pool_instance = None

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
    def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "Generator[PsycopgSyncDriver, None, None]":
        """Provide a driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsycopgSyncDriver instance.
        """
        with self.provide_connection(*args, **kwargs) as conn:
            final_statement_config = statement_config or self.statement_config
            yield self.driver_type(connection=conn, statement_config=final_statement_config)

    def provide_pool(self, *args: Any, **kwargs: Any) -> "ConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for Psycopg types.

        This provides all Psycopg-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({"PsycopgSyncConnection": PsycopgSyncConnection, "PsycopgSyncCursor": PsycopgSyncCursor})
        return namespace


class PsycopgAsyncConfig(AsyncDatabaseConfig[PsycopgAsyncConnection, AsyncConnectionPool, PsycopgAsyncDriver]):
    """Configuration for Psycopg asynchronous database connections with direct field-based configuration."""

    driver_type: ClassVar[type[PsycopgAsyncDriver]] = PsycopgAsyncDriver
    connection_type: "ClassVar[type[PsycopgAsyncConnection]]" = PsycopgAsyncConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[PsycopgPoolParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[AsyncConnectionPool]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
    ) -> None:
        """Initialize Psycopg asynchronous configuration.

        Args:
            pool_config: Pool configuration parameters (TypedDict or dict)
            pool_instance: Existing pool instance to use
            statement_config: Default SQL statement configuration
            migration_config: Migration configuration
        """
        processed_pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in processed_pool_config:
            extras = processed_pool_config.pop("extra")
            processed_pool_config.update(extras)

        super().__init__(
            pool_config=processed_pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or psycopg_statement_config,
        )

    async def _create_pool(self) -> "AsyncConnectionPool":
        """Create the actual async connection pool."""

        all_config = dict(self.pool_config)

        pool_parameters = {
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

        # Capture autocommit setting before configuring the pool
        autocommit_setting = all_config.get("autocommit")

        async def configure_connection(conn: "PsycopgAsyncConnection") -> None:
            conn.row_factory = dict_row
            if autocommit_setting is not None:
                await conn.set_autocommit(autocommit_setting)

            try:
                from pgvector.psycopg import register_vector_async

                await register_vector_async(conn)
                logger.debug("pgvector registered successfully for psycopg async connection")
            except ImportError:
                pass
            except Exception as e:
                logger.debug("Failed to register pgvector for psycopg async: %s", e)

        pool_parameters["configure"] = all_config.pop("configure", configure_connection)

        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        if conninfo:
            pool = AsyncConnectionPool(conninfo, open=False, **pool_parameters)
        else:
            kwargs = all_config.pop("kwargs", {})
            all_config.update(kwargs)
            pool = AsyncConnectionPool("", kwargs=all_config, open=False, **pool_parameters)

        await pool.open()

        return pool

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.pool_instance:
            return

        try:
            if hasattr(self.pool_instance, "_closed"):
                self.pool_instance._closed = True

            await self.pool_instance.close()
        finally:
            self.pool_instance = None

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
    async def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "AsyncGenerator[PsycopgAsyncDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsycopgAsyncDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as conn:
            final_statement_config = statement_config or psycopg_statement_config
            yield self.driver_type(connection=conn, statement_config=final_statement_config)

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for Psycopg async types.

        This provides all Psycopg async-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({"PsycopgAsyncConnection": PsycopgAsyncConnection, "PsycopgAsyncCursor": PsycopgAsyncCursor})
        return namespace
