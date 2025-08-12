"""SQLSpec extension for Sanic applications."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, overload

from sqlspec.base import SQLSpec as SQLSpecBase
from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, DriverT, SyncConfigT
from sqlspec.extensions.sanic.config import DatabaseConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sanic import Sanic

    from sqlspec.typing import ConnectionT, PoolT

logger = get_logger("extensions.sanic")

__all__ = ("SQLSpec",)


class SQLSpec(SQLSpecBase):
    """SQLSpec integration for Sanic applications."""

    __slots__ = ("_app", "_configs")

    def __init__(self, config: SyncConfigT | AsyncConfigT | DatabaseConfig | list[DatabaseConfig]) -> None:
        """Initialize SQLSpec for Sanic.

        Args:
            config: Database configuration(s) for SQLSpec.
        """
        super().__init__()
        self._app: Sanic | None = None

        if isinstance(config, DatabaseConfigProtocol):
            self._configs: list[DatabaseConfig] = [DatabaseConfig(config=config)]
        elif isinstance(config, DatabaseConfig):
            self._configs = [config]
        else:
            self._configs = config

    @property
    def config(self) -> list[DatabaseConfig]:
        """Return the database configurations.

        Returns:
            List of database configurations.
        """
        return self._configs

    def init_app(self, app: Sanic) -> None:
        """Initialize SQLSpec with Sanic application.

        Args:
            app: The Sanic application instance.
        """
        self._app = app

        # Initialize each database configuration
        for db_config in self._configs:
            # Add the configuration to SQLSpec base
            annotation = self.add_config(db_config.config)
            db_config.annotation = annotation  # type: ignore[attr-defined]

            # Initialize with the app
            db_config.init_app(app)

        # Store reference in app context
        app.ctx.sqlspec = self

    def get_session(self, request: Any, config: SyncConfigT | AsyncConfigT | None = None) -> DriverT:
        """Get a database session for the given configuration from request context.

        Args:
            request: The Sanic request object.
            config: Configuration instance to get session for. If None, uses first config.

        Returns:
            Database driver/session instance.

        Raises:
            KeyError: If configuration is not found.
        """
        if config is None:
            if not self._configs:
                msg = "No database configurations available"
                raise RuntimeError(msg)
            config = self._configs[0].config

        # Find the database config for this configuration
        db_config = None
        for cfg in self._configs:
            if config in (cfg.config, cfg.annotation):  # type: ignore[attr-defined]
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Check if we already have a connection in request context
        connection = getattr(request.ctx, db_config.connection_key, None)
        if connection is None:
            msg = f"No connection available for {config}. Ensure middleware is enabled."
            raise RuntimeError(msg)

        # Create session using provider
        if db_config.session_provider:
            # For Sanic, we need to handle this synchronously since session access is typically sync
            import asyncio

            async def get_session_async() -> DriverT:
                async with db_config.session_provider(connection) as session:
                    return session

            return asyncio.create_task(get_session_async())
        # Fallback: create driver directly
        return db_config.config.driver_type(connection=connection)  # type: ignore[attr-defined]

    def get_engine(self, request: Any, config: SyncConfigT | AsyncConfigT | None = None) -> Any:
        """Get database engine from request context.

        Args:
            request: The Sanic request object.
            config: Configuration instance to get engine for.

        Returns:
            Database engine instance.

        Raises:
            KeyError: If configuration is not found.
        """
        if config is None:
            if not self._configs:
                msg = "No database configurations available"
                raise RuntimeError(msg)
            config = self._configs[0].config

        # Find the database config for this configuration
        db_config = None
        for cfg in self._configs:
            if config in (cfg.config, cfg.annotation):  # type: ignore[attr-defined]
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        return getattr(request.app.ctx, db_config.engine_key, None)

    @overload
    def provide_session(self, config: SyncConfigT) -> AsyncGenerator[DriverT, None]: ...

    @overload
    def provide_session(self, config: AsyncConfigT) -> AsyncGenerator[DriverT, None]: ...

    @overload
    def provide_session(self, config: type[SyncConfigT | AsyncConfigT]) -> AsyncGenerator[DriverT, None]: ...

    @asynccontextmanager
    async def provide_session(
        self, config: SyncConfigT | AsyncConfigT | type[SyncConfigT | AsyncConfigT]
    ) -> AsyncGenerator[DriverT, None]:
        """Provide a database session for the given configuration.

        Args:
            config: Configuration instance or type to get session for.

        Yields:
            Database driver/session instance.

        Raises:
            KeyError: If configuration is not found.
        """
        # Find the database config for this configuration
        db_config = None
        for cfg in self._configs:
            if config in (cfg.config, cfg.annotation):  # type: ignore[attr-defined]
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Get connection and create session
        if db_config.connection_provider:
            async with db_config.connection_provider() as connection:
                if db_config.session_provider:
                    async with db_config.session_provider(connection) as session:
                        yield session
                else:
                    # Fallback: create driver directly
                    yield db_config.config.driver_type(connection=connection)  # type: ignore[attr-defined]
        else:
            # Fallback: use base class session management
            async with super().provide_session(config) as session:
                yield session

    @overload
    def provide_connection(self, config: SyncConfigT) -> AsyncGenerator[ConnectionT, None]: ...

    @overload
    def provide_connection(self, config: AsyncConfigT) -> AsyncGenerator[ConnectionT, None]: ...

    @overload
    def provide_connection(self, config: type[SyncConfigT | AsyncConfigT]) -> AsyncGenerator[ConnectionT, None]: ...

    @asynccontextmanager
    async def provide_connection(
        self, config: SyncConfigT | AsyncConfigT | type[SyncConfigT | AsyncConfigT]
    ) -> AsyncGenerator[ConnectionT, None]:
        """Provide a database connection for the given configuration.

        Args:
            config: Configuration instance or type to get connection for.

        Yields:
            Database connection instance.

        Raises:
            KeyError: If configuration is not found.
        """
        # Find the database config for this configuration
        db_config = None
        for cfg in self._configs:
            if config in (cfg.config, cfg.annotation):  # type: ignore[attr-defined]
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Get connection
        if db_config.connection_provider:
            async with db_config.connection_provider() as connection:
                yield connection
        else:
            # Fallback: use base class connection management
            async with super().provide_connection(config) as connection:
                yield connection

    @overload
    def provide_pool(self, config: SyncConfigT) -> PoolT: ...

    @overload
    def provide_pool(self, config: AsyncConfigT) -> PoolT: ...

    @overload
    def provide_pool(self, config: type[SyncConfigT | AsyncConfigT]) -> PoolT: ...

    async def provide_pool(self, config: SyncConfigT | AsyncConfigT | type[SyncConfigT | AsyncConfigT]) -> PoolT:
        """Provide a database pool for the given configuration.

        Args:
            config: Configuration instance or type to get pool for.

        Returns:
            Database connection pool.

        Raises:
            KeyError: If configuration is not found.
        """
        # Find the database config for this configuration
        db_config = None
        for cfg in self._configs:
            if config in (cfg.config, cfg.annotation):  # type: ignore[attr-defined]
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Get pool
        if db_config.pool_provider:
            return await db_config.pool_provider()
        # Fallback: create pool directly
        from sqlspec.utils.sync_tools import ensure_async_

        return await ensure_async_(db_config.config.create_pool)()

    def get_annotation(
        self, key: str | SyncConfigT | AsyncConfigT | type[SyncConfigT | AsyncConfigT]
    ) -> type[SyncConfigT | AsyncConfigT]:
        """Return the annotation for the given configuration.

        Args:
            key: The configuration instance, type, or key to lookup.

        Returns:
            The annotation for the configuration.

        Raises:
            KeyError: If no configuration is found for the given key.
        """
        for cfg in self._configs:
            if key in (cfg.config, cfg.annotation, cfg.connection_key, cfg.pool_key):
                return cfg.annotation  # type: ignore[attr-defined]
        msg = f"No configuration found for {key}"
        raise KeyError(msg)
