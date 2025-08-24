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

    from sqlspec.loader import SQLFileLoader
    from sqlspec.typing import ConnectionT, PoolT

logger = get_logger("extensions.sanic")

__all__ = ("SQLSpec",)


class SQLSpec(SQLSpecBase):
    """SQLSpec integration for Sanic applications."""

    __slots__ = ("_app", "_configs")

    def __init__(
        self,
        config: SyncConfigT | AsyncConfigT | DatabaseConfig | list[DatabaseConfig],
        *,
        loader: SQLFileLoader | None = None,
    ) -> None:
        """Initialize SQLSpec for Sanic.

        Args:
            config: Database configuration(s) for SQLSpec.
            loader: Optional SQL file loader instance.
        """
        super().__init__(loader=loader)
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
            # Generate unique annotation type for this config
            config_type = type(db_config.config)
            db_config.annotation = config_type  # type: ignore[attr-defined]

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
            if config in (cfg.config, getattr(cfg, "annotation", None)):
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Check if we already have a session in request context
        session = getattr(request.ctx, db_config.session_key, None)
        if session is not None:
            return session

        # Check if we have a connection in request context
        connection = getattr(request.ctx, db_config.connection_key, None)
        if connection is None:
            msg = f"No connection available for {config}. Ensure middleware is enabled."
            raise RuntimeError(msg)

        # Create driver directly using connection
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
            if config in (cfg.config, getattr(cfg, "annotation", None)):
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        return getattr(request.app.ctx, db_config.pool_key, None)

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
            if config in (cfg.config, getattr(cfg, "annotation", None)):
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
            # Fallback: use config's provide_session method
            with db_config.config.provide_session() as session:
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
            if config in (cfg.config, getattr(cfg, "annotation", None)):
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
            # Fallback: use config's provide_connection method
            with db_config.config.provide_connection() as connection:
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
            if config in (cfg.config, getattr(cfg, "annotation", None)):
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
            annotation = getattr(cfg, "annotation", None)
            if key in (cfg.config, annotation, cfg.connection_key, cfg.pool_key):
                if annotation is None:
                    msg = "Annotation not set for configuration. Ensure the extension has been initialized."
                    raise AttributeError(msg)
                return annotation
        msg = f"No configuration found for {key}"
        raise KeyError(msg)
