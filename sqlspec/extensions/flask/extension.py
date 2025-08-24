"""SQLSpec extension for Flask applications."""

from __future__ import annotations

from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, overload

from sqlspec.base import SQLSpec as SQLSpecBase
from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, DriverT, SyncConfigT
from sqlspec.extensions.flask.config import DatabaseConfig
from sqlspec.utils.logging import get_logger
from sqlspec.utils.portal import PortalProvider

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from flask import Flask

    from sqlspec.loader import SQLFileLoader


logger = get_logger("extensions.flask")

__all__ = ("SQLSpec",)


class SQLSpec(SQLSpecBase):
    """SQLSpec integration for Flask applications."""

    __slots__ = ("_app", "_configs", "_portal_started")

    def __init__(
        self,
        config: SyncConfigT | AsyncConfigT | DatabaseConfig | list[DatabaseConfig],
        *,
        loader: SQLFileLoader | None = None,
    ) -> None:
        """Initialize SQLSpec for Flask.

        Args:
            config: Database configuration(s) for SQLSpec.
            loader: Optional SQL file loader instance.
        """
        super().__init__(loader=loader)
        self._app: Flask | None = None
        self._portal_started = False

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

    def add_config(self, config: SyncConfigT | AsyncConfigT) -> type[SyncConfigT | AsyncConfigT]:
        """Add a configuration instance to the registry.

        Args:
            config: The configuration instance to add.

        Returns:
            The type of the added configuration for use as a registry key.
        """
        config_type = type(config)
        self._base_configs[config_type] = config
        return config_type

    def init_app(self, app: Flask) -> None:
        """Initialize SQLSpec with Flask application.

        Args:
            app: The Flask application instance.
        """
        self._app = app

        # Start the portal provider for async operations
        self._ensure_portal_started()

        # Initialize each database configuration
        for db_config in self._configs:
            # Add the configuration to SQLSpec base
            annotation = self.add_config(db_config.config)
            db_config.annotation = annotation  # type: ignore[attr-defined]

            # Initialize with the app
            db_config.init_app(app)

        # Register shutdown handler for the portal
        self._register_shutdown_handler(app)

    def _ensure_portal_started(self) -> None:
        """Ensure the portal provider is started for async operations."""
        if not self._portal_started and self._has_async_config():
            portal = PortalProvider()
            if not portal.is_running:
                portal.start()
            self._portal_started = True

    def _has_async_config(self) -> bool:
        """Check if any configurations are async.

        Returns:
            True if at least one configuration supports async operations.
        """
        return any(hasattr(cfg.config, "is_async") and cfg.config.is_async for cfg in self._configs)  # type: ignore[attr-defined]

    def _register_shutdown_handler(self, app: Flask) -> None:
        """Register application shutdown handler to clean up portal.

        Args:
            app: The Flask application instance.
        """

        @app.teardown_appcontext
        def shutdown_portal(exception: Exception | None = None) -> None:
            """Clean up portal on application shutdown."""
            if self._portal_started:
                portal = PortalProvider()
                if portal.is_running:
                    portal.stop()
                self._portal_started = False

    def get_session(self, config: SyncConfigT | AsyncConfigT | None = None) -> DriverT:
        """Get a database session for the given configuration.

        Args:
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
            # Check if annotation attribute exists and matches, otherwise check config directly
            annotation = getattr(cfg, "annotation", None)
            if config in (cfg.config, annotation) or (annotation is None and config == cfg.config):
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        from flask import g

        # Check if we already have a session in Flask's g object
        session_key = f"_sqlspec_session_{db_config.connection_key}"
        session = getattr(g, session_key, None)
        if session is not None:
            return session

        # Check if we already have a connection in Flask's g object
        connection = getattr(g, db_config.connection_key, None)
        if connection is None:
            # Create new connection using provider
            if db_config.connection_provider:
                connection = db_config.connection_provider()
                setattr(g, db_config.connection_key, connection)
            else:
                msg = f"No connection provider available for {config}"
                raise RuntimeError(msg)

        # Create session using provider
        if db_config.session_provider:
            session = db_config.session_provider(connection)
        else:
            # Fallback: create driver directly
            session = db_config.config.driver_type(connection=connection)  # type: ignore[attr-defined]

        # Cache session in Flask's g object
        setattr(g, session_key, session)
        return session

    def get_async_session(self, config: AsyncConfigT | None = None) -> DriverT:
        """Get an async database session for the given configuration.

        Args:
            config: Async configuration instance to get session for.

        Returns:
            Database driver/session instance.

        Raises:
            KeyError: If configuration is not found.
            ValueError: If configuration is not async.
        """
        if config is not None and not (hasattr(config, "is_async") and config.is_async):  # type: ignore[attr-defined]
            msg = "Configuration must be async"
            raise ValueError(msg)

        return self.get_session(config)

    def get_sync_session(self, config: SyncConfigT | None = None) -> DriverT:
        """Get a sync database session for the given configuration.

        Args:
            config: Sync configuration instance to get session for.

        Returns:
            Database driver/session instance.

        Raises:
            KeyError: If configuration is not found.
            ValueError: If configuration is async.
        """
        if config is not None and hasattr(config, "is_async") and config.is_async:  # type: ignore[attr-defined]
            msg = "Configuration must be sync"
            raise ValueError(msg)

        return self.get_session(config)

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
            # Check if annotation attribute exists and matches, otherwise check config directly
            annotation = getattr(cfg, "annotation", None)
            if config in (cfg.config, annotation) or (annotation is None and config == cfg.config):
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Get connection and create session
        if db_config.connection_provider:
            connection = db_config.connection_provider()
            try:
                if db_config.session_provider:
                    yield db_config.session_provider(connection)
                else:
                    # Fallback: create driver directly
                    yield db_config.config.driver_type(connection=connection)  # type: ignore[attr-defined]
            finally:
                # Clean up connection
                if hasattr(connection, "close") and callable(connection.close):
                    with suppress(Exception):
                        connection.close()
        # Fallback: create session directly from config
        elif hasattr(db_config.config, "create_pool"):
            from sqlspec.utils.sync_tools import ensure_async_

            pool = await ensure_async_(db_config.config.create_pool)()  # type: ignore[attr-defined]
            connection_cm = db_config.config.provide_connection(pool)  # type: ignore[attr-defined]

            try:
                if hasattr(connection_cm, "__aenter__"):
                    async with connection_cm as conn:
                        yield db_config.config.driver_type(connection=conn)  # type: ignore[attr-defined]
                elif hasattr(connection_cm, "__enter__"):
                    # Sync context manager
                    with connection_cm as conn:
                        yield db_config.config.driver_type(connection=conn)  # type: ignore[attr-defined]
                else:
                    # Not a context manager, try to use as connection directly
                    conn = await connection_cm if hasattr(connection_cm, "__await__") else connection_cm
                    try:
                        yield db_config.config.driver_type(connection=conn)  # type: ignore[attr-defined]
                    finally:
                        if hasattr(conn, "close") and callable(conn.close):
                            with suppress(Exception):
                                if hasattr(conn.close, "__await__"):
                                    await conn.close()
                                else:
                                    conn.close()
            finally:
                if hasattr(db_config.config, "close_pool"):
                    with suppress(Exception):
                        await ensure_async_(db_config.config.close_pool)()  # type: ignore[attr-defined]
        else:
            msg = f"Configuration {config} does not support async session creation"
            raise RuntimeError(msg)

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
            if key in {cfg.config, annotation, cfg.connection_key, cfg.pool_key}:
                return annotation or type(cfg.config)  # type: ignore[attr-defined]
        msg = f"No configuration found for {key}"
        raise KeyError(msg)
