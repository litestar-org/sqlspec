import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.starlette._state import SQLSpecConfigState
from sqlspec.extensions.starlette._utils import get_or_create_session, get_state_value
from sqlspec.extensions.starlette.middleware import (
    CorrelationMiddleware,
    SQLSpecAutocommitMiddleware,
    SQLSpecManualMiddleware,
)
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from starlette.applications import Starlette
    from starlette.requests import Request

__all__ = ("SQLSpecPlugin",)

logger = get_logger("sqlspec.extensions.starlette")

DEFAULT_COMMIT_MODE = "manual"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"


class SQLSpecPlugin:
    """SQLSpec integration for Starlette applications.

    Provides middleware-based session management, automatic transaction handling,
    and connection pooling lifecycle management.

    Example:
        from starlette.applications import Starlette
        from sqlspec import SQLSpec
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.extensions.starlette import SQLSpecPlugin

        sqlspec = SQLSpec()
        sqlspec.add_config(AsyncpgConfig(
            bind_key="default",
            connection_config={"dsn": "postgresql://localhost/mydb"},
            extension_config={
                "starlette": {
                    "commit_mode": "autocommit",
                    "session_key": "db"
                }
            }
        ))

        app = Starlette()
        db_ext = SQLSpecPlugin(sqlspec, app)

        @app.route("/users")
        async def list_users(request):
            db = db_ext.get_session(request)
            result = await db.execute("SELECT * FROM users")
            return JSONResponse({"users": result.all()})
    """

    __slots__ = ("_config_states", "_correlation_middleware_added", "_sqlspec")

    def __init__(self, sqlspec: SQLSpec, app: "Starlette | None" = None) -> None:
        """Initialize SQLSpec Starlette extension.

        Args:
            sqlspec: Pre-configured SQLSpec instance with registered configs.
            app: Optional Starlette application to initialize immediately.
        """
        self._sqlspec = sqlspec
        self._config_states: list[SQLSpecConfigState] = []
        self._correlation_middleware_added = False

        for cfg in self._sqlspec.configs.values():
            settings = self._extract_starlette_settings(cfg)
            state = self._create_config_state(cfg, settings)
            self._config_states.append(state)

        if app is not None:
            self.init_app(app)
        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="starlette",
            stage="init",
            config_count=len(self._config_states),
        )

    def _extract_starlette_settings(self, config: Any) -> "dict[str, Any]":
        """Extract Starlette settings from config.extension_config.

        Args:
            config: Database configuration instance.

        Returns:
            Dictionary of Starlette-specific settings.
        """
        starlette_config = config.extension_config.get("starlette", {})

        connection_key = starlette_config.get("connection_key", DEFAULT_CONNECTION_KEY)
        pool_key = starlette_config.get("pool_key", DEFAULT_POOL_KEY)
        session_key = starlette_config.get("session_key", DEFAULT_SESSION_KEY)
        commit_mode = starlette_config.get("commit_mode", DEFAULT_COMMIT_MODE)

        if not config.supports_connection_pooling and pool_key == DEFAULT_POOL_KEY:
            pool_key = f"_{DEFAULT_POOL_KEY}_{id(config)}"

        enable_correlation = starlette_config.get("enable_correlation_middleware", False)
        correlation_header = starlette_config.get("correlation_header", "x-request-id")
        correlation_headers = starlette_config.get("correlation_headers")
        if correlation_headers is not None:
            correlation_headers = tuple(correlation_headers)
        auto_trace_headers = starlette_config.get("auto_trace_headers", True)

        return {
            "connection_key": connection_key,
            "pool_key": pool_key,
            "session_key": session_key,
            "commit_mode": commit_mode,
            "extra_commit_statuses": starlette_config.get("extra_commit_statuses"),
            "extra_rollback_statuses": starlette_config.get("extra_rollback_statuses"),
            "disable_di": starlette_config.get("disable_di", False),
            "enable_correlation_middleware": enable_correlation,
            "correlation_header": correlation_header,
            "correlation_headers": correlation_headers,
            "auto_trace_headers": auto_trace_headers,
        }

    def _create_config_state(self, config: Any, settings: "dict[str, Any]") -> SQLSpecConfigState:
        """Create configuration state object.

        Args:
            config: Database configuration instance.
            settings: Extracted Starlette settings.

        Returns:
            Configuration state instance.
        """
        return SQLSpecConfigState(
            config=config,
            connection_key=settings["connection_key"],
            pool_key=settings["pool_key"],
            session_key=settings["session_key"],
            commit_mode=settings["commit_mode"],
            extra_commit_statuses=settings["extra_commit_statuses"],
            extra_rollback_statuses=settings["extra_rollback_statuses"],
            disable_di=settings["disable_di"],
            enable_correlation_middleware=settings["enable_correlation_middleware"],
            correlation_header=settings["correlation_header"],
            correlation_headers=settings["correlation_headers"],
            auto_trace_headers=settings["auto_trace_headers"],
        )

    def init_app(self, app: "Starlette") -> None:
        """Initialize Starlette application with SQLSpec.

        Validates configuration, wraps lifespan, and adds middleware.

        Args:
            app: Starlette application instance.
        """
        self._validate_unique_keys()

        original_lifespan = app.router.lifespan_context

        @asynccontextmanager
        async def combined_lifespan(app: "Starlette") -> "AsyncGenerator[None, None]":
            async with self.lifespan(app), original_lifespan(app):
                yield

        app.router.lifespan_context = combined_lifespan

        for config_state in self._config_states:
            if not config_state.disable_di:
                self._add_middleware(app, config_state)

        # Add correlation middleware if any config enables it (only add once)
        self._add_correlation_middleware(app)

        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="starlette",
            stage="configured",
            config_count=len(self._config_states),
        )

    def _validate_unique_keys(self) -> None:
        """Validate that all state keys are unique across configs.

        Raises:
            ImproperConfigurationError: If duplicate keys found.
        """
        all_keys: set[str] = set()

        for state in self._config_states:
            keys = {state.connection_key, state.pool_key, state.session_key}
            duplicates = all_keys & keys

            if duplicates:
                msg = f"Duplicate state keys found: {duplicates}"
                raise ImproperConfigurationError(msg)

            all_keys.update(keys)

    def _add_middleware(self, app: "Starlette", config_state: SQLSpecConfigState) -> None:
        """Add transaction middleware for configuration.

        Args:
            app: Starlette application instance.
            config_state: Configuration state.
        """
        if config_state.commit_mode == "manual":
            app.add_middleware(SQLSpecManualMiddleware, config_state=config_state)
        elif config_state.commit_mode == "autocommit":
            app.add_middleware(SQLSpecAutocommitMiddleware, config_state=config_state, include_redirect=False)
        elif config_state.commit_mode == "autocommit_include_redirect":
            app.add_middleware(SQLSpecAutocommitMiddleware, config_state=config_state, include_redirect=True)

    def _add_correlation_middleware(self, app: "Starlette") -> None:
        """Add correlation middleware if any config enables it.

        Only adds the middleware once, using settings from the first config
        that enables it.

        Args:
            app: Starlette application instance.
        """
        if self._correlation_middleware_added:
            return

        # Find first config that enables correlation middleware
        for config_state in self._config_states:
            if config_state.enable_correlation_middleware:
                app.add_middleware(
                    CorrelationMiddleware,
                    primary_header=config_state.correlation_header,
                    additional_headers=config_state.correlation_headers,
                    auto_trace_headers=config_state.auto_trace_headers,
                )
                self._correlation_middleware_added = True
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "extension.init",
                    framework="starlette",
                    stage="correlation_middleware",
                    primary_header=config_state.correlation_header,
                )
                break

    @asynccontextmanager
    async def lifespan(self, app: "Starlette") -> "AsyncGenerator[None, None]":
        """Manage connection pool lifecycle.

        Args:
            app: Starlette application instance.

        Yields:
            None
        """
        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                pool = await config_state.config.create_pool()
                setattr(app.state, config_state.pool_key, pool)
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "session.create",
                    framework="starlette",
                    session_key=config_state.session_key,
                    pool_key=config_state.pool_key,
                )

        try:
            yield
        finally:
            for config_state in self._config_states:
                if config_state.config.supports_connection_pooling:
                    close_result = config_state.config.close_pool()
                    if close_result is not None:
                        await close_result
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "session.close",
                        framework="starlette",
                        session_key=config_state.session_key,
                        pool_key=config_state.pool_key,
                    )

    def get_session(self, request: "Request", key: "str | None" = None) -> Any:
        """Get or create database session for request.

        Sessions are cached per request to ensure consistency.

        Args:
            request: Starlette request instance.
            key: Optional session key to retrieve specific database session.

        Returns:
            Database session (driver instance).
        """
        config_state = self._config_states[0] if key is None else self._get_config_state_by_key(key)

        return get_or_create_session(request, config_state)

    def get_connection(self, request: "Request", key: "str | None" = None) -> Any:
        """Get database connection from request state.

        Args:
            request: Starlette request instance.
            key: Optional session key to retrieve specific database connection.

        Returns:
            Database connection object.
        """
        config_state = self._config_states[0] if key is None else self._get_config_state_by_key(key)

        return get_state_value(request.state, config_state.connection_key)

    def _get_config_state_by_key(self, key: str) -> SQLSpecConfigState:
        """Get configuration state by session key.

        Args:
            key: Session key to search for.

        Returns:
            Configuration state matching the key.

        Raises:
            ValueError: If no configuration found with the specified key.
        """
        for state in self._config_states:
            if state.session_key == key:
                return state

        msg = f"No configuration found with session_key: {key}"
        raise ValueError(msg)
