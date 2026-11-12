"""Starlette extension for SQLSpec database session and middleware management."""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast, overload

from starlette.middleware import Middleware

from sqlspec.base import SQLSpec, coerce_sqlspec
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.starlette._state import SQLSpecConfigState
from sqlspec.extensions.starlette._utils import (
    get_or_create_driver_session,
    get_or_create_session,
    get_state_value,
    has_state_value,
)
from sqlspec.extensions.starlette.middleware import (
    CorrelationMiddleware,
    SQLCommenterMiddleware,
    SQLSpecAutocommitMiddleware,
    SQLSpecManualMiddleware,
    SQLSpecTenantRequestMiddleware,
    acquire_pending_connection_async,
    acquire_pending_connection_sync,
    has_tenant_strategy,
)
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.sync_tools import ensure_async_

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from starlette.applications import Starlette
    from starlette.requests import Request

    from sqlspec.config import (
        AsyncConfigT,
        AsyncDatabaseConfig,
        DatabaseConfigProtocol,
        NoPoolAsyncConfig,
        NoPoolSyncConfig,
        SyncConfigT,
        SyncDatabaseConfig,
    )
    from sqlspec.loader import SQLFileLoader

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
    """

    __slots__ = ("_config_states", "_correlation_middleware_added", "_sqlcommenter_middleware_added", "_sqlspec")

    def __init__(
        self,
        sqlspec: "SQLSpec | DatabaseConfigProtocol[Any, Any, Any] | Sequence[DatabaseConfigProtocol[Any, Any, Any]]",
        app: "Starlette | None" = None,
        *,
        loader: "SQLFileLoader | None" = None,
    ) -> None:
        """Initialize SQLSpec Starlette extension.

        Args:
            sqlspec: Pre-configured SQLSpec registry, a single database
                configuration, or a sequence of database configurations.
            app: Optional Starlette application to initialize immediately.
            loader: Optional SQL file loader attached to the registry.
        """
        self._sqlspec = coerce_sqlspec(sqlspec, loader=loader)
        self._config_states: list[SQLSpecConfigState] = []
        self._correlation_middleware_added = False
        self._sqlcommenter_middleware_added = False

        for cfg in self._sqlspec.configs.values():
            settings = self._extract_extension_settings(cfg)
            state = self._config_state(cfg, settings)
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

    @property
    def sqlspec(self) -> SQLSpec:
        """Return the underlying SQLSpec registry."""
        return self._sqlspec

    @property
    def config(self) -> "list[DatabaseConfigProtocol[Any, Any, Any]]":
        """Return the registered database configurations."""
        return [state.config for state in self._config_states]

    def init_app(self, app: "Starlette") -> None:
        """Initialize Starlette application with SQLSpec.

        Validates configuration, wraps lifespan, and adds middleware.

        Args:
            app: Starlette application instance.
        """
        if getattr(app.state, "sqlspec_plugin", None) is self:
            return

        self._ensure_unique_keys()
        app.state.sqlspec_plugin = self

        original_lifespan = app.router.lifespan_context

        @asynccontextmanager
        async def combined_lifespan(app: "Starlette") -> "AsyncGenerator[None, None]":
            async with self.lifespan(app), original_lifespan(app):
                yield

        app.router.lifespan_context = combined_lifespan

        for config_state in self._config_states:
            if not config_state.disable_di:
                self._add_middleware(app, config_state)
        if any(has_tenant_strategy(state.config) for state in self._config_states):
            app.user_middleware.append(Middleware(SQLSpecTenantRequestMiddleware, config_states=self._config_states))

        self._add_correlation_middleware(app)
        self._add_sqlcommenter_middleware(app)

        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="starlette",
            stage="configured",
            config_count=len(self._config_states),
        )

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
                pool = await ensure_async_(config_state.config.create_pool)()
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
                    try:
                        await ensure_async_(config_state.config.close_pool)()
                    except Exception as exc:
                        log_with_context(
                            logger,
                            logging.ERROR,
                            "session.close",
                            framework="starlette",
                            session_key=config_state.session_key,
                            pool_key=config_state.pool_key,
                            status="failed",
                            error_type=type(exc).__name__,
                        )
                    else:
                        log_with_context(
                            logger,
                            logging.DEBUG,
                            "session.close",
                            framework="starlette",
                            session_key=config_state.session_key,
                            pool_key=config_state.pool_key,
                        )

    @overload
    def get_config(self, name: None = None) -> "DatabaseConfigProtocol[Any, Any, Any]": ...
    @overload
    def get_config(
        self, name: "type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]]"
    ) -> "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]": ...
    @overload
    def get_config(
        self, name: "type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]"
    ) -> "AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]": ...
    @overload
    def get_config(self, name: "SyncConfigT") -> "SyncConfigT": ...
    @overload
    def get_config(self, name: "AsyncConfigT") -> "AsyncConfigT": ...
    @overload
    def get_config(self, name: str) -> "DatabaseConfigProtocol[Any, Any, Any]": ...
    def get_config(
        self,
        name: "str | type[DatabaseConfigProtocol[Any, Any, Any]] | DatabaseConfigProtocol[Any, Any, Any] | None" = None,
    ) -> "DatabaseConfigProtocol[Any, Any, Any]":
        """Return a registered database configuration by identifier, or the sole configuration when omitted.

        Args:
            name: Optional ``bind_key``, ``session_key``, ``connection_key``,
                ``pool_key``, unique config class, or config instance.

        Returns:
            Matching database configuration.

        Raises:
            KeyError: No matching configuration exists, or ``name`` is ambiguous.
        """
        if name is None:
            if len(self._config_states) == 1:
                return self._config_states[0].config
            if not self._config_states:
                msg = "No database configurations are registered on SQLSpecPlugin."
                raise KeyError(msg)
            msg = "Multiple database configurations are registered on SQLSpecPlugin; specify a key, type, or instance."
            raise KeyError(msg)
        if isinstance(name, str):
            for state in self._config_states:
                if getattr(state.config, "bind_key", None) == name:
                    return state.config
            for state in self._config_states:
                if name in {state.session_key, state.connection_key, state.pool_key}:
                    return state.config
            msg = f"No configuration found for key '{name}'."
            raise KeyError(msg)
        if isinstance(name, type):
            matches = [state.config for state in self._config_states if type(state.config) is name]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                msg = f"Multiple configurations share type '{name.__name__}'; look up by session_key or bind_key."
                raise KeyError(msg)
            msg = f"No configuration found for type '{name.__name__}'."
            raise KeyError(msg)
        for state in self._config_states:
            if state.config is name:
                return state.config
        msg = f"No configuration found for '{name!r}'."
        raise KeyError(msg)

    def get_session(self, request: "Request", key: Any = None) -> Any:
        """Get or create database session for request.

        Sessions are cached per request to ensure consistency. A synchronous
        configuration checks out the request connection on this call. An asynchronous
        configuration whose connection is not open yet returns a session that opens it
        on the first awaited call, so ``await session.select(...)``,
        ``async for row in session.select_stream(...)``, and
        ``async with session.transaction():`` work unchanged.

        Args:
            request: Starlette request instance.
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Database session (driver instance).
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        return get_or_create_session(request, config_state)

    async def get_session_async(self, request: "Request", key: Any = None) -> Any:
        """Get or create database session asynchronously, acquiring deferred tenant leases on demand.

        Args:
            request: Starlette request instance.
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Database session (driver instance).
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        if not has_state_value(request.state, config_state.connection_key):
            await acquire_pending_connection_async(request, config_state)
        session_instance_key = f"{config_state.session_key}_instance"
        existing = get_state_value(request.state, session_instance_key, None)
        if existing is not None:
            return existing
        return cast("Any", get_or_create_driver_session(request, config_state))

    def get_connection(self, request: "Request", key: Any = None) -> Any:
        """Get database connection from request state, checking it out on first access.

        Args:
            request: Starlette request instance.
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Database connection object.

        Raises:
            ImproperConfigurationError: The configuration is asynchronous and its connection is
                not open yet; use ``await plugin.get_connection_async(request)``.
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        if not has_state_value(request.state, config_state.connection_key):
            return acquire_pending_connection_sync(request, config_state)
        return get_state_value(request.state, config_state.connection_key)

    async def get_connection_async(self, request: "Request", key: Any = None) -> Any:
        """Get database connection asynchronously, acquiring deferred tenant leases on demand.

        Args:
            request: Starlette request instance.
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Database connection object.
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        if not has_state_value(request.state, config_state.connection_key):
            return await acquire_pending_connection_async(request, config_state)
        return get_state_value(request.state, config_state.connection_key)

    def _extract_extension_settings(self, config: Any) -> "dict[str, Any]":
        """Extract Starlette settings from config.extension_config.

        Args:
            config: Database configuration instance.

        Returns:
            Dictionary of Starlette-specific settings.
        """
        framework_config = config.extension_config.get("starlette", {})
        pool_key = framework_config.get("pool_key", DEFAULT_POOL_KEY)
        if not config.supports_connection_pooling and pool_key == DEFAULT_POOL_KEY:
            pool_key = f"_{DEFAULT_POOL_KEY}_{id(config)}"
        correlation_headers = framework_config.get("correlation_headers")
        return {
            "connection_key": framework_config.get("connection_key", DEFAULT_CONNECTION_KEY),
            "pool_key": pool_key,
            "session_key": framework_config.get("session_key", DEFAULT_SESSION_KEY),
            "commit_mode": framework_config.get("commit_mode", DEFAULT_COMMIT_MODE),
            "extra_commit_statuses": framework_config.get("extra_commit_statuses"),
            "extra_rollback_statuses": framework_config.get("extra_rollback_statuses"),
            "disable_di": framework_config.get("disable_di", False),
            "enable_correlation_middleware": framework_config.get("enable_correlation_middleware", True),
            "correlation_header": framework_config.get("correlation_header", "x-request-id"),
            "correlation_headers": tuple(correlation_headers) if correlation_headers is not None else None,
            "auto_trace_headers": framework_config.get("auto_trace_headers", True),
            "enable_sqlcommenter_middleware": framework_config.get("enable_sqlcommenter_middleware", True),
            "tenant_extractor": framework_config.get("tenant_extractor"),
            "sqlcommenter_framework": framework_config.get("sqlcommenter_framework", "starlette"),
        }

    def _config_state(self, config: Any, settings: "dict[str, Any]") -> SQLSpecConfigState:
        """Create configuration state object.

        Args:
            config: Database configuration instance.
            settings: Extracted framework settings.

        Returns:
            Configuration state instance.

        Raises:
            ImproperConfigurationError: A tenant extractor is configured without a tenant strategy.
        """
        if settings.get("tenant_extractor") is not None and not has_tenant_strategy(config):
            msg = "tenant_extractor requires driver_features['tenant_strategy'] on the same configuration"
            raise ImproperConfigurationError(msg)
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
            enable_sqlcommenter_middleware=settings["enable_sqlcommenter_middleware"],
            sqlcommenter_framework=settings["sqlcommenter_framework"],
            tenant_extractor=settings.get("tenant_extractor"),
        )

    def _ensure_unique_keys(self) -> None:
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
        elif config_state.commit_mode in {"autocommit", "autocommit_include_redirect"}:
            app.add_middleware(SQLSpecAutocommitMiddleware, config_state=config_state)

    def _add_correlation_middleware(self, app: "Starlette") -> None:
        """Add correlation middleware if any config enables it.

        Only adds the middleware once, using settings from the first config
        that enables it.

        Args:
            app: Starlette application instance.
        """
        if self._correlation_middleware_added:
            return

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

    def _add_sqlcommenter_middleware(self, app: "Starlette") -> None:
        """Add SQLCommenter middleware if any config enables it.

        Only adds the middleware once, using the framework name from the first
        config that enables it.

        Args:
            app: Starlette application instance.
        """
        if self._sqlcommenter_middleware_added:
            return

        for config_state in self._config_states:
            if config_state.enable_sqlcommenter_middleware and config_state.config.statement_config.enable_sqlcommenter:
                app.add_middleware(SQLCommenterMiddleware, framework=config_state.sqlcommenter_framework)
                self._sqlcommenter_middleware_added = True
                log_with_context(
                    logger, logging.DEBUG, "extension.init", framework="starlette", stage="sqlcommenter_middleware"
                )
                break

    def _config_state_by_key(self, key: Any) -> SQLSpecConfigState:
        """Get configuration state by session/connection/pool/bind key, unique type, or instance.

        Args:
            key: Configuration identifier to search for.

        Returns:
            Configuration state matching the identifier.

        Raises:
            ValueError: If no unique configuration matches the identifier.
        """
        if isinstance(key, str):
            for state in self._config_states:
                if state.session_key == key:
                    return state
            for state in self._config_states:
                if key in {state.connection_key, state.pool_key, getattr(state.config, "bind_key", None)}:
                    return state
            msg = f"No configuration found with session_key: {key}"
            raise ValueError(msg)
        if isinstance(key, type):
            matches = [state for state in self._config_states if type(state.config) is key]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                msg = f"Multiple configurations share type '{key.__name__}'; specify a session_key or bind_key."
                raise ValueError(msg)
            msg = f"No configuration found with type: {key.__name__}"
            raise ValueError(msg)
        for state in self._config_states:
            if state.config is key:
                return state
        msg = f"No configuration found for: {key!r}"
        raise ValueError(msg)
