"""Starlette extension for SQLSpec database session and middleware management."""

import datetime as dt
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast, overload

from starlette.middleware import Middleware

from sqlspec.base import SQLSpec, coerce_sqlspec
from sqlspec.core._cursor import resolve_plugin_cursor_settings
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.starlette._state import CommitMode, SQLSpecConfigState
from sqlspec.extensions.starlette._utils import (
    get_or_create_db_session,
    get_or_create_db_session_async,
    get_state_value,
    has_state_value,
)
from sqlspec.extensions.starlette.middleware import (
    CorrelationMiddleware,
    SQLCommenterMiddleware,
    SQLSpecTenantRequestMiddleware,
    SQLSpecTransactionMiddleware,
    ensure_request_connection_async,
    ensure_request_connection_sync,
    get_request_resource,
    get_request_resource_async,
    has_tenant_strategy,
)
from sqlspec.utils.config_tools import normalize_extension_status_codes, resolve_extension_commit_mode
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.routing import validate_routing_middleware_settings
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

DEFAULT_COMMIT_MODE: CommitMode = "autocommit"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"


class SQLSpecPlugin:
    """SQLSpec integration for Starlette applications.

    Provides middleware-based session management, automatic transaction handling,
    and connection pooling lifecycle management.
    """

    __slots__ = (
        "_config_states",
        "_correlation_middleware_added",
        "_pagination_cursor_encrypted",
        "_pagination_cursor_secret",
        "_pagination_cursor_ttl",
        "_sqlcommenter_middleware_added",
        "_sqlspec",
    )

    def __init__(
        self,
        sqlspec: "SQLSpec | DatabaseConfigProtocol[Any, Any, Any] | Sequence[DatabaseConfigProtocol[Any, Any, Any]]",
        app: "Starlette | None" = None,
        *,
        loader: "SQLFileLoader | None" = None,
        pagination_cursor_secret: str | bytes | None = None,
        pagination_cursor_ttl: float | dt.timedelta | None = None,
        pagination_cursor_encrypted: bool | None = None,
    ) -> None:
        """Initialize SQLSpec Starlette extension.

        Args:
            sqlspec: Pre-configured SQLSpec registry, a single database
                configuration, or a sequence of database configurations.
            app: Optional Starlette application to initialize immediately.
            loader: Optional SQL file loader attached to the registry.
            pagination_cursor_secret: Optional application-wide cursor signing secret.
            pagination_cursor_ttl: Optional default cursor expiration window.
            pagination_cursor_encrypted: Optional default cursor encryption flag.
        """
        self._sqlspec = coerce_sqlspec(sqlspec, loader=loader)
        self._config_states: list[SQLSpecConfigState] = []
        self._correlation_middleware_added = False
        self._sqlcommenter_middleware_added = False

        extracted_settings: list[dict[str, Any]] = []
        for cfg in self._sqlspec.configs.values():
            settings = self._extract_extension_settings(cfg)
            extracted_settings.append(settings)
            state = self._config_state(cfg, settings)
            self._config_states.append(state)

        self._pagination_cursor_secret, self._pagination_cursor_ttl, self._pagination_cursor_encrypted = (
            resolve_plugin_cursor_settings(
                extracted_settings,
                pagination_cursor_secret=pagination_cursor_secret,
                pagination_cursor_ttl=pagination_cursor_ttl,
                pagination_cursor_encrypted=pagination_cursor_encrypted,
            )
        )

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

    def _tenant_configs(self) -> "tuple[Any, ...]":
        """Return the tenant-enabled configurations in registration order."""
        return tuple(state.config for state in self._config_states if has_tenant_strategy(state.config))

    @property
    def pagination_cursor_secret(self) -> str | bytes | None:
        """Return resolved application-wide cursor secret."""
        return self._pagination_cursor_secret

    @property
    def pagination_cursor_ttl(self) -> int | float | dt.timedelta | None:
        """Return resolved application-wide cursor TTL."""
        return self._pagination_cursor_ttl

    @property
    def pagination_cursor_encrypted(self) -> bool | None:
        """Return resolved application-wide cursor encryption setting."""
        return self._pagination_cursor_encrypted

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

        active_states = [state for state in self._config_states if not state.disable_di]
        if active_states:
            app.add_middleware(SQLSpecTransactionMiddleware, config_states=active_states)
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
                if config_state.config.has_replicas:
                    setattr(
                        app.state,
                        config_state.replica_pool_key,
                        config_state.config.replica_groups.get("default", config_state.config.replica_groups),
                    )
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
                if name in {
                    state.session_key,
                    state.connection_key,
                    state.pool_key,
                    state.replica_session_key,
                    state.replica_connection_key,
                    state.replica_pool_key,
                    state.routing_session_key,
                }:
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
        return get_or_create_db_session(request, config_state)

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
            await ensure_request_connection_async(request, config_state)
        return cast("Any", await get_or_create_db_session_async(request, config_state))

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
            return ensure_request_connection_sync(request, config_state)
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
            return await ensure_request_connection_async(request, config_state)
        return get_state_value(request.state, config_state.connection_key)

    def get_db_replica_session(self, request: "Request", key: Any = None) -> Any:
        """Return a request-owned replica session."""
        selected = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = selected.replica_session_key
        if resource_key is None:
            msg = "routing_session_key must be configured to use routing sessions"
            raise ImproperConfigurationError(msg)
        return get_request_resource(request, selected, resource_key, "session")

    async def get_db_replica_session_async(self, request: "Request", key: Any = None) -> Any:
        """Open and return a request-owned replica session."""
        selected = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = selected.replica_session_key
        if resource_key is None:
            msg = "routing_session_key must be configured to use routing sessions"
            raise ImproperConfigurationError(msg)
        return await get_request_resource_async(request, selected, resource_key, "session")

    def get_replica_connection(self, request: "Request", key: Any = None) -> Any:
        """Return a request-owned replica connection."""
        selected = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = selected.replica_connection_key
        if resource_key is None:
            msg = "routing_session_key must be configured to use routing sessions"
            raise ImproperConfigurationError(msg)
        return get_request_resource(request, selected, resource_key, "connection")

    async def get_replica_connection_async(self, request: "Request", key: Any = None) -> Any:
        """Open and return a request-owned replica connection."""
        selected = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = selected.replica_connection_key
        if resource_key is None:
            msg = "routing_session_key must be configured to use routing sessions"
            raise ImproperConfigurationError(msg)
        return await get_request_resource_async(request, selected, resource_key, "connection")

    def get_db_routing_session(self, request: "Request", key: Any = None) -> Any:
        """Return a request-owned routing session."""
        selected = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = selected.routing_session_key
        if resource_key is None:
            msg = "routing_session_key must be configured to use routing sessions"
            raise ImproperConfigurationError(msg)
        return get_request_resource(request, selected, resource_key, "routing")

    async def get_db_routing_session_async(self, request: "Request", key: Any = None) -> Any:
        """Open and return a request-owned routing session."""
        selected = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = selected.routing_session_key
        if resource_key is None:
            msg = "routing_session_key must be configured to use routing sessions"
            raise ImproperConfigurationError(msg)
        return await get_request_resource_async(request, selected, resource_key, "routing")

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
        connection_key = framework_config.get("connection_key", DEFAULT_CONNECTION_KEY)
        session_key = framework_config.get("session_key", DEFAULT_SESSION_KEY)
        correlation_headers = framework_config.get("correlation_headers")
        return {
            "connection_key": framework_config.get("connection_key", DEFAULT_CONNECTION_KEY),
            "pool_key": pool_key,
            "session_key": session_key,
            "replica_connection_key": framework_config.get(
                "replica_connection_key",
                "db_replica_connection" if connection_key == DEFAULT_CONNECTION_KEY else f"{connection_key}_replica",
            ),
            "replica_pool_key": framework_config.get(
                "replica_pool_key", "db_replica_pool" if pool_key == DEFAULT_POOL_KEY else f"{pool_key}_replica"
            ),
            "replica_session_key": framework_config.get(
                "replica_session_key",
                "db_replica_session" if session_key == DEFAULT_SESSION_KEY else f"{session_key}_replica",
            ),
            "routing_session_key": framework_config.get("routing_session_key"),
            "default_routing_mode": framework_config.get("default_routing_mode", "auto"),
            "enable_routing_middleware": framework_config.get("enable_routing_middleware", True),
            "routing_header": framework_config.get("routing_header"),
            "primary_pin_cookie": framework_config.get("primary_pin_cookie"),
            "primary_pin_secret": framework_config.get("primary_pin_secret"),
            "commit_mode": resolve_extension_commit_mode(config, framework_config, DEFAULT_COMMIT_MODE),
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
            "pagination_cursor_secret": framework_config.get("pagination_cursor_secret"),
            "pagination_cursor_ttl": framework_config.get("pagination_cursor_ttl"),
            "pagination_cursor_encrypted": framework_config.get("pagination_cursor_encrypted"),
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
        enable_routing, routing_header, primary_pin_cookie, primary_pin_secret = validate_routing_middleware_settings(
            enable_routing_middleware=settings.get("enable_routing_middleware", True),
            routing_header=settings.get("routing_header"),
            primary_pin_cookie=settings.get("primary_pin_cookie"),
            primary_pin_secret=settings.get("primary_pin_secret"),
        )
        return SQLSpecConfigState(
            config=config,
            connection_key=settings["connection_key"],
            pool_key=settings["pool_key"],
            session_key=settings["session_key"],
            commit_mode=settings["commit_mode"],
            extra_commit_statuses=normalize_extension_status_codes(settings["extra_commit_statuses"]),
            extra_rollback_statuses=normalize_extension_status_codes(settings["extra_rollback_statuses"]),
            disable_di=bool(settings["disable_di"]),
            enable_correlation_middleware=bool(settings["enable_correlation_middleware"]),
            correlation_header=settings["correlation_header"],
            correlation_headers=settings["correlation_headers"],
            auto_trace_headers=bool(settings["auto_trace_headers"]),
            enable_sqlcommenter_middleware=bool(settings["enable_sqlcommenter_middleware"]),
            sqlcommenter_framework=settings["sqlcommenter_framework"],
            tenant_extractor=settings.get("tenant_extractor"),
            replica_connection_key=settings.get(
                "replica_connection_key",
                "db_replica_connection"
                if settings["connection_key"] == DEFAULT_CONNECTION_KEY
                else f"{settings['connection_key']}_replica",
            ),
            replica_pool_key=settings.get(
                "replica_pool_key",
                "db_replica_pool" if settings["pool_key"] == DEFAULT_POOL_KEY else f"{settings['pool_key']}_replica",
            ),
            replica_session_key=settings.get(
                "replica_session_key",
                "db_replica_session"
                if settings["session_key"] == DEFAULT_SESSION_KEY
                else f"{settings['session_key']}_replica",
            ),
            routing_session_key=settings.get("routing_session_key"),
            default_routing_mode=settings.get("default_routing_mode", "auto"),
            enable_routing_middleware=enable_routing,
            routing_header=routing_header,
            primary_pin_cookie=primary_pin_cookie,
            primary_pin_secret=primary_pin_secret,
        )

    def _ensure_unique_keys(self) -> None:
        """Validate that all state keys are unique across configs.

        Raises:
            ImproperConfigurationError: If duplicate keys found.
        """
        all_keys: set[str] = set()
        for state in self._config_states:
            keys = [
                key
                for key in (
                    state.connection_key,
                    state.pool_key,
                    state.session_key,
                    state.replica_connection_key,
                    state.replica_pool_key,
                    state.replica_session_key,
                    state.routing_session_key,
                )
                if key is not None
            ]
            duplicates = all_keys.intersection(keys)
            if duplicates or len(keys) != len(set(keys)):
                msg = f"Duplicate state keys found: {duplicates or keys}"
                raise ImproperConfigurationError(msg)
            all_keys.update(keys)

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
                if key in {
                    state.connection_key,
                    state.pool_key,
                    state.replica_session_key,
                    state.replica_connection_key,
                    state.replica_pool_key,
                    state.routing_session_key,
                    getattr(state.config, "bind_key", None),
                }:
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
