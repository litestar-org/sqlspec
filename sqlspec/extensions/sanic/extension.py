"""Sanic extension for SQLSpec database session and lifecycle management."""

import asyncio
import contextlib
import logging
from collections.abc import Mapping
from contextlib import AbstractContextManager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Final, NoReturn, cast, overload

from sqlspec.base import SQLSpec, coerce_sqlspec
from sqlspec.core import CorrelationExtractor
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.driver._routing import AsyncRoutingSession
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions._deferred_session import DeferredAsyncSession, raise_async_connection_required
from sqlspec.extensions.sanic._state import SanicConfigState
from sqlspec.extensions.sanic._utils import (
    get_context_value,
    get_or_create_session,
    get_or_create_session_async,
    get_request_tenant,
    has_context_value,
    pop_context_value,
    set_context_value,
    set_request_tenant,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.routing import RoutingContext, RoutingContextPayload, RoutingEntry, apply_request_routing_hints
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_
from sqlspec.utils.tenant import TenantContext, TenantContextPayload, TenantRequestBinding, is_tenant_bypass_route
from sqlspec.utils.type_guards import has_name

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from sanic import Sanic

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

logger = get_logger("sqlspec.extensions.sanic")
_ROUTING_SCOPE_KEY: Final = "sqlspec_routing_token"
_RESOURCE_CONTEXTS_KEY: Final = "sqlspec_read_routing_contexts"
_TENANT_SCOPE_KEY: Final = "sqlspec_tenant_token"

_REQUEST_EXCEPTION_KEY: Final = "sqlspec_request_exception"
_ACTIVE_REQUEST_KEY: Final = "_sqlspec_active_request"
_CURRENT_REQUEST: ContextVar[Any | None] = ContextVar("_sqlspec_sanic_active_request", default=None)
DEFAULT_COMMIT_MODE = "manual"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"


class SQLSpecPlugin:
    """SQLSpec integration for Sanic applications.

    Provides Sanic-native configuration parsing and request helper methods.
    Runtime listener and middleware behavior is registered by ``init_app``.
    """

    __slots__ = (
        "_config_states",
        "_extractor",
        "_lifecycle_listeners_added",
        "_request_middleware_added",
        "_sqlspec",
        "_tenant_states",
    )

    def __init__(
        self,
        sqlspec: "SQLSpec | DatabaseConfigProtocol[Any, Any, Any] | Sequence[DatabaseConfigProtocol[Any, Any, Any]]",
        app: "Sanic[Any, Any] | None" = None,
        *,
        loader: "SQLFileLoader | None" = None,
    ) -> None:
        """Initialize SQLSpec Sanic extension.

        Args:
            sqlspec: Pre-configured SQLSpec registry, a single database
                configuration, or a sequence of database configurations.
            app: Optional Sanic application to initialize immediately.
            loader: Optional SQL file loader attached to the registry.
        """
        self._sqlspec = coerce_sqlspec(sqlspec, loader=loader)
        self._config_states: list[SanicConfigState] = []
        self._tenant_states: dict[int, SanicConfigState] | None = None
        self._lifecycle_listeners_added = False
        self._request_middleware_added = False

        for cfg in self._sqlspec.configs.values():
            settings = self._extract_extension_settings(cfg)
            state = self._config_state(cfg, settings)
            self._config_states.append(state)

        correlation_state = self._first_correlation_state()
        self._extractor = (
            CorrelationExtractor(
                primary_header=correlation_state.correlation_header,
                additional_headers=correlation_state.correlation_headers,
                auto_trace_headers=correlation_state.auto_trace_headers,
            )
            if correlation_state is not None
            else None
        )

        if app is not None:
            self.init_app(app)

        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="sanic",
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

    def init_app(self, app: "Sanic[Any, Any]") -> None:
        """Initialize Sanic application with SQLSpec.

        Args:
            app: Sanic application instance.
        """
        self._ensure_unique_keys()
        setattr(app.ctx, "sqlspec_plugin", self)
        self._add_lifecycle_listeners(app)
        self._add_request_middleware(app)

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
        """Return a registered database configuration by identifier, or the sole configuration when omitted."""
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
                    state.read_session_key,
                    state.read_connection_key,
                    state.read_pool_key,
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

    @staticmethod
    def set_request_tenant(
        request: Any,
        tenant_id: "str | UUID | TenantContextPayload",
        *,
        variables: "Mapping[str, str] | None" = None,
        key: str | None = None,
    ) -> TenantContextPayload:
        """Store an authorized tenant receipt on a Sanic ``request.ctx``."""
        return set_request_tenant(request, tenant_id, variables=variables, key=key)

    def get_session(self, request: Any, key: Any = None) -> Any:
        """Get or create database session for request.

        A synchronous configuration checks out the request connection on this call. An
        asynchronous configuration whose connection is not open yet returns a session that
        opens it on the first awaited call, so ``await session.select(...)``,
        ``async for row in session.select_stream(...)``, and
        ``async with session.transaction():`` work unchanged.

        Args:
            request: Sanic request instance.
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Database session driver instance.
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        session_key = self._deferred_session_key(config_state)
        deferred = get_context_value(request.ctx, session_key, None)
        if deferred is not None:
            return deferred
        if not has_context_value(request.ctx, config_state.connection_key) and has_context_value(
            request.ctx, self._deferred_key(config_state)
        ):
            if config_state.config.is_async:
                deferred = self._deferred_session(request, config_state)
                set_context_value(request.ctx, session_key, deferred)
                return deferred
            self._acquire_deferred_connection_sync(request, config_state)
        return get_or_create_session(request, config_state)

    async def get_session_async(self, request: Any, key: Any = None) -> Any:
        """Get or create database session asynchronously, acquiring deferred tenant leases on demand."""
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        await self.get_connection_async(request, key)
        return await get_or_create_session_async(request, config_state)

    def get_connection(self, request: Any, key: Any = None) -> Any:
        """Get database connection from request context, checking it out on first access.

        Args:
            request: Sanic request instance.
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Database connection object.

        Raises:
            ImproperConfigurationError: The configuration is asynchronous and its connection is
                not open yet; use ``await plugin.get_connection_async(request)``.
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        if not has_context_value(request.ctx, config_state.connection_key) and has_context_value(
            request.ctx, self._deferred_key(config_state)
        ):
            return self._acquire_deferred_connection_sync(request, config_state)
        return get_context_value(request.ctx, config_state.connection_key)

    async def get_connection_async(self, request: Any, key: Any = None) -> Any:
        """Get the request connection asynchronously, checking it out on first access."""
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        if not has_context_value(request.ctx, config_state.connection_key):
            lock = get_context_value(request.ctx, self._deferred_key(config_state), None)
            if isinstance(lock, asyncio.Lock):
                async with lock:
                    if not has_context_value(request.ctx, config_state.connection_key):
                        tenant = _has_tenant_strategy(config_state.config)
                        payload = await TenantContext.arequire(config_state.config) if tenant else None
                        await self._acquire_request_connection(request, config_state, payload, bind_payload=tenant)
                        pop_context_value(request.ctx, self._deferred_key(config_state))
        return get_context_value(request.ctx, config_state.connection_key)

    def get_read_session(self, request: Any, key: Any = None) -> Any:
        """Return the request's lazy read session."""
        return self._read_routing_resource(request, key, "session")

    async def get_read_session_async(self, request: Any, key: Any = None) -> Any:
        """Open and return the request's lazy read session."""
        return await self._read_routing_resource_async(request, key, "session")

    def get_read_connection(self, request: Any, key: Any = None) -> Any:
        """Return the request's lazy read connection."""
        return self._read_routing_resource(request, key, "connection")

    async def get_read_connection_async(self, request: Any, key: Any = None) -> Any:
        """Open and return the request's lazy read connection."""
        return await self._read_routing_resource_async(request, key, "connection")

    def get_routing_session(self, request: Any, key: Any = None) -> Any:
        """Return the request's lazy routing session."""
        return self._read_routing_resource(request, key, "routing")

    async def get_routing_session_async(self, request: Any, key: Any = None) -> Any:
        """Open and return the request's lazy routing session."""
        return await self._read_routing_resource_async(request, key, "routing")

    def _deferred_session(self, request: Any, config_state: SanicConfigState) -> DeferredAsyncSession:
        """Return a session that opens the asynchronous request connection on its first awaited call."""

        def current_session() -> Any:
            if has_context_value(request.ctx, config_state.connection_key):
                return get_or_create_session(request, config_state)
            return None

        return DeferredAsyncSession(
            config_state.config,
            lambda: self.get_session_async(request, config_state.config),
            current_session,
            "await plugin.get_session_async(request)",
        )

    def _resource_key(self, state: SanicConfigState, kind: str) -> str:
        key = (
            state.read_session_key
            if kind == "session"
            else state.read_connection_key
            if kind == "connection"
            else state.routing_session_key
        )
        if key is None:
            msg = "routing_session_key must be configured to use routing sessions"
            raise ImproperConfigurationError(msg)
        return key

    def _read_routing_resource(self, request: Any, key: Any, kind: str) -> Any:
        state = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = self._resource_key(state, kind)
        current = get_context_value(request.ctx, resource_key, None)
        if current is not None:
            return current
        if state.config.is_async:
            if kind == "connection":
                raise_async_connection_required("await plugin.get_read_connection_async(request)")
            deferred_key = f"{resource_key}_deferred"
            current = get_context_value(request.ctx, deferred_key, None)
            if current is None:
                accessor = (
                    "await plugin.get_routing_session_async(request)"
                    if kind == "routing"
                    else "await plugin.get_read_session_async(request)"
                )
                current = DeferredAsyncSession(
                    state.config,
                    lambda: self._read_routing_resource_async(request, state.config, kind),
                    lambda: get_context_value(request.ctx, resource_key, None),
                    accessor,
                    target_type=AsyncRoutingSession if kind == "routing" else None,
                )
                set_context_value(request.ctx, deferred_key, current)
            return current
        captured = TenantContext.require(state.config) if _has_tenant_strategy(state.config) else None
        context = self._new_read_routing_context(state, kind)
        with TenantContext.bind(captured):
            current = context.__enter__()
        self._publish_read_routing_resource(request, state, resource_key, context, current, captured)
        return current

    def _new_read_routing_context(self, state: SanicConfigState, kind: str) -> Any:
        if kind == "routing":
            return state.config.provide_routing_session()
        if kind == "session":
            return state.config.provide_read_session()
        return state.config.provide_read_connection()

    def _publish_read_routing_resource(
        self,
        request: Any,
        state: SanicConfigState,
        key: str,
        context: Any,
        resource: Any,
        captured: TenantContextPayload | None,
    ) -> None:
        contexts = get_context_value(request.ctx, _RESOURCE_CONTEXTS_KEY, None)
        if contexts is None:
            contexts = []
            set_context_value(request.ctx, _RESOURCE_CONTEXTS_KEY, contexts)
        contexts.append((state, key, context, resource, captured))
        set_context_value(request.ctx, key, resource)

    async def _read_routing_resource_async(self, request: Any, key: Any, kind: str) -> Any:
        state = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = self._resource_key(state, kind)
        current = get_context_value(request.ctx, resource_key, None)
        if current is not None:
            return current
        lock_key = f"{resource_key}_lock"
        lock = get_context_value(request.ctx, lock_key, None)
        if lock is None:
            lock = asyncio.Lock()
            set_context_value(request.ctx, lock_key, lock)
        async with lock:
            current = get_context_value(request.ctx, resource_key, None)
            if current is None:
                captured = await TenantContext.arequire(state.config) if _has_tenant_strategy(state.config) else None
                with TenantContext.bind(captured):
                    context = with_ensure_async_(self._new_read_routing_context(state, kind))
                    current = await context.__aenter__()
                self._publish_read_routing_resource(request, state, resource_key, context, current, captured)
            return current

    async def _close_read_routing_resources(self, request: Any, response: Any, *, cancelled: bool = False) -> None:
        contexts = pop_context_value(request.ctx, _RESOURCE_CONTEXTS_KEY) or []
        primary_error: BaseException | None = None
        for state, key, context, resource, captured in reversed(contexts):
            error = get_context_value(request.ctx, _REQUEST_EXCEPTION_KEY, None)
            if error is None and cancelled:
                error = asyncio.CancelledError()
            try:
                if key == state.routing_session_key and state.commit_mode != "manual" and resource.pinned_to_primary:
                    if error is None and state.should_commit(self._response_status_code(response)):
                        try:
                            await ensure_async_(resource.commit)()
                        except BaseException:
                            with contextlib.suppress(Exception):
                                await ensure_async_(resource.rollback)()
                            raise
                    else:
                        await ensure_async_(resource.rollback)()
            except BaseException as finalization_error:
                error = finalization_error
                primary_error = _select_teardown_exception(primary_error, finalization_error)
            finally:
                try:
                    exit_args = (type(error), error, error.__traceback__) if error is not None else (None, None, None)
                    with TenantContext.bind(captured):
                        await with_ensure_async_(context).__aexit__(*exit_args)
                except BaseException as cleanup_error:
                    primary_error = _select_teardown_exception(primary_error, cleanup_error)
                finally:
                    pop_context_value(request.ctx, key)
                    _detach_deferred_session(pop_context_value(request.ctx, f"{key}_deferred"))
                    pop_context_value(request.ctx, f"{key}_lock")
        for state in self._config_states:
            for resource_key in (state.read_connection_key, state.read_session_key, state.routing_session_key):
                if resource_key is not None:
                    _detach_deferred_session(pop_context_value(request.ctx, f"{resource_key}_deferred"))
                    pop_context_value(request.ctx, f"{resource_key}_lock")
        if primary_error is not None:
            raise primary_error

    def _extract_extension_settings(self, config: Any) -> "dict[str, Any]":
        """Extract Sanic settings from config.extension_config.

        Args:
            config: Database configuration instance.

        Returns:
            Dictionary of Sanic-specific settings.
        """
        framework_config = config.extension_config.get("sanic", {})
        pool_key = framework_config.get("pool_key", DEFAULT_POOL_KEY)
        if not config.supports_connection_pooling and pool_key == DEFAULT_POOL_KEY:
            pool_key = f"_{DEFAULT_POOL_KEY}_{id(config)}"
        correlation_headers = framework_config.get("correlation_headers")
        return {
            "connection_key": framework_config.get("connection_key", DEFAULT_CONNECTION_KEY),
            "pool_key": pool_key,
            "session_key": framework_config.get("session_key", DEFAULT_SESSION_KEY),
            "read_session_key": framework_config.get(
                "read_session_key",
                "read_session"
                if framework_config.get("session_key", DEFAULT_SESSION_KEY) == DEFAULT_SESSION_KEY
                else f"{framework_config['session_key']}_read",
            ),
            "read_connection_key": framework_config.get(
                "read_connection_key",
                "read_connection"
                if framework_config.get("connection_key", DEFAULT_CONNECTION_KEY) == DEFAULT_CONNECTION_KEY
                else f"{framework_config['connection_key']}_read",
            ),
            "read_pool_key": framework_config.get(
                "read_pool_key", "read_pool" if pool_key == DEFAULT_POOL_KEY else f"{pool_key}_read"
            ),
            "routing_session_key": framework_config.get("routing_session_key"),
            "default_routing_mode": framework_config.get("default_routing_mode", "auto"),
            "enable_routing_middleware": framework_config.get("enable_routing_middleware", True),
            "routing_header": framework_config.get("routing_header"),
            "write_pin_cookie": framework_config.get("write_pin_cookie"),
            "routing_secret": framework_config.get("routing_secret"),
            "commit_mode": framework_config.get("commit_mode", DEFAULT_COMMIT_MODE),
            "extra_commit_statuses": framework_config.get("extra_commit_statuses"),
            "extra_rollback_statuses": framework_config.get("extra_rollback_statuses"),
            "disable_di": framework_config.get("disable_di", False),
            "tenant_extractor": framework_config.get("tenant_extractor"),
            "enable_correlation_middleware": framework_config.get("enable_correlation_middleware", True),
            "correlation_header": framework_config.get("correlation_header", "x-request-id"),
            "correlation_headers": tuple(correlation_headers) if correlation_headers is not None else None,
            "auto_trace_headers": framework_config.get("auto_trace_headers", True),
            "enable_sqlcommenter_middleware": framework_config.get("enable_sqlcommenter_middleware", True),
            "sqlcommenter_framework": framework_config.get("sqlcommenter_framework", "sanic"),
        }

    def _config_state(self, config: Any, settings: "dict[str, Any]") -> SanicConfigState:
        """Create configuration state object.

        Args:
            config: Database configuration instance.
            settings: Extracted Sanic settings.

        Returns:
            Configuration state instance.

        Raises:
            ImproperConfigurationError: A tenant extractor is configured without a tenant strategy.
        """
        if settings["tenant_extractor"] is not None and not _has_tenant_strategy(config):
            msg = "tenant_extractor requires driver_features['tenant_strategy'] on the same configuration"
            raise ImproperConfigurationError(msg)
        return SanicConfigState(
            config=config,
            connection_key=settings["connection_key"],
            pool_key=settings["pool_key"],
            session_key=settings["session_key"],
            commit_mode=settings["commit_mode"],
            extra_commit_statuses=settings["extra_commit_statuses"],
            extra_rollback_statuses=settings["extra_rollback_statuses"],
            disable_di=settings["disable_di"],
            tenant_extractor=settings["tenant_extractor"],
            enable_correlation_middleware=settings["enable_correlation_middleware"],
            correlation_header=settings["correlation_header"],
            correlation_headers=settings["correlation_headers"],
            auto_trace_headers=settings["auto_trace_headers"],
            enable_sqlcommenter_middleware=settings["enable_sqlcommenter_middleware"],
            sqlcommenter_framework=settings["sqlcommenter_framework"],
            read_session_key=settings["read_session_key"],
            read_connection_key=settings["read_connection_key"],
            read_pool_key=settings["read_pool_key"],
            routing_session_key=settings["routing_session_key"],
            default_routing_mode=settings["default_routing_mode"],
            enable_routing_middleware=settings["enable_routing_middleware"],
            routing_header=settings["routing_header"],
            write_pin_cookie=settings["write_pin_cookie"],
            routing_secret=settings["routing_secret"],
        )

    def _tenant_state_for(self, key: Any) -> Any:
        """Return the tenant-enabled state for a configuration key, built once per plugin."""
        states = self._tenant_states
        if states is None:
            tenant_states = [state for state in self._config_states if _has_tenant_strategy(state.config)]
            states = {id(state.config): state for state in tenant_states}
            states[0] = tenant_states[0]
            self._tenant_states = states
        return states.get(id(key), states[0])

    def _resolve_route_tenant(self, request: Any, key: Any) -> Any:
        """Resolve the route bypass marker, request receipt, or extractor for a configuration."""
        config_state = self._tenant_state_for(key)
        route = getattr(request, "route", None)
        if is_tenant_bypass_route(getattr(route, "handler", None)):
            return TenantContextPayload.bypass()
        receipt = get_request_tenant(request, config_state)
        if receipt is not None:
            return receipt
        if config_state.tenant_extractor is not None:
            return config_state.tenant_extractor(request)
        return None

    def _acquire_deferred_connection_sync(self, request: Any, config_state: SanicConfigState) -> Any:
        config = config_state.config
        if config.is_async:
            raise_async_connection_required("await plugin.get_connection_async(request)")
        tenant = _has_tenant_strategy(config)
        payload = TenantContext.require(config) if tenant else None
        payload_key = self._tenant_payload_key(config_state)
        if tenant:
            set_context_value(request.ctx, payload_key, payload)
        try:
            with TenantContext.bind(payload) if tenant else contextlib.nullcontext():
                if config.supports_connection_pooling:
                    pool = get_context_value(request.app.ctx, config_state.pool_key)
                    cm = cast("AbstractContextManager[Any]", config.provide_connection(pool))
                    connection = cm.__enter__()
                    set_context_value(request.ctx, self._connection_manager_key(config_state), with_ensure_async_(cm))
                elif _has_tenant_strategy(config):
                    cm = cast("AbstractContextManager[Any]", config.provide_connection())
                    connection = cm.__enter__()
                    set_context_value(request.ctx, self._connection_manager_key(config_state), with_ensure_async_(cm))
                else:
                    connection = config.create_connection()
        except BaseException:
            pop_context_value(request.ctx, payload_key)
            raise
        pop_context_value(request.ctx, self._deferred_key(config_state))
        set_context_value(request.ctx, config_state.connection_key, connection)
        return connection

    def _reset_tenant(self, request: Any) -> None:
        token = pop_context_value(request.ctx, _TENANT_SCOPE_KEY)
        if token is not None:
            TenantContext.reset(token)
        self._reset_routing(request)

    def _reset_routing(self, request: Any) -> None:
        token = pop_context_value(request.ctx, _ROUTING_SCOPE_KEY)
        if token is not None:
            RoutingContext.reset(token)

    def _add_lifecycle_listeners(self, app: "Sanic[Any, Any]") -> None:
        """Register Sanic server lifecycle listeners.

        Args:
            app: Sanic application instance.
        """
        if self._lifecycle_listeners_added:
            return

        app.before_server_start(self._before_server_start)
        app.after_server_stop(self._after_server_stop)
        self._lifecycle_listeners_added = True

    def _add_request_middleware(self, app: "Sanic[Any, Any]") -> None:
        """Register Sanic request and response middleware.

        Args:
            app: Sanic application instance.
        """
        if self._request_middleware_added or not self._needs_request_middleware():
            return

        app.on_request(self._on_request)
        cast("Any", app).on_response(self._on_response)
        app.add_signal(self._on_lifecycle_exception, "http.lifecycle.exception")
        app.add_signal(self._on_lifecycle_response, "http.lifecycle.response")
        app.add_signal(self._on_request_complete, "http.lifecycle.complete")
        self._request_middleware_added = True

    def _bind_active_request(self, request: Any) -> None:
        """Record active request in task and TCP connection contexts for disconnect cleanup.

        Args:
            request: Sanic request instance.
        """
        _CURRENT_REQUEST.set(request)
        conn_info = getattr(request, "conn_info", None)
        if conn_info is not None and hasattr(conn_info, "ctx"):
            set_context_value(conn_info.ctx, _ACTIVE_REQUEST_KEY, request)

    def _unbind_active_request(self, request: Any) -> None:
        """Clear active request tracking from task and TCP connection contexts.

        Args:
            request: Sanic request instance.
        """
        if _CURRENT_REQUEST.get() is request:
            _CURRENT_REQUEST.set(None)
        conn_info = getattr(request, "conn_info", None)
        if (
            conn_info is not None
            and hasattr(conn_info, "ctx")
            and get_context_value(conn_info.ctx, _ACTIVE_REQUEST_KEY, None) is request
        ):
            pop_context_value(conn_info.ctx, _ACTIVE_REQUEST_KEY)

    async def _on_lifecycle_exception(self, request: Any, exception: BaseException) -> None:
        """Record an unhandled exception on request context for connection cleanup.

        Args:
            request: Sanic request instance.
            exception: Unhandled exception raised during request lifecycle.
        """
        if request is not None and hasattr(request, "ctx"):
            set_context_value(request.ctx, _REQUEST_EXCEPTION_KEY, exception)

    async def _on_lifecycle_response(self, request: Any = None, response: Any = None, **_: Any) -> None:
        """Finalize request connections after the handler returns and restore tenant context.

        Sanic dispatches ``http.lifecycle.response`` once the handler finishes, including after
        handler-driven ``request.respond()`` streaming, before transmitting the terminal frame.

        Args:
            request: Sanic request instance.
            response: Sanic response instance.
            **_: Additional signal context arguments.
        """
        if request is None or not hasattr(request, "ctx"):
            return
        try:
            binding = TenantContext.current_binding()
            if isinstance(binding, TenantRequestBinding) and has_context_value(request.ctx, _TENANT_SCOPE_KEY):
                await binding.afreeze()
            await self._finalize_request(request, response)
        finally:
            self._reset_tenant(request)

    async def _on_request_complete(self, conn_info: Any = None, request: Any = None, **_: Any) -> None:
        """Release any residual request connections and contexts when a request aborts or disconnects.

        Args:
            conn_info: Optional Sanic connection info passed by ``http.lifecycle.complete``.
            request: Optional Sanic request instance.
            **_: Additional signal context arguments.
        """
        target_request = request
        if target_request is None:
            target_request = _CURRENT_REQUEST.get()
        if target_request is None and conn_info is not None and hasattr(conn_info, "ctx"):
            target_request = pop_context_value(conn_info.ctx, _ACTIVE_REQUEST_KEY)
        if target_request is None or not hasattr(target_request, "ctx"):
            return

        self._unbind_active_request(target_request)
        has_residual = any(
            not state.disable_di
            and (
                has_context_value(target_request.ctx, state.connection_key)
                or has_context_value(target_request.ctx, self._connection_manager_key(state))
            )
            for state in self._config_states
        )
        primary_exc: BaseException | None = None
        try:
            if has_residual:
                if get_context_value(target_request.ctx, _REQUEST_EXCEPTION_KEY, None) is None:
                    set_context_value(target_request.ctx, _REQUEST_EXCEPTION_KEY, asyncio.CancelledError())
                for config_state in self._config_states[::-1]:
                    if config_state.disable_di:
                        continue
                    try:
                        exc_override: BaseException | None = None
                        try:
                            connection = get_context_value(target_request.ctx, config_state.connection_key, None)
                            if connection is not None and config_state.commit_mode != "manual":
                                with contextlib.suppress(Exception):
                                    await self._rollback_target(target_request, connection, config_state)
                        except BaseException as rollback_exc:
                            exc_override = rollback_exc
                            raise
                        finally:
                            await self._release_request_connection(
                                target_request, config_state, exc_override=exc_override
                            )
                    except BaseException as exc:
                        primary_exc = _select_teardown_exception(primary_exc, exc)
            else:
                for config_state in self._config_states:
                    if not config_state.disable_di:
                        pop_context_value(target_request.ctx, self._deferred_key(config_state))
                        _detach_deferred_session(
                            pop_context_value(target_request.ctx, self._deferred_session_key(config_state))
                        )
            try:
                await self._close_read_routing_resources(target_request, None, cancelled=True)
            except BaseException as error:
                primary_exc = _select_teardown_exception(primary_exc, error)
            if primary_exc is not None:
                raise primary_exc
        finally:
            try:
                self._restore_observability_contexts(target_request, None)
            finally:
                self._reset_tenant(target_request)

    async def _on_request(self, request: Any) -> None:
        """Prepare request-scoped state; connections are checked out on first access.

        Args:
            request: Sanic request instance.
        """
        self._bind_active_request(request)
        routing_states = [state for state in self._config_states if state.enable_routing_middleware]
        if routing_states and not has_context_value(request.ctx, _ROUTING_SCOPE_KEY):
            payload = RoutingContext.current() or RoutingContextPayload()
            for state in routing_states:
                if state.default_routing_mode == "primary" and payload.get(state.config.bind_key) is None:
                    payload = payload.with_entry(state.config.bind_key, RoutingEntry("primary"))
                payload = apply_request_routing_hints(
                    payload,
                    bind_key=state.config.bind_key,
                    routing_header_value=request.headers.get(state.routing_header)
                    if state.routing_header is not None
                    else None,
                    cookie_value=request.cookies.get(state.write_pin_cookie)
                    if state.write_pin_cookie is not None
                    else None,
                    write_pin_cookie=state.write_pin_cookie,
                    routing_secret=state.routing_secret,
                )
            set_context_value(request.ctx, _ROUTING_SCOPE_KEY, RoutingContext.set(payload))
        self._set_observability_contexts(request)
        try:
            tenant_keys = tuple(state.config for state in self._config_states if _has_tenant_strategy(state.config))
            if tenant_keys and not has_context_value(request.ctx, _TENANT_SCOPE_KEY):
                binding = TenantRequestBinding(
                    lambda key: self._resolve_route_tenant(request, key),
                    keys=tenant_keys,
                    ambient=TenantContext.current_binding(),
                    missing=lambda _key: _reject_missing_tenant(request),
                )
                set_context_value(request.ctx, _TENANT_SCOPE_KEY, TenantContext.bind_request(binding))
            for config_state in self._config_states:
                if not config_state.disable_di:
                    set_context_value(request.ctx, self._deferred_key(config_state), asyncio.Lock())
        except BaseException as exc:
            try:
                if request is not None and hasattr(request, "ctx"):
                    set_context_value(request.ctx, _REQUEST_EXCEPTION_KEY, exc)
                raise
            finally:
                try:
                    self._restore_observability_contexts(request, None)
                    self._unbind_active_request(request)
                finally:
                    self._reset_tenant(request)

    async def _on_response(self, request: Any, response: Any) -> None:
        """Freeze pending tenant resolutions and restore observability contexts for the response.

        Request connections remain active on ``request.ctx`` until ``http.lifecycle.response``
        runs after the handler returns, so handler-driven streaming keeps its connection.

        Args:
            request: Sanic request instance.
            response: Sanic response instance.
        """
        try:
            binding = TenantContext.current_binding()
            if isinstance(binding, TenantRequestBinding) and has_context_value(request.ctx, _TENANT_SCOPE_KEY):
                await binding.afreeze()
        finally:
            self._restore_observability_contexts(request, response)

    async def _finalize_request(self, request: Any, response: Any) -> None:
        """Finalize and release all request-scoped database connections."""
        primary_exc: BaseException | None = None
        try:
            for config_state in self._config_states[::-1]:
                if config_state.disable_di:
                    continue
                try:
                    await self._finalize_request_connection(request, response, config_state)
                except BaseException as exc:
                    primary_exc = _select_teardown_exception(primary_exc, exc)
            try:
                await self._close_read_routing_resources(request, response)
            except BaseException as error:
                primary_exc = _select_teardown_exception(primary_exc, error)
            if primary_exc is not None:
                raise primary_exc
        finally:
            self._restore_observability_contexts(request, response)
            self._unbind_active_request(request)
            self._reset_routing(request)

    def _needs_request_middleware(self) -> bool:
        """Return whether this plugin should register request middleware.

        Returns:
            ``True`` when connection management or observability middleware is enabled.
        """
        return any(
            not state.disable_di
            or _has_tenant_strategy(state.config)
            or state.enable_correlation_middleware
            or self._state_enables_sqlcommenter(state)
            for state in self._config_states
        )

    def _set_observability_contexts(self, request: Any) -> None:
        """Set request-scoped observability contexts.

        Args:
            request: Sanic request instance.
        """
        self._set_correlation_context(request)
        self._set_sqlcommenter_context(request)

    def _restore_observability_contexts(self, request: Any, response: Any | None) -> None:
        """Restore request-scoped observability contexts.

        Args:
            request: Sanic request instance.
            response: Sanic response instance, if one is available.
        """
        self._restore_sqlcommenter_context(request)
        self._restore_correlation_context(request, response)

    def _set_correlation_context(self, request: Any) -> None:
        """Set CorrelationContext for this request when enabled.

        Args:
            request: Sanic request instance.
        """
        if self._extractor is None:
            return

        correlation_id = self._extractor.extract(lambda header: request.headers.get(header))
        set_context_value(request.ctx, "_sqlspec_previous_correlation_id", CorrelationContext.get())
        set_context_value(request.ctx, "_sqlspec_correlation_id", correlation_id)
        set_context_value(request.ctx, "correlation_id", correlation_id)
        CorrelationContext.set(correlation_id)

    def _restore_correlation_context(self, request: Any, response: Any | None) -> None:
        """Restore CorrelationContext after this request.

        Args:
            request: Sanic request instance.
            response: Sanic response instance, if one is available.
        """
        if not has_context_value(request.ctx, "_sqlspec_previous_correlation_id"):
            return

        correlation_id = pop_context_value(request.ctx, "_sqlspec_correlation_id")
        if response is not None and correlation_id is not None and hasattr(response, "headers"):
            response.headers["X-Correlation-ID"] = correlation_id

        previous = pop_context_value(request.ctx, "_sqlspec_previous_correlation_id")
        pop_context_value(request.ctx, "correlation_id")
        CorrelationContext.set(previous)

    def _set_sqlcommenter_context(self, request: Any) -> None:
        """Set SQLCommenterContext for this request when enabled.

        Args:
            request: Sanic request instance.
        """
        config_state = self._first_sqlcommenter_state()
        if config_state is None:
            return

        attrs = {"framework": config_state.sqlcommenter_framework, "route": self._request_route(request)}
        action = self._request_action(request)
        if action is not None:
            attrs["action"] = action

        set_context_value(request.ctx, "_sqlspec_previous_sqlcommenter", SQLCommenterContext.get())
        SQLCommenterContext.set(attrs)

    def _restore_sqlcommenter_context(self, request: Any) -> None:
        """Restore SQLCommenterContext after this request.

        Args:
            request: Sanic request instance.
        """
        if not has_context_value(request.ctx, "_sqlspec_previous_sqlcommenter"):
            return
        previous = pop_context_value(request.ctx, "_sqlspec_previous_sqlcommenter")
        SQLCommenterContext.set(previous)

    def _first_correlation_state(self) -> SanicConfigState | None:
        """Return the first config state with correlation enabled.

        Returns:
            Matching configuration state, if any.
        """
        for config_state in self._config_states:
            if config_state.enable_correlation_middleware:
                return config_state
        return None

    def _first_sqlcommenter_state(self) -> SanicConfigState | None:
        """Return the first config state with SQLCommenter enabled.

        Returns:
            Matching configuration state, if any.
        """
        for config_state in self._config_states:
            if self._state_enables_sqlcommenter(config_state):
                return config_state
        return None

    def _state_enables_sqlcommenter(self, config_state: SanicConfigState) -> bool:
        """Return whether one config state enables SQLCommenter middleware.

        Args:
            config_state: Configuration state.

        Returns:
            ``True`` when SQLCommenter middleware should run.
        """
        statement_config = config_state.config.statement_config
        return bool(
            config_state.enable_sqlcommenter_middleware and getattr(statement_config, "enable_sqlcommenter", False)
        )

    def _request_route(self, request: Any) -> str:
        """Return the best available Sanic route template.

        Args:
            request: Sanic request instance.

        Returns:
            Route template or path.
        """
        return str(getattr(request, "uri_template", None) or getattr(request, "path", ""))

    def _request_action(self, request: Any) -> str | None:
        """Return the best available Sanic handler action.

        Args:
            request: Sanic request instance.

        Returns:
            Handler/action name when available.
        """
        endpoint = getattr(request, "endpoint", None)
        if isinstance(endpoint, str) and endpoint:
            return endpoint.rsplit(".", 1)[-1]
        if has_name(endpoint):
            return endpoint.__name__

        route = getattr(request, "route", None)
        handler = getattr(route, "handler", None)
        if has_name(handler):
            return handler.__name__

        name = getattr(request, "name", None)
        if isinstance(name, str) and name:
            return name.rsplit(".", 1)[-1]
        return None

    async def _acquire_request_connection(
        self,
        request: Any,
        config_state: SanicConfigState,
        payload: TenantContextPayload | None = None,
        *,
        bind_payload: bool = False,
    ) -> None:
        """Acquire and store a connection for one config state.

        Args:
            request: Sanic request instance.
            config_state: Configuration state.
            payload: Optional immutable tenant payload bound around lease acquisition.
            bind_payload: Whether ``payload`` was explicitly resolved for this connection.
        """
        config = config_state.config
        payload_key = self._tenant_payload_key(config_state)
        if bind_payload:
            set_context_value(request.ctx, payload_key, payload)
        try:
            if config.supports_connection_pooling:
                pool = get_context_value(request.app.ctx, config_state.pool_key)
                if bind_payload:
                    with TenantContext.bind(payload):
                        connection_manager = with_ensure_async_(config.provide_connection(pool))
                        connection = await connection_manager.__aenter__()
                else:
                    connection_manager = with_ensure_async_(config.provide_connection(pool))
                    connection = await connection_manager.__aenter__()
                set_context_value(request.ctx, self._connection_manager_key(config_state), connection_manager)
            elif _has_tenant_strategy(config):
                with TenantContext.bind(payload) if bind_payload else contextlib.nullcontext():
                    connection_manager = with_ensure_async_(config.provide_connection())
                    connection = await connection_manager.__aenter__()
                set_context_value(request.ctx, self._connection_manager_key(config_state), connection_manager)
            elif bind_payload:
                with TenantContext.bind(payload):
                    connection = await ensure_async_(config.create_connection)()
            else:
                connection = await ensure_async_(config.create_connection)()
        except BaseException:
            pop_context_value(request.ctx, payload_key)
            raise

        set_context_value(request.ctx, config_state.connection_key, connection)

    async def _finalize_request_connection(self, request: Any, response: Any, config_state: SanicConfigState) -> None:
        """Commit or rollback, then release one request connection.

        Args:
            request: Sanic request instance.
            response: Sanic response instance.
            config_state: Configuration state.
        """
        pop_context_value(request.ctx, self._deferred_key(config_state))
        connection = get_context_value(request.ctx, config_state.connection_key, None)
        if connection is None:
            await self._release_request_connection(request, config_state)
            return

        exc_override: BaseException | None = None
        try:
            if config_state.commit_mode != "manual":
                has_exception = get_context_value(request.ctx, _REQUEST_EXCEPTION_KEY, None) is not None
                status_code = self._response_status_code(response)
                if not has_exception and self._should_commit(config_state, status_code):
                    try:
                        await self._commit_target(request, connection, config_state)
                    except BaseException:
                        with contextlib.suppress(Exception):
                            await self._rollback_target(request, connection, config_state)
                        raise
                else:
                    await self._rollback_target(request, connection, config_state)
        except BaseException as finalization_exc:
            exc_override = finalization_exc
            raise
        finally:
            await self._release_request_connection(request, config_state, exc_override=exc_override)

    async def _commit_target(self, request: Any, connection: Any, config_state: SanicConfigState) -> None:
        """Commit transaction using active session or connection."""
        session = get_context_value(request.ctx, f"{config_state.session_key}_instance", None)
        if (
            session is not None
            and getattr(session, "_auto_begin_on_write", False) is True
            and not session._connection_in_transaction()
        ):
            return
        target = session if session is not None and hasattr(session, "commit") else connection
        if hasattr(target, "commit"):
            await ensure_async_(target.commit)()

    async def _rollback_target(self, request: Any, connection: Any, config_state: SanicConfigState) -> None:
        """Roll back transaction using active session or connection."""
        session = get_context_value(request.ctx, f"{config_state.session_key}_instance", None)
        if (
            session is not None
            and getattr(session, "_auto_begin_on_write", False) is True
            and not session._connection_in_transaction()
        ):
            return
        target = session if session is not None and hasattr(session, "rollback") else connection
        if hasattr(target, "rollback"):
            await ensure_async_(target.rollback)()

    async def _release_request_connection(
        self, request: Any, config_state: SanicConfigState, exc_override: BaseException | None = None
    ) -> None:
        """Release and clear one request connection.

        Args:
            request: Sanic request instance.
            config_state: Configuration state.
            exc_override: Optional exception overriding any request-recorded exception.
        """
        pop_context_value(request.ctx, self._deferred_key(config_state))
        connection = pop_context_value(request.ctx, config_state.connection_key)
        session = pop_context_value(request.ctx, f"{config_state.session_key}_instance")
        deferred = pop_context_value(request.ctx, self._deferred_session_key(config_state))
        connection_manager = pop_context_value(request.ctx, self._connection_manager_key(config_state))
        payload_key = self._tenant_payload_key(config_state)
        has_payload = has_context_value(request.ctx, payload_key)
        captured_payload = pop_context_value(request.ctx, payload_key)

        if deferred is not None:
            deferred_detach = getattr(deferred, "_detach_connection", None)
            if deferred_detach is not None:
                deferred_detach()

        if connection is None and connection_manager is None and session is None:
            return

        try:
            if session is not None:
                try:
                    runtime = getattr(session, "observability", None)
                    if runtime is not None and not runtime.is_idle:
                        await runtime.emit_session_end_async(session)
                finally:
                    detach = getattr(session, "_detach_connection", None)
                    if detach is not None:
                        detach()
        finally:
            if connection is not None or connection_manager is not None:
                exc = (
                    exc_override
                    if exc_override is not None
                    else get_context_value(request.ctx, _REQUEST_EXCEPTION_KEY, None)
                )
                exit_args = (type(exc), exc, exc.__traceback__) if exc is not None else (None, None, None)

                if connection_manager is not None:
                    if has_payload:
                        with TenantContext.bind(captured_payload):
                            await connection_manager.__aexit__(*exit_args)
                    else:
                        await connection_manager.__aexit__(*exit_args)
                elif connection is not None:
                    if has_payload:
                        with TenantContext.bind(captured_payload):
                            await ensure_async_(connection.close)()
                    else:
                        await ensure_async_(connection.close)()

    def _should_commit(self, config_state: SanicConfigState, status_code: int) -> bool:
        """Determine whether a response status should commit.

        Args:
            config_state: Configuration state.
            status_code: HTTP response status.

        Returns:
            ``True`` when the transaction should commit.
        """
        return config_state.should_commit(status_code)

    def _response_status_code(self, response: Any) -> int:
        """Return a Sanic response status code.

        Args:
            response: Sanic response instance.

        Returns:
            HTTP status code.
        """
        return int(getattr(response, "status", getattr(response, "status_code", 500)))

    def _connection_manager_key(self, config_state: SanicConfigState) -> str:
        """Return the request context key for a connection manager.

        Args:
            config_state: Configuration state.

        Returns:
            Request context key.
        """
        return f"{config_state.connection_key}_context_manager"

    def _tenant_payload_key(self, config_state: SanicConfigState) -> str:
        return f"{config_state.connection_key}_tenant_payload"

    def _deferred_session_key(self, config_state: SanicConfigState) -> str:
        return f"{config_state.session_key}_deferred"

    def _deferred_key(self, config_state: SanicConfigState) -> str:
        return f"{config_state.connection_key}_deferred_tenant"

    async def _before_server_start(self, app: Any, *_: Any) -> None:
        """Create configured connection pools before the worker starts.

        Args:
            app: Sanic application instance.
            *_: Optional Sanic listener arguments.
        """
        for config_state in self._config_states:
            if not config_state.config.supports_connection_pooling:
                continue
            if has_context_value(app.ctx, config_state.pool_key):
                continue

            try:
                pool = await ensure_async_(config_state.config.create_pool)()
                set_context_value(app.ctx, config_state.pool_key, pool)
                if config_state.config.has_replicas:
                    set_context_value(
                        app.ctx,
                        config_state.read_pool_key,
                        config_state.config.replica_groups.get("default", config_state.config.replica_groups),
                    )
            except Exception:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "pool.create.failed",
                    framework="sanic",
                    pool_key=config_state.pool_key,
                    session_key=config_state.session_key,
                )
                raise
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.create",
                framework="sanic",
                pool_key=config_state.pool_key,
                session_key=config_state.session_key,
            )

    async def _after_server_stop(self, app: Any, *_: Any) -> None:
        """Close configured connection pools after the worker stops.

        Args:
            app: Sanic application instance.
            *_: Optional Sanic listener arguments.
        """
        for config_state in self._config_states:
            if not config_state.config.supports_connection_pooling:
                continue
            if not has_context_value(app.ctx, config_state.pool_key):
                continue

            try:
                await ensure_async_(config_state.config.close_pool)()
            except Exception as exc:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "pool.close.failed",
                    framework="sanic",
                    pool_key=config_state.pool_key,
                    session_key=config_state.session_key,
                    error_type=type(exc).__name__,
                )
            else:
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "pool.close",
                    framework="sanic",
                    pool_key=config_state.pool_key,
                    session_key=config_state.session_key,
                )
            finally:
                pop_context_value(app.ctx, config_state.pool_key)
                pop_context_value(app.ctx, config_state.read_pool_key)

    def _ensure_unique_keys(self) -> None:
        """Validate that all context keys are unique across configs.

        Raises:
            ImproperConfigurationError: If duplicate keys are found.
        """
        all_keys: set[str] = set()
        for state in self._config_states:
            keys = [
                key
                for key in (
                    state.connection_key,
                    state.pool_key,
                    state.session_key,
                    state.read_session_key,
                    state.read_connection_key,
                    state.read_pool_key,
                    state.routing_session_key,
                )
                if key is not None
            ]
            duplicates = all_keys.intersection(keys)
            if duplicates or len(keys) != len(set(keys)):
                msg = f"Duplicate context keys found: {duplicates}"
                raise ImproperConfigurationError(msg)
            all_keys.update(keys)

    def _config_state_by_key(self, key: Any) -> SanicConfigState:
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
                    state.read_session_key,
                    state.read_connection_key,
                    state.read_pool_key,
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


def _reject_missing_tenant(request: Any) -> NoReturn:
    """Raise the Sanic response for a request without an authorized tenant."""
    from sanic.exceptions import Forbidden, Unauthorized

    user = getattr(getattr(request, "ctx", None), "user", None)
    if user is None or not bool(getattr(user, "is_authenticated", True)):
        msg = "Authentication required"
        raise Unauthorized(msg)
    msg = "An authorized tenant is required"
    raise Forbidden(msg)


def _has_tenant_strategy(config: Any) -> bool:
    features = getattr(config, "driver_features", None)
    return isinstance(features, Mapping) and features.get("tenant_strategy") not in (None, False)


def _select_teardown_exception(current: BaseException | None, candidate: BaseException) -> BaseException:
    """Select the primary teardown exception to re-raise after completing all targets."""
    if current is None:
        return candidate
    if isinstance(current, Exception) and not isinstance(candidate, Exception):
        return candidate
    return current


def _detach_deferred_session(deferred: Any) -> None:
    """Detach an unawaited deferred session handle when present."""
    if deferred is not None:
        detach = getattr(deferred, "_detach_connection", None)
        if detach is not None:
            detach()
