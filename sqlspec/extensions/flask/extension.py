"""Flask extension for SQLSpec database integration."""

import atexit
import inspect
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, Literal, NoReturn, overload

from sqlspec.base import SQLSpec, coerce_sqlspec
from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig
from sqlspec.core import CorrelationExtractor
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions._deferred_session import PortalSession, call_portal_with_context
from sqlspec.extensions.flask._state import FlaskConfigState
from sqlspec.extensions.flask._utils import (
    get_context_value,
    get_or_create_session,
    get_request_tenant,
    has_context_value,
    pop_context_value,
    set_context_value,
    set_request_tenant,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.portal import PortalProvider
from sqlspec.utils.routing import RoutingContext, RoutingContextPayload, RoutingEntry, apply_request_routing_hints
from sqlspec.utils.tenant import (
    TenantContext,
    TenantContextPayload,
    TenantRequestBinding,
    coerce_tenant_payload,
    is_tenant_bypass_route,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence
    from uuid import UUID

    from flask import Flask, Response

    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, NoPoolSyncConfig, SyncConfigT, SyncDatabaseConfig
    from sqlspec.loader import SQLFileLoader

__all__ = ("SQLSpecPlugin",)

logger = get_logger("sqlspec.extensions.flask")

DEFAULT_COMMIT_MODE: Literal["manual"] = "manual"
DEFAULT_SESSION_KEY = "db_session"
_ROUTING_SCOPE_KEY: Final = "sqlspec_routing_token"
_RESOURCE_CONTEXTS_KEY: Final = "sqlspec_read_routing_contexts"
_TENANT_SCOPE_KEY: Final = "sqlspec_tenant_token"
_TENANT_BINDING_KEY: Final = "sqlspec_tenant_binding"


class SQLSpecPlugin:
    """Flask extension for SQLSpec database integration.

    Provides request-scoped session management, automatic transaction handling,
    and async adapter support via portal pattern.
    """

    def __init__(
        self,
        sqlspec: "SQLSpec | DatabaseConfigProtocol[Any, Any, Any] | Sequence[DatabaseConfigProtocol[Any, Any, Any]]",
        app: "Flask | None" = None,
        *,
        loader: "SQLFileLoader | None" = None,
    ) -> None:
        """Initialize Flask extension with a SQLSpec registry or database configuration(s).

        Args:
            sqlspec: SQLSpec registry, a single database configuration, or a
                sequence of database configurations.
            app: Optional Flask application to initialize immediately.
            loader: Optional SQL file loader attached to the registry.
        """
        self._sqlspec = coerce_sqlspec(sqlspec, loader=loader)
        self._config_states: list[FlaskConfigState] = []
        self._tenant_states: dict[int, FlaskConfigState] | None = None
        self._portal: PortalProvider | None = None
        self._has_async_configs = False
        self._cleanup_registered = False
        self._shutdown_complete = False
        self._enable_correlation = False
        self._enable_sqlcommenter = False
        self._extractor: CorrelationExtractor | None = None

        for cfg in self._sqlspec.configs.values():
            state = self._config_state(cfg)
            self._config_states.append(state)

            if state.is_async:
                self._has_async_configs = True

            if state.enable_correlation_middleware and not self._enable_correlation:
                self._enable_correlation = True
                self._extractor = CorrelationExtractor(
                    primary_header=state.correlation_header,
                    additional_headers=state.correlation_headers,
                    auto_trace_headers=state.auto_trace_headers,
                )
            if (
                state.enable_sqlcommenter_middleware
                and state.config.statement_config.enable_sqlcommenter
                and not self._enable_sqlcommenter
            ):
                self._enable_sqlcommenter = True

        if app is not None:
            self.init_app(app)

    @property
    def sqlspec(self) -> SQLSpec:
        """Return the underlying SQLSpec registry."""
        return self._sqlspec

    @property
    def config(self) -> "list[DatabaseConfigProtocol[Any, Any, Any]]":
        """Return the registered database configurations."""
        return [state.config for state in self._config_states]

    def init_app(self, app: "Flask") -> None:
        """Initialize Flask application with SQLSpec.

        Validates configuration, creates portal if needed, creates pools,
        and registers hooks.

        Args:
            app: Flask application to initialize.

        Raises:
            ImproperConfigurationError: If extension already registered or keys not unique.
        """
        if "sqlspec" in app.extensions:
            msg = "SQLSpec extension already registered on this Flask application"
            raise ImproperConfigurationError(msg)

        self._ensure_unique_keys()

        if self._has_async_configs:
            self._portal = PortalProvider()
            self._portal.start()
            log_with_context(logger, logging.DEBUG, "extension.init", framework="flask", stage="portal_started")

        pools: dict[str, Any] = {}
        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                if config_state.is_async:
                    pool = self._call_portal(config_state, config_state.config.create_pool)
                else:
                    pool = config_state.config.create_pool()
                pools[config_state.session_key] = pool
                log_with_context(
                    logger, logging.DEBUG, "session.create", framework="flask", session_key=config_state.session_key
                )

        app.extensions["sqlspec"] = {"plugin": self, "pools": pools}

        if any(
            not state.disable_di
            or _has_tenant_strategy(state.config)
            or state.enable_correlation_middleware
            or (state.enable_sqlcommenter_middleware and state.config.statement_config.enable_sqlcommenter)
            for state in self._config_states
        ):
            app.before_request(self._before_request_handler)
            app.after_request(self._after_request_handler)
            app.teardown_appcontext(self._teardown_appcontext_handler)
            if any(_has_tenant_strategy(state.config) for state in self._config_states):
                from flask import appcontext_pushed

                appcontext_pushed.connect(_rebind_streamed_tenant, app, weak=False)

        self._register_shutdown_hook()

        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="flask",
            stage="configured",
            config_count=len(self._config_states),
            async_enabled=self._has_async_configs,
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
                    state.read_session_key,
                    state.read_connection_key,
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
        tenant_id: "str | UUID | TenantContextPayload",
        *,
        variables: "Mapping[str, str] | None" = None,
        key: str | None = None,
    ) -> TenantContextPayload:
        """Store an authorized tenant receipt on ``flask.g`` for the active request."""
        return set_request_tenant(tenant_id, variables=variables, key=key)

    def get_session(self, key: Any = None) -> Any:
        """Get or create database session for current request.

        Sessions are cached per request for consistency, and the request connection
        is checked out on first access. An asynchronous configuration returns a
        blocking session whose calls run on the plugin's portal event loop, so a
        synchronous view calls ``session.select(...)`` without ``await``.

        Args:
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Database session (driver instance).
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        self._ensure_request_connection(config_state)
        session = get_or_create_session(config_state, self._portal.portal if self._portal else None)
        if isinstance(session, PortalSession):
            session._routing_bind_key = config_state.config.bind_key
        return session

    def get_connection(self, key: Any = None) -> Any:
        """Get database connection for current request, acquiring deferred tenant leases on demand.

        Args:
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Raw database connection.
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        return self._ensure_request_connection(config_state)

    def get_read_session(self, key: Any = None) -> Any:
        """Return the request's read session, bridging async drivers through the portal."""
        return self._get_read_routing_resource(key, "session")

    def get_read_connection(self, key: Any = None) -> Any:
        """Return the request-owned raw read connection."""
        return self._get_read_routing_resource(key, "connection")

    def get_routing_session(self, key: Any = None) -> Any:
        """Return the request's lazy automatic routing facade."""
        return self._get_read_routing_resource(key, "routing")

    def shutdown(self) -> None:
        """Dispose connection pools and stop async portal."""

        if self._shutdown_complete:
            return

        self._shutdown_complete = True

        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                self._close_pool_state(config_state)

        if self._portal is not None:
            try:
                self._portal.stop()
            except Exception as exc:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "extension.init",
                    framework="flask",
                    stage="shutdown",
                    status="failed",
                    error_type=type(exc).__name__,
                )
            finally:
                self._portal = None

    def _call_portal(
        self,
        state: FlaskConfigState,
        callback: Any,
        *args: Any,
        tenant_override: TenantContextPayload | Literal["ambient"] | None = "ambient",
        **kwargs: Any,
    ) -> Any:
        if self._portal is None:
            msg = "Async database configuration requires a running portal"
            raise ImproperConfigurationError(msg)
        timeout = kwargs.pop("timeout", 300.0)

        async def invoke() -> Any:
            result = callback(*args, **kwargs)
            return await result if inspect.isawaitable(result) else result

        return call_portal_with_context(
            self._portal.portal,
            invoke,
            tenant_override=tenant_override,
            routing_bind_key=state.config.bind_key,
            timeout=timeout,
        )

    def _get_read_routing_resource(self, key: Any, kind: str) -> Any:
        from flask import g

        state = self._config_states[0] if key is None else self._config_state_by_key(key)
        resource_key = (
            state.read_session_key
            if kind == "session"
            else state.read_connection_key
            if kind == "connection"
            else state.routing_session_key
        )
        if resource_key is None:
            msg = "routing_session_key must be configured to use routing sessions"
            raise ImproperConfigurationError(msg)
        current = get_context_value(g, resource_key, None)
        if current is not None:
            return current
        captured_payload = (
            TenantContext.require(state.config) if _has_tenant_strategy(state.config) else _current_tenant_payload()
        )
        context: Any
        with TenantContext.bind(captured_payload):
            if kind == "routing":
                context = state.config.provide_routing_session()
            elif kind == "session":
                context = state.config.provide_read_session()
            else:
                context = state.config.provide_read_connection()
            if state.is_async:
                assert self._portal is not None
                current = self._call_portal(state, context.__aenter__, tenant_override=captured_payload)
                if kind != "connection":
                    current = PortalSession(current, self._portal.portal, routing_bind_key=state.config.bind_key)
            else:
                current = context.__enter__()
        contexts = get_context_value(g, _RESOURCE_CONTEXTS_KEY, None)
        if contexts is None:
            contexts = []
            set_context_value(g, _RESOURCE_CONTEXTS_KEY, contexts)
        contexts.append((state, resource_key, context, current, captured_payload))
        set_context_value(g, resource_key, current)
        return current

    def _config_state(self, config: Any) -> FlaskConfigState:
        """Create configuration state from database config.

        Args:
            config: Database configuration instance.

        Returns:
            FlaskConfigState instance.
        """
        flask_config = config.extension_config.get("flask", {})

        session_key = flask_config.get("session_key", DEFAULT_SESSION_KEY)
        connection_key = flask_config.get("connection_key", f"sqlspec_connection_{session_key}")
        commit_mode = flask_config.get("commit_mode", DEFAULT_COMMIT_MODE)
        extra_commit_statuses = flask_config.get("extra_commit_statuses")
        extra_rollback_statuses = flask_config.get("extra_rollback_statuses")
        disable_di = flask_config.get("disable_di", False)

        enable_correlation = flask_config.get("enable_correlation_middleware", True)
        correlation_header = flask_config.get("correlation_header", "x-request-id")
        correlation_headers = flask_config.get("correlation_headers")
        if correlation_headers is not None:
            correlation_headers = tuple(correlation_headers)
        auto_trace_headers = flask_config.get("auto_trace_headers", True)
        enable_sqlcommenter = flask_config.get("enable_sqlcommenter_middleware", True)

        is_async = isinstance(config, (AsyncDatabaseConfig, NoPoolAsyncConfig))
        if flask_config.get("tenant_extractor") is not None and not _has_tenant_strategy(config):
            msg = "tenant_extractor requires driver_features['tenant_strategy'] on the same configuration"
            raise ImproperConfigurationError(msg)

        return FlaskConfigState(
            config=config,
            connection_key=connection_key,
            session_key=session_key,
            commit_mode=commit_mode,
            extra_commit_statuses=extra_commit_statuses,
            extra_rollback_statuses=extra_rollback_statuses,
            is_async=is_async,
            tenant_extractor=flask_config.get("tenant_extractor"),
            disable_di=disable_di,
            enable_correlation_middleware=enable_correlation,
            correlation_header=correlation_header,
            correlation_headers=correlation_headers,
            auto_trace_headers=auto_trace_headers,
            enable_sqlcommenter_middleware=enable_sqlcommenter,
            read_session_key=flask_config.get(
                "read_session_key", "read_session" if session_key == DEFAULT_SESSION_KEY else f"{session_key}_read"
            ),
            read_connection_key=flask_config.get(
                "read_connection_key",
                "read_connection"
                if connection_key == f"sqlspec_connection_{DEFAULT_SESSION_KEY}"
                else f"{connection_key}_read",
            ),
            routing_session_key=flask_config.get("routing_session_key"),
            default_routing_mode=flask_config.get("default_routing_mode", "auto"),
            enable_routing_middleware=flask_config.get("enable_routing_middleware", True),
            routing_header=flask_config.get("routing_header"),
            write_pin_cookie=flask_config.get("write_pin_cookie"),
            routing_secret=flask_config.get("routing_secret"),
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
                    state.session_key,
                    state.read_connection_key,
                    state.read_session_key,
                    state.routing_session_key,
                )
                if key is not None
            ]
            duplicates = all_keys.intersection(keys)
            if duplicates or len(keys) != len(set(keys)):
                msg = f"Duplicate state keys found: {duplicates}. Use unique session_key values."
                raise ImproperConfigurationError(msg)
            all_keys.update(keys)

    def _register_shutdown_hook(self) -> None:
        """Register shutdown hook for pool and portal cleanup."""

        if self._cleanup_registered:
            return

        atexit.register(self.shutdown)
        self._cleanup_registered = True

    def _before_request_handler(self) -> None:
        """Prepare request-scoped state before the view runs.

        Binds the lazy tenant resolver and the correlation and SQLCommenter contexts. Database
        connections are checked out on first session or connection access, not here.

        Note:
            Imports flask components dynamically at runtime to avoid hard dependency.
        """
        from flask import g, request

        routing_states = [state for state in self._config_states if state.enable_routing_middleware]
        if routing_states and not has_context_value(g, _ROUTING_SCOPE_KEY):
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
            set_context_value(g, _ROUTING_SCOPE_KEY, RoutingContext.set(payload))

        if self._enable_correlation and self._extractor is not None:
            correlation_id = self._extractor.extract(lambda h: request.headers.get(h))
            set_context_value(g, "correlation_id", correlation_id)
            CorrelationContext.set(correlation_id)

        if self._enable_sqlcommenter:
            attrs: dict[str, str] = {"route": request.path, "framework": "flask"}
            endpoint = request.endpoint
            if endpoint is not None:
                attrs["action"] = endpoint
            SQLCommenterContext.set(attrs)

        tenant_keys = tuple(state.config for state in self._config_states if _has_tenant_strategy(state.config))
        if tenant_keys and not has_context_value(g, _TENANT_SCOPE_KEY):
            binding = TenantRequestBinding(
                self._resolve_route_tenant,
                keys=tenant_keys,
                ambient=TenantContext.current_binding(),
                missing=_reject_missing_tenant,
            )
            set_context_value(g, _TENANT_BINDING_KEY, binding)
            set_context_value(g, _TENANT_SCOPE_KEY, TenantContext.bind_request(binding))

    def _after_request_handler(self, response: "Response") -> "Response":
        """Handle transaction after request based on response status.

        Note:
            Imports flask components dynamically at runtime to avoid hard dependency.

        Args:
            response: Flask response object.

        Returns:
            Response object with correlation ID header if enabled.
        """
        from flask import g

        binding = TenantContext.current_binding()
        if isinstance(binding, TenantRequestBinding) and has_context_value(g, _TENANT_SCOPE_KEY):
            binding.freeze()

        if self._enable_correlation:
            correlation_id = get_context_value(g, "correlation_id", None)
            if correlation_id:
                response.headers["X-Correlation-ID"] = correlation_id

        if response.is_streamed:
            entries: list[tuple[Any, ...]] = []
            for config_state in self._config_states:
                if config_state.disable_di:
                    continue
                connection = get_context_value(g, config_state.connection_key, None)
                if connection is None:
                    continue
                cache_key = f"sqlspec_session_cache_{config_state.session_key}"
                blocking_key = f"sqlspec_portal_session_{config_state.session_key}"
                ctx_key = f"{config_state.connection_key}_ctx"
                payload_key = f"{config_state.connection_key}_tenant_payload"
                session = pop_context_value(g, cache_key)
                pop_context_value(g, blocking_key)
                pop_context_value(g, config_state.connection_key)
                conn_ctx = pop_context_value(g, ctx_key)
                has_captured_payload = has_context_value(g, payload_key)
                captured_payload = pop_context_value(g, payload_key)
                entries.append((config_state, session, connection, conn_ctx, captured_payload, has_captured_payload))
            raw_contexts = pop_context_value(g, _RESOURCE_CONTEXTS_KEY) or []
            resource_contexts = list(reversed(raw_contexts))
            for _, resource_key, _, _, _ in resource_contexts:
                pop_context_value(g, resource_key)
            if entries or resource_contexts:
                release = _StreamedResponseRelease(
                    self, response.status_code, entries, resource_contexts=resource_contexts
                )
                response.response = release.track(response.response)
                response.call_on_close(release)
            return response

        for config_state in self._config_states:
            if config_state.disable_di:
                continue

            if config_state.commit_mode == "manual":
                continue

            cache_key = f"sqlspec_session_cache_{config_state.session_key}"
            session = get_context_value(g, cache_key, None)
            connection = get_context_value(g, config_state.connection_key, None)

            if session is None and connection is None:
                continue

            if config_state.should_commit(response.status_code):
                self._execute_commit(session, config_state, connection=connection)
            elif config_state.should_rollback(response.status_code):
                self._execute_rollback(session, config_state, connection=connection)

        for config_state in self._config_states:
            if config_state.routing_session_key is None or config_state.commit_mode == "manual":
                continue
            facade = get_context_value(g, config_state.routing_session_key, None)
            if facade is not None and facade.pinned_to_primary:
                if config_state.should_commit(response.status_code):
                    self._execute_commit(facade, config_state)
                elif config_state.should_rollback(response.status_code):
                    self._execute_rollback(facade, config_state)
        return response

    def _teardown_appcontext_handler(self, _exc: "BaseException | None" = None) -> None:
        """Release request connections and always restore the caller's tenant context."""
        from flask import g

        try:
            self._close_request_connections(_exc)
        finally:
            token = pop_context_value(g, _TENANT_SCOPE_KEY)
            if token is not None:
                TenantContext.reset(token)
            routing_token = pop_context_value(g, _ROUTING_SCOPE_KEY)
            if routing_token is not None:
                RoutingContext.reset(routing_token)

    def _close_request_connections(self, _exc: "BaseException | None" = None) -> None:
        """Clean up connections when request context ends.

        Rolls back uncommitted transactions when an unhandled exception occurred,
        closes all connections, cleans up g object, and clears correlation context.

        Note:
            Imports flask components dynamically at runtime to avoid hard dependency.

        Args:
            _exc: Exception that occurred (if any).
        """
        from flask import g

        if self._enable_correlation:
            CorrelationContext.clear()
            if has_context_value(g, "correlation_id"):
                pop_context_value(g, "correlation_id")

        if self._enable_sqlcommenter:
            SQLCommenterContext.set(None)

        exc_info = (type(_exc), _exc, _exc.__traceback__) if _exc is not None else (None, None, None)

        contexts = pop_context_value(g, _RESOURCE_CONTEXTS_KEY) or []
        resource_errors: list[BaseException] = []
        for config_state, resource_key, context, resource, captured_payload in reversed(contexts):
            try:
                if (
                    resource_key == config_state.routing_session_key
                    and _exc is not None
                    and config_state.commit_mode != "manual"
                    and resource.pinned_to_primary
                ):
                    self._execute_rollback(resource, config_state)
                if config_state.is_async:
                    assert self._portal is not None
                    self._call_portal(config_state, context.__aexit__, *exc_info, tenant_override=captured_payload)
                else:
                    with TenantContext.bind(captured_payload):
                        context.__exit__(*exc_info)
            except BaseException as error:
                resource_errors.append(error)
            finally:
                pop_context_value(g, resource_key)

        for config_state in self._config_states:
            if config_state.disable_di:
                continue

            cache_key = f"sqlspec_session_cache_{config_state.session_key}"
            blocking_key = f"sqlspec_portal_session_{config_state.session_key}"
            ctx_key = f"{config_state.connection_key}_ctx"
            payload_key = f"{config_state.connection_key}_tenant_payload"

            session = get_context_value(g, cache_key, None)
            connection = get_context_value(g, config_state.connection_key, None)
            conn_ctx = get_context_value(g, ctx_key, None)
            has_captured_payload = has_context_value(g, payload_key)
            captured_payload = pop_context_value(g, payload_key)

            try:
                if (
                    _exc is not None
                    and config_state.commit_mode != "manual"
                    and (session is not None or connection is not None)
                ):
                    self._execute_rollback(session, config_state, connection=connection)
            finally:
                try:
                    if session is not None or connection is not None or conn_ctx is not None:
                        self._release_config_connection(
                            config_state,
                            session,
                            connection,
                            conn_ctx,
                            captured_payload,
                            has_captured_payload,
                            exc_info,
                        )
                finally:
                    pop_context_value(g, config_state.connection_key)
                    pop_context_value(g, ctx_key)
                    pop_context_value(g, cache_key)
                    pop_context_value(g, blocking_key)

        if resource_errors:
            raise resource_errors[0]

    def _release_config_connection(
        self,
        config_state: FlaskConfigState,
        session: Any,
        connection: Any,
        conn_ctx: Any,
        captured_payload: "TenantContextPayload | None",
        has_captured_payload: bool,
        exc_info: "tuple[Any, Any, Any]",
    ) -> None:
        """Emit session end, detach the session, and exit the request connection context."""
        try:
            if session is not None:
                try:
                    runtime = getattr(session, "observability", None)
                    if runtime is not None and not runtime.is_idle:
                        runtime.emit_session_end_sync(session)
                finally:
                    detach = getattr(session, "_detach_connection", None)
                    if detach is not None:
                        detach()
        finally:
            if connection is not None or conn_ctx is not None:
                try:
                    if conn_ctx is not None:
                        if config_state.is_async:
                            if has_captured_payload:
                                self._call_portal(
                                    config_state, _exit_tenant_connection, conn_ctx, captured_payload, exc_info
                                )
                            else:
                                self._call_portal(config_state, conn_ctx.__aexit__, *exc_info)
                        elif has_captured_payload:
                            with TenantContext.bind(captured_payload):
                                conn_ctx.__exit__(*exc_info)
                        else:
                            conn_ctx.__exit__(*exc_info)
                    elif connection is not None:
                        if config_state.is_async:
                            self._call_portal(config_state, connection.close)
                        else:
                            connection.close()
                except Exception as exc:
                    log_with_context(
                        logger,
                        logging.ERROR,
                        "session.close",
                        framework="flask",
                        session_key=config_state.session_key,
                        operation="connection",
                        status="failed",
                        error_type=type(exc).__name__,
                    )

    def _invoke_extractor(self, config_state: FlaskConfigState, request: Any) -> TenantContextPayload | None:
        extractor = config_state.tenant_extractor
        if extractor is None:
            return None
        raw = extractor(request) if config_state.extractor_takes_request else extractor()
        if inspect.isawaitable(raw):
            close = getattr(raw, "close", None)
            if callable(close):
                close()
            msg = "Flask tenant_extractor must be a synchronous callable"
            raise ImproperConfigurationError(msg)
        return coerce_tenant_payload(raw)

    def _tenant_state_for(self, key: Any) -> Any:
        """Return the tenant-enabled state for a configuration key, built once per plugin."""
        states = self._tenant_states
        if states is None:
            tenant_states = [state for state in self._config_states if _has_tenant_strategy(state.config)]
            states = {id(state.config): state for state in tenant_states}
            states[0] = tenant_states[0]
            self._tenant_states = states
        return states.get(id(key), states[0])

    def _resolve_route_tenant(self, key: Any) -> TenantContextPayload | None:
        """Resolve the route bypass marker, request receipt, or extractor for a configuration."""
        from flask import current_app, request

        config_state = self._tenant_state_for(key)
        endpoint = request.endpoint
        if endpoint is not None and is_tenant_bypass_route(current_app.view_functions.get(endpoint)):
            return TenantContextPayload.bypass()
        receipt = get_request_tenant(config_state)
        if receipt is not None:
            return coerce_tenant_payload(receipt)
        if config_state.tenant_extractor is not None:
            return self._invoke_extractor(config_state, request)
        return None

    def _ensure_request_connection(self, config_state: FlaskConfigState) -> Any:
        from flask import current_app, g

        if has_context_value(g, config_state.connection_key) or config_state.disable_di:
            return get_context_value(g, config_state.connection_key, None)
        if not _has_tenant_strategy(config_state.config):
            return self._acquire_config_connection(current_app, g, config_state, None, bind_payload=False)
        payload = TenantContext.require(config_state.config)
        return self._acquire_config_connection(current_app, g, config_state, payload, bind_payload=True)

    def _acquire_config_connection(
        self,
        app: "Flask",
        g_obj: Any,
        config_state: FlaskConfigState,
        payload: TenantContextPayload | None,
        *,
        bind_payload: bool,
    ) -> Any:
        payload_key = f"{config_state.connection_key}_tenant_payload"
        set_context_value(g_obj, payload_key, payload if bind_payload else _current_tenant_payload())
        try:
            conn_ctx: Any = None
            if config_state.config.supports_connection_pooling:
                pool = app.extensions["sqlspec"]["pools"][config_state.session_key]
                with TenantContext.bind(payload) if bind_payload else _null_bind():
                    conn_ctx = config_state.config.provide_connection(pool)
            elif _has_tenant_strategy(config_state.config):
                with TenantContext.bind(payload) if bind_payload else _null_bind():
                    conn_ctx = config_state.config.provide_connection()

            if conn_ctx is not None:
                if config_state.is_async:
                    assert self._portal is not None
                    if bind_payload:
                        connection = self._call_portal(config_state, _enter_tenant_connection, conn_ctx, payload)
                    else:
                        connection = self._call_portal(config_state, conn_ctx.__aenter__)
                elif bind_payload:
                    with TenantContext.bind(payload):
                        connection = conn_ctx.__enter__()
                else:
                    connection = conn_ctx.__enter__()
                set_context_value(g_obj, f"{config_state.connection_key}_ctx", conn_ctx)
            elif config_state.is_async:
                connection = self._call_portal(config_state, config_state.config.create_connection)
            else:
                connection = config_state.config.create_connection()
        except BaseException:
            pop_context_value(g_obj, payload_key)
            raise

        set_context_value(g_obj, config_state.connection_key, connection)
        return connection

    def _config_state_by_key(self, key: Any) -> FlaskConfigState:
        """Get config state by session/connection/bind key, unique type, or instance.

        Args:
            key: Configuration identifier to look up.

        Returns:
            FlaskConfigState for the identifier.

        Raises:
            ImproperConfigurationError: If identifier is not found or ambiguous.
        """
        if isinstance(key, str):
            for state in self._config_states:
                if state.session_key == key:
                    return state
            for state in self._config_states:
                if key in {
                    state.connection_key,
                    state.read_session_key,
                    state.read_connection_key,
                    state.routing_session_key,
                    getattr(state.config, "bind_key", None),
                }:
                    return state
            msg = f"No configuration found for key: {key}"
            raise ImproperConfigurationError(msg)
        if isinstance(key, type):
            matches = [state for state in self._config_states if type(state.config) is key]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                msg = f"Multiple configurations share type '{key.__name__}'; specify a session_key or bind_key."
                raise ImproperConfigurationError(msg)
            msg = f"No configuration found for type: {key.__name__}"
            raise ImproperConfigurationError(msg)
        for state in self._config_states:
            if state.config is key:
                return state
        msg = f"No configuration found for key: {key!r}"
        raise ImproperConfigurationError(msg)

    def _close_pool_state(self, config_state: FlaskConfigState) -> None:
        """Close pool associated with configuration state."""

        try:
            if config_state.is_async:
                if self._portal is None:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "session.close",
                        framework="flask",
                        session_key=config_state.session_key,
                        operation="pool",
                        status="skipped",
                        reason="portal_not_initialized",
                    )
                    return
                _ = self._call_portal(config_state, config_state.config.close_pool)
            else:
                config_state.config.close_pool()
            log_with_context(
                logger,
                logging.DEBUG,
                "session.close",
                framework="flask",
                session_key=config_state.session_key,
                operation="pool",
                status="complete",
            )
        except Exception as exc:
            log_with_context(
                logger,
                logging.ERROR,
                "session.close",
                framework="flask",
                session_key=config_state.session_key,
                operation="pool",
                status="failed",
                error_type=type(exc).__name__,
            )

    def _execute_commit(self, session: Any, config_state: FlaskConfigState, connection: Any = None) -> None:
        """Execute commit on session or connection.

        Args:
            session: Database session.
            config_state: Configuration state.
            connection: Optional raw connection when request context has already exited.
        """
        if (
            session is not None
            and getattr(session, "_auto_begin_on_write", False) is True
            and not session._connection_in_transaction()
        ):
            return
        try:
            if session is not None and hasattr(session, "commit"):
                target = session
            elif connection is not None:
                target = connection
            else:
                target = self.get_connection(config_state.session_key)
            if config_state.is_async:
                self._call_portal(config_state, target.commit)
            else:
                target.commit()
        except BaseException as exc:
            log_with_context(
                logger,
                logging.ERROR,
                "session.close",
                framework="flask",
                session_key=config_state.session_key,
                operation="commit",
                status="failed",
                error_type=type(exc).__name__,
            )
            self._execute_rollback(session, config_state, connection=connection)
            raise

    def _execute_rollback(self, session: Any, config_state: FlaskConfigState, connection: Any = None) -> None:
        """Execute rollback on session or connection.

        Args:
            session: Database session.
            config_state: Configuration state.
            connection: Optional raw connection when request context has already exited.
        """
        if (
            session is not None
            and getattr(session, "_auto_begin_on_write", False) is True
            and not session._connection_in_transaction()
        ):
            return
        try:
            if session is not None and hasattr(session, "rollback"):
                target = session
            elif connection is not None:
                target = connection
            else:
                target = self.get_connection(config_state.session_key)
            if config_state.is_async:
                self._call_portal(config_state, target.rollback)
            else:
                target.rollback()
        except Exception as exc:
            log_with_context(
                logger,
                logging.DEBUG,
                "session.close",
                framework="flask",
                session_key=config_state.session_key,
                operation="rollback",
                status="failed",
                error_type=type(exc).__name__,
            )


class _StreamedResponseRelease:
    """Finalize and release request connections when a streamed response closes."""

    __slots__ = ("_completed", "_entries", "_plugin", "_resource_contexts", "_status_code")

    def __init__(
        self,
        plugin: SQLSpecPlugin,
        status_code: int,
        entries: "list[tuple[Any, ...]]",
        *,
        resource_contexts: "list[tuple[Any, ...]] | None" = None,
    ) -> None:
        self._plugin = plugin
        self._status_code = status_code
        self._entries = entries
        self._resource_contexts = resource_contexts or []
        self._completed = False

    def track(self, iterable: "Iterable[Any]") -> "Iterator[Any]":
        """Yield every item from the streamed response body and record full completion."""
        close = getattr(iterable, "close", None)
        try:
            yield from iterable
            self._completed = True
        finally:
            if callable(close):
                close()

    def __call__(self) -> None:
        """Finalize transactions and release connections once when the response closes."""
        if not self._entries and not self._resource_contexts:
            return
        entries = list(self._entries)
        self._entries.clear()
        resource_contexts = list(self._resource_contexts)
        self._resource_contexts.clear()
        primary_exc: BaseException | None = None
        for config_state, resource_key, context, resource, captured_payload in resource_contexts:
            try:
                try:
                    if (
                        resource_key == config_state.routing_session_key
                        and config_state.commit_mode != "manual"
                        and resource.pinned_to_primary
                    ):
                        if self._completed and config_state.should_commit(self._status_code):
                            self._plugin._execute_commit(resource, config_state)
                        else:
                            self._plugin._execute_rollback(resource, config_state)
                finally:
                    if config_state.is_async:
                        self._plugin._call_portal(
                            config_state, context.__aexit__, None, None, None, tenant_override=captured_payload
                        )
                    else:
                        with TenantContext.bind(captured_payload):
                            context.__exit__(None, None, None)
            except BaseException as exc:
                if primary_exc is None or (isinstance(primary_exc, Exception) and not isinstance(exc, Exception)):
                    primary_exc = exc
        for config_state, session, connection, conn_ctx, captured_payload, has_captured_payload in entries:
            try:
                try:
                    if config_state.commit_mode != "manual" and (session is not None or connection is not None):
                        if self._completed and config_state.should_commit(self._status_code):
                            self._plugin._execute_commit(session, config_state, connection=connection)
                        else:
                            self._plugin._execute_rollback(session, config_state, connection=connection)
                finally:
                    self._plugin._release_config_connection(
                        config_state,
                        session,
                        connection,
                        conn_ctx,
                        captured_payload,
                        has_captured_payload,
                        (None, None, None),
                    )
            except BaseException as exc:
                if primary_exc is None or (isinstance(primary_exc, Exception) and not isinstance(exc, Exception)):
                    primary_exc = exc
        if primary_exc is not None:
            raise primary_exc


def _rebind_streamed_tenant(_app: Any, **_: Any) -> None:
    """Rebind the frozen request binding when ``stream_with_context`` pushes the context again."""
    from flask import g

    binding = g.get(_TENANT_BINDING_KEY, None)
    if isinstance(binding, TenantRequestBinding) and not has_context_value(g, _TENANT_SCOPE_KEY):
        set_context_value(g, _TENANT_SCOPE_KEY, TenantContext.bind_request(binding))


def _reject_missing_tenant(_key: Any) -> NoReturn:
    """Raise the Flask response for a request without an authorized tenant."""
    from flask import g
    from werkzeug.exceptions import Forbidden, Unauthorized

    user = g.get("user", None) or g.get("_login_user", None)
    if user is None or not bool(getattr(user, "is_authenticated", True)):
        raise Unauthorized(description="Authentication required")
    raise Forbidden(description="An authorized tenant is required")


def _has_tenant_strategy(config: Any) -> bool:
    features = getattr(config, "driver_features", None)
    return isinstance(features, Mapping) and features.get("tenant_strategy") not in (None, False)


class _NullContext:
    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, *args: object) -> None:
        return None


_NULL_CONTEXT: Final = _NullContext()


def _null_bind() -> _NullContext:
    return _NULL_CONTEXT


async def _enter_tenant_connection(conn_ctx: Any, payload: TenantContextPayload | None) -> Any:
    with TenantContext.bind(payload):
        return await conn_ctx.__aenter__()


async def _exit_tenant_connection(
    conn_ctx: Any, payload: TenantContextPayload | None, exc_info: tuple[Any, ...]
) -> Any:
    with TenantContext.bind(payload):
        return await conn_ctx.__aexit__(*exc_info)


def _current_tenant_payload() -> TenantContextPayload | None:
    binding = TenantContext.current_binding()
    return binding if isinstance(binding, TenantContextPayload) else None
