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
from sqlspec.utils.tenant import (
    TenantContext,
    TenantContextPayload,
    TenantRequestBinding,
    coerce_tenant_payload,
    is_tenant_bypass_route,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from flask import Flask, Response

    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, NoPoolSyncConfig, SyncConfigT, SyncDatabaseConfig
    from sqlspec.loader import SQLFileLoader

__all__ = ("SQLSpecPlugin",)

logger = get_logger("sqlspec.extensions.flask")

DEFAULT_COMMIT_MODE: Literal["manual"] = "manual"
DEFAULT_SESSION_KEY = "db_session"
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
                    pool = self._portal.portal.call(config_state.config.create_pool)  # type: ignore[union-attr,arg-type]
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
                if name in {state.session_key, state.connection_key}:
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

        Sessions are cached per request for consistency, and tenant-enforced
        connections are acquired lazily on first access.

        Args:
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Database session (driver instance).
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        self._ensure_request_connection(config_state)
        return get_or_create_session(config_state, self._portal.portal if self._portal else None)

    def get_connection(self, key: Any = None) -> Any:
        """Get database connection for current request, acquiring deferred tenant leases on demand.

        Args:
            key: Optional session key, bind key, config type, or config instance.

        Returns:
            Raw database connection.
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        return self._ensure_request_connection(config_state)

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
        )

    def _ensure_unique_keys(self) -> None:
        """Validate that all state keys are unique across configs.

        Raises:
            ImproperConfigurationError: If duplicate keys found.
        """
        all_keys: set[str] = set()
        for state in self._config_states:
            keys = {state.connection_key, state.session_key}
            duplicates = all_keys & keys
            if duplicates:
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

        for config_state in self._config_states:
            if config_state.disable_di:
                continue

            if config_state.commit_mode == "manual":
                continue

            cache_key = f"sqlspec_session_cache_{config_state.session_key}"
            session = get_context_value(g, cache_key, None)

            if session is None and get_context_value(g, config_state.connection_key, None) is None:
                continue

            if config_state.should_commit(response.status_code):
                self._execute_commit(session, config_state)
            elif config_state.should_rollback(response.status_code):
                self._execute_rollback(session, config_state)

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

        for config_state in self._config_states:
            if config_state.disable_di:
                continue

            cache_key = f"sqlspec_session_cache_{config_state.session_key}"
            session = get_context_value(g, cache_key, None)
            connection = get_context_value(g, config_state.connection_key, None)
            ctx_key = f"{config_state.connection_key}_ctx"
            conn_ctx = get_context_value(g, ctx_key, None)

            if (
                _exc is not None
                and config_state.commit_mode != "manual"
                and (session is not None or connection is not None)
            ):
                self._execute_rollback(session, config_state)

            payload_key = f"{config_state.connection_key}_tenant_payload"
            has_captured_payload = has_context_value(g, payload_key)
            captured_payload = pop_context_value(g, payload_key)

            if connection is not None:
                try:
                    if conn_ctx is not None:
                        if config_state.is_async:
                            if has_captured_payload:
                                self._portal.portal.call(  # type: ignore[union-attr]
                                    _exit_tenant_connection, conn_ctx, captured_payload, exc_info
                                )
                            else:
                                self._portal.portal.call(conn_ctx.__aexit__, *exc_info)  # type: ignore[union-attr]
                        elif has_captured_payload:
                            with TenantContext.bind(captured_payload):
                                conn_ctx.__exit__(*exc_info)
                        else:
                            conn_ctx.__exit__(*exc_info)
                    elif config_state.is_async:
                        self._portal.portal.call(connection.close)  # type: ignore[union-attr]
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

                if has_context_value(g, config_state.connection_key):
                    pop_context_value(g, config_state.connection_key)
                if has_context_value(g, ctx_key):
                    pop_context_value(g, ctx_key)

            if has_context_value(g, cache_key):
                pop_context_value(g, cache_key)

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
        if bind_payload:
            set_context_value(g_obj, payload_key, payload)
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
                        connection = self._portal.portal.call(_enter_tenant_connection, conn_ctx, payload)
                    else:
                        connection = self._portal.portal.call(conn_ctx.__aenter__)
                elif bind_payload:
                    with TenantContext.bind(payload):
                        connection = conn_ctx.__enter__()
                else:
                    connection = conn_ctx.__enter__()
                set_context_value(g_obj, f"{config_state.connection_key}_ctx", conn_ctx)
            elif config_state.is_async:
                connection = self._portal.portal.call(config_state.config.create_connection)  # type: ignore[union-attr,arg-type]
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
                if key in {state.connection_key, getattr(state.config, "bind_key", None)}:
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
                _ = self._portal.portal.call(config_state.config.close_pool)  # type: ignore[arg-type]
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

    def _execute_commit(self, session: Any, config_state: FlaskConfigState) -> None:
        """Execute commit on session or connection.

        Args:
            session: Database session.
            config_state: Configuration state.
        """
        try:
            target = (
                session
                if session is not None and hasattr(session, "commit")
                else self.get_connection(config_state.session_key)
            )
            if config_state.is_async:
                self._portal.portal.call(target.commit)  # type: ignore[union-attr]
            else:
                target.commit()
        except Exception as exc:
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
            self._execute_rollback(session, config_state)
            raise

    def _execute_rollback(self, session: Any, config_state: FlaskConfigState) -> None:
        """Execute rollback on session or connection.

        Args:
            session: Database session.
            config_state: Configuration state.
        """
        try:
            target = (
                session
                if session is not None and hasattr(session, "rollback")
                else self.get_connection(config_state.session_key)
            )
            if config_state.is_async:
                self._portal.portal.call(target.rollback)  # type: ignore[union-attr]
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
