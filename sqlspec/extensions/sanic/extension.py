"""Sanic extension for SQLSpec database session and lifecycle management."""

import asyncio
import contextlib
import logging
from contextvars import ContextVar
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.base import SQLSpec
from sqlspec.core import CorrelationExtractor
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic._state import SanicConfigState
from sqlspec.extensions.sanic._utils import (
    get_context_value,
    get_or_create_session,
    has_context_value,
    pop_context_value,
    set_context_value,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_
from sqlspec.utils.tenant import TenantContext, coerce_tenant_payload
from sqlspec.utils.type_guards import has_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from sanic import Sanic

__all__ = ("SQLSpecPlugin",)

logger = get_logger("sqlspec.extensions.sanic")

_REQUEST_EXCEPTION_KEY: Final = "sqlspec_request_exception"
_ACTIVE_REQUEST_KEY: Final = "_sqlspec_active_request"
_CURRENT_REQUEST: ContextVar[Any | None] = ContextVar("_sqlspec_sanic_active_request", default=None)
DEFAULT_COMMIT_MODE = "manual"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"


def _select_teardown_exception(current: BaseException | None, candidate: BaseException) -> BaseException:
    """Select the primary teardown exception to re-raise after completing all targets."""
    if current is None:
        return candidate
    if isinstance(current, Exception) and not isinstance(candidate, Exception):
        return candidate
    return current


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
        "_tenant_extractor",
    )

    def __init__(self, sqlspec: SQLSpec, app: "Sanic[Any, Any] | None" = None) -> None:
        """Initialize SQLSpec Sanic extension.

        Args:
            sqlspec: Pre-configured SQLSpec instance with registered configs.
            app: Optional Sanic application to initialize immediately.
        """
        self._sqlspec = sqlspec
        self._config_states: list[SanicConfigState] = []
        self._lifecycle_listeners_added = False
        self._request_middleware_added = False
        self._tenant_extractor: Callable[[Any], Any] | None = None

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

    def init_app(self, app: "Sanic[Any, Any]") -> None:
        """Initialize Sanic application with SQLSpec.

        Args:
            app: Sanic application instance.
        """
        self._ensure_unique_keys()
        self._configure_tenant_extractor()
        setattr(app.ctx, "sqlspec_plugin", self)
        self._add_lifecycle_listeners(app)
        self._add_request_middleware(app)

    def get_session(self, request: Any, key: "str | None" = None) -> Any:
        """Get or create database session for request.

        Args:
            request: Sanic request instance.
            key: Optional session key to retrieve a specific database session.

        Returns:
            Database session driver instance.
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        return get_or_create_session(request, config_state)

    def get_connection(self, request: Any, key: "str | None" = None) -> Any:
        """Get database connection from request context.

        Args:
            request: Sanic request instance.
            key: Optional session key to retrieve a specific database connection.

        Returns:
            Database connection object.
        """
        config_state = self._config_states[0] if key is None else self._config_state_by_key(key)
        return get_context_value(request.ctx, config_state.connection_key)

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
        """
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
        )

    def _configure_tenant_extractor(self) -> None:
        for state in self._config_states:
            if state.disable_di:
                continue
            extractor = state.tenant_extractor
            if state.config.driver_features.get("tenant_strategy") is not None and extractor is None:
                msg = f"Sanic session {state.session_key!r} requires tenant_extractor for tenant strategy"
                raise ImproperConfigurationError(msg)
            if extractor is None:
                continue
            if not callable(extractor):
                msg = "Sanic tenant_extractor must be callable"
                raise ImproperConfigurationError(msg)
            if self._tenant_extractor is not None and self._tenant_extractor is not extractor:
                msg = "Sanic configurations must share the same tenant_extractor"
                raise ImproperConfigurationError(msg)
            self._tenant_extractor = extractor

    async def _bind_tenant(self, request: Any) -> None:
        if self._tenant_extractor is None:
            return
        raw = self._tenant_extractor(request)
        if isawaitable(raw):
            raw = await raw
        payload = coerce_tenant_payload(raw)
        set_context_value(request.ctx, "sqlspec_tenant_token", TenantContext.set(payload))

    def _reset_tenant(self, request: Any) -> None:
        token = pop_context_value(request.ctx, "sqlspec_tenant_token")
        if token is not None:
            TenantContext.reset(token)

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
            if primary_exc is not None:
                raise primary_exc
        finally:
            try:
                self._restore_observability_contexts(target_request, None)
            finally:
                self._reset_tenant(target_request)

    async def _on_request(self, request: Any) -> None:
        """Acquire request-scoped connections.

        Args:
            request: Sanic request instance.
        """
        self._bind_active_request(request)
        self._set_observability_contexts(request)
        acquired_states: list[SanicConfigState] = []
        try:
            await self._bind_tenant(request)
            for config_state in self._config_states:
                if config_state.disable_di:
                    continue
                await self._acquire_request_connection(request, config_state)
                acquired_states.append(config_state)
        except BaseException as exc:
            try:
                if request is not None and hasattr(request, "ctx"):
                    set_context_value(request.ctx, _REQUEST_EXCEPTION_KEY, exc)
                primary_exc: BaseException | None = None
                for config_state in acquired_states[::-1]:
                    try:
                        await self._release_request_connection(request, config_state)
                    except BaseException as release_exc:
                        primary_exc = _select_teardown_exception(primary_exc, release_exc)
                if primary_exc is not None:
                    if not isinstance(exc, Exception) and isinstance(primary_exc, Exception):
                        raise
                    raise primary_exc
                raise
            finally:
                try:
                    self._restore_observability_contexts(request, None)
                    self._unbind_active_request(request)
                finally:
                    self._reset_tenant(request)

    async def _on_response(self, request: Any, response: Any) -> None:
        """Finalize request-scoped connections.

        Args:
            request: Sanic request instance.
            response: Sanic response instance.
        """
        primary_exc: BaseException | None = None
        try:
            for config_state in self._config_states[::-1]:
                if config_state.disable_di:
                    continue
                try:
                    await self._finalize_request_connection(request, response, config_state)
                except BaseException as exc:
                    primary_exc = _select_teardown_exception(primary_exc, exc)
            if primary_exc is not None:
                raise primary_exc
        finally:
            try:
                self._restore_observability_contexts(request, response)
                self._unbind_active_request(request)
            finally:
                self._reset_tenant(request)

    def _needs_request_middleware(self) -> bool:
        """Return whether this plugin should register request middleware.

        Returns:
            ``True`` when connection management or observability middleware is enabled.
        """
        return any(
            not state.disable_di or state.enable_correlation_middleware or self._state_enables_sqlcommenter(state)
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

    async def _acquire_request_connection(self, request: Any, config_state: SanicConfigState) -> None:
        """Acquire and store a connection for one config state.

        Args:
            request: Sanic request instance.
            config_state: Configuration state.
        """
        config = config_state.config

        if config.supports_connection_pooling:
            pool = get_context_value(request.app.ctx, config_state.pool_key)
            connection_manager = with_ensure_async_(config.provide_connection(pool))
            connection = await connection_manager.__aenter__()
            set_context_value(request.ctx, self._connection_manager_key(config_state), connection_manager)
        else:
            connection = await ensure_async_(config.create_connection)()

        set_context_value(request.ctx, config_state.connection_key, connection)

    async def _finalize_request_connection(self, request: Any, response: Any, config_state: SanicConfigState) -> None:
        """Commit or rollback, then release one request connection.

        Args:
            request: Sanic request instance.
            response: Sanic response instance.
            config_state: Configuration state.
        """
        connection = get_context_value(request.ctx, config_state.connection_key, None)
        if connection is None:
            if has_context_value(request.ctx, self._connection_manager_key(config_state)):
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
        target = session if session is not None and hasattr(session, "commit") else connection
        if hasattr(target, "commit"):
            await ensure_async_(target.commit)()

    async def _rollback_target(self, request: Any, connection: Any, config_state: SanicConfigState) -> None:
        """Roll back transaction using active session or connection."""
        session = get_context_value(request.ctx, f"{config_state.session_key}_instance", None)
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
        connection = pop_context_value(request.ctx, config_state.connection_key)
        pop_context_value(request.ctx, f"{config_state.session_key}_instance")
        connection_manager = pop_context_value(request.ctx, self._connection_manager_key(config_state))

        if connection is None and connection_manager is None:
            return

        exc = exc_override if exc_override is not None else get_context_value(request.ctx, _REQUEST_EXCEPTION_KEY, None)
        exit_args = (type(exc), exc, exc.__traceback__) if exc is not None else (None, None, None)

        if config_state.config.supports_connection_pooling:
            if connection_manager is not None:
                await connection_manager.__aexit__(*exit_args)
            return

        if connection is not None:
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

    def _ensure_unique_keys(self) -> None:
        """Validate that all context keys are unique across configs.

        Raises:
            ImproperConfigurationError: If duplicate keys are found.
        """
        all_keys: set[str] = set()
        for state in self._config_states:
            keys = {state.connection_key, state.pool_key, state.session_key}
            duplicates = all_keys & keys
            if duplicates:
                msg = f"Duplicate context keys found: {duplicates}"
                raise ImproperConfigurationError(msg)
            all_keys.update(keys)

    def _config_state_by_key(self, key: str) -> SanicConfigState:
        """Get configuration state by session key.

        Args:
            key: Session key to search for.

        Returns:
            Configuration state matching the key.

        Raises:
            ValueError: If no configuration is found with the specified key.
        """
        for state in self._config_states:
            if state.session_key == key:
                return state
        msg = f"No configuration found with session_key: {key}"
        raise ValueError(msg)
