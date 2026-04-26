import logging
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from sanic import Sanic

__all__ = ("SQLSpecPlugin",)

logger = get_logger("sqlspec.extensions.sanic")

DEFAULT_COMMIT_MODE = "manual"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"
HTTP_200_OK = 200
HTTP_300_MULTIPLE_CHOICES = 300
HTTP_400_BAD_REQUEST = 400


class SQLSpecPlugin:
    """SQLSpec integration for Sanic applications.

    Provides Sanic-native configuration parsing and request helper methods.
    Runtime listener and middleware behavior is registered by ``init_app``.

    Example:
        from sanic import Sanic
        from sqlspec import SQLSpec
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.extensions.sanic import SQLSpecPlugin

        sqlspec = SQLSpec()
        sqlspec.add_config(
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/mydb"},
                extension_config={
                    "sanic": {
                        "commit_mode": "autocommit",
                        "session_key": "db",
                    }
                },
            )
        )

        app = Sanic("app")
        db_ext = SQLSpecPlugin(sqlspec, app)
    """

    __slots__ = ("_config_states", "_lifecycle_listeners_added", "_request_middleware_added", "_sqlspec")

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

        for cfg in self._sqlspec.configs.values():
            settings = self._extract_sanic_settings(cfg)
            state = self._create_config_state(cfg, settings)
            self._config_states.append(state)

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

    def _extract_sanic_settings(self, config: Any) -> "dict[str, Any]":
        """Extract Sanic settings from config.extension_config.

        Args:
            config: Database configuration instance.

        Returns:
            Dictionary of Sanic-specific settings.
        """
        sanic_config = config.extension_config.get("sanic", {})

        connection_key = sanic_config.get("connection_key", DEFAULT_CONNECTION_KEY)
        pool_key = sanic_config.get("pool_key", DEFAULT_POOL_KEY)
        session_key = sanic_config.get("session_key", DEFAULT_SESSION_KEY)
        commit_mode = sanic_config.get("commit_mode", DEFAULT_COMMIT_MODE)

        if not config.supports_connection_pooling and pool_key == DEFAULT_POOL_KEY:
            pool_key = f"_{DEFAULT_POOL_KEY}_{id(config)}"

        correlation_headers = sanic_config.get("correlation_headers")
        if correlation_headers is not None:
            correlation_headers = tuple(correlation_headers)

        return {
            "connection_key": connection_key,
            "pool_key": pool_key,
            "session_key": session_key,
            "commit_mode": commit_mode,
            "extra_commit_statuses": sanic_config.get("extra_commit_statuses"),
            "extra_rollback_statuses": sanic_config.get("extra_rollback_statuses"),
            "disable_di": sanic_config.get("disable_di", False),
            "enable_correlation_middleware": sanic_config.get("enable_correlation_middleware", False),
            "correlation_header": sanic_config.get("correlation_header", "x-request-id"),
            "correlation_headers": correlation_headers,
            "auto_trace_headers": sanic_config.get("auto_trace_headers", True),
            "enable_sqlcommenter_middleware": sanic_config.get("enable_sqlcommenter_middleware", True),
            "sqlcommenter_framework": sanic_config.get("sqlcommenter_framework", "sanic"),
        }

    def _create_config_state(self, config: Any, settings: "dict[str, Any]") -> SanicConfigState:
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
            enable_correlation_middleware=settings["enable_correlation_middleware"],
            correlation_header=settings["correlation_header"],
            correlation_headers=settings["correlation_headers"],
            auto_trace_headers=settings["auto_trace_headers"],
            enable_sqlcommenter_middleware=settings["enable_sqlcommenter_middleware"],
            sqlcommenter_framework=settings["sqlcommenter_framework"],
        )

    def init_app(self, app: "Sanic[Any, Any]") -> None:
        """Initialize Sanic application with SQLSpec.

        Args:
            app: Sanic application instance.
        """
        self._validate_unique_keys()
        setattr(app.ctx, "sqlspec_plugin", self)
        self._add_lifecycle_listeners(app)
        self._add_request_middleware(app)

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
        app.on_response(self._on_response)
        self._request_middleware_added = True

    async def _on_request(self, request: Any) -> None:
        """Acquire request-scoped connections.

        Args:
            request: Sanic request instance.
        """
        self._set_observability_contexts(request)
        acquired_states: list[SanicConfigState] = []
        try:
            for config_state in self._config_states:
                if config_state.disable_di:
                    continue
                await self._acquire_request_connection(request, config_state)
                acquired_states.append(config_state)
        except Exception:
            for config_state in reversed(acquired_states):
                await self._release_request_connection(request, config_state)
            self._restore_observability_contexts(request, None)
            raise

    async def _on_response(self, request: Any, response: Any) -> None:
        """Finalize request-scoped connections.

        Args:
            request: Sanic request instance.
            response: Sanic response instance.
        """
        try:
            for config_state in reversed(self._config_states):
                if config_state.disable_di:
                    continue
                await self._finalize_request_connection(request, response, config_state)
        finally:
            self._restore_observability_contexts(request, response)

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
        config_state = self._first_correlation_state()
        if config_state is None:
            return

        extractor = CorrelationExtractor(
            primary_header=config_state.correlation_header,
            additional_headers=config_state.correlation_headers,
            auto_trace_headers=config_state.auto_trace_headers,
        )
        correlation_id = extractor.extract(lambda header: request.headers.get(header))
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
        if endpoint is not None and hasattr(endpoint, "__name__"):
            return endpoint.__name__

        route = getattr(request, "route", None)
        handler = getattr(route, "handler", None)
        if handler is not None and hasattr(handler, "__name__"):
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
            connection_manager = with_ensure_async_(config.provide_connection(pool))  # type: ignore[union-attr]
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
            return

        try:
            if config_state.commit_mode != "manual":
                status_code = self._response_status_code(response)
                if self._should_commit(config_state, status_code):
                    await ensure_async_(connection.commit)()
                else:
                    await ensure_async_(connection.rollback)()
        finally:
            await self._release_request_connection(request, config_state)

    async def _release_request_connection(self, request: Any, config_state: SanicConfigState) -> None:
        """Release and clear one request connection.

        Args:
            request: Sanic request instance.
            config_state: Configuration state.
        """
        connection = pop_context_value(request.ctx, config_state.connection_key)
        pop_context_value(request.ctx, f"{config_state.session_key}_instance")

        if connection is None:
            pop_context_value(request.ctx, self._connection_manager_key(config_state))
            return

        if config_state.config.supports_connection_pooling:
            connection_manager = pop_context_value(request.ctx, self._connection_manager_key(config_state))
            if connection_manager is not None:
                await connection_manager.__aexit__(None, None, None)
            return

        await ensure_async_(connection.close)()

    def _should_commit(self, config_state: SanicConfigState, status_code: int) -> bool:
        """Determine whether a response status should commit.

        Args:
            config_state: Configuration state.
            status_code: HTTP response status.

        Returns:
            ``True`` when the transaction should commit.
        """
        extra_commit = config_state.extra_commit_statuses or set()
        extra_rollback = config_state.extra_rollback_statuses or set()

        if status_code in extra_commit:
            return True
        if status_code in extra_rollback:
            return False

        if HTTP_200_OK <= status_code < HTTP_300_MULTIPLE_CHOICES:
            return True
        return bool(
            config_state.commit_mode == "autocommit_include_redirect"
            and HTTP_300_MULTIPLE_CHOICES <= status_code < HTTP_400_BAD_REQUEST
        )

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
            except Exception:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "pool.close.failed",
                    framework="sanic",
                    pool_key=config_state.pool_key,
                    session_key=config_state.session_key,
                )
                raise
            pop_context_value(app.ctx, config_state.pool_key)
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.close",
                framework="sanic",
                pool_key=config_state.pool_key,
                session_key=config_state.session_key,
            )

    def _validate_unique_keys(self) -> None:
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

    def get_session(self, request: Any, key: "str | None" = None) -> Any:
        """Get or create database session for request.

        Args:
            request: Sanic request instance.
            key: Optional session key to retrieve a specific database session.

        Returns:
            Database session driver instance.
        """
        config_state = self._config_states[0] if key is None else self._get_config_state_by_key(key)
        return get_or_create_session(request, config_state)

    def get_connection(self, request: Any, key: "str | None" = None) -> Any:
        """Get database connection from request context.

        Args:
            request: Sanic request instance.
            key: Optional session key to retrieve a specific database connection.

        Returns:
            Database connection object.
        """
        config_state = self._config_states[0] if key is None else self._get_config_state_by_key(key)
        return get_context_value(request.ctx, config_state.connection_key)

    def _get_config_state_by_key(self, key: str) -> SanicConfigState:
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
