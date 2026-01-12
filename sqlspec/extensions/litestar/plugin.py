import logging
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, NoReturn, cast, overload

from litestar.di import Provide
from litestar.middleware import DefineMiddleware
from litestar.plugins import CLIPlugin, InitPluginProtocol

from sqlspec.base import SQLSpec
from sqlspec.config import (
    AsyncConfigT,
    AsyncDatabaseConfig,
    DatabaseConfigProtocol,
    DriverT,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    SyncConfigT,
    SyncDatabaseConfig,
)
from sqlspec.core.filters import OffsetPagination
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import (
    delete_sqlspec_scope_state,
    get_sqlspec_scope_state,
    set_sqlspec_scope_state,
)
from sqlspec.extensions.litestar.handlers import (
    autocommit_handler_maker,
    connection_provider_maker,
    lifespan_handler_maker,
    manual_handler_maker,
    pool_provider_maker,
    session_provider_maker,
)
from sqlspec.typing import NUMPY_INSTALLED, ConnectionT, PoolT, SchemaT
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.serializers import numpy_array_dec_hook, numpy_array_enc_hook, numpy_array_predicate

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    from litestar import Litestar
    from litestar.config.app import AppConfig
    from litestar.datastructures.state import State
    from litestar.types import ASGIApp, BeforeMessageSendHookHandler, Receive, Scope, Send
    from rich_click import Group

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.loader import SQLFileLoader

logger = get_logger("sqlspec.extensions.litestar")

CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]
DEFAULT_COMMIT_MODE: CommitMode = "manual"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"
DEFAULT_CORRELATION_HEADER = "x-request-id"
TRACE_CONTEXT_FALLBACK_HEADERS: tuple[str, ...] = (
    DEFAULT_CORRELATION_HEADER,
    "x-correlation-id",
    "traceparent",
    "x-cloud-trace-context",
    "grpc-trace-bin",
    "x-amzn-trace-id",
    "x-b3-traceid",
    "x-client-trace-id",
)
CORRELATION_STATE_KEY = "sqlspec_correlation_id"

__all__ = (
    "CORRELATION_STATE_KEY",
    "DEFAULT_COMMIT_MODE",
    "DEFAULT_CONNECTION_KEY",
    "DEFAULT_CORRELATION_HEADER",
    "DEFAULT_POOL_KEY",
    "DEFAULT_SESSION_KEY",
    "TRACE_CONTEXT_FALLBACK_HEADERS",
    "CommitMode",
    "CorrelationMiddleware",
    "PluginConfigState",
    "SQLSpecPlugin",
)


def _encode_offset_pagination(value: OffsetPagination[Any]) -> dict[str, Any]:
    return {"items": value.items, "limit": value.limit, "offset": value.offset, "total": value.total}


def _normalize_header_list(headers: Any) -> list[str]:
    if headers is None:
        return []
    if isinstance(headers, str):
        return [headers.lower()]
    if isinstance(headers, Iterable):
        normalized: list[str] = []
        for header in headers:
            if not isinstance(header, str):
                msg = "litestar correlation headers must be strings"
                raise ImproperConfigurationError(msg)
            normalized.append(header.lower())
        return normalized
    msg = "litestar correlation_headers must be a string or iterable of strings"
    raise ImproperConfigurationError(msg)


def _dedupe_headers(headers: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for header in headers:
        lowered = header.lower()
        if lowered in seen or not lowered:
            continue
        seen.add(lowered)
        ordered.append(lowered)
    return ordered


def _build_correlation_headers(*, primary: str, configured: list[str], auto_trace_headers: bool) -> tuple[str, ...]:
    header_order: list[str] = [primary.lower()]
    header_order.extend(configured)
    if auto_trace_headers:
        header_order.extend(TRACE_CONTEXT_FALLBACK_HEADERS)
    return tuple(_dedupe_headers(header_order))


class CorrelationMiddleware:
    __slots__ = ("_app", "_headers")

    def __init__(self, app: "ASGIApp", *, headers: tuple[str, ...]) -> None:
        self._app = app
        self._headers = headers

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        scope_type = scope.get("type")
        if str(scope_type) != "http" or not self._headers:
            await self._app(scope, receive, send)
            return

        header_value: str | None = None
        raw_headers = scope.get("headers") or []
        for header in self._headers:
            for name, value in raw_headers:
                if name.decode().lower() == header:
                    header_value = value.decode()
                    break
            if header_value:
                break
        if not header_value:
            header_value = CorrelationContext.generate()

        previous_correlation_id = CorrelationContext.get()
        CorrelationContext.set(header_value)
        set_sqlspec_scope_state(scope, CORRELATION_STATE_KEY, header_value)
        try:
            await self._app(scope, receive, send)
        finally:
            with suppress(KeyError):
                delete_sqlspec_scope_state(scope, CORRELATION_STATE_KEY)
            CorrelationContext.set(previous_correlation_id)


@dataclass
class PluginConfigState:
    """Internal state for each database configuration."""

    config: "DatabaseConfigProtocol[Any, Any, Any]"
    connection_key: str
    pool_key: str
    session_key: str
    commit_mode: CommitMode
    extra_commit_statuses: "set[int] | None"
    extra_rollback_statuses: "set[int] | None"
    enable_correlation_middleware: bool
    correlation_header: str
    correlation_headers: tuple[str, ...] = field(init=False)
    disable_di: bool
    connection_provider: "Callable[[State, Scope], AsyncGenerator[Any, None]]" = field(init=False)
    pool_provider: "Callable[[State, Scope], Any]" = field(init=False)
    session_provider: "Callable[..., AsyncGenerator[Any, None]]" = field(init=False)
    before_send_handler: "BeforeMessageSendHookHandler" = field(init=False)
    lifespan_handler: "Callable[[Litestar], AbstractAsyncContextManager[None]]" = field(init=False)
    annotation: "type[DatabaseConfigProtocol[Any, Any, Any]]" = field(init=False)


class SQLSpecPlugin(InitPluginProtocol, CLIPlugin):
    """Litestar plugin for SQLSpec database integration.

    Automatically configures NumPy array serialization when NumPy is installed,
    enabling seamless bidirectional conversion between NumPy arrays and JSON
    for vector embedding workflows.

    Session Table Migrations:
        The Litestar extension includes migrations for creating session storage tables.
        To include these migrations in your database migration workflow, add 'litestar'
        to the include_extensions list in your migration configuration.

    Example:
        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/db"},
            extension_config={
                "litestar": {
                    "session_table": "custom_sessions"  # Optional custom table name
                }
            },
            migration_config={
                "script_location": "migrations",
                "include_extensions": ["litestar"],  # Simple string list only
            }
        )

        The session table migration will automatically use the appropriate column types
        for your database dialect (JSONB for PostgreSQL, JSON for MySQL, TEXT for SQLite).

        Extension migrations use the ext_litestar_ prefix (e.g., ext_litestar_0001) to
        prevent version conflicts with application migrations.
    """

    __slots__ = ("_correlation_headers", "_plugin_configs", "_sqlspec")

    def __init__(self, sqlspec: SQLSpec, *, loader: "SQLFileLoader | None" = None) -> None:
        """Initialize SQLSpec plugin.

        Args:
            sqlspec: Pre-configured SQLSpec instance with registered database configs.
            loader: Optional SQL file loader instance (SQLSpec may already have one).
        """
        self._sqlspec = sqlspec

        self._plugin_configs: list[PluginConfigState] = []
        for cfg in self._sqlspec.configs.values():
            config_union = cast(
                "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]",
                cfg,
            )
            settings = self._extract_litestar_settings(config_union)
            state = self._create_config_state(config_union, settings)
            self._plugin_configs.append(state)

        correlation_headers: list[str] = []
        for state in self._plugin_configs:
            if not state.enable_correlation_middleware:
                continue
            for header in state.correlation_headers:
                if header not in correlation_headers:
                    correlation_headers.append(header)
        self._correlation_headers = tuple(correlation_headers)
        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="litestar",
            stage="init",
            config_count=len(self._plugin_configs),
            correlation_headers=len(self._correlation_headers),
        )

    def _extract_litestar_settings(
        self,
        config: "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]",
    ) -> "dict[str, Any]":
        """Extract Litestar settings from config.extension_config."""
        litestar_config = config.extension_config.get("litestar", {})

        connection_key = litestar_config.get("connection_key", DEFAULT_CONNECTION_KEY)
        pool_key = litestar_config.get("pool_key", DEFAULT_POOL_KEY)
        session_key = litestar_config.get("session_key", DEFAULT_SESSION_KEY)
        commit_mode = litestar_config.get("commit_mode", DEFAULT_COMMIT_MODE)

        if not config.supports_connection_pooling and pool_key == DEFAULT_POOL_KEY:
            pool_key = f"_{DEFAULT_POOL_KEY}_{id(config)}"

        correlation_header = str(litestar_config.get("correlation_header", DEFAULT_CORRELATION_HEADER)).lower()
        configured_headers = _normalize_header_list(litestar_config.get("correlation_headers"))
        auto_trace_headers = bool(litestar_config.get("auto_trace_headers", True))

        return {
            "connection_key": connection_key,
            "pool_key": pool_key,
            "session_key": session_key,
            "commit_mode": commit_mode,
            "extra_commit_statuses": litestar_config.get("extra_commit_statuses"),
            "extra_rollback_statuses": litestar_config.get("extra_rollback_statuses"),
            "enable_correlation_middleware": litestar_config.get("enable_correlation_middleware", True),
            "correlation_header": correlation_header,
            "correlation_headers": _build_correlation_headers(
                primary=correlation_header, configured=configured_headers, auto_trace_headers=auto_trace_headers
            ),
            "disable_di": litestar_config.get("disable_di", False),
        }

    def _create_config_state(
        self,
        config: "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]",
        settings: "dict[str, Any]",
    ) -> PluginConfigState:
        """Create plugin state with handlers for the given configuration."""
        state = PluginConfigState(
            config=config,
            connection_key=settings["connection_key"],
            pool_key=settings["pool_key"],
            session_key=settings["session_key"],
            commit_mode=settings["commit_mode"],
            extra_commit_statuses=settings.get("extra_commit_statuses"),
            extra_rollback_statuses=settings.get("extra_rollback_statuses"),
            enable_correlation_middleware=settings["enable_correlation_middleware"],
            correlation_header=settings["correlation_header"],
            disable_di=settings["disable_di"],
        )
        state.correlation_headers = tuple(settings["correlation_headers"])

        if not state.disable_di:
            self._setup_handlers(state)
        return state

    def _setup_handlers(self, state: PluginConfigState) -> None:
        """Setup handlers for the plugin state."""
        connection_key = state.connection_key
        pool_key = state.pool_key
        commit_mode = state.commit_mode
        config = state.config
        is_async = config.is_async

        state.connection_provider = connection_provider_maker(config, pool_key, connection_key)
        state.pool_provider = pool_provider_maker(config, pool_key)
        state.session_provider = session_provider_maker(config, connection_key)
        state.lifespan_handler = lifespan_handler_maker(config, pool_key)

        if commit_mode == "manual":
            state.before_send_handler = manual_handler_maker(connection_key, is_async)
        else:
            commit_on_redirect = commit_mode == "autocommit_include_redirect"
            state.before_send_handler = autocommit_handler_maker(
                connection_key, is_async, commit_on_redirect, state.extra_commit_statuses, state.extra_rollback_statuses
            )

    @property
    def config(
        self,
    ) -> "list[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]":
        """Return the plugin configurations.

        Returns:
            List of database configurations.
        """
        return [
            cast(
                "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]",
                state.config,
            )
            for state in self._plugin_configs
        ]

    def on_cli_init(self, cli: "Group") -> None:
        """Configure CLI commands for SQLSpec database operations.

        Args:
            cli: The Click command group to add commands to.
        """
        from sqlspec.extensions.litestar.cli import database_group

        cli.add_command(database_group)

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Configure Litestar application with SQLSpec database integration.

        Automatically registers NumPy array serialization when NumPy is installed.

        Args:
            app_config: The Litestar application configuration instance.

        Returns:
            The updated application configuration instance.
        """
        self._validate_dependency_keys()

        def store_sqlspec_in_state() -> None:
            app_config.state.sqlspec = self

        app_config.on_startup.append(store_sqlspec_in_state)
        app_config.signature_types.extend([SQLSpec, DatabaseConfigProtocol, SyncConfigT, AsyncConfigT])

        signature_namespace = {"ConnectionT": ConnectionT, "PoolT": PoolT, "DriverT": DriverT, "SchemaT": SchemaT}

        for state in self._plugin_configs:
            state.annotation = type(state.config)
            app_config.signature_types.append(state.annotation)
            app_config.signature_types.append(state.config.connection_type)
            app_config.signature_types.append(state.config.driver_type)

            signature_namespace.update(state.config.get_signature_namespace())

            if not state.disable_di:
                app_config.before_send.append(state.before_send_handler)
                app_config.lifespan.append(state.lifespan_handler)
                app_config.dependencies.update({
                    state.connection_key: Provide(state.connection_provider),
                    state.pool_key: Provide(state.pool_provider),
                    state.session_key: Provide(state.session_provider),
                })

        if signature_namespace:
            app_config.signature_namespace.update(signature_namespace)

        if app_config.type_encoders is None:
            app_config.type_encoders = {OffsetPagination: _encode_offset_pagination}
        else:
            encoders_dict = dict(app_config.type_encoders)
            encoders_dict[OffsetPagination] = _encode_offset_pagination
            app_config.type_encoders = encoders_dict

        if NUMPY_INSTALLED:
            import numpy as np

            if app_config.type_encoders is None:
                app_config.type_encoders = {np.ndarray: numpy_array_enc_hook}
            else:
                encoders_dict = dict(app_config.type_encoders)
                encoders_dict[np.ndarray] = numpy_array_enc_hook
                app_config.type_encoders = encoders_dict

            if app_config.type_decoders is None:
                app_config.type_decoders = [(numpy_array_predicate, numpy_array_dec_hook)]  # type: ignore[list-item]
            else:
                decoders_list = list(app_config.type_decoders)
                decoders_list.append((numpy_array_predicate, numpy_array_dec_hook))  # type: ignore[arg-type]
                app_config.type_decoders = decoders_list

        if self._correlation_headers:
            middleware = DefineMiddleware(CorrelationMiddleware, headers=self._correlation_headers)
            existing_middleware = list(app_config.middleware or [])
            existing_middleware.append(middleware)
            app_config.middleware = existing_middleware

        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="litestar",
            stage="configured",
            config_count=len(self._plugin_configs),
            correlation_headers=len(self._correlation_headers),
            numpy_enabled=bool(NUMPY_INSTALLED),
        )
        return app_config

    def get_annotations(
        self,
    ) -> "list[type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]]":
        """Return the list of annotations.

        Returns:
            List of annotations.
        """
        return [
            cast(
                "type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]",
                state.annotation,
            )
            for state in self._plugin_configs
        ]

    def get_annotation(
        self,
        key: "str | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]",
    ) -> "type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]":
        """Return the annotation for the given configuration.

        Args:
            key: The configuration instance or key to lookup.

        Raises:
            KeyError: If no configuration is found for the given key.

        Returns:
            The annotation for the configuration.
        """
        for state in self._plugin_configs:
            if key in {state.config, state.annotation} or key in {state.connection_key, state.pool_key}:
                return cast(
                    "type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]",
                    state.annotation,
                )

        msg = f"No configuration found for {key}"
        raise KeyError(msg)

    @overload
    def get_config(
        self, name: "type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]]"
    ) -> "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]": ...

    @overload
    def get_config(
        self, name: "type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]"
    ) -> "AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]": ...

    @overload
    def get_config(
        self, name: str
    ) -> "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]": ...

    def get_config(
        self, name: "type[DatabaseConfigProtocol[Any, Any, Any]] | str | Any"
    ) -> "DatabaseConfigProtocol[Any, Any, Any]":
        """Get a configuration instance by name.

        Args:
            name: The configuration identifier.

        Raises:
            KeyError: If no configuration is found for the given name.

        Returns:
            The configuration instance for the specified name.
        """
        if isinstance(name, str):
            for state in self._plugin_configs:
                if name in {state.connection_key, state.pool_key, state.session_key}:
                    return cast("DatabaseConfigProtocol[Any, Any, Any]", state.config)  # type: ignore[redundant-cast]

        for state in self._plugin_configs:
            if name in {state.config, state.annotation}:
                return cast("DatabaseConfigProtocol[Any, Any, Any]", state.config)  # type: ignore[redundant-cast]

        msg = f"No database configuration found for name '{name}'. Available keys: {self._get_available_keys()}"
        raise KeyError(msg)

    def provide_request_session(
        self, key: "str | SyncConfigT | AsyncConfigT | type[SyncConfigT | AsyncConfigT]", state: "State", scope: "Scope"
    ) -> "SyncDriverAdapterBase | AsyncDriverAdapterBase":
        """Provide a database session for the specified configuration key from request scope.

        Args:
            key: The configuration identifier (same as get_config).
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A driver session instance for the specified database configuration.
        """
        plugin_state = self._get_plugin_state(key)
        session_scope_key = f"{plugin_state.session_key}_instance"

        session = get_sqlspec_scope_state(scope, session_scope_key)
        if session is not None:
            return cast("SyncDriverAdapterBase | AsyncDriverAdapterBase", session)

        connection = get_sqlspec_scope_state(scope, plugin_state.connection_key)
        if connection is None:
            self._raise_missing_connection(plugin_state.connection_key)

        session = plugin_state.config.driver_type(
            connection=connection,
            statement_config=plugin_state.config.statement_config,
            driver_features=plugin_state.config.driver_features,
        )
        set_sqlspec_scope_state(scope, session_scope_key, session)

        return cast("SyncDriverAdapterBase | AsyncDriverAdapterBase", session)

    def provide_sync_request_session(
        self, key: "str | SyncConfigT | type[SyncConfigT]", state: "State", scope: "Scope"
    ) -> "SyncDriverAdapterBase":
        """Provide a sync database session for the specified configuration key from request scope.

        Args:
            key: The sync configuration identifier.
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A sync driver session instance for the specified database configuration.
        """
        session = self.provide_request_session(key, state, scope)
        return cast("SyncDriverAdapterBase", session)

    def provide_async_request_session(
        self, key: "str | AsyncConfigT | type[AsyncConfigT]", state: "State", scope: "Scope"
    ) -> "AsyncDriverAdapterBase":
        """Provide an async database session for the specified configuration key from request scope.

        Args:
            key: The async configuration identifier.
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            An async driver session instance for the specified database configuration.
        """
        session = self.provide_request_session(key, state, scope)
        return cast("AsyncDriverAdapterBase", session)

    def provide_request_connection(
        self, key: "str | SyncConfigT | AsyncConfigT | type[SyncConfigT | AsyncConfigT]", state: "State", scope: "Scope"
    ) -> "Any":
        """Provide a database connection for the specified configuration key from request scope.

        Args:
            key: The configuration identifier (same as get_config).
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A database connection instance for the specified database configuration.
        """
        plugin_state = self._get_plugin_state(key)
        connection = get_sqlspec_scope_state(scope, plugin_state.connection_key)
        if connection is None:
            self._raise_missing_connection(plugin_state.connection_key)

        return connection

    def _get_plugin_state(
        self, key: "str | SyncConfigT | AsyncConfigT | type[SyncConfigT | AsyncConfigT]"
    ) -> PluginConfigState:
        """Get plugin state for a configuration by key."""
        if isinstance(key, str):
            for state in self._plugin_configs:
                if key in {state.connection_key, state.pool_key, state.session_key}:
                    return state

        for state in self._plugin_configs:
            if key in {state.config, state.annotation}:
                return state

        self._raise_config_not_found(key)
        return None

    def _get_available_keys(self) -> "list[str]":
        """Get a list of all available configuration keys for error messages."""
        keys = []
        for state in self._plugin_configs:
            keys.extend([state.connection_key, state.pool_key, state.session_key])
        return keys

    def _validate_dependency_keys(self) -> None:
        """Validate that connection and pool keys are unique across configurations."""
        connection_keys = [state.connection_key for state in self._plugin_configs]
        pool_keys = [state.pool_key for state in self._plugin_configs]

        if len(set(connection_keys)) != len(connection_keys):
            self._raise_duplicate_connection_keys()

        if len(set(pool_keys)) != len(pool_keys):
            self._raise_duplicate_pool_keys()

    def _raise_missing_connection(self, connection_key: str) -> None:
        """Raise error when connection is not found in scope."""
        msg = f"No database connection found in scope for key '{connection_key}'. "
        msg += "Ensure the connection dependency is properly configured and available."
        raise ImproperConfigurationError(detail=msg)

    def _raise_config_not_found(self, key: Any) -> NoReturn:
        """Raise error when configuration is not found."""
        msg = f"No database configuration found for name '{key}'. Available keys: {self._get_available_keys()}"
        raise KeyError(msg)

    def _raise_duplicate_connection_keys(self) -> None:
        """Raise error when connection keys are not unique."""
        msg = "When using multiple database configuration, each configuration must have a unique `connection_key`."
        raise ImproperConfigurationError(detail=msg)

    def _raise_duplicate_pool_keys(self) -> None:
        """Raise error when pool keys are not unique."""
        msg = "When using multiple database configuration, each configuration must have a unique `pool_key`."
        raise ImproperConfigurationError(detail=msg)
