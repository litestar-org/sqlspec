"""Litestar plugin for SQLSpec database configuration, lifecycle, and dependency injection."""

import asyncio
import logging
from collections.abc import Iterable, MutableMapping
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, NoReturn, TypeAlias, cast, overload

from litestar.constants import HTTP_RESPONSE_START
from litestar.di import Provide
from litestar.enums import ScopeType
from litestar.exceptions import ClientException, HTTPException, NotFoundException
from litestar.exceptions.responses import create_exception_response
from litestar.middleware import DefineMiddleware
from litestar.plugins import CLIPlugin, InitPluginProtocol, OpenAPISchemaPlugin
from litestar.status_codes import HTTP_409_CONFLICT, HTTP_500_INTERNAL_SERVER_ERROR
from litestar.utils.scope.state import ScopeState

from sqlspec.base import SQLSpec, coerce_sqlspec
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
from sqlspec.core import CorrelationExtractor, OffsetPagination
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.exceptions import ImproperConfigurationError, IntegrityError, NotFoundError
from sqlspec.extensions.litestar._utils import (
    delete_sqlspec_scope_state,
    get_sqlspec_scope_state,
    set_sqlspec_scope_state,
)
from sqlspec.extensions.litestar.cli import database_group
from sqlspec.extensions.litestar.handlers import (
    _REQUEST_EXCEPTION_KEY,
    _capture_connection_tenant,
    _exit_connection_context,
    autocommit_handler_maker,
    connection_provider_maker,
    has_tenant_strategy,
    lifespan_handler_maker,
    manual_handler_maker,
    pool_provider_maker,
    record_request_exception,
    session_provider_maker,
    tenant_payload_key,
)
from sqlspec.extensions.litestar.tenant import (
    TenantResolutionSettings,
    raise_missing_tenant_http_exception,
    receipt_state_key,
    resolve_route_tenant,
)
from sqlspec.typing import NUMPY_INSTALLED, ConnectionT, PoolT, SchemaT, import_optional_attr
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.serializers import DEFAULT_TYPE_ENCODERS, numpy_array_dec_hook
from sqlspec.utils.sync_tools import ensure_async_
from sqlspec.utils.tenant import TenantContext, TenantRequestBinding

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from litestar import Litestar, Request, Response
    from litestar._openapi.schema_generation.schema import SchemaCreator
    from litestar.config.app import AppConfig
    from litestar.datastructures.state import State
    from litestar.openapi.spec import Schema
    from litestar.types import (
        ASGIApp,
        BeforeMessageSendHookHandler,
        ExceptionHandlersMap,
        Message,
        Middleware,
        Receive,
        Scope,
        Send,
    )
    from litestar.typing import FieldDefinition
    from rich_click import Group

    from sqlspec.config import LitestarConfig
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.loader import SQLFileLoader

    AnyDatabaseConfig: TypeAlias = (
        SyncDatabaseConfig[Any, Any, Any]
        | NoPoolSyncConfig[Any, Any]
        | AsyncDatabaseConfig[Any, Any, Any]
        | NoPoolAsyncConfig[Any, Any]
    )

__all__ = (
    "CORRELATION_STATE_KEY",
    "DEFAULT_COMMIT_MODE",
    "DEFAULT_CONNECTION_KEY",
    "DEFAULT_CORRELATION_HEADER",
    "DEFAULT_POOL_KEY",
    "DEFAULT_SESSION_KEY",
    "TRACE_CONTEXT_FALLBACK_HEADERS",
    "CommitMode",
    "ConnectionCleanupMiddleware",
    "CorrelationMiddleware",
    "PluginConfigState",
    "SQLSpecPlugin",
    "_OffsetPaginationSchemaPlugin",
    "integrity_error_handler",
    "not_found_error_handler",
)

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
_LITESTAR_NUMPY_ARRAY_TYPE: type[Any] | None = None


def not_found_error_handler(request: "Request[Any, Any, Any]", exc: NotFoundError) -> "Response[Any]":
    """Return a 404 response whose detail is the :class:`~sqlspec.exceptions.NotFoundError` message.

    A handler for status 404 or for the HTTP exception type renders it, otherwise Litestar's
    default exception response. Handlers for :class:`~sqlspec.exceptions.NotFoundError` or a
    subclass take precedence; handlers for its base classes or status 500 do not.
    """
    http_exc = NotFoundException(detail=str(exc) or "Not Found")
    http_exc.__cause__ = exc
    return _render_http_exception(request, http_exc, _resolve_exception_handlers(request))


def integrity_error_handler(request: "Request[Any, Any, Any]", exc: IntegrityError) -> "Response[Any]":
    """Return a 409 response with the generic detail ``"Conflict"``.

    A handler for status 409 or for the HTTP exception type renders it, otherwise Litestar's
    default exception response. Handlers for :class:`~sqlspec.exceptions.IntegrityError` or a
    subclass take precedence, and handlers for a later class in the error's MRO or for status
    500 receive the original exception.
    """
    handlers = _resolve_exception_handlers(request)
    exc_mro = type(exc).__mro__
    broader_handler = next(
        (handlers[cls] for cls in exc_mro[exc_mro.index(IntegrityError) + 1 :] if cls in handlers),
        handlers.get(HTTP_500_INTERNAL_SERVER_ERROR),
    )
    if broader_handler is not None:
        return broader_handler(request, exc)
    http_exc = ClientException(status_code=HTTP_409_CONFLICT, detail="Conflict")
    http_exc.__cause__ = exc
    return _render_http_exception(request, http_exc, handlers)


class CorrelationMiddleware:
    """ASGI middleware that binds a correlation ID to each HTTP request.

    The first non-empty value among ``headers``, checked in order, becomes the
    correlation ID after surrounding whitespace is trimmed and the value is
    truncated to 128 characters; when no header has a value, or the trimmed
    value is empty, a new ID is generated. The ID is set on
    :class:`~sqlspec.utils.correlation.CorrelationContext` and in the request scope
    for the duration of the request, then the previous ID is restored. Non-HTTP
    scopes, and instances created with an empty ``headers`` tuple, pass through
    unchanged.

    Args:
        app: The downstream ASGI application.
        headers: Request header names to check, in priority order.
    """

    __slots__ = ("_app", "_extractor", "_headers")

    def __init__(self, app: "ASGIApp", *, headers: tuple[str, ...]) -> None:
        self._app = app
        self._headers = headers
        self._extractor = (
            CorrelationExtractor(
                primary_header=headers[0],
                additional_headers=headers[1:] if len(headers) > 1 else None,
                auto_trace_headers=False,
            )
            if headers
            else None
        )

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        scope_type = scope.get("type")
        if str(scope_type) != "http" or self._extractor is None:
            await self._app(scope, receive, send)
            return

        raw_headers = scope.get("headers") or []
        header_dict = {name.decode().lower(): value.decode() for name, value in raw_headers}
        header_value = self._extractor.extract(lambda header: header_dict.get(header))

        previous_correlation_id = CorrelationContext.get()
        CorrelationContext.set(header_value)
        set_sqlspec_scope_state(scope, CORRELATION_STATE_KEY, header_value)
        try:
            await self._app(scope, receive, send)
        finally:
            with suppress(KeyError):
                delete_sqlspec_scope_state(scope, CORRELATION_STATE_KEY)
            CorrelationContext.set(previous_correlation_id)


@dataclass(frozen=True)
class _TenantResolutionTarget:
    """Resolution inputs for one tenant-enabled configuration."""

    config: Any
    settings: TenantResolutionSettings
    receipt_state_keys: tuple[str, ...]


class TenantRequestMiddleware:
    """Bind one lazy tenant resolver for each HTTP or WebSocket request.

    Installed only when a registered configuration enables ``tenant_strategy``. Request start
    costs one context variable assignment; route bypass, connection receipts, the extractor, and
    verified auth resolution run when a lease, session, or ``TenantContext`` first needs the
    tenant. Pending resolutions finish before the final response message, and the binding is
    reset when the ASGI call returns.
    """

    __slots__ = ("_app", "_default", "_keys", "_targets")

    def __init__(self, app: "ASGIApp", *, targets: "tuple[_TenantResolutionTarget, ...]") -> None:
        self._app = app
        self._targets = {id(target.config): target for target in targets}
        self._keys = tuple(target.config for target in targets)
        self._default = targets[0] if targets else None

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope.get("type") not in {ScopeType.HTTP, ScopeType.WEBSOCKET} or self._default is None:
            await self._app(scope, receive, send)
            return
        binding = TenantRequestBinding(
            lambda key: self._resolve(scope, key),
            keys=self._keys,
            ambient=TenantContext.current_binding(),
            missing=lambda _key: raise_missing_tenant_http_exception(cast("Mapping[str, Any]", scope)),
        )
        token = TenantContext.bind_request(binding)
        try:
            if scope.get("type") == ScopeType.HTTP:

                async def frozen_send(message: "Message") -> None:
                    message_type = cast("str", message["type"])
                    if message_type == "http.response.pathsend" or (
                        message_type == "http.response.body" and not message.get("more_body", False)
                    ):
                        await binding.afreeze()
                    await send(message)

                await self._app(scope, receive, frozen_send)
            else:
                await self._app(scope, receive, send)
        finally:
            TenantContext.reset(token)

    def _resolve(self, scope: "Scope", key: Any) -> Any:
        target = self._targets.get(id(key), self._default)
        if target is None:
            return None
        return resolve_route_tenant(cast("MutableMapping[str, Any]", scope), target.settings, target.receipt_state_keys)


class ConnectionCleanupMiddleware:
    """ASGI middleware guaranteeing residual HTTP request connections are finalized and released.

    Cleans up handed-over injected connections when a request aborts before
    ``http.response.start`` as well as on-demand connections acquired after
    ``http.response.start`` during streaming responses or ``after_response`` hooks.
    """

    __slots__ = ("_app", "_targets")

    def __init__(self, app: "ASGIApp", *, targets: "tuple[_ConnectionCleanupTarget, ...]") -> None:
        self._app = app
        self._targets = targets

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope.get("type") != ScopeType.HTTP or not self._targets:
            await self._app(scope, receive, send)
            return

        response_started = False
        response_status: int | None = None

        async def send_wrapper(message: "Message") -> None:
            nonlocal response_started, response_status
            if message["type"] == HTTP_RESPONSE_START:
                response_started = True
                response_status = message["status"]
            await send(message)

        caught_exc: BaseException | None = None
        try:
            await self._app(scope, receive, send_wrapper)
        except BaseException as exc:
            caught_exc = exc
            raise
        finally:
            started = response_started or bool(ScopeState.from_scope(scope).response_started)
            try:
                await self._cleanup_scope(
                    scope, caught_exc=caught_exc, response_started=started, response_status=response_status
                )
            except Exception:
                if caught_exc is None or isinstance(caught_exc, Exception):
                    raise

    async def _cleanup_scope(
        self, scope: "Scope", *, caught_exc: BaseException | None, response_started: bool, response_status: int | None
    ) -> None:
        recorded_exc = get_sqlspec_scope_state(scope, _REQUEST_EXCEPTION_KEY)
        if caught_exc is not None:
            effective_exc: BaseException | None = caught_exc
        elif recorded_exc is not None:
            effective_exc = recorded_exc
        elif not response_started:
            effective_exc = asyncio.CancelledError()
        else:
            effective_exc = None

        primary_exc: BaseException | None = None
        for target in self._targets:
            try:
                ctx_scope_key = f"{target.connection_key}_ctx"
                provider_owned_key = f"{target.connection_key}_provider_owned"
                session_scope_key = f"{target.session_key}_instance"
                connection = get_sqlspec_scope_state(scope, target.connection_key)
                connection_cm = get_sqlspec_scope_state(scope, ctx_scope_key, pop=True)

                if connection is None and connection_cm is None:
                    delete_sqlspec_scope_state(scope, session_scope_key)
                    continue

                if effective_exc is not None and get_sqlspec_scope_state(scope, _REQUEST_EXCEPTION_KEY) is None:
                    set_sqlspec_scope_state(scope, _REQUEST_EXCEPTION_KEY, effective_exc)

                exc_override: BaseException | None = None
                try:
                    if connection is not None and target.commit_mode != "manual":
                        if effective_exc is not None:
                            with suppress(Exception):
                                await ensure_async_(connection.rollback)()
                        else:
                            commit_range = range(
                                200, 400 if target.commit_mode == "autocommit_include_redirect" else 300
                            )
                            should_commit = (
                                response_status is not None
                                and (response_status in commit_range or response_status in target.extra_commit_statuses)
                                and response_status not in target.extra_rollback_statuses
                            )
                            if should_commit:
                                try:
                                    await ensure_async_(connection.commit)()
                                except BaseException:
                                    with suppress(Exception):
                                        await ensure_async_(connection.rollback)()
                                    raise
                            else:
                                await ensure_async_(connection.rollback)()
                except BaseException as finalization_exc:
                    exc_override = finalization_exc
                    raise
                finally:
                    try:
                        await _exit_connection_context(
                            connection_cm,
                            connection,
                            scope,
                            exc_override=exc_override,
                            connection_key=target.connection_key,
                        )
                    finally:
                        delete_sqlspec_scope_state(scope, target.connection_key)
                        delete_sqlspec_scope_state(scope, provider_owned_key)
                        delete_sqlspec_scope_state(scope, session_scope_key)
            except BaseException as exc:
                primary_exc = _select_teardown_exception(primary_exc, exc)

        if primary_exc is not None:
            raise primary_exc


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
    enable_sqlcommenter_middleware: bool
    correlation_headers: tuple[str, ...] = field(init=False)
    disable_di: bool
    manage_lifespan: bool
    tenant_settings: TenantResolutionSettings = field(default_factory=TenantResolutionSettings)
    connection_provider: "Callable[[State, Scope], AsyncGenerator[Any, None]] | None" = field(default=None, init=False)
    pool_provider: "Callable[[State, Scope], Any] | None" = field(default=None, init=False)
    session_provider: "Callable[..., AsyncGenerator[Any, None]] | None" = field(default=None, init=False)
    before_send_handler: "BeforeMessageSendHookHandler | None" = field(default=None, init=False)
    lifespan_handler: "Callable[[Litestar], AbstractAsyncContextManager[None]] | None" = field(default=None, init=False)
    annotation: "type[DatabaseConfigProtocol[Any, Any, Any]] | None" = field(default=None, init=False)


class SQLSpecPlugin(InitPluginProtocol, CLIPlugin):
    """Litestar plugin for SQLSpec database integration.

    Automatically configures NumPy array serialization when NumPy is installed,
    enabling seamless bidirectional conversion between NumPy arrays and JSON
    for vector embedding workflows.

    Session Table Migrations:
        The Litestar extension includes migrations for creating session storage tables.
        To include these migrations in your database migration workflow, add 'litestar'
        to the include_extensions list in your migration configuration.
    """

    __slots__ = ("_correlation_headers", "_enable_sqlcommenter_middleware", "_plugin_configs", "_sqlspec")

    def __init__(
        self,
        sqlspec: "SQLSpec | AnyDatabaseConfig | Sequence[DatabaseConfigProtocol[Any, Any, Any]]",
        *,
        loader: "SQLFileLoader | None" = None,
    ) -> None:
        """Initialize SQLSpec plugin.

        Args:
            sqlspec: Pre-configured SQLSpec registry, a single database
                configuration, or a sequence of database configurations.
            loader: Optional SQL file loader instance attached to the registry.
        """
        self._sqlspec = coerce_sqlspec(sqlspec, loader=loader)

        self._plugin_configs: list[PluginConfigState] = []
        for cfg in self._sqlspec.configs.values():
            config_union = cast("AnyDatabaseConfig", cfg)
            settings = self._extract_extension_settings(config_union)
            state = self._config_state(config_union, settings)
            self._plugin_configs.append(state)

        correlation_headers: list[str] = []
        enable_sqlcommenter = False
        for state in self._plugin_configs:
            if state.enable_sqlcommenter_middleware and state.config.statement_config.enable_sqlcommenter:
                enable_sqlcommenter = True
            if not state.enable_correlation_middleware:
                continue
            for header in state.correlation_headers:
                if header not in correlation_headers:
                    correlation_headers.append(header)
        self._correlation_headers = tuple(correlation_headers)
        self._enable_sqlcommenter_middleware = enable_sqlcommenter
        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="litestar",
            stage="init",
            config_count=len(self._plugin_configs),
            correlation_headers=len(self._correlation_headers),
        )

    @property
    def sqlspec(self) -> SQLSpec:
        """Return the underlying SQLSpec registry."""
        return self._sqlspec

    @property
    def config(self) -> "list[AnyDatabaseConfig]":
        """Return the plugin configurations.

        Returns:
            List of database configurations.
        """
        return [cast("AnyDatabaseConfig", state.config) for state in self._plugin_configs]

    def on_cli_init(self, cli: "Group") -> None:
        """Configure CLI commands for SQLSpec database operations.

        Args:
            cli: The Click command group to add commands to.
        """
        cli.add_command(database_group)

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Configure Litestar application with SQLSpec database integration.

        Automatically registers NumPy array serialization when NumPy is installed.

        Args:
            app_config: The Litestar application configuration instance.

        Returns:
            The updated application configuration instance.
        """
        self._ensure_dependency_keys()
        app_config.state.sqlspec = self
        existing_plugins = cast("tuple[SQLSpecPlugin, ...]", getattr(app_config.state, "_sqlspec_plugins", ()))
        if self not in existing_plugins:
            app_config.state._sqlspec_plugins = (*existing_plugins, self)
        app_config.signature_types.extend([SQLSpec, DatabaseConfigProtocol, SyncConfigT, AsyncConfigT])

        signature_namespace = {"ConnectionT": ConnectionT, "PoolT": PoolT, "DriverT": DriverT, "SchemaT": SchemaT}

        for state in self._plugin_configs:
            state.annotation = type(state.config)
            app_config.signature_types.append(state.annotation)
            app_config.signature_types.append(state.config.connection_type)
            app_config.signature_types.append(state.config.driver_type)

            signature_namespace.update(state.config.get_signature_namespace())

            if state.manage_lifespan:
                if state.lifespan_handler is None:
                    self._raise_lifespan_handler_not_initialized(state.session_key)
                app_config.lifespan.append(state.lifespan_handler)

            if not state.disable_di:
                if (
                    state.connection_provider is None
                    or state.pool_provider is None
                    or state.session_provider is None
                    or state.before_send_handler is None
                ):
                    self._raise_di_handlers_not_initialized(state.session_key)
                app_config.before_send.append(state.before_send_handler)
                app_config.dependencies.update({
                    state.connection_key: Provide(state.connection_provider),
                    state.pool_key: Provide(state.pool_provider),
                    state.session_key: Provide(state.session_provider),
                })

        if self._plugin_configs:
            if app_config.after_exception is None:
                app_config.after_exception = []
            if record_request_exception not in app_config.after_exception:
                app_config.after_exception.append(record_request_exception)

        if signature_namespace:
            app_config.signature_namespace.update(signature_namespace)

        if NUMPY_INSTALLED and (ndarray_type := _get_litestar_numpy_array_type()) is not None:
            import numpy as np

            numpy_namespace = _NumpySignatureNamespace(np, ndarray_type)
            app_config.signature_namespace.update({
                "np": numpy_namespace,
                "numpy": numpy_namespace,
                "ndarray": ndarray_type,
            })

        if not any(isinstance(p, _OffsetPaginationSchemaPlugin) for p in app_config.plugins):
            app_config.plugins.append(_OffsetPaginationSchemaPlugin())

        if app_config.exception_handlers is None:
            app_config.exception_handlers = {}
        app_config.exception_handlers.setdefault(NotFoundError, not_found_error_handler)
        app_config.exception_handlers.setdefault(IntegrityError, integrity_error_handler)

        app_config.type_encoders = {**DEFAULT_TYPE_ENCODERS, **(app_config.type_encoders or {})}
        sqlspec_decoders = _build_litestar_type_decoders()
        if sqlspec_decoders:
            app_config.type_decoders = [*(app_config.type_decoders or []), *sqlspec_decoders]

        new_middlewares: list[DefineMiddleware] = []
        tenant_targets = self._tenant_targets()
        if self._correlation_headers:
            existing_correlation = _find_correlation_middleware(app_config.middleware)
            if existing_correlation is not None:
                headers_honored = self._correlation_headers == TRACE_CONTEXT_FALLBACK_HEADERS or (
                    isinstance(existing_correlation, DefineMiddleware)
                    and tuple(existing_correlation.kwargs.get("headers", ())) == self._correlation_headers
                )
                log_with_context(
                    logger,
                    logging.DEBUG if headers_honored else logging.WARNING,
                    "extension.init",
                    framework="litestar",
                    stage="correlation_middleware_skipped",
                    reason="already_installed",
                    unapplied_correlation_headers=None if headers_honored else list(self._correlation_headers),
                )
            else:
                new_middlewares.append(DefineMiddleware(CorrelationMiddleware, headers=self._correlation_headers))
        if self._enable_sqlcommenter_middleware:
            new_middlewares.append(DefineMiddleware(SQLCommenterMiddleware))
        cleanup_targets = tuple(
            _ConnectionCleanupTarget(
                connection_key=state.connection_key,
                session_key=state.session_key,
                commit_mode=state.commit_mode,
                extra_commit_statuses=frozenset(state.extra_commit_statuses or ()),
                extra_rollback_statuses=frozenset(state.extra_rollback_statuses or ()),
            )
            for state in self._plugin_configs
        )
        if cleanup_targets:
            existing_cleanup = _find_cleanup_middleware(app_config.middleware)
            if isinstance(existing_cleanup, DefineMiddleware):
                prior_targets = tuple(existing_cleanup.kwargs.get("targets", ()))
                seen_keys = {item.connection_key for item in prior_targets}
                merged_targets = prior_targets + tuple(
                    item for item in cleanup_targets if item.connection_key not in seen_keys
                )
                existing_cleanup.kwargs["targets"] = merged_targets
            elif existing_cleanup is None:
                new_middlewares.append(DefineMiddleware(ConnectionCleanupMiddleware, targets=cleanup_targets))
        if new_middlewares:
            app_config.middleware = [*new_middlewares, *(app_config.middleware or [])]
        if tenant_targets:
            app_config.middleware = [
                *(app_config.middleware or []),
                DefineMiddleware(TenantRequestMiddleware, targets=tenant_targets),
            ]

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

    def get_annotations(self) -> "list[type[AnyDatabaseConfig]]":
        """Return the list of annotations.

        Returns:
            List of annotations.
        """
        annotations: list[type[AnyDatabaseConfig]] = []
        for state in self._plugin_configs:
            if state.annotation is None:
                self._raise_plugin_not_registered()
            annotations.append(cast("type[AnyDatabaseConfig]", state.annotation))
        return annotations

    def get_annotation(self, key: "str | AnyDatabaseConfig | type[AnyDatabaseConfig]") -> "type[AnyDatabaseConfig]":
        """Return the annotation for the given configuration.

        Args:
            key: The configuration instance or key to lookup.

        Raises:
            KeyError: If no configuration is found for the given key.

        Returns:
            The annotation for the configuration.
        """
        state = self._get_plugin_state(key)
        if state.annotation is None:
            self._raise_plugin_not_registered()
        return cast("type[AnyDatabaseConfig]", state.annotation)

    @overload
    def get_config(self, name: None = None) -> "AnyDatabaseConfig": ...
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
    def get_config(self, name: str) -> "AnyDatabaseConfig": ...
    def get_config(
        self, name: "type[DatabaseConfigProtocol[Any, Any, Any]] | str | Any" = None
    ) -> "DatabaseConfigProtocol[Any, Any, Any]":
        """Get a configuration instance by name, or the sole configuration when omitted.

        Registry identifiers are available as soon as the plugin is constructed: a
        config instance, its concrete config type when exactly one configuration has
        that type, or a non-null ``bind_key``. A ``bind_key`` wins over a dependency
        key of the same value. The generated Litestar dependency keys
        (``session_key``, ``connection_key``, ``pool_key``) resolve only after the
        plugin is registered with a Litestar application.

        Args:
            name: Optional configuration identifier. Defaults to the sole registered
                configuration when ``None``.

        Raises:
            KeyError: If no configuration is found for ``name``, if ``name`` is
                ``None`` when zero or multiple configurations are registered, or if
                several configurations share the requested concrete type.

        Returns:
            The configuration instance for the specified identifier.
        """
        if name is None:
            if len(self._plugin_configs) == 1:
                return self._plugin_configs[0].config
            if not self._plugin_configs:
                msg = "No database configurations are registered on SQLSpecPlugin."
                raise KeyError(msg)
            msg = (
                "Multiple database configurations are registered on SQLSpecPlugin; "
                f"pass a configuration key, type, or instance. Available keys: {self._get_available_keys()}"
            )
            raise KeyError(msg)
        state = self._registry_state(name) or self._dependency_state(name)
        return cast("DatabaseConfigProtocol[Any, Any, Any]", state.config)  # type: ignore[redundant-cast]

    def _tenant_targets(self) -> "tuple[_TenantResolutionTarget, ...]":
        """Return the resolution inputs of every tenant-enabled configuration."""
        return tuple(
            _TenantResolutionTarget(
                config=state.config,
                settings=state.tenant_settings,
                receipt_state_keys=tuple(
                    receipt_state_key(key)
                    for key in (state.session_key, state.connection_key, getattr(state.config, "bind_key", None))
                    if isinstance(key, str) and key
                ),
            )
            for state in self._plugin_configs
            if has_tenant_strategy(state.config)
        )

    def resolve_tenant_settings(
        self, key: "str | AnyDatabaseConfig | type[AnyDatabaseConfig] | None" = None
    ) -> TenantResolutionSettings:
        """Resolve tenant extraction settings for a registered Litestar configuration.

        Args:
            key: Optional configuration key, concrete type, or instance. When
                omitted, prefers the sole tenant-enforced configuration, then the
                sole configuration, or raises when multiple tenant databases define
                distinct settings.

        Returns:
            The active ``TenantResolutionSettings`` for ``key``.

        Raises:
            KeyError: ``key`` does not match any registered configuration.
            ImproperConfigurationError: ``key`` is omitted and multiple tenant
                databases configure distinct ``TenantResolutionSettings``.
        """
        if key is not None:
            state = self._registry_state(key) or self._dependency_state(key)
            return state.tenant_settings
        tenant_states = [state for state in self._plugin_configs if has_tenant_strategy(state.config)]
        if len(tenant_states) == 1:
            return tenant_states[0].tenant_settings
        if tenant_states:
            first = tenant_states[0].tenant_settings
            if any(state.tenant_settings != first for state in tenant_states[1:]):
                msg = (
                    "Multiple tenant-scoped Litestar configurations have distinct tenant resolution settings; "
                    "pass config_key=... to require_tenant()."
                )
                raise ImproperConfigurationError(msg)
            return first
        if len(self._plugin_configs) == 1:
            return self._plugin_configs[0].tenant_settings
        if self._plugin_configs and all(
            state.tenant_settings == self._plugin_configs[0].tenant_settings for state in self._plugin_configs[1:]
        ):
            return self._plugin_configs[0].tenant_settings
        return TenantResolutionSettings()

    @overload
    def provide_request_session(
        self,
        key: "SyncDatabaseConfig[Any, Any, DriverT] | NoPoolSyncConfig[Any, DriverT] | type[SyncDatabaseConfig[Any, Any, DriverT] | NoPoolSyncConfig[Any, DriverT]]",
        state: "State",
        scope: "Scope",
    ) -> "DriverT": ...
    @overload
    def provide_request_session(
        self,
        key: "AsyncDatabaseConfig[Any, Any, DriverT] | NoPoolAsyncConfig[Any, DriverT] | type[AsyncDatabaseConfig[Any, Any, DriverT] | NoPoolAsyncConfig[Any, DriverT]]",
        state: "State",
        scope: "Scope",
    ) -> "DriverT": ...
    @overload
    def provide_request_session(
        self, key: str, state: "State", scope: "Scope"
    ) -> "SyncDriverAdapterBase | AsyncDriverAdapterBase": ...
    def provide_request_session(
        self, key: "str | AnyDatabaseConfig | type[AnyDatabaseConfig]", state: "State", scope: "Scope"
    ) -> "SyncDriverAdapterBase | AsyncDriverAdapterBase":
        """Provide a database session for the specified configuration key from request scope.

        This method requires the connection to already exist in scope.
        For on-demand connection creation, use ``provide_request_session_sync`` or
        ``provide_request_session_async`` instead.

        Args:
            key: The configuration identifier (same as get_config).
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A driver session instance for the specified database configuration.
        """
        plugin_state = self._get_plugin_state(key)
        connection = get_sqlspec_scope_state(scope, plugin_state.connection_key)
        if connection is None:
            self._raise_missing_connection(plugin_state.connection_key)
        return self._create_session(plugin_state, connection, scope)

    @overload
    def provide_request_session_sync(
        self,
        key: "SyncDatabaseConfig[Any, Any, DriverT] | NoPoolSyncConfig[Any, DriverT]",
        state: "State",
        scope: "Scope",
    ) -> "DriverT": ...
    @overload
    def provide_request_session_sync(
        self,
        key: "type[SyncDatabaseConfig[Any, Any, DriverT] | NoPoolSyncConfig[Any, DriverT]]",
        state: "State",
        scope: "Scope",
    ) -> "DriverT": ...
    @overload
    def provide_request_session_sync(self, key: str, state: "State", scope: "Scope") -> "SyncDriverAdapterBase": ...
    def provide_request_session_sync(
        self,
        key: "str | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]]",
        state: "State",
        scope: "Scope",
    ) -> "SyncDriverAdapterBase | Any":
        """Provide a sync database session for the specified configuration key from request scope.

        If no connection exists in scope, one will be created from the pool and stored
        in scope for reuse. The connection will be cleaned up by the before_send handler.

        For async configurations, use ``provide_request_session_async`` instead.

        Args:
            key: The configuration identifier (same as get_config).
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A sync driver session instance for the specified database configuration.
        """
        plugin_state = self._get_plugin_state(key)
        connection = self._ensure_connection_sync(plugin_state, state, scope)
        return cast("SyncDriverAdapterBase", self._create_session(plugin_state, connection, scope))

    @overload
    async def provide_request_session_async(
        self,
        key: "AsyncDatabaseConfig[Any, Any, DriverT] | NoPoolAsyncConfig[Any, DriverT]",
        state: "State",
        scope: "Scope",
    ) -> "DriverT": ...
    @overload
    async def provide_request_session_async(
        self,
        key: "type[AsyncDatabaseConfig[Any, Any, DriverT] | NoPoolAsyncConfig[Any, DriverT]]",
        state: "State",
        scope: "Scope",
    ) -> "DriverT": ...
    @overload
    async def provide_request_session_async(
        self, key: str, state: "State", scope: "Scope"
    ) -> "AsyncDriverAdapterBase": ...
    async def provide_request_session_async(
        self,
        key: "str | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]",
        state: "State",
        scope: "Scope",
    ) -> "AsyncDriverAdapterBase | Any":
        """Provide an async database session for the specified configuration key from request scope.

        If no connection exists in scope, one will be created from the pool and stored
        in scope for reuse. The connection will be cleaned up by the before_send handler.

        For sync configurations, use ``provide_request_session`` instead.

        Args:
            key: The configuration identifier (same as get_config).
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            An async driver session instance for the specified database configuration.
        """
        plugin_state = self._get_plugin_state(key)
        connection = await self._ensure_connection_async(plugin_state, state, scope)
        return cast("AsyncDriverAdapterBase", self._create_session(plugin_state, connection, scope))

    @overload
    def provide_request_connection(
        self,
        key: "SyncDatabaseConfig[ConnectionT, Any, Any] | NoPoolSyncConfig[ConnectionT, Any] | AsyncDatabaseConfig[ConnectionT, Any, Any] | NoPoolAsyncConfig[ConnectionT, Any]",
        state: "State",
        scope: "Scope",
    ) -> "ConnectionT": ...
    @overload
    def provide_request_connection(
        self,
        key: "type[SyncDatabaseConfig[ConnectionT, Any, Any] | NoPoolSyncConfig[ConnectionT, Any] | AsyncDatabaseConfig[ConnectionT, Any, Any] | NoPoolAsyncConfig[ConnectionT, Any]]",
        state: "State",
        scope: "Scope",
    ) -> "ConnectionT": ...
    @overload
    def provide_request_connection(self, key: str, state: "State", scope: "Scope") -> Any: ...
    def provide_request_connection(
        self, key: "str | AnyDatabaseConfig | type[AnyDatabaseConfig]", state: "State", scope: "Scope"
    ) -> Any:
        """Provide a database connection for the specified configuration key from request scope.

        This method requires the connection to already exist in scope.
        For on-demand connection creation, use ``provide_request_connection_sync`` or
        ``provide_request_connection_async`` instead.

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

    @overload
    def provide_request_connection_sync(
        self,
        key: "SyncDatabaseConfig[ConnectionT, Any, Any] | NoPoolSyncConfig[ConnectionT, Any]",
        state: "State",
        scope: "Scope",
    ) -> "ConnectionT": ...
    @overload
    def provide_request_connection_sync(
        self,
        key: "type[SyncDatabaseConfig[ConnectionT, Any, Any] | NoPoolSyncConfig[ConnectionT, Any]]",
        state: "State",
        scope: "Scope",
    ) -> "ConnectionT": ...
    @overload
    def provide_request_connection_sync(self, key: str, state: "State", scope: "Scope") -> Any: ...
    def provide_request_connection_sync(
        self,
        key: "str | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]]",
        state: "State",
        scope: "Scope",
    ) -> Any:
        """Provide a sync database connection for the specified configuration key from request scope.

        If no connection exists in scope, one will be created from the pool and stored
        in scope for reuse. The connection will be cleaned up by the before_send handler.

        For async configurations, use ``provide_request_connection_async`` instead.

        Args:
            key: The configuration identifier (same as get_config).
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A database connection instance for the specified database configuration.
        """
        plugin_state = self._get_plugin_state(key)
        return self._ensure_connection_sync(plugin_state, state, scope)

    @overload
    async def provide_request_connection_async(
        self,
        key: "AsyncDatabaseConfig[ConnectionT, Any, Any] | NoPoolAsyncConfig[ConnectionT, Any]",
        state: "State",
        scope: "Scope",
    ) -> "ConnectionT": ...
    @overload
    async def provide_request_connection_async(
        self,
        key: "type[AsyncDatabaseConfig[ConnectionT, Any, Any] | NoPoolAsyncConfig[ConnectionT, Any]]",
        state: "State",
        scope: "Scope",
    ) -> "ConnectionT": ...
    @overload
    async def provide_request_connection_async(self, key: str, state: "State", scope: "Scope") -> Any: ...
    async def provide_request_connection_async(
        self,
        key: "str | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]",
        state: "State",
        scope: "Scope",
    ) -> Any:
        """Provide an async database connection for the specified configuration key from request scope.

        If no connection exists in scope, one will be created from the pool and stored
        in scope for reuse. The connection will be cleaned up by the before_send handler.

        For sync configurations, use ``provide_request_connection`` instead.

        Args:
            key: The configuration identifier (same as get_config).
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A database connection instance for the specified database configuration.
        """
        plugin_state = self._get_plugin_state(key)
        return await self._ensure_connection_async(plugin_state, state, scope)

    def _extract_extension_settings(self, config: "AnyDatabaseConfig") -> "dict[str, Any]":
        """Extract Litestar settings from config.extension_config."""
        litestar_config = cast("LitestarConfig", config.extension_config.get("litestar", {}))

        connection_key = litestar_config.get("connection_key", DEFAULT_CONNECTION_KEY)
        pool_key = litestar_config.get("pool_key", DEFAULT_POOL_KEY)
        session_key = litestar_config.get("session_key", DEFAULT_SESSION_KEY)
        commit_mode = litestar_config.get("commit_mode", DEFAULT_COMMIT_MODE)

        if not config.supports_connection_pooling and pool_key == DEFAULT_POOL_KEY:
            pool_key = f"_{DEFAULT_POOL_KEY}_{id(config)}"

        correlation_header = str(litestar_config.get("correlation_header", DEFAULT_CORRELATION_HEADER)).lower()
        configured_headers = _normalize_header_list(litestar_config.get("correlation_headers"))
        auto_trace_headers = bool(litestar_config.get("auto_trace_headers", True))

        disable_di = litestar_config.get("disable_di", False)

        return {
            "tenant_settings": TenantResolutionSettings.from_extension_config(litestar_config),
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
            "disable_di": disable_di,
            "manage_lifespan": litestar_config.get("manage_lifespan", not disable_di),
            "enable_sqlcommenter_middleware": litestar_config.get("enable_sqlcommenter_middleware", True),
        }

    def _config_state(self, config: "AnyDatabaseConfig", settings: "dict[str, Any]") -> PluginConfigState:
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
            enable_sqlcommenter_middleware=settings["enable_sqlcommenter_middleware"],
            disable_di=settings["disable_di"],
            manage_lifespan=settings["manage_lifespan"],
            tenant_settings=settings["tenant_settings"],
        )
        state.correlation_headers = tuple(settings["correlation_headers"])

        if state.manage_lifespan:
            state.lifespan_handler = lifespan_handler_maker(config, state.pool_key)
        if not state.disable_di:
            self._setup_di_handlers(state)
        return state

    def _setup_di_handlers(self, state: PluginConfigState) -> None:
        """Build the dependency providers and before-send handler for the plugin state."""
        connection_key = state.connection_key
        pool_key = state.pool_key
        commit_mode = state.commit_mode
        config = state.config

        state.connection_provider = connection_provider_maker(config, pool_key, connection_key)
        state.pool_provider = pool_provider_maker(config, pool_key)
        state.session_provider = session_provider_maker(config, connection_key)

        if commit_mode == "manual":
            state.before_send_handler = manual_handler_maker(connection_key)
        else:
            commit_on_redirect = commit_mode == "autocommit_include_redirect"
            state.before_send_handler = autocommit_handler_maker(
                connection_key, commit_on_redirect, state.extra_commit_statuses, state.extra_rollback_statuses
            )

    def _ensure_connection_sync(self, plugin_state: PluginConfigState, state: "State", scope: "Scope") -> Any:
        """Ensure a connection exists in scope, creating one from the pool if needed (sync)."""
        connection = get_sqlspec_scope_state(scope, plugin_state.connection_key)
        if connection is not None:
            return connection

        pool = state.get(plugin_state.pool_key)
        if pool is None:
            self._raise_missing_connection(plugin_state.connection_key)

        payload = TenantContext.require(plugin_state.config) if has_tenant_strategy(plugin_state.config) else None
        payload_key = tenant_payload_key(plugin_state.connection_key)
        set_sqlspec_scope_state(scope, payload_key, payload)
        try:
            with TenantContext.bind(payload):
                cm = cast("AbstractContextManager[Any]", plugin_state.config.provide_connection(pool))
                connection = cm.__enter__()
        except BaseException:
            delete_sqlspec_scope_state(scope, payload_key)
            raise
        set_sqlspec_scope_state(scope, f"{plugin_state.connection_key}_ctx", cm)
        set_sqlspec_scope_state(scope, plugin_state.connection_key, connection)
        return connection

    async def _ensure_connection_async(self, plugin_state: PluginConfigState, state: "State", scope: "Scope") -> Any:
        """Ensure a connection exists in scope, creating one from the pool if needed (async)."""
        connection = get_sqlspec_scope_state(scope, plugin_state.connection_key)
        if connection is not None:
            return connection

        pool = state.get(plugin_state.pool_key)
        if pool is None:
            self._raise_missing_connection(plugin_state.connection_key)

        payload = await _capture_connection_tenant(plugin_state.config, scope, plugin_state.connection_key)
        try:
            with TenantContext.bind(payload):
                cm = cast("AbstractAsyncContextManager[Any]", plugin_state.config.provide_connection(pool))
                connection = await cm.__aenter__()
        except BaseException:
            delete_sqlspec_scope_state(scope, tenant_payload_key(plugin_state.connection_key))
            raise
        set_sqlspec_scope_state(scope, f"{plugin_state.connection_key}_ctx", cm)
        set_sqlspec_scope_state(scope, plugin_state.connection_key, connection)
        return connection

    def _create_session(
        self, plugin_state: PluginConfigState, connection: Any, scope: "Scope"
    ) -> "SyncDriverAdapterBase | AsyncDriverAdapterBase":
        """Create a session from a connection and store it in scope."""
        session_scope_key = f"{plugin_state.session_key}_instance"

        session = get_sqlspec_scope_state(scope, session_scope_key)
        if session is not None and getattr(session, "connection", None) is connection:
            return cast("SyncDriverAdapterBase | AsyncDriverAdapterBase", session)

        session = plugin_state.config.driver_type(
            connection=connection,
            statement_config=plugin_state.config.statement_config,
            driver_features=plugin_state.config.driver_features,
        )
        set_sqlspec_scope_state(scope, session_scope_key, session)
        return cast("SyncDriverAdapterBase | AsyncDriverAdapterBase", session)

    def _registry_state(self, name: Any) -> "PluginConfigState | None":
        """Resolve plugin state from registry identity, independent of Litestar registration.

        Args:
            name: A ``bind_key``, a config instance, or a concrete config type.

        Raises:
            KeyError: If several configurations share the requested concrete type.

        Returns:
            The matching plugin state, or ``None`` when the identifier is not a
            registry identity.
        """
        if isinstance(name, str):
            for state in self._plugin_configs:
                if state.config.bind_key == name:
                    return state
            return None

        if isinstance(name, type):
            matches = [state for state in self._plugin_configs if type(state.config) is name]
            if len(matches) > 1:
                self._raise_ambiguous_config_type(name, matches)
            return matches[0] if matches else None

        for state in self._plugin_configs:
            if state.config is name:
                return state
        return None

    def _dependency_state(self, name: Any) -> PluginConfigState:
        """Resolve plugin state from a Litestar dependency key, which exists only after registration.

        Args:
            name: A ``session_key``, ``connection_key``, or ``pool_key``.

        Raises:
            KeyError: If ``name`` is not a known dependency key.

        Returns:
            The matching plugin state.
        """
        if isinstance(name, str) and any(
            name in {state.connection_key, state.pool_key, state.session_key} for state in self._plugin_configs
        ):
            if any(state.annotation is None for state in self._plugin_configs):
                self._raise_plugin_not_registered()
            return self._get_plugin_state(name)
        self._raise_config_not_found(name)
        msg = "unreachable"
        raise AssertionError(msg)

    def _get_plugin_state(
        self, key: "str | DatabaseConfigProtocol[Any, Any, Any] | type[DatabaseConfigProtocol[Any, Any, Any]]"
    ) -> PluginConfigState:
        """Get plugin state for a configuration by Litestar dependency key, instance, or annotation."""
        if isinstance(key, str):
            for state in self._plugin_configs:
                if key in {state.connection_key, state.pool_key, state.session_key}:
                    return state

        for state in self._plugin_configs:
            if state.annotation is None:
                self._raise_plugin_not_registered()
            if key in {state.config, state.annotation}:
                return state

        self._raise_config_not_found(key)
        msg = "unreachable"
        raise AssertionError(msg)

    def _get_available_keys(self) -> "list[str]":
        """Get a list of all available configuration keys for error messages."""
        keys: list[str] = []
        for state in self._plugin_configs:
            for key in (state.config.bind_key, state.connection_key, state.pool_key, state.session_key):
                if key is not None and key not in keys:
                    keys.append(key)
        return keys

    def _ensure_dependency_keys(self) -> None:
        """Validate that connection and pool keys are unique across configurations."""
        connection_keys = [state.connection_key for state in self._plugin_configs]
        pool_keys = [state.pool_key for state in self._plugin_configs]

        if len(set(connection_keys)) != len(connection_keys):
            self._raise_duplicate_connection_keys()

        if len(set(pool_keys)) != len(pool_keys):
            self._raise_duplicate_pool_keys()

    def _raise_missing_connection(self, connection_key: str) -> NoReturn:
        """Raise error when connection is not found in scope."""
        msg = f"No database connection found in scope for key '{connection_key}'. "
        msg += "Ensure the connection dependency is properly configured and available."
        raise ImproperConfigurationError(detail=msg)

    def _raise_plugin_not_registered(self) -> NoReturn:
        """Raise error when plugin state is accessed before app registration."""
        msg = (
            "SQLSpec plugin has not been registered with a Litestar application. "
            "Register the plugin via Litestar(plugins=[...]) so on_app_init runs before accessing annotations."
        )
        raise ImproperConfigurationError(msg)

    def _raise_di_handlers_not_initialized(self, session_key: str) -> NoReturn:
        """Raise error when DI handlers are missing for a DI-enabled configuration."""
        msg = (
            f"Dependency injection handlers were not initialized for configuration '{session_key}'. "
            "Handlers are created during plugin construction for configurations with dependency injection enabled."
        )
        raise ImproperConfigurationError(msg)

    def _raise_lifespan_handler_not_initialized(self, session_key: str) -> NoReturn:
        """Raise error when the lifespan handler is missing for a lifespan-managed configuration."""
        msg = (
            f"Lifespan handler was not initialized for configuration '{session_key}'. "
            "Handlers are created during plugin construction for configurations with lifespan management enabled."
        )
        raise ImproperConfigurationError(msg)

    def _raise_config_not_found(self, key: Any) -> NoReturn:
        """Raise error when configuration is not found."""
        msg = f"No database configuration found for name '{key}'. Available keys: {self._get_available_keys()}"
        raise KeyError(msg)

    def _raise_ambiguous_config_type(self, config_type: type, candidates: "list[PluginConfigState]") -> NoReturn:
        """Raise error when several configurations share one concrete config type."""
        bind_keys = [state.config.bind_key or "<unbound>" for state in candidates]
        msg = (
            f"Multiple database configurations use the type '{config_type.__name__}'. "
            f"Look one up by its bind key instead. Candidate bind keys: {bind_keys}"
        )
        raise KeyError(msg)

    def _raise_duplicate_connection_keys(self) -> None:
        """Raise error when connection keys are not unique."""
        msg = "When using multiple database configuration, each configuration must have a unique `connection_key`."
        raise ImproperConfigurationError(detail=msg)

    def _raise_duplicate_pool_keys(self) -> None:
        """Raise error when pool keys are not unique."""
        msg = "When using multiple database configuration, each configuration must have a unique `pool_key`."
        raise ImproperConfigurationError(detail=msg)


class SQLCommenterMiddleware:
    """ASGI middleware that populates SQLCommenterContext with Litestar request attributes.

    Extracts route, controller, and action from the Litestar scope and sets them
    in :class:`~sqlspec.core.sqlcommenter.SQLCommenterContext` for the
    duration of the request.
    """

    __slots__ = ("app",)

    def __init__(self, app: "ASGIApp") -> None:
        self.app = app

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if str(scope.get("type")) != "http":
            await self.app(scope, receive, send)
            return

        attrs: dict[str, str] = {"route": scope.get("path", ""), "framework": "litestar"}
        handler = scope.get("route_handler")
        if handler is not None and hasattr(handler, "fn") and hasattr(handler, "owner"):
            fn = handler.fn
            if fn is not None:
                fn_name = getattr(fn, "__name__", None)
                if isinstance(fn_name, str):
                    attrs["action"] = fn_name
            owner = handler.owner
            if owner is not None:
                owner_name = getattr(owner, "__name__", None)
                if isinstance(owner_name, str):
                    attrs["controller"] = owner_name

        previous = SQLCommenterContext.get()
        SQLCommenterContext.set(attrs)
        try:
            await self.app(scope, receive, send)
        finally:
            SQLCommenterContext.set(previous)


@dataclass(frozen=True, slots=True)
class _ConnectionCleanupTarget:
    """Configuration descriptor for ASGI residual connection cleanup."""

    connection_key: str
    session_key: str
    commit_mode: CommitMode
    extra_commit_statuses: frozenset[int]
    extra_rollback_statuses: frozenset[int]


class _OffsetPaginationSchemaPlugin(OpenAPISchemaPlugin):
    """OpenAPI schema plugin expanding OffsetPagination[T] into a concrete schema.

    Expands the dataclass container into an object schema with items, limit,
    offset, and total, independently of Litestar's dataclass introspection.
    """

    @staticmethod
    def is_plugin_supported_type(value: Any) -> bool:
        origin = getattr(value, "__origin__", value)
        return isinstance(origin, type) and issubclass(origin, OffsetPagination)

    def to_openapi_schema(self, field_definition: "FieldDefinition", schema_creator: "SchemaCreator") -> "Schema":
        from litestar.openapi.spec import OpenAPIType, Schema
        from litestar.typing import FieldDefinition

        inner_type: Any = Any
        inner_args = getattr(field_definition, "inner_types", ())
        if inner_args:
            inner_type = inner_args[0].annotation

        item_schema = schema_creator.for_field_definition(FieldDefinition.from_annotation(inner_type))

        return Schema(
            type=OpenAPIType.OBJECT,
            properties={
                "items": Schema(type=OpenAPIType.ARRAY, items=item_schema),
                "limit": Schema(type=OpenAPIType.INTEGER),
                "offset": Schema(type=OpenAPIType.INTEGER),
                "total": Schema(type=OpenAPIType.INTEGER),
            },
            required=["items", "limit", "offset", "total"],
        )


class _NumpySignatureNamespace:
    """Proxy ``numpy`` module namespace that maps ``ndarray`` to SQLSpec's decoder-safe subclass."""

    __slots__ = ("_numpy", "ndarray")

    def __init__(self, numpy_module: Any, ndarray_type: type[Any]) -> None:
        self._numpy = numpy_module
        self.ndarray = ndarray_type

    def __getattr__(self, name: str) -> Any:
        return getattr(self._numpy, name)


def _select_teardown_exception(current: BaseException | None, candidate: BaseException) -> BaseException:
    """Select the primary teardown exception to re-raise after completing all targets."""
    if current is None:
        return candidate
    if isinstance(current, Exception) and not isinstance(candidate, Exception):
        return candidate
    return current


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


def _find_correlation_middleware(middleware: "Iterable[Middleware] | None") -> "Middleware | None":
    """Return the first entry in the middleware stack that installs :class:`CorrelationMiddleware`.

    Matches :class:`CorrelationMiddleware` or a subclass, either as a bare class or wrapped in
    :class:`litestar.middleware.DefineMiddleware`. Factories such as :func:`functools.partial`,
    middleware added after the plugin's ``on_app_init`` runs, and router, controller, or route
    middleware are not detected.
    """
    for entry in middleware or ():
        candidate = entry.middleware if isinstance(entry, DefineMiddleware) else entry
        if isinstance(candidate, type) and issubclass(candidate, CorrelationMiddleware):
            return entry
    return None


def _find_cleanup_middleware(middleware: "Iterable[Middleware] | None") -> "Middleware | None":
    """Return the first entry in the middleware stack that installs :class:`ConnectionCleanupMiddleware`."""
    for entry in middleware or ():
        candidate = entry.middleware if isinstance(entry, DefineMiddleware) else entry
        if isinstance(candidate, type) and issubclass(candidate, ConnectionCleanupMiddleware):
            return entry
    return None


def _resolve_exception_handlers(request: "Request[Any, Any, Any]") -> "ExceptionHandlersMap":
    """Return the exception handlers resolved for the request's route, or the application's handlers."""
    route_handler = request.scope.get("route_handler")
    if route_handler is not None:
        return route_handler.resolve_exception_handlers()
    return request.app.exception_handlers


def _render_http_exception(
    request: "Request[Any, Any, Any]", http_exc: HTTPException, handlers: "ExceptionHandlersMap"
) -> "Response[Any]":
    """Render an HTTP exception with the handler Litestar would select for it.

    A handler for the status code wins, then a handler for the first class in the exception's
    MRO; Litestar's default exception response is used when no handler matches.
    """
    handler = handlers.get(http_exc.status_code) or next(
        (handlers[cls] for cls in type(http_exc).__mro__ if cls in handlers), None
    )
    if handler is None:
        return create_exception_response(request=request, exc=http_exc)
    return handler(request, http_exc)


def _get_litestar_numpy_array_type() -> type[Any] | None:
    """Return a mutable ndarray subclass Litestar can attach a decoder to."""
    if not NUMPY_INSTALLED:
        return None

    global _LITESTAR_NUMPY_ARRAY_TYPE
    if _LITESTAR_NUMPY_ARRAY_TYPE is None:
        import numpy as np

        class SQLSpecNumpyArray(np.ndarray):
            pass

        _LITESTAR_NUMPY_ARRAY_TYPE = SQLSpecNumpyArray
    return _LITESTAR_NUMPY_ARRAY_TYPE


def _litestar_numpy_array_predicate(target_type: Any) -> bool:
    ndarray_type = _get_litestar_numpy_array_type()
    return ndarray_type is not None and target_type is ndarray_type


def _litestar_numpy_array_dec_hook(target_type: type[Any], value: Any) -> Any:
    decoded = numpy_array_dec_hook(target_type, value)
    if NUMPY_INSTALLED:
        import numpy as np

        if isinstance(decoded, np.ndarray) and isinstance(target_type, type) and not isinstance(decoded, target_type):
            return decoded.view(target_type)
    return decoded


def _build_litestar_type_decoders() -> "list[tuple[Callable[[Any], bool], Callable[[type, Any], Any]]]":
    """Build the Litestar-specific ``type_decoders`` list.

    Decoders are predicate-tuples consumed by Litestar's request-body parsing,
    not part of sqlspec's serializer registry — so they live here rather than
    in :data:`sqlspec.utils.serializers.DEFAULT_TYPE_ENCODERS`.
    """
    decoders: list[tuple[Callable[[Any], bool], Callable[[type, Any], Any]]] = []
    if NUMPY_INSTALLED:
        decoders.append((_litestar_numpy_array_predicate, _litestar_numpy_array_dec_hook))
    uuid_utils_uuid = import_optional_attr("uuid_utils", "UUID")
    if uuid_utils_uuid is not None:
        decoders.append((lambda t: t is uuid_utils_uuid, lambda t, v: t(str(v))))
    return decoders
