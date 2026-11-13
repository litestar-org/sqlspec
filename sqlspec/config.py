"""Database configuration surfaces for SQLSpec adapters.

This module is intentionally interpreted even though compiled modules consume
its config classes. The public configuration API is stability-critical for
compiled callers: keep constructor fields, protocol attributes, migration
refresh behavior, storage capability hooks, and provider context managers
runtime-visible and backwards coherent. Move small pure helpers into compiled
modules only after proving the boundary with installed-wheel smoke coverage.
"""

import asyncio
import sys
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from contextlib import (
    AbstractAsyncContextManager,
    AbstractContextManager,
    AsyncExitStack,
    ExitStack,
    asynccontextmanager,
    contextmanager,
)
from difflib import get_close_matches
from inspect import Signature, isawaitable, iscoroutinefunction, signature
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, NotRequired, Self, TypeAlias, TypeVar, cast

from typing_extensions import TypedDict

from sqlspec.core import StatementCacheHolder, TypeCoercionCapabilities, resolve_statement_cache_size
from sqlspec.core.config_runtime import (
    build_default_statement_config,
    close_async_pool,
    close_sync_pool,
    create_async_pool,
    create_sync_pool,
    seed_runtime_driver_features,
)
from sqlspec.core.routing import ReplicaHealthBreaker, ReplicaSelector, ReplicaSpec, SelectionStrategy, build_selector
from sqlspec.exceptions import (
    DatabaseConnectionError,
    ImproperConfigurationError,
    MissingDependencyError,
    OperationalError,
    ReplicaUnavailableError,
    RoutingContextError,
)
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.loader import SQLFileLoader
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker
from sqlspec.migrations.utils import ExtensionMigrationMode, resolve_extension_migration_mode
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime
from sqlspec.typing import ConnectionT, PoolT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.routing import RoutingContext, RoutingEntry
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Generator, MutableMapping
    from uuid import UUID

    from sqlspec.core import StatementConfig
    from sqlspec.core.tenant import TenantStrategy
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.driver._routing import AsyncDriverT, AsyncRoutingSession, SyncDriverT, SyncRoutingSession
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
    from sqlspec.storage import StorageCapabilities
    from sqlspec.utils.tenant import TenantExtractorValue


__all__ = (
    "MIGRATION_CONFIG_KEYS",
    "MIGRATION_TEMPLATES_KEYS",
    "ADKConfig",
    "AsyncConfigT",
    "AsyncDatabaseConfig",
    "ConfigT",
    "ConnectionT",
    "DatabaseConfigProtocol",
    "DriverT",
    "EventsConfig",
    "ExtensionConfigs",
    "ExtensionMigrationMode",
    "FastAPIConfig",
    "FlaskConfig",
    "LifecycleConfig",
    "LitestarConfig",
    "MigrationConfig",
    "MigrationTemplates",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "OpenTelemetryConfig",
    "PoolT",
    "PrometheusConfig",
    "PythonTemplateOverride",
    "ReadReplicaGroup",
    "SQLTemplateOverride",
    "SanicConfig",
    "StarletteConfig",
    "SyncConfigT",
    "SyncDatabaseConfig",
    "TypeCoercionCapabilities",
    "resolve_extension_migration_mode",
    "validate_migration_config_keys",
)

AsyncConfigT = TypeVar("AsyncConfigT", bound="AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]")
SyncConfigT = TypeVar("SyncConfigT", bound="SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]")
ConfigT = TypeVar(
    "ConfigT",
    bound="AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]",
)

DriverT = TypeVar("DriverT", bound="SyncDriverAdapterBase | AsyncDriverAdapterBase")
ReplicaItemInput: TypeAlias = "DatabaseConfigProtocol[Any, Any, Any] | ReplicaSpec | Mapping[str, Any] | str | bool"
ReplicaGroupInput: TypeAlias = "ReadReplicaGroup[Any] | Sequence[ReplicaItemInput]"
ReplicasInput: TypeAlias = "bool | ReplicaItemInput | ReplicaGroupInput | Mapping[str, Any]"

logger = get_logger("sqlspec.config")

DRIVER_FEATURE_LIFECYCLE_HOOKS: dict[str, str | None] = {
    "on_connection_create": "connection",
    "on_connection_destroy": "connection",
    "on_pool_create": "pool",
    "on_pool_destroying": "pool",
    "on_pool_destroy": "pool",
    "on_session_start": "session",
    "on_session_end": "session",
}


class LifecycleConfig(TypedDict):
    """Lifecycle callback hooks for database adapters.

    Each hook accepts a list of callables receiving a single
    :data:`~sqlspec.observability.LifecycleContext` (``dict[str, Any]``)
    mapping containing the ``config`` key (plus ``bind_key`` and
    ``correlation_id`` when set) and event-specific payload keys.
    """

    on_connection_create: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked after a raw driver connection is created (``connection`` key)."""
    on_connection_destroy: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked before closing or destroying a driver connection (``connection`` key)."""
    on_pool_create: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked after the adapter connection pool is initialized (``pool`` key)."""
    on_pool_destroying: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked immediately before shutting down the connection pool (``pool`` key)."""
    on_pool_destroy: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked after the connection pool has finished closing (``pool`` key)."""
    on_session_start: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked when a driver session context begins (``session`` key)."""
    on_session_end: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked when a driver session context exits (``session`` key)."""
    on_query_start: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked before statement execution with ``LifecycleContext`` keys ``sql``, ``parameters``, ``driver``, ``operation``, ``is_many``, and ``is_script``."""
    on_query_complete: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked on completion with ``LifecycleContext`` keys ``sql``, ``parameters``, ``driver``, ``operation``, ``is_many``, ``is_script``, and ``rows_affected``."""
    on_error: NotRequired[list[Callable[[dict[str, Any]], Any]]]
    """Callbacks invoked on query failure with ``LifecycleContext`` keys ``exception``, ``sql``, ``parameters``, ``driver``, ``operation``, ``is_many``, and ``is_script``."""


class SQLTemplateOverride(TypedDict):
    """Overrides for the SQL migration template."""

    header: NotRequired[str]
    """First line of the generated file. Supports the migration template placeholders."""

    metadata: NotRequired["list[str]"]
    """Comment lines rendered beneath the header."""

    body: NotRequired[str]
    """Migration body containing the up and down named statements."""

    description_key: NotRequired["str | list[str]"]
    """Metadata label(s) the description is read back from. Defaults to 'Description'."""


class PythonTemplateOverride(TypedDict):
    """Overrides for the Python migration template."""

    docstring: NotRequired[str]
    """Module docstring contents. Supports the migration template placeholders."""

    body: NotRequired[str]
    """Module body defining the up and down functions."""

    imports: NotRequired["list[str]"]
    """Import lines rendered between the docstring and the body."""

    description_key: NotRequired["str | list[str]"]
    """Docstring label(s) the description is read back from. Defaults to 'Description'."""


class MigrationTemplates(TypedDict):
    """Template overrides applied when generating migration files.

    Placeholders available to every fragment: ``title``, ``version``, ``message``,
    ``description``, ``created_at``, ``author``, ``adapter``, and ``project_slug``.
    """

    sql: NotRequired[SQLTemplateOverride]
    """Overrides for generated ``.sql`` migrations."""

    py: NotRequired[PythonTemplateOverride]
    """Overrides for generated ``.py`` migrations."""

    title: NotRequired[str]
    """Title used when ``MigrationConfig.title`` is omitted."""


class MigrationConfig(TypedDict):
    """Configuration options for database migrations.

    All fields are optional with default values.
    """

    script_location: NotRequired["str | Path"]
    """Path to the migrations directory. Accepts string or Path object. Defaults to 'migrations'."""

    version_table_name: NotRequired[str]
    """Name of the table used to track applied migrations. Defaults to 'ddl_migrations'."""

    default_schema: NotRequired[str]
    """Schema applied to migration sessions before user migration SQL runs, when supported by the adapter.
    Can be overridden on individual migration files via the ``-- schema: <name>`` directive.
    """

    version_table_schema: NotRequired[str]
    """Schema that stores the migration tracking table. Defaults to default_schema when omitted."""

    project_root: NotRequired[str]
    """Path to the project root directory. Used for relative path resolution."""

    author: NotRequired[str]
    """Author recorded on generated migration files. Defaults to the detected git user."""

    enabled: NotRequired[bool]
    """Whether this configuration should be included in CLI operations. Defaults to True."""

    auto_sync: NotRequired[bool]
    """Enable automatic version reconciliation during upgrade. When enabled (default), SQLSpec automatically updates database tracking when migrations are renamed from timestamp to sequential format. Defaults to True."""

    strict_ordering: NotRequired[bool]
    """Enforce strict migration ordering. When enabled, prevents out-of-order migrations from being applied. Defaults to False."""

    include_extensions: NotRequired["list[str]"]
    """List of extension names whose migrations should be included. Extension migrations maintain separate versioning and are prefixed with 'ext_{name}_'.

    Note: Extensions with migration support (litestar, adk, events) are auto-included when
    their settings are present in ``extension_config``, as is any extension whose settings
    declare ``migrations_path``. Use ``exclude_extensions`` to opt out.
    """

    exclude_extensions: NotRequired["list[str]"]
    """
    List of extension names to exclude from automatic migration inclusion.

    When an extension is configured in ``extension_config``, its migrations are automatically
    included. Use this to prevent that for specific extensions:
    """

    transactional: NotRequired[bool]
    """Wrap migrations in transactions when supported. When enabled (default for adapters that support it), each migration runs in a transaction that is committed on success or rolled back on failure. This prevents partial migrations from leaving the database in an inconsistent state. Requires adapter support for transactional DDL. Defaults to True for PostgreSQL, SQLite, and DuckDB; False for MySQL, Oracle, and BigQuery. Individual migrations can override this with a '-- transactional: false' comment."""

    use_logger: NotRequired[bool]
    """
    Use Python logger instead of Rich console for migration output.

    When True, migration progress is logged via structlog/logging instead of being
    printed to the console with Rich formatting. This is useful for programmatic
    usage where console output is not desired.

    Can be overridden per-call via the ``use_logger`` parameter on ``migrate_up()``
    and ``migrate_down()`` methods.

    Defaults to False (Rich console output).
    """

    echo: NotRequired[bool]
    """Echo migration output to the console.

    When False, console output is suppressed. This is useful for script or CI
    environments that need quiet stdout.

    Defaults to True.
    """

    summary_only: NotRequired[bool]
    """Emit a single summary log entry for migration commands.

    When True and ``use_logger`` is enabled, per-migration output is suppressed
    in favor of a single structured summary log event.

    Defaults to False.
    """

    default_format: NotRequired["Literal['sql', 'py']"]
    """File format used by ``create-migration`` when none is requested. Defaults to 'sql'."""

    title: NotRequired[str]
    """Title rendered into generated migration files. Defaults to 'SQLSpec Migration'."""

    templates: NotRequired[MigrationTemplates]
    """Template fragment overrides applied when generating migration files."""

    schema_dump_path: NotRequired["str | Path | None"]
    """Default file path written by ``dump_schema()`` when ``output_path`` is omitted."""

    auto_dump_schema: NotRequired["bool | str | Path | None"]
    """Automatically dump the current database schema after ``upgrade()`` applies migrations.

    ``True`` writes to ``schema_dump_path`` (or ``<script_location>/schema.sql``); a path writes there. The dump runs
    after the migrations have committed, so a dump failure is logged as a warning and does not fail the upgrade.
    """


MIGRATION_CONFIG_KEYS: "frozenset[str]" = MigrationConfig.__required_keys__ | MigrationConfig.__optional_keys__
MIGRATION_TEMPLATES_KEYS: "frozenset[str]" = MigrationTemplates.__required_keys__ | MigrationTemplates.__optional_keys__
_TEMPLATE_FRAGMENT_KEYS: "dict[str, frozenset[str]]" = {
    "sql": SQLTemplateOverride.__required_keys__ | SQLTemplateOverride.__optional_keys__,
    "py": PythonTemplateOverride.__required_keys__ | PythonTemplateOverride.__optional_keys__,
}


def validate_migration_config_keys(migration_config: "Mapping[str, Any]") -> None:
    """Reject migration configuration keys that SQLSpec does not read.

    Args:
        migration_config: Migration configuration mapping to check.

    Raises:
        ImproperConfigurationError: If the mapping contains an unrecognized key.
    """
    lines = _report_unknown_keys(migration_config, MIGRATION_CONFIG_KEYS, "", "")
    templates = migration_config.get("templates")
    if templates is not None:
        lines.extend(_report_template_keys(templates))
    if lines:
        raise ImproperConfigurationError(" ".join(lines))


class FlaskConfig(TypedDict):
    """Configuration options for Flask SQLSpec extension.

    All fields are optional with sensible defaults. Use in extension_config["flask"]:
    """

    tenant_extractor: NotRequired[Callable[..., "TenantExtractorValue"]]
    """Return the authorized tenant payload, string, UUID, or None (no tenant from this source) for the request. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start. Accepts zero arguments or the Flask request."""

    connection_key: NotRequired[str]
    """Key for storing the connection in Flask g once it is checked out on first access. Default: auto-generated from session_key."""

    session_key: NotRequired[str]
    """Key for accessing session via plugin.get_session(). Default: 'db_session'."""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'.
    - manual: No automatic commits, user handles explicitly
    - autocommit: Commits on 2xx status, rollback otherwise
    - autocommit_include_redirect: Commits on 2xx-3xx status, rollback otherwise
    """

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: None."""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: None."""

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Flask extension will not register request hooks for managing
    database connections and sessions. Users are responsible for managing the
    database lifecycle manually via their own DI solution.
    """

    enable_sqlcommenter_middleware: NotRequired[bool]
    """Control automatic SQLCommenter context population. Default: True.
    When the driver's :class:`~sqlspec.core.statement.StatementConfig` has
    ``enable_sqlcommenter=True``, request attributes are populated automatically.
    Set to ``False`` to explicitly disable this behavior.
    """

    enable_correlation_middleware: NotRequired[bool]
    """Enable request correlation ID middleware. Default: True"""

    correlation_header: NotRequired[str]
    """HTTP header to read the request correlation ID from when middleware is enabled. Default: ``X-Request-ID``"""

    correlation_headers: NotRequired[tuple[str, ...] | list[str]]
    """Additional HTTP headers to read as correlation ID fallbacks."""

    auto_trace_headers: NotRequired[bool]
    """Read standard trace context headers as correlation ID fallbacks. Default: True."""

    read_session_key: NotRequired[str]
    """Request key for a read session."""
    read_connection_key: NotRequired[str]
    """Request key for a read connection."""
    routing_session_key: NotRequired[str]
    """Request key for a routing session."""
    default_routing_mode: NotRequired[Literal["primary", "auto"]]
    """Default request routing mode."""
    enable_routing_middleware: NotRequired[bool]
    """Install request-scoped routing middleware."""
    routing_header: NotRequired[str]
    """Trusted application routing header name."""
    write_pin_cookie: NotRequired[str | None]
    """Optional application write-pin cookie name."""


class LitestarConfig(TypedDict):
    """Configuration options for Litestar SQLSpec plugin.

    All fields are optional with sensible defaults.
    """

    tenant_extractor: NotRequired["Callable[[MutableMapping[str, Any]], Any]"]
    """Trusted scope callable returning a tenant identity, payload, None (no tenant from this source), or an awaitable. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start."""

    tenant_user_attr: NotRequired[str]
    """Verified direct tenant attribute or mapping key. Default: 'tenant_id'."""

    tenant_memberships_attr: NotRequired[str]
    """Verified membership attribute or mapping key. Default: 'tenant_ids'."""

    tenant_variables_attr: NotRequired[str]
    """Extra session variables attribute or mapping key. Default: 'tenant_variables'."""

    tenant_jwt_claim: NotRequired[str]
    """Direct tenant key in verified auth claims or extras. Default: 'tenant_id'."""

    tenant_path_parameter: NotRequired[str | None]
    """Route parameter checked against verified memberships. Default: 'tenant_id'."""

    tenant_session_key: NotRequired[str | None]
    """Session selector checked against verified memberships. Default: 'tenant_id'."""

    tenant_header: NotRequired[str | None]
    """Optional header selector checked against verified memberships. Default: None."""

    migrations_path: NotRequired[str | Path]
    """Directory containing this extension's migrations, or a ``'<dotted.module>:<subdir>'`` specification.

    Overrides the default ``sqlspec.extensions.<name>`` lookup. Setting this auto-includes the
    extension in ``migration_config["include_extensions"]``.
    """

    session_table: NotRequired["bool | str"]
    """Enable session table for server-side session storage.

    - ``True``: Use default table name ('litestar_session')
    - ``"custom_name"``: Use custom table name

    When set, litestar extension migrations are auto-included to create the session table.
    If you're only using litestar for DI/connection management (not session storage),
    leave this unset to skip the migrations.
    """

    connection_key: NotRequired[str]
    """Key for storing connection in ASGI scope. Default: 'db_connection'"""

    pool_key: NotRequired[str]
    """Key for storing connection pool in application state. Default: 'db_pool'"""

    session_key: NotRequired[str]
    """Key for storing session in ASGI scope. Default: 'db_session'"""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'"""

    enable_correlation_middleware: NotRequired[bool]
    """Enable request correlation ID middleware. Default: True"""

    correlation_header: NotRequired[str]
    """HTTP header to read the request correlation ID from when middleware is enabled. Default: ``X-Request-ID``"""

    correlation_headers: NotRequired[tuple[str, ...] | list[str]]
    """Additional HTTP headers to read as correlation ID fallbacks."""

    auto_trace_headers: NotRequired[bool]
    """Read standard trace context headers as correlation ID fallbacks. Default: True."""

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: set()"""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: set()"""

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Litestar plugin will not register dependency providers for database
    connections, pools, and sessions, or the per-request handler that commits and closes
    request connections. Pool startup and shutdown follow ``manage_lifespan``, which
    defaults to False when ``disable_di`` is True.
    """

    manage_lifespan: NotRequired[bool]
    """Register the plugin's pool lifespan handler. Default: the inverse of ``disable_di``.
    When True, the Litestar plugin creates the configuration's pool on application startup,
    stores it in application state under ``pool_key``, and closes it on shutdown, whether or
    not ``disable_di`` is set. Set to True alongside ``disable_di=True`` to keep pool lifecycle
    management while another DI solution provides connections and sessions. When False, the
    application creates and closes the pool itself; with dependency injection enabled, the
    plugin's providers still read the pool from application state under ``pool_key``.
    """

    enable_sqlcommenter_middleware: NotRequired[bool]
    """Control automatic SQLCommenter middleware registration. Default: True.
    When the driver's :class:`~sqlspec.core.statement.StatementConfig` has
    ``enable_sqlcommenter=True``, the middleware is registered automatically.
    Set to ``False`` to explicitly disable middleware registration even when
    SQLCommenter is enabled on the driver config.
    """

    manage_schema: NotRequired[bool]
    """Apply additive session-table reconciliation. Default: True."""

    create_schema: NotRequired[bool]
    """Create a missing session table during managed reconciliation. Default: True."""

    migration_mode: NotRequired[ExtensionMigrationMode]
    """Extension migration strategy (``'versioned'`` or ``'reconcile'``). Default: ``'reconcile'``."""

    read_session_key: NotRequired[str]
    """Request key for a read session."""
    read_connection_key: NotRequired[str]
    """Request key for a read connection."""
    routing_session_key: NotRequired[str]
    """Request key for a routing session."""
    default_routing_mode: NotRequired[Literal["primary", "auto"]]
    """Default request routing mode."""
    enable_routing_middleware: NotRequired[bool]
    """Install request-scoped routing middleware."""
    routing_header: NotRequired[str]
    """Trusted application routing header name."""
    write_pin_cookie: NotRequired[str | None]
    """Optional application write-pin cookie name."""
    read_pool_key: NotRequired[str]
    """Request key for the selected read pool."""


class StarletteConfig(TypedDict):
    """Configuration options for Starlette SQLSpec extension.

    All fields are optional with sensible defaults. Use in extension_config["starlette"]:
    """

    tenant_extractor: NotRequired[Callable[[Any], Any]]
    """Trusted scope extractor returning a tenant payload, string, UUID, None (no tenant from this source), or an awaitable. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start."""

    connection_key: NotRequired[str]
    """Key for storing the connection in request.state once it is checked out on first access. Default: 'db_connection'"""

    pool_key: NotRequired[str]
    """Key for storing connection pool in app.state. Default: 'db_pool'"""

    session_key: NotRequired[str]
    """Key for storing session in request.state. Default: 'db_session'"""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'

    - manual: No automatic commit/rollback
    - autocommit: Commit on 2xx, rollback otherwise
    - autocommit_include_redirect: Commit on 2xx-3xx, rollback otherwise
    """

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: set()"""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: set()"""

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Starlette/FastAPI extension will not add middleware for managing
    database connections and sessions. Users are responsible for managing the
    database lifecycle manually via their own DI solution.
    """

    enable_sqlcommenter_middleware: NotRequired[bool]
    """Control automatic SQLCommenter middleware registration. Default: True.
    When the driver's :class:`~sqlspec.core.statement.StatementConfig` has
    ``enable_sqlcommenter=True``, the middleware is registered automatically.
    Set to ``False`` to explicitly disable middleware registration.
    """

    sqlcommenter_framework: NotRequired[str]
    """Framework name for SQLCommenter attributes. Default: 'starlette'.
    Set to 'fastapi' when using FastAPI.
    """

    enable_correlation_middleware: NotRequired[bool]
    """Enable request correlation ID middleware. Default: True"""

    correlation_header: NotRequired[str]
    """HTTP header to read the request correlation ID from when middleware is enabled. Default: ``X-Request-ID``"""

    correlation_headers: NotRequired[tuple[str, ...] | list[str]]
    """Additional HTTP headers to read as correlation ID fallbacks."""

    auto_trace_headers: NotRequired[bool]
    """Read standard trace context headers as correlation ID fallbacks. Default: True."""

    read_session_key: NotRequired[str]
    """Request key for a read session."""
    read_connection_key: NotRequired[str]
    """Request key for a read connection."""
    routing_session_key: NotRequired[str]
    """Request key for a routing session."""
    default_routing_mode: NotRequired[Literal["primary", "auto"]]
    """Default request routing mode."""
    enable_routing_middleware: NotRequired[bool]
    """Install request-scoped routing middleware."""
    routing_header: NotRequired[str]
    """Trusted application routing header name."""
    write_pin_cookie: NotRequired[str | None]
    """Optional application write-pin cookie name."""
    read_pool_key: NotRequired[str]
    """Request key for the selected read pool."""


class FastAPIConfig(StarletteConfig):
    """Configuration options for FastAPI SQLSpec extension.

    All fields are optional with sensible defaults. Use in ``extension_config["fastapi"]``.
    SQLCommenter defaults the framework attribute to ``"fastapi"``.
    """


class SanicConfig(TypedDict):
    """Configuration options for Sanic SQLSpec extension.

    All fields are optional with sensible defaults. Use in ``extension_config["sanic"]``.
    """

    tenant_extractor: NotRequired[Callable[[Any], "TenantExtractorValue | Awaitable[TenantExtractorValue]"]]
    """Return the authorized tenant payload, string, UUID, or None (no tenant from this source), optionally as an awaitable. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start."""

    connection_key: NotRequired[str]
    """Key for storing the connection in request.ctx once it is checked out on first access. Default: 'db_connection'"""

    pool_key: NotRequired[str]
    """Key for storing connection pool in app.ctx. Default: 'db_pool'"""

    session_key: NotRequired[str]
    """Key for storing session in request.ctx. Default: 'db_session'"""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'

    - manual: No automatic commit/rollback
    - autocommit: Commit on 2xx, rollback otherwise
    - autocommit_include_redirect: Commit on 2xx-3xx, rollback otherwise
    """

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: set()"""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: set()"""

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Sanic extension will not register request middleware for
    managing database connections and sessions. Users are responsible for
    managing the database lifecycle manually via their own DI solution.
    """

    enable_correlation_middleware: NotRequired[bool]
    """Enable request correlation ID middleware. Default: True."""

    correlation_header: NotRequired[str]
    """HTTP header to read the request correlation ID from when middleware is enabled. Default: ``X-Request-ID``."""

    correlation_headers: NotRequired[tuple[str, ...] | list[str]]
    """Additional HTTP headers to read as correlation ID fallbacks."""

    auto_trace_headers: NotRequired[bool]
    """Read standard trace context headers as correlation ID fallbacks. Default: True."""

    enable_sqlcommenter_middleware: NotRequired[bool]
    """Control automatic SQLCommenter middleware registration. Default: True.
    When the driver's :class:`~sqlspec.core.statement.StatementConfig` has
    ``enable_sqlcommenter=True``, the middleware is registered automatically.
    Set to ``False`` to explicitly disable middleware registration.
    """

    sqlcommenter_framework: NotRequired[str]
    """Framework name for SQLCommenter attributes. Default: 'sanic'."""

    read_session_key: NotRequired[str]
    """Request key for a read session."""
    read_connection_key: NotRequired[str]
    """Request key for a read connection."""
    routing_session_key: NotRequired[str]
    """Request key for a routing session."""
    default_routing_mode: NotRequired[Literal["primary", "auto"]]
    """Default request routing mode."""
    enable_routing_middleware: NotRequired[bool]
    """Install request-scoped routing middleware."""
    routing_header: NotRequired[str]
    """Trusted application routing header name."""
    write_pin_cookie: NotRequired[str | None]
    """Optional application write-pin cookie name."""
    read_pool_key: NotRequired[str]
    """Request key for the selected read pool."""


class ADKConfig(TypedDict):
    """Configuration options for ADK session and memory store extension.

    All fields are optional with sensible defaults. Use in extension_config["adk"]:

    Configuration supports three deployment scenarios:
        1. SQLSpec manages everything (runtime + migrations)
        2. SQLSpec runtime only (external migration tools like Alembic/Flyway)
        3. Selective features (sessions OR memory, not both)
    """

    migrations_path: NotRequired[str | Path]
    """Directory containing this extension's migrations, or a ``'<dotted.module>:<subdir>'`` specification.

    Overrides the default ``sqlspec.extensions.<name>`` lookup. Setting this auto-includes the
    extension in ``migration_config["include_extensions"]``.
    """

    manage_schema: NotRequired[bool]
    """Apply additive target-schema reconciliation. Default: True."""

    create_schema: NotRequired[bool]
    """Create missing ADK tables during managed reconciliation. Default: True."""

    enable_sessions: NotRequired[bool]
    """Enable the session store at runtime and in packaged migrations. Default: True.

    When False the session service is unavailable, session store operations are
    disabled, and packaged ADK migrations emit no session, event, state, or
    metadata DDL.
    """

    enable_memory: NotRequired[bool]
    """Enable the memory store at runtime and in packaged migrations. Default: True.

    When False the memory service is unavailable, memory store operations are
    disabled, and packaged ADK migrations emit no memory table DDL or PostgreSQL
    vector extension statement.
    """

    session_table: NotRequired[str]
    """Name of the sessions table. Default: 'adk_session'"""

    events_table: NotRequired[str]
    """Name of the events table. Default: 'adk_event'"""

    app_state_table: NotRequired[str]
    """Name of the app-scoped state table. Default: 'adk_app_state'"""

    user_state_table: NotRequired[str]
    """Name of the user-scoped state table. Default: 'adk_user_state'"""

    metadata_table: NotRequired[str]
    """Name of the internal metadata table. Default: 'adk_internal_metadata'"""

    memory_table: NotRequired[str]
    """Name of the memory entries table. Default: 'adk_memory'"""

    artifact_table: NotRequired[str]
    """Name of the artifact metadata table. Default: 'adk_artifact'"""

    artifact_storage_uri: NotRequired[str]
    """
    Base URI for artifact content storage.

    Points to a ``sqlspec/storage/`` backend where artifact binary content
    is stored. Can be a direct URI (``s3://bucket/path``, ``file:///path``)
    or a registered alias in the storage registry.
    """

    memory_use_fts: NotRequired[bool]
    """Enable full-text search when supported. Default: False.

    When True, adapters will use their native FTS capabilities where available:
    - PostgreSQL: to_tsvector/to_tsquery with GIN index
    - SQLite: FTS5 virtual table
    - DuckDB: FTS extension with match_bm25
    - Oracle: CONTAINS() with CTXSYS.CONTEXT index
    - Spanner: TOKENIZE_FULLTEXT with search index
    - MySQL: MATCH...AGAINST with FULLTEXT index

    When False, adapters use simple LIKE/ILIKE queries (works without indexes).
    """

    memory_max_results: NotRequired[int]
    """Maximum number of results for memory search queries. Default: 20.

    Limits the number of memory entries returned by search_memory().
    Can be overridden per-query via the limit parameter.
    """

    owner_id_column: NotRequired[str]
    """
    Optional owner ID column definition to link sessions/memories to a user, tenant, team, or other entity.

    Format: "column_name TYPE [NOT NULL] REFERENCES table(column) [options...]"

    The entire definition is passed through to DDL verbatim. We only parse
    the column name (first word) for use in INSERT/SELECT statements.

    This column is added to both session and memory tables for consistent
    multi-tenant isolation.

    Supports:
        - Foreign key constraints: REFERENCES table(column)
        - Nullable or NOT NULL
        - CASCADE options: ON DELETE CASCADE, ON UPDATE CASCADE
        - Dialect-specific options (DEFERRABLE, ENABLE VALIDATE, etc.)
        - Plain columns without FK (just extra column storage)
    """

    migration_mode: NotRequired[ExtensionMigrationMode]
    """Extension migration strategy (``'versioned'`` or ``'reconcile'``). Default: ``'reconcile'``."""

    read_config_enabled: NotRequired[bool]
    """Enable optional read configuration for ADK lookups."""
    read_bind_group: NotRequired[str | None]
    """Named replica group used for ADK reads."""


class EventsConfig(TypedDict):
    """Configuration options for the events extension.

    Use in ``extension_config["events"]``.
    """

    migrations_path: NotRequired[str | Path]
    """Directory containing this extension's migrations, or a ``'<dotted.module>:<subdir>'`` specification.

    Overrides the default ``sqlspec.extensions.<name>`` lookup. Setting this auto-includes the
    extension in ``migration_config["include_extensions"]``.
    """

    manage_schema: NotRequired[bool]
    """Apply additive target-schema reconciliation. Default: True."""

    create_schema: NotRequired[bool]
    """Create the queue table during managed reconciliation. Default: True."""

    backend: NotRequired[Literal["notify", "notify_queue", "poll_queue", "aq", "txeventq"]]
    """Backend implementation. PostgreSQL adapters default to 'notify', others to 'poll_queue'.

    - notify: Transient PostgreSQL LISTEN/NOTIFY wakeup; not a durable event ledger
    - notify_queue: Durable table queue with PostgreSQL LISTEN/NOTIFY wakeups
    - poll_queue: Durable table queue discovered by polling
    - aq: Oracle Advanced Queuing
    - txeventq: Oracle Transactional Event Queues
    """

    queue_table: NotRequired[str]
    """Name of the fallback queue table. Defaults to 'sqlspec_event_queue'."""

    lease_seconds: NotRequired[int]
    """Lease duration for claimed events before they can be retried. Defaults to 30 seconds."""

    retention_seconds: NotRequired[int]
    """Retention window for acknowledged events before cleanup. Defaults to 86400 (24 hours)."""

    poll_interval: NotRequired[float]
    """Compatibility alias for event_poll_interval. Defaults to 1.0."""

    event_poll_interval: NotRequired[float]
    """Durable event reconciliation interval in seconds. Takes precedence over poll_interval."""

    select_for_update: NotRequired[bool]
    """Use SELECT FOR UPDATE locking when claiming events. Defaults to False."""

    skip_locked: NotRequired[bool]
    """Use SKIP LOCKED for non-blocking event claims. Defaults to False."""

    shard_count: NotRequired[int]
    """Number of hash shards for queue table and index prefixing (e.g. Cloud Spanner). Defaults to 1."""

    migration_mode: NotRequired[ExtensionMigrationMode]
    """Extension migration strategy (``'versioned'`` or ``'reconcile'``). Default: ``'reconcile'``."""


class OpenTelemetryConfig(TypedDict):
    """Configuration options for OpenTelemetry integration.

    Use in ``extension_config["otel"]``.
    """

    enabled: NotRequired[bool]
    """Enable the extension. Default: True."""

    enable_spans: NotRequired[bool]
    """Enable span emission (set False to disable while keeping other settings)."""

    resource_attributes: NotRequired[dict[str, Any]]
    """Additional resource attributes passed to the tracer provider factory."""

    tracer_provider: NotRequired[Any]
    """Tracer provider instance to reuse. Mutually exclusive with ``tracer_provider_factory``."""

    tracer_provider_factory: NotRequired[Callable[[], Any]]
    """Factory returning a tracer provider. Invoked lazily when spans are needed."""


class PrometheusConfig(TypedDict):
    """Configuration options for Prometheus metrics.

    Use in ``extension_config["prometheus"]``.
    """

    enabled: NotRequired[bool]
    """Enable the extension. Default: True."""

    namespace: NotRequired[str]
    """Prometheus metric namespace. Default: ``"sqlspec"``."""

    subsystem: NotRequired[str]
    """Prometheus metric subsystem. Default: ``"driver"``."""

    registry: NotRequired[Any]
    """Custom Prometheus registry (defaults to the global registry)."""

    label_names: NotRequired[tuple[str, ...]]
    """Labels applied to metrics. Default: ("driver", "operation")."""

    duration_buckets: NotRequired[tuple[float, ...]]
    """Histogram buckets for query duration (seconds)."""


ExtensionConfigs: TypeAlias = dict[
    str,
    dict[str, Any]
    | LitestarConfig
    | FastAPIConfig
    | StarletteConfig
    | SanicConfig
    | FlaskConfig
    | ADKConfig
    | EventsConfig
    | OpenTelemetryConfig
    | PrometheusConfig,
]


class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the stability-critical config contract.

    Compiled callers rely on these attributes and methods remaining
    runtime-visible while ``sqlspec.config`` stays interpreted. Changes to
    migration setup, pool/session provider behavior, storage capabilities, or
    observability bootstrap must preserve this contract or move behind a
    separately verified compiled helper boundary.
    """

    __slots__ = (
        "_is_replica",
        "_migration_commands",
        "_migration_config",
        "_migration_loader",
        "_migration_lock",
        "_observability_runtime",
        "_replica_groups",
        "_storage_capabilities",
        "_tenant_key",
        "bind_key",
        "connection_config",
        "connection_instance",
        "driver_features",
        "extension_config",
        "observability_config",
        "statement_config",
    )

    _tenant_key: Any
    _is_replica: bool
    _replica_groups: "dict[str, ReadReplicaGroup[Any]]"
    _migration_loader: "SQLFileLoader | None"
    _migration_commands: "SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] | None"
    _migration_lock: "threading.RLock"
    _migration_config: "dict[str, Any] | MigrationConfig"
    driver_type: "ClassVar[type[Any]]"
    connection_type: "ClassVar[type[Any]]"
    migration_tracker_type: "ClassVar[type[Any]]"
    _connection_context_class: "ClassVar[type[Any]]"
    _session_factory_class: "ClassVar[type[Any]]"
    _session_context_class: "ClassVar[type[Any]]"
    _default_statement_config: "ClassVar[StatementConfig]"
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_arrow_streaming: "ClassVar[bool]" = False
    supports_native_row_streaming: "ClassVar[bool]" = False
    supports_migration_schemas: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    supports_reliable_rowcount: "ClassVar[bool]" = True
    supports_tenant_strategy: "ClassVar[bool]" = False
    supports_tenant_session_baseline: "ClassVar[bool]" = False
    tenant_strategy_shorthands: "ClassVar[frozenset[str]]" = frozenset()
    tenant_strategy_type: "ClassVar[type[Any] | None]" = None
    tenant_lease_type: "ClassVar[type[Any] | None]" = None
    default_storage_profile: "ClassVar[str | None]" = None
    storage_partition_strategies: "ClassVar[tuple[str, ...]]" = ("fixed",)
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="native"
    )
    adapter_name: "ClassVar[str | None]" = None
    events_store_class: "ClassVar[type[Any] | None]" = None
    adk_store_class: "ClassVar[type[Any] | None]" = None
    adk_memory_store_class: "ClassVar[type[Any] | None]" = None
    litestar_store_class: "ClassVar[type[Any] | None]" = None
    bind_key: "str | None"
    statement_config: "StatementConfig"
    connection_config: "dict[str, Any]"
    connection_instance: "PoolT | None"
    extension_config: "ExtensionConfigs"
    driver_features: "dict[str, Any]"
    _storage_capabilities: "StorageCapabilities | None"
    observability_config: "ObservabilityConfig | None"
    _observability_runtime: "ObservabilityRuntime | None"

    def derive_pool(
        self,
        *,
        connection_config_overrides: "Mapping[str, Any] | None" = None,
        driver_features_overrides: "Mapping[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        bind_key: "str | None" = None,
        is_replica: bool = False,
        **overrides: Any,
    ) -> Self:
        """Clone this configuration with overridden connection/pool or feature settings and ``connection_instance=None``."""
        new_conn_cfg: dict[str, Any] = dict(self.connection_config)
        connection_overrides: dict[str, Any] = dict(connection_config_overrides or {})
        new_driver_features: dict[str, Any] = {
            key: value
            for key, value in self.driver_features.items()
            if key not in {"_statement_cache_holder", "storage_capabilities"}
        }
        pool_session_cb = getattr(self, "_pool_session_callback", None)
        if pool_session_cb is not None and "session_callback" not in new_conn_cfg:
            new_conn_cfg["session_callback"] = pool_session_cb
        user_conn_hook = getattr(self, "_user_connection_hook", None)
        if user_conn_hook is not None and "on_connection_create" not in new_driver_features:
            new_driver_features["on_connection_create"] = user_conn_hook
        if connection_config_overrides:
            new_conn_cfg.update(connection_config_overrides)
        if driver_features_overrides:
            new_driver_features.update(driver_features_overrides)
        for key, val in overrides.items():
            if key in self.driver_features or key in DRIVER_FEATURE_LIFECYCLE_HOOKS:
                new_driver_features[key] = val
            else:
                new_conn_cfg[key] = val
                connection_overrides[key] = val
        new_conn_cfg = self._prepare_derived_connection_config(new_conn_cfg, connection_overrides)
        derived = cast("Callable[..., Self]", type(self))(
            is_replica=is_replica,
            replicas=None,
            connection_config=new_conn_cfg,
            connection_instance=None,
            migration_config=dict(self.migration_config) if migration_config is None else migration_config,
            statement_config=self.statement_config if statement_config is None else statement_config,
            driver_features=new_driver_features,
            bind_key=self.bind_key if bind_key is None else bind_key,
            extension_config=cast("ExtensionConfigs", dict(self.extension_config))
            if extension_config is None
            else extension_config,
            observability_config=self.observability_config if observability_config is None else observability_config,
        )

        if bind_key is None or bind_key == self.bind_key:
            derived._tenant_key = self._tenant_key
        return derived

    def _allow_replica_fallback(self, _error: Exception) -> bool:
        """Return whether a failed reader checkout may fall back to this primary."""
        return True

    def _prepare_derived_connection_config(
        self, config: dict[str, Any], overrides: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Normalize merged clone options while preserving explicit overrides."""
        return config

    @property
    def is_replica(self) -> bool:
        """Whether this configuration represents a read replica."""
        return self._is_replica

    @property
    def has_replicas(self) -> bool:
        """Whether this configuration owns any replica groups."""
        return bool(self._replica_groups)

    @property
    def supports_read_routing(self) -> bool:
        """Whether replica pools or an adapter-specific read provider are available."""
        return self.has_replicas or type(self).provide_read_session not in {
            DatabaseConfigProtocol.provide_read_session,
            NoPoolSyncConfig.provide_read_session,
            NoPoolAsyncConfig.provide_read_session,
            SyncDatabaseConfig.provide_read_session,
            AsyncDatabaseConfig.provide_read_session,
        }

    @property
    def replica_groups(self) -> "Mapping[str, ReadReplicaGroup[Any]]":
        """Return a read-only view of named replica groups."""
        return MappingProxyType(self._replica_groups)

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(spec_input, bool):
            msg = f"{type(self).__name__} does not support replicas=True shorthand; pass explicit replica endpoints or overrides"
            raise ImproperConfigurationError(msg)
        if isinstance(spec_input, str):
            key = next(
                (
                    key
                    for key in ("dsn", "conninfo", "uri", "url", "database")
                    if isinstance(self.connection_config.get(key), str)
                ),
                "dsn",
            )
            return {"connection_config_overrides": {key: spec_input}}
        return dict(spec_input)

    def _normalize_replica(self, item: ReplicaItemInput) -> ReplicaSpec:
        spec = item if isinstance(item, ReplicaSpec) else ReplicaSpec(item)
        config = spec.config
        if not isinstance(config, DatabaseConfigProtocol):
            config = self.derive_pool(is_replica=True, **self._build_replica_spec_overrides(config))
        if config is self or config.has_replicas or config.is_async != self.is_async:
            msg = "Replica configurations must be distinct, unnested, and use the primary execution mode"
            raise ImproperConfigurationError(msg)
        config._is_replica = True
        if config.bind_key is None or config.bind_key == self.bind_key:
            config._tenant_key = self._tenant_key
        return ReplicaSpec(config, weight=spec.weight, name=spec.name, max_staleness_seconds=spec.max_staleness_seconds)

    def _normalize_replicas(self, replicas: "ReplicasInput | None") -> "dict[str, ReadReplicaGroup[Any]]":
        if not replicas:
            return {}
        if self._is_replica:
            msg = "A replica configuration cannot declare nested replicas"
            raise ImproperConfigurationError(msg)
        groups: Mapping[Any, Any]
        if isinstance(replicas, Mapping):
            grouped = all(
                isinstance(value, ReadReplicaGroup)
                or (isinstance(value, Sequence) and not isinstance(value, (str, bytes, Mapping)))
                for value in replicas.values()
            )
            if grouped:
                groups = replicas
            elif any(isinstance(value, ReadReplicaGroup) for value in replicas.values()):
                msg = "Cannot mix ReadReplicaGroup entries with scalar connection override keys in replicas mapping"
                raise ImproperConfigurationError(msg)
            else:
                groups = {"default": [replicas]}
        elif isinstance(replicas, ReadReplicaGroup) or (
            isinstance(replicas, Sequence) and not isinstance(replicas, (str, bytes))
        ):
            groups = {"default": replicas}
        else:
            groups = {"default": [replicas]}
        result: dict[str, ReadReplicaGroup[Any]] = {}
        for name, items in groups.items():
            if not isinstance(name, str) or not name:
                msg = "Replica group names must be nonempty strings"
                raise ImproperConfigurationError(msg)
            if isinstance(items, ReadReplicaGroup):
                for spec in items.specs:
                    self._normalize_replica(spec)
                result[name] = items
            else:
                result[name] = ReadReplicaGroup([self._normalize_replica(item) for item in items], name=name)
        return result

    def _resolve_read_group(self, bind_group: str | None) -> "tuple[ReadReplicaGroup[Any] | None, RoutingEntry | None]":
        entry = RoutingContext.get_entry(self.bind_key)
        if bind_group is None and entry is not None and (entry.mode == "primary" or entry.pinned_by_write):
            return None, entry
        effective_group = bind_group if bind_group is not None else entry.bind_group if entry is not None else None
        if not self._replica_groups and effective_group is None:
            return None, entry
        group_key = effective_group if effective_group is not None else "default"
        if group_key in self._replica_groups:
            return self._replica_groups[group_key], entry
        if effective_group is None and (entry is None or entry.mode != "replica"):
            return None, entry
        msg = f"Unknown replica bind_group {group_key!r}; available groups: {sorted(self._replica_groups)}"
        raise RoutingContextError(msg)

    def provide_routing_session(
        self,
        *,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
    ) -> "SyncRoutingSession[Any] | AsyncRoutingSession[Any]":
        """Construct a lazy routing facade for this configuration.

        Args:
            bind_group: Replica group used for eligible reads.
            statement_config: Statement configuration override.
            tenant: Tenant identity or payload for physical checkouts.
            bypass_tenant: Explicitly bypass tenant setup.

        Returns:
            A facade matching this configuration's execution mode.
        """
        from sqlspec.driver._routing import AsyncRoutingSession, SyncRoutingSession

        if self.is_async:
            async_config = cast("AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]", self)
            return AsyncRoutingSession(
                async_config,
                bind_group=bind_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
            )
        sync_config = cast("SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]", self)
        return SyncRoutingSession(
            sync_config,
            bind_group=bind_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
        )

    def provide_read_connection(
        self, *args: Any, bind_group: str | None = None, **kwargs: Any
    ) -> "AbstractContextManager[ConnectionT] | AbstractAsyncContextManager[ConnectionT]":
        """Provide a replica connection, honoring scoped primary pins."""
        return cast(
            "AbstractContextManager[ConnectionT] | AbstractAsyncContextManager[ConnectionT]",
            self._provide_read(False, args, kwargs, bind_group),
        )

    def provide_read_session(
        self,
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractContextManager[DriverT] | AbstractAsyncContextManager[DriverT]":
        """Provide a read session while retaining the selected config's tenant lease."""
        kwargs.update(statement_config=statement_config, tenant=tenant, bypass_tenant=bypass_tenant)
        return cast(
            "AbstractContextManager[DriverT] | AbstractAsyncContextManager[DriverT]",
            self._provide_read(True, args, kwargs, bind_group),
        )

    def _provide_read(
        self, session: bool, args: tuple[Any, ...], kwargs: dict[str, Any], bind_group: str | None
    ) -> Any:
        group, entry = self._resolve_read_group(bind_group)
        if group is not None:
            provider = group.provide_session if session else group.provide_connection
            return provider(
                *args,
                primary=None if entry is not None and entry.mode == "replica" else self,
                causal_token=None if entry is None else entry.causal_token,
                max_staleness_seconds=None if entry is None else entry.max_staleness_seconds,
                **kwargs,
            )
        if entry is not None and entry.mode == "replica":
            msg = "No read replica is configured for the requested bind"
            raise ReplicaUnavailableError(msg)
        receipt = _ReadSelectionReceipt()
        provider = self.provide_session if session else self.provide_connection
        context = provider(*args, **kwargs)
        if self.is_async:
            return _ReadAsyncContext(
                self._primary_read_async(cast("AbstractAsyncContextManager[Any]", context), receipt), receipt
            )
        return _ReadSyncContext(self._primary_read_sync(cast("AbstractContextManager[Any]", context), receipt), receipt)

    @contextmanager
    def _primary_read_sync(
        self, context: "AbstractContextManager[Any]", receipt: "_ReadSelectionReceipt"
    ) -> "Generator[Any]":
        with context as value:
            receipt.config = self
            yield value

    @asynccontextmanager
    async def _primary_read_async(
        self, context: "AbstractAsyncContextManager[Any]", receipt: "_ReadSelectionReceipt"
    ) -> "AsyncGenerator[Any]":
        async with context as value:
            receipt.config = self
            yield value

    def _create_replica_pools_sync(self) -> None:
        for group in self._replica_groups.values():
            group.create_pools()

    async def _create_replica_pools_async(self) -> None:
        for group in self._replica_groups.values():
            await cast("Awaitable[list[Any]]", group.create_pools())

    def _close_pools_sync(self, close_primary: Callable[[], None] | None = None) -> None:
        error: BaseException | None = None
        try:
            if close_primary is not None:
                close_primary()
            self.connection_instance = None
        except BaseException as exc:
            error = exc
        for group in self._replica_groups.values():
            try:
                group.close_pools()
            except BaseException as exc:
                error = error or exc
        if error is not None:
            raise error

    async def _close_pools_async(self, close_primary: "Callable[[], Awaitable[None]] | None" = None) -> None:
        error: BaseException | None = None
        try:
            if close_primary is not None:
                await close_primary()
            self.connection_instance = None
        except BaseException as exc:
            error = exc
        for group in self._replica_groups.values():
            try:
                await cast("Awaitable[None]", group.close_pools())
            except BaseException as exc:
                error = error or exc
        if error is not None:
            raise error

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: object) -> bool:
        return self is other

    def __repr__(self) -> str:
        parts = ", ".join([
            f"connection_instance={self.connection_instance!r}",
            f"migration_config={self.migration_config!r}",
        ])
        return f"{type(self).__name__}({parts})"

    @property
    def migration_config(self) -> "dict[str, Any] | MigrationConfig":
        """Return the current migration configuration."""
        return self._migration_config

    @migration_config.setter
    def migration_config(self, value: "dict[str, Any] | MigrationConfig | None") -> None:
        """Store migration configuration and refresh derived migration helpers."""
        resolved = dict(cast("dict[str, Any]", value) or {})
        validate_migration_config_keys(resolved)
        Path(resolved.get("script_location", "migrations"))
        if "project_root" in resolved:
            Path(resolved["project_root"])
        if any(key in resolved for key in ("templates", "title", "default_format")):
            from sqlspec.migrations.templates import build_template_settings

            build_template_settings(resolved)
        with self._migration_lock:
            object.__setattr__(self, "_migration_config", resolved)
            self._migration_loader = None
            self._migration_commands = None
            if self._has_initialized_attribute("extension_config"):
                self._ensure_extension_migrations()

    def set_migration_config(self, config: "dict[str, Any] | MigrationConfig") -> None:
        """Attach migration configuration after initial config creation.

        This is equivalent to setting ``migration_config`` directly but provides
        a discoverable method for post-construction configuration.

        Args:
            config: Migration configuration dictionary.
        """
        self.migration_config = config

    def storage_capabilities(self) -> "StorageCapabilities":
        """Return cached storage capabilities for this configuration."""

        if self._storage_capabilities is None:
            self._storage_capabilities = self._build_storage_capabilities()
        return cast("StorageCapabilities", dict(self._storage_capabilities))

    def reset_storage_capabilities_cache(self) -> None:
        """Clear the cached capability snapshot."""

        self._storage_capabilities = None

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return default event runtime hints for this configuration."""

        return EventRuntimeHints()

    def attach_observability(self, registry_config: "ObservabilityConfig | None") -> None:
        """Attach merged observability runtime composed from registry and adapter overrides."""
        merged = ObservabilityConfig.merge(registry_config, self.observability_config)
        self._observability_runtime = ObservabilityRuntime(
            merged, bind_key=self.bind_key, config_name=type(self).__name__
        )
        for group in self._replica_groups.values():
            group.attach_observability(registry_config)

    def get_observability_runtime(self) -> "ObservabilityRuntime":
        """Return the attached runtime, creating a disabled instance when missing."""

        if self._observability_runtime is None:
            self.attach_observability(None)
        if self._observability_runtime is None:
            msg = "ObservabilityRuntime was not set by attach_observability; this is a bug"
            raise RuntimeError(msg)
        return self._observability_runtime

    @abstractmethod
    def create_connection(self) -> "ConnectionT | Awaitable[ConnectionT]":
        """Create and return a new database connection."""
        raise NotImplementedError

    @abstractmethod
    def provide_connection(
        self, *args: Any, **kwargs: Any
    ) -> "AbstractContextManager[ConnectionT] | AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    @abstractmethod
    def provide_session(
        self, *args: Any, **kwargs: Any
    ) -> "AbstractContextManager[DriverT] | AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    @abstractmethod
    def create_pool(self) -> "PoolT | Awaitable[PoolT]":
        """Create and return connection pool."""
        raise NotImplementedError

    @abstractmethod
    def close_pool(self) -> "Awaitable[None] | None":
        """Terminate the connection pool."""
        raise NotImplementedError

    @abstractmethod
    def provide_pool(
        self, *args: Any, **kwargs: Any
    ) -> "PoolT | Awaitable[PoolT] | AbstractContextManager[PoolT] | AbstractAsyncContextManager[PoolT]":
        """Provide pool instance."""
        raise NotImplementedError

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for this database configuration.

        Returns a dictionary of type names to objects (classes, functions, or
        other callables) that should be registered with Litestar's signature
        namespace to prevent serialization attempts on database-specific
        structures.

        Returns:
            Dictionary mapping type names to objects.
        """
        return {}

    def get_migration_loader(self) -> "SQLFileLoader":
        """Get the SQL loader for migration files.

        Provides access to migration SQL files loaded from the configured
        script_location directory. Files are loaded lazily on first access.

        Returns:
            SQLFileLoader instance with migration files loaded.
        """
        return self._ensure_migration_loader()

    def load_migration_sql_files(self, *paths: "str | Path") -> None:
        """Load additional migration SQL files from specified paths.

        Args:
            *paths: One or more file paths or directory paths to load migration SQL files from.
        """

        loader = self._ensure_migration_loader()
        for path in paths:
            path_obj = Path(path)
            if path_obj.exists():
                loader.load_sql(path_obj)
                logger.debug("Loaded migration SQL files from %s", path_obj)
            else:
                logger.warning("Migration path does not exist: %s", path_obj)

    def get_migration_commands(self) -> "SyncMigrationCommands[Any] | AsyncMigrationCommands[Any]":
        """Get migration commands for this configuration.

        The first call constructs the tracker and discovers extension migrations.
        Call this during application startup to surface their initialization errors
        eagerly. Subsequent calls reuse the helper until configuration changes.

        Returns:
            MigrationCommands instance configured for this database.
        """
        return self._ensure_migration_commands()

    def add_extension_migrations(
        self,
        name: str,
        migrations_path: "str | Path",
        settings: "dict[str, Any] | None" = None,
        *,
        migration_mode: ExtensionMigrationMode | None = None,
    ) -> None:
        """Register migrations shipped by a package outside the ``sqlspec.extensions`` namespace.

        Records the extension under ``extension_config``, opts it into
        ``migration_config["include_extensions"]``, and invalidates cached migration
        commands so the extension is discovered on their next access. Migrations
        are versioned under the ``ext_{name}_`` prefix, so ``name`` must stay stable
        once migrations are applied.

        Args:
            name: Extension name, used as the ``ext_{name}_`` version prefix.
            migrations_path: Directory containing the migrations, or a
                ``'<dotted.module>:<subdir>'`` specification.
            settings: Extension settings passed to its migrations. Merged into any
                settings already registered under ``name``.
            migration_mode: Optional explicit migration mode (``'versioned'`` or ``'reconcile'``).
        """
        with self._migration_lock:
            extension_config = cast("dict[str, Any]", self.extension_config)
            existing = extension_config.get(name)
            merged: dict[str, Any] = dict(existing) if isinstance(existing, Mapping) else {}
            if settings:
                merged.update(settings)
            merged["migrations_path"] = migrations_path
            resolve_extension_migration_mode(migration_mode, merged, extension_name=name)
            if migration_mode is not None:
                merged["migration_mode"] = migration_mode
            extension_config[name] = merged

            migration_config = cast("dict[str, Any]", self.migration_config)
            include_extensions = migration_config.get("include_extensions")
            include_list = list(include_extensions) if include_extensions else []
            changed = merged != existing or name not in include_list
            if name not in include_list:
                include_list.append(name)
            migration_config["include_extensions"] = include_list

            if changed:
                self._migration_commands = None

    def remove_extension_migrations(self, name: str) -> bool:
        """Unregister migrations previously registered for an extension.

        Removes the extension entry from ``extension_config`` and from
        ``migration_config["include_extensions"]``. If anything
        was removed, cached migration commands are invalidated so the extension is no
        longer discovered on their next access.

        Args:
            name: Extension name to unregister.

        Returns:
            True if the extension was found and removed, False otherwise.
        """
        with self._migration_lock:
            removed = False
            extension_config = cast("dict[str, Any]", self.extension_config)
            if isinstance(extension_config, dict) and name in extension_config:
                del extension_config[name]
                removed = True

            migration_config = cast("dict[str, Any]", self.migration_config)
            include_extensions = migration_config.get("include_extensions")
            if include_extensions is not None and name in include_extensions:
                migration_config["include_extensions"] = [ext for ext in include_extensions if ext != name]
                removed = True

            if removed:
                self._migration_commands = None
                return True

            return False

    @abstractmethod
    def migrate_up(
        self,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
        *,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> "Awaitable[None] | None":
        """Apply database migrations up to specified revision.

        Args:
            revision: Target revision or "head" for latest. Defaults to "head".
            allow_missing: Allow out-of-order migrations. Defaults to False.
            auto_sync: Auto-reconcile renamed migrations. Defaults to True.
            dry_run: Show what would be done without applying. Defaults to False.
            use_logger: Use Python logger instead of Rich console for output.
                Defaults to False. Can be set via MigrationConfig for persistent default.
            echo: Echo output to the console. Defaults to True when unset.
            summary_only: Emit a single summary log entry when logger output is enabled.
        """
        raise NotImplementedError

    @abstractmethod
    def migrate_down(
        self,
        revision: str = "-1",
        *,
        dry_run: bool = False,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> "Awaitable[None] | None":
        """Apply database migrations down to specified revision.

        Args:
            revision: Target revision, "-1" for one step back, or "base" for all migrations. Defaults to "-1".
            dry_run: Show what would be done without applying. Defaults to False.
            use_logger: Use Python logger instead of Rich console for output.
                Defaults to False. Can be set via MigrationConfig for persistent default.
            echo: Echo output to the console. Defaults to True when unset.
            summary_only: Emit a single summary log entry when logger output is enabled.
        """
        raise NotImplementedError

    @abstractmethod
    def get_current_migration(self, verbose: bool = False) -> "Awaitable[str | None] | str | None":
        """Get the current migration version.

        Args:
            verbose: Whether to show detailed migration history. Defaults to False.

        Returns:
            Current migration version or None if no migrations applied.
        """
        raise NotImplementedError

    @abstractmethod
    def create_migration(self, message: str, file_type: str = "sql") -> "Awaitable[None] | None":
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py'). Defaults to 'sql'.
        """
        raise NotImplementedError

    @abstractmethod
    def init_migrations(self, directory: "str | None" = None, package: bool = True) -> "Awaitable[None] | None":
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in. Uses script_location from migration_config if not provided.
            package: Whether to create __init__.py file. Defaults to True.
        """
        raise NotImplementedError

    @abstractmethod
    def stamp_migration(self, revision: str) -> "Awaitable[None] | None":
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        raise NotImplementedError

    @abstractmethod
    def fix_migrations(
        self, dry_run: bool = False, update_database: bool = True, yes: bool = False
    ) -> "Awaitable[None] | None":
        """Convert timestamp migrations to sequential format.

        Implements hybrid versioning workflow where development uses timestamps
        and production uses sequential numbers. Creates backup before changes
        and provides rollback on errors.

        Args:
            dry_run: Preview changes without applying. Defaults to False.
            update_database: Update migration records in database. Defaults to True.
            yes: Skip confirmation prompt. Defaults to False.
        """
        raise NotImplementedError

    def _has_initialized_attribute(self, attribute_name: str) -> bool:
        """Return whether a slot-backed attribute has been initialized."""
        try:
            object.__getattribute__(self, attribute_name)
        except AttributeError:
            return False
        return True

    def _ensure_extension_migrations(self) -> None:
        """Auto-include extension migrations when extension_config has them configured.

        Extensions with migration support are automatically included in
        ``migration_config["include_extensions"]`` based on their settings:

        - **litestar**: Only when ``session_table`` is set (for session storage)
        - **adk**: When any adk settings are present
        - **events**: When any events settings are present

        Any other extension is auto-included when its settings declare ``migrations_path``,
        which is how packages outside the ``sqlspec.extensions`` namespace ship migrations.

        Use ``exclude_extensions`` to opt out of auto-inclusion.
        """
        extension_settings = cast("dict[str, Any]", self.extension_config)
        migration_config = cast("dict[str, Any]", self.migration_config)

        for ext_name, ext_settings in extension_settings.items():
            if isinstance(ext_settings, Mapping):
                resolve_extension_migration_mode(None, dict(ext_settings), extension_name=ext_name)

        exclude_extensions = migration_config.get("exclude_extensions", [])
        if isinstance(exclude_extensions, tuple):
            exclude_extensions = list(exclude_extensions)  # pyright: ignore

        extensions_to_add: list[str] = []

        litestar_settings = extension_settings.get("litestar")
        if (
            isinstance(litestar_settings, Mapping)
            and "session_table" in litestar_settings
            and "litestar" not in exclude_extensions
        ):
            extensions_to_add.append("litestar")

        adk_settings = extension_settings.get("adk")
        if adk_settings is not None and "adk" not in exclude_extensions:
            from sqlspec.extensions.adk._config_utils import _ensure_adk_store_registration

            _ensure_adk_store_registration(self)
            extensions_to_add.append("adk")

        events_settings = extension_settings.get("events")
        if events_settings is not None and "events" not in exclude_extensions:
            extensions_to_add.append("events")

        for ext_name, ext_settings in extension_settings.items():
            if ext_name in extensions_to_add or ext_name in exclude_extensions:
                continue
            if isinstance(ext_settings, Mapping) and "migrations_path" in ext_settings:
                extensions_to_add.append(ext_name)

        if not extensions_to_add:
            return

        include_extensions = migration_config.get("include_extensions")
        include_list = list(include_extensions) if include_extensions else []
        for ext in extensions_to_add:
            if ext not in include_list:
                include_list.append(ext)
        migration_config["include_extensions"] = include_list

    def _build_storage_capabilities(self) -> "StorageCapabilities":
        arrow_dependency_needed = self.supports_native_arrow_export or self.supports_native_arrow_import
        parquet_dependency_needed = self.supports_native_parquet_export or self.supports_native_parquet_import

        pyarrow_dependency_ready = (
            self._dependency_available(ensure_pyarrow)
            if (arrow_dependency_needed or parquet_dependency_needed)
            else False
        )
        arrow_dependency_ready = pyarrow_dependency_ready if arrow_dependency_needed else False
        parquet_dependency_ready = pyarrow_dependency_ready if parquet_dependency_needed else False

        capabilities: StorageCapabilities = {
            "arrow_export_enabled": bool(self.supports_native_arrow_export and arrow_dependency_ready),
            "arrow_import_enabled": bool(self.supports_native_arrow_import and arrow_dependency_ready),
            "parquet_export_enabled": bool(self.supports_native_parquet_export and parquet_dependency_ready),
            "parquet_import_enabled": bool(self.supports_native_parquet_import and parquet_dependency_ready),
            "partition_strategies": list(self.storage_partition_strategies),
        }
        if self.default_storage_profile is not None:
            capabilities["default_storage_profile"] = self.default_storage_profile
        return capabilities

    def _init_observability(self, observability_config: "ObservabilityConfig | None" = None) -> None:
        """Initialize observability attributes for the configuration."""

        self.observability_config = observability_config
        self._observability_runtime = None

    def _configure_observability_extensions(self) -> None:
        """Apply extension_config hooks (otel/prometheus) to ObservabilityConfig."""

        config_map = cast("dict[str, Any]", self.extension_config)
        if not config_map:
            return
        updated = self.observability_config

        otel_config = cast("OpenTelemetryConfig | None", config_map.get("otel"))
        if otel_config and otel_config.get("enabled", True):
            from sqlspec.extensions import otel as otel_extension

            updated = otel_extension.enable_tracing(
                base_config=updated,
                resource_attributes=otel_config.get("resource_attributes"),
                tracer_provider=otel_config.get("tracer_provider"),
                tracer_provider_factory=otel_config.get("tracer_provider_factory"),
                enable_spans=otel_config.get("enable_spans", True),
            )

        prom_config = cast("PrometheusConfig | None", config_map.get("prometheus"))
        if prom_config and prom_config.get("enabled", True):
            from sqlspec.extensions import prometheus as prometheus_extension

            label_names = tuple(prom_config.get("label_names", ("driver", "operation")))
            duration_buckets = prom_config.get("duration_buckets")
            if duration_buckets is not None:
                duration_buckets = tuple(duration_buckets)

            updated = prometheus_extension.enable_metrics(
                base_config=updated,
                namespace=prom_config.get("namespace", "sqlspec"),
                subsystem=prom_config.get("subsystem", "driver"),
                registry=prom_config.get("registry"),
                label_names=label_names,
                duration_buckets=duration_buckets,
            )

        if updated is not self.observability_config:
            self.observability_config = updated

    def _attach_lifecycle_hooks(self) -> None:
        lifecycle_hooks: dict[str, list[Callable[[dict[str, Any]], None]]] = {}

        for hook_name, context_key in DRIVER_FEATURE_LIFECYCLE_HOOKS.items():
            callback = self.driver_features.pop(hook_name, None)
            if callback is None:
                continue
            callbacks = callback if isinstance(callback, (list, tuple)) else (callback,)  # pyright: ignore
            wrapped_callbacks = [self._adapt_lifecycle_hook(cb, context_key) for cb in callbacks]  # pyright: ignore
            lifecycle_hooks.setdefault(hook_name, []).extend(wrapped_callbacks)

        if not lifecycle_hooks:
            return

        lifecycle_config = cast("LifecycleConfig", lifecycle_hooks)
        override = ObservabilityConfig(lifecycle=lifecycle_config)
        if self.observability_config is None:
            self.observability_config = override
        else:
            self.observability_config = ObservabilityConfig.merge(self.observability_config, override)

    @staticmethod
    def _adapt_lifecycle_hook(
        callback: Callable[..., Any], context_key: str | None
    ) -> Callable[[dict[str, Any]], None]:
        try:
            hook_signature: Signature = signature(callback)
        except (TypeError, ValueError):  # pragma: no cover
            hook_signature = Signature()

        positional_params = [
            param
            for param in hook_signature.parameters.values()
            if param.kind in {param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD} and param.default is param.empty
        ]
        expects_argument = bool(positional_params)

        return _DriverFeatureHookWrapper(callback, context_key, expects_argument)

    def _prepare_driver(self, driver: DriverT) -> DriverT:
        """Attach observability runtime to driver instances before returning them."""

        driver.attach_observability(self.get_observability_runtime())
        if (
            driver.statement_config is self.statement_config
            and self.driver_features.get("_statement_cache_holder") is not driver._cache_holder
        ):
            self.driver_features["_statement_cache_holder"] = driver._cache_holder
        return driver

    @staticmethod
    def _dependency_available(checker: "Callable[[], None]") -> bool:
        try:
            checker()
        except MissingDependencyError:
            return False
        return True

    def _ensure_migration_loader(self) -> "SQLFileLoader":
        """Get the migration SQL loader and auto-load files if needed.

        Returns:
            SQLFileLoader instance for migration files.
        """
        loader = self._migration_loader
        if loader is not None and loader.list_files():
            return loader
        with self._migration_lock:
            loader = self._migration_loader
            if loader is None:
                loader = SQLFileLoader(runtime=self.get_observability_runtime())
            migration_path = Path(self.migration_config.get("script_location", "migrations"))
            if migration_path.exists() and not loader.list_files():
                loader.load_sql(migration_path)
                logger.debug("Auto-loaded migration SQL files from %s", migration_path)
            self._migration_loader = loader
            return loader

    def _ensure_migration_commands(self) -> "SyncMigrationCommands[Any] | AsyncMigrationCommands[Any]":
        """Get the migration commands instance.

        Returns:
            MigrationCommands instance for this config.
        """
        commands = self._migration_commands
        if commands is not None:
            return commands
        with self._migration_lock:
            commands = self._migration_commands
            if commands is None:
                from sqlspec.migrations.commands import create_migration_commands

                commands = create_migration_commands(self)  # pyright: ignore
                self._migration_commands = commands
            return commands

    def _reject_unexpected_kwargs(self, kwargs: "dict[str, Any]") -> None:
        """Raise ``TypeError`` when construction receives unrecognized keyword arguments."""
        if kwargs:
            unexpected = ", ".join(sorted(kwargs))
            msg = f"{type(self).__name__}.__init__() got unexpected keyword arguments: {unexpected}"
            raise TypeError(msg)

    def _default_tenant_strategy(self) -> "TenantStrategy":
        """Build the strategy selected by ``True`` or an adapter shorthand."""
        strategy_type = self.tenant_strategy_type
        if strategy_type is None:
            msg = f"{type(self).__name__} does not support tenant_strategy"
            raise ImproperConfigurationError(msg)
        return cast("TenantStrategy", strategy_type())

    def _resolve_tenant_strategy(self, strategy: Any) -> "TenantStrategy | None":
        """Normalize disabled tenancy, adapter shorthands, and structural strategy instances.

        Raises:
            ImproperConfigurationError: The shorthand or strategy is unsupported.
        """
        if strategy is None or strategy is False:
            return None
        if self.tenant_strategy_type is not None and (
            strategy is True or (isinstance(strategy, str) and strategy in self.tenant_strategy_shorthands)
        ):
            return self._default_tenant_strategy()
        if isinstance(strategy, (str, bool)):
            msg = f"{type(self).__name__} does not support tenant_strategy shorthand {strategy!r}"
            raise ImproperConfigurationError(msg)
        if callable(getattr(strategy, "checkout", None)) and callable(getattr(strategy, "checkin", None)):
            return cast("TenantStrategy", strategy)
        msg = f"{type(self).__name__} received an invalid tenant_strategy: {strategy!r}"
        raise ImproperConfigurationError(msg)

    def _create_tenant_lease(self) -> Any:
        """Build one fail-closed lease for a connection or session acquisition.

        Returns:
            The adapter lease, or None when no tenant strategy is configured.

        Raises:
            ImproperConfigurationError: A strategy is configured without a lease implementation.
        """
        strategy = self.driver_features.get("tenant_strategy")
        if strategy is None:
            return None
        lease_type = self.tenant_lease_type
        if lease_type is None:
            msg = f"{type(self).__name__} has a tenant_strategy but no tenant lease implementation"
            raise ImproperConfigurationError(msg)
        lease = lease_type(self, strategy)
        lease.tenant_key = self._tenant_key
        return lease

    def _require_leased_tenant_connection(self) -> None:
        """Reject standalone connections that would bypass tenant leases.

        Raises:
            ImproperConfigurationError: A tenant strategy is configured.
        """
        if self.driver_features.get("tenant_strategy") is not None:
            msg = (
                f"{type(self).__name__} tenant configurations require provide_connection() or provide_session() leases"
            )
            raise ImproperConfigurationError(msg)

    @contextmanager
    def _provide_sync_tenant_session(
        self, tenant: "str | UUID | TenantContextPayload", *args: Any, **kwargs: Any
    ) -> "Generator[DriverT]":
        """Keep an explicit tenant bound until the session finishes cleanup.

        Yields:
            The driver session with the explicit tenant active.
        """
        if self.driver_features.get("tenant_strategy") is None:
            msg = "provide_session(tenant=...) requires a configured tenant_strategy"
            raise ImproperConfigurationError(msg)
        with TenantContext.scope(tenant):
            context = cast("AbstractContextManager[DriverT]", self.provide_session(*args, **kwargs))
            with context as session:
                yield session

    @asynccontextmanager
    async def _provide_async_tenant_session(
        self, tenant: "str | UUID | TenantContextPayload", *args: Any, **kwargs: Any
    ) -> "AsyncGenerator[DriverT]":
        """Keep an explicit tenant bound through asynchronous session cleanup.

        Yields:
            The driver session with the explicit tenant active.
        """
        if self.driver_features.get("tenant_strategy") is None:
            msg = "provide_session(tenant=...) requires a configured tenant_strategy"
            raise ImproperConfigurationError(msg)
        with TenantContext.scope(tenant):
            context = cast("AbstractAsyncContextManager[DriverT]", self.provide_session(*args, **kwargs))
            async with context as session:
                yield session

    def _init_config_state(
        self,
        *,
        connection_config: "dict[str, Any] | None",
        connection_instance: "Any",
        migration_config: "dict[str, Any] | MigrationConfig | None",
        statement_config: "StatementConfig | None",
        driver_features: "dict[str, Any] | None",
        bind_key: "str | None",
        extension_config: "ExtensionConfigs | None",
        observability_config: "ObservabilityConfig | None",
        default_dialect: str,
        replicas: "ReplicasInput | None" = None,
        is_replica: bool = False,
    ) -> None:
        """Populate the configuration state shared by every base config class.

        Assigns identity and connection attributes, initializes observability and
        migration helper caches, resolves the statement configuration against
        ``default_dialect``, seeds runtime driver features from storage
        capabilities, and attaches lifecycle and observability extensions.
        """
        self._is_replica = is_replica
        self._replica_groups = {}
        self._migration_lock = threading.RLock()
        self._migration_loader = None
        self._migration_commands = None
        self.bind_key = bind_key
        self._tenant_key = self
        self.connection_instance = connection_instance
        self.connection_config = connection_config or {}
        self.extension_config = extension_config or {}
        self.migration_config = migration_config or {}
        self._init_observability(observability_config)
        self.statement_config = statement_config or build_default_statement_config(default_dialect)
        self._storage_capabilities = None
        strategy = self._resolve_tenant_strategy((driver_features or {}).get("tenant_strategy"))
        if driver_features is not None and "tenant_strategy" in driver_features:
            driver_features = {**driver_features, "tenant_strategy": strategy}
        if strategy is not None:
            if not self.supports_tenant_strategy:
                msg = f"{type(self).__name__} does not support tenant_strategy"
                raise ImproperConfigurationError(msg)
            if any(
                iscoroutinefunction(method) is not self.is_async for method in (strategy.checkout, strategy.checkin)
            ):
                msg = f"{type(self).__name__} tenant_strategy must match its async execution mode ({self.is_async})"
                raise ImproperConfigurationError(msg)
        if "tenant_session_baseline" in (driver_features or {}) and not self.supports_tenant_session_baseline:
            msg = f"{type(self).__name__} does not support tenant_session_baseline"
            raise ImproperConfigurationError(msg)
        self.driver_features = seed_runtime_driver_features(driver_features, self.storage_capabilities())
        self.driver_features["_statement_cache_holder"] = StatementCacheHolder(
            self.statement_config, resolve_statement_cache_size(self.driver_features)
        )
        self._attach_lifecycle_hooks()
        self._configure_observability_extensions()
        self._replica_groups = self._normalize_replicas(replicas)

    def _provide_connection_impl(self, *args: Any, **kwargs: Any) -> Any:
        """Build the connection context manager shared by pooled configs."""
        return self._connection_context_class(self)

    def _provide_session_impl(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> Any:
        """Build the session context manager shared by pooled configs."""
        handler = self._session_factory_class(self)
        return self._session_context_class(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or self._default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )


class _SyncMigrationMixin:
    """Shared sync migration convenience methods."""

    __slots__ = ()

    def migrate_up(
        self: Any,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
        *,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Apply database migrations up to specified revision."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.upgrade(
            revision, allow_missing, auto_sync, dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only
        )

    def migrate_down(
        self: Any,
        revision: str = "-1",
        *,
        dry_run: bool = False,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Apply database migrations down to specified revision."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.downgrade(revision, dry_run=dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only)

    def get_current_migration(self: Any, verbose: bool = False) -> "str | None":
        """Get the current migration version."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        return commands.current(verbose=verbose)

    def create_migration(self: Any, message: str, file_type: str = "sql") -> None:
        """Create a new migration file."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.revision(message, file_type)

    def init_migrations(self: Any, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure."""
        if directory is None:
            migration_config = self.migration_config or {}
            directory = str(migration_config.get("script_location") or "migrations")

        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.init(directory, package)

    def stamp_migration(self: Any, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.stamp(revision)

    def fix_migrations(self: Any, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.fix(dry_run, update_database, yes)


class _AsyncMigrationMixin:
    """Shared async migration convenience methods."""

    __slots__ = ()

    async def migrate_up(
        self: Any,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
        *,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Apply database migrations up to specified revision."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.upgrade(
            revision, allow_missing, auto_sync, dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only
        )

    async def migrate_down(
        self: Any,
        revision: str = "-1",
        *,
        dry_run: bool = False,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Apply database migrations down to specified revision."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.downgrade(revision, dry_run=dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only)

    async def get_current_migration(self: Any, verbose: bool = False) -> "str | None":
        """Get the current migration version."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        return await commands.current(verbose=verbose)

    async def create_migration(self: Any, message: str, file_type: str = "sql") -> None:
        """Create a new migration file."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.revision(message, file_type)

    async def init_migrations(self: Any, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure."""
        if directory is None:
            migration_config = self.migration_config or {}
            directory = str(migration_config.get("script_location") or "migrations")

        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.init(directory, package)

    async def stamp_migration(self: Any, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.stamp(revision)

    async def fix_migrations(self: Any, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.fix(dry_run, update_database, yes)


class NoPoolSyncConfig(_SyncMigrationMixin, DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for sync database configurations that do not implement a pool."""

    __slots__ = ()
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    migration_tracker_type: "ClassVar[type[Any]]" = SyncMigrationTracker

    def __init__(
        self,
        *,
        connection_config: dict[str, Any] | None = None,
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        replicas: "ReplicasInput | None" = None,
        is_replica: bool = False,
        **kwargs: Any,
    ) -> None:
        self._reject_unexpected_kwargs(kwargs)
        self._init_config_state(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
            default_dialect="sqlite",
        )

    def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_read_connection(
        self, *args: Any, bind_group: str | None = None, **kwargs: Any
    ) -> "AbstractContextManager[ConnectionT]":
        """Provide a read connection with this config's execution mode."""
        return cast(
            "AbstractContextManager[ConnectionT]",
            super().provide_read_connection(*args, bind_group=bind_group, **kwargs),
        )

    def provide_read_session(
        self,
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractContextManager[DriverT]":
        """Provide a read session with this config's execution mode."""
        return cast(
            "AbstractContextManager[DriverT]",
            super().provide_read_session(
                *args,
                bind_group=bind_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            ),
        )

    def provide_routing_session(
        self: "NoPoolSyncConfig[ConnectionT, SyncDriverT]",
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "SyncRoutingSession[SyncDriverT]":
        """Construct a lazy session that routes each operation safely."""
        from sqlspec.driver._routing import SyncRoutingSession

        return SyncRoutingSession(
            self,
            *args,
            bind_group=bind_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    def create_pool(self) -> None:
        self._create_replica_pools_sync()

    def close_pool(self) -> None:
        self._close_pools_sync()

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class NoPoolAsyncConfig(_AsyncMigrationMixin, DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for async database configurations that do not implement a pool."""

    __slots__ = ()
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = False
    migration_tracker_type: "ClassVar[type[Any]]" = AsyncMigrationTracker

    def __init__(
        self,
        *,
        connection_config: "dict[str, Any] | None" = None,
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        replicas: "ReplicasInput | None" = None,
        is_replica: bool = False,
        **kwargs: Any,
    ) -> None:
        self._reject_unexpected_kwargs(kwargs)
        self._init_config_state(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
            default_dialect="sqlite",
        )

    async def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_read_connection(
        self, *args: Any, bind_group: str | None = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a read connection with this config's execution mode."""
        return cast(
            "AbstractAsyncContextManager[ConnectionT]",
            super().provide_read_connection(*args, bind_group=bind_group, **kwargs),
        )

    def provide_read_session(
        self,
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a read session with this config's execution mode."""
        return cast(
            "AbstractAsyncContextManager[DriverT]",
            super().provide_read_session(
                *args,
                bind_group=bind_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            ),
        )

    def provide_routing_session(
        self: "NoPoolAsyncConfig[ConnectionT, AsyncDriverT]",
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AsyncRoutingSession[AsyncDriverT]":
        """Construct a lazy session that routes each operation safely."""
        from sqlspec.driver._routing import AsyncRoutingSession

        return AsyncRoutingSession(
            self,
            *args,
            bind_group=bind_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    async def create_pool(self) -> None:
        await self._create_replica_pools_async()

    async def close_pool(self) -> None:
        await self._close_pools_async()

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class SyncDatabaseConfig(_SyncMigrationMixin, DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Base class for sync database configurations with connection pooling."""

    __slots__ = ("_pool_lock",)
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = True
    migration_tracker_type: "ClassVar[type[Any]]" = SyncMigrationTracker

    def __init__(
        self,
        *,
        connection_config: "dict[str, Any] | None" = None,
        connection_instance: "PoolT | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        replicas: "ReplicasInput | None" = None,
        is_replica: bool = False,
        **kwargs: Any,
    ) -> None:
        self._reject_unexpected_kwargs(kwargs)
        self._init_config_state(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
            default_dialect="postgres",
        )
        self._pool_lock = threading.Lock()

    def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        existing_pool = self.connection_instance
        if existing_pool is not None:
            self._create_replica_pools_sync()
            return existing_pool

        created_pool = create_sync_pool(
            None,
            self._pool_lock,
            lambda: self.connection_instance,
            self._create_pool,
            self.get_observability_runtime().emit_pool_create_sync,
        )
        self.connection_instance = created_pool
        self._create_replica_pools_sync()
        return cast("PoolT", created_pool)

    def close_pool(self) -> None:
        """Close the primary and every replica, preserving the first error."""
        runtime = self.get_observability_runtime()
        self._close_pools_sync(
            lambda: close_sync_pool(
                self.connection_instance,
                self._close_pool,
                runtime.emit_pool_destroy_sync,
                runtime.emit_pool_destroying_sync,
            )
        )

    def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        return self.create_pool()

    def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        return cast("AbstractContextManager[ConnectionT]", self._provide_connection_impl(*args, **kwargs))

    def provide_read_connection(
        self, *args: Any, bind_group: str | None = None, **kwargs: Any
    ) -> "AbstractContextManager[ConnectionT]":
        """Provide a read connection with this config's execution mode."""
        return cast(
            "AbstractContextManager[ConnectionT]",
            super().provide_read_connection(*args, bind_group=bind_group, **kwargs),
        )

    def provide_read_session(
        self,
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractContextManager[DriverT]":
        """Provide a read session with this config's execution mode."""
        return cast(
            "AbstractContextManager[DriverT]",
            super().provide_read_session(
                *args,
                bind_group=bind_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            ),
        )

    def provide_routing_session(
        self: "SyncDatabaseConfig[ConnectionT, PoolT, SyncDriverT]",
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "SyncRoutingSession[SyncDriverT]":
        """Construct a lazy session that routes each operation safely."""
        from sqlspec.driver._routing import SyncRoutingSession

        return SyncRoutingSession(
            self,
            *args,
            bind_group=bind_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractContextManager[DriverT]":
        """Provide a session, optionally binding or bypassing a tenant until cleanup completes.

        Args:
            *args: Adapter-specific session arguments.
            statement_config: Statement configuration override.
            tenant: Tenant identity or payload; None inherits the current scope.
            bypass_tenant: Explicitly bypass tenant setup for this session.
            **kwargs: Adapter-specific session options.

        Returns:
            A driver session context manager.
        """
        if bypass_tenant:
            if tenant is not None:
                msg = "provide_session() cannot combine tenant with bypass_tenant=True"
                raise ImproperConfigurationError(msg)
            return self._provide_sync_tenant_session(
                TenantContextPayload.bypass(), *args, statement_config=statement_config, **kwargs
            )
        if tenant is not None:
            return self._provide_sync_tenant_session(tenant, *args, statement_config=statement_config, **kwargs)
        return cast(
            "AbstractContextManager[DriverT]",
            self._provide_session_impl(*args, statement_config=statement_config, **kwargs),
        )

    @abstractmethod
    def _create_pool(self) -> PoolT:
        """Actual pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    def _close_pool(self) -> None:
        """Actual pool destruction implementation."""
        raise NotImplementedError


class AsyncDatabaseConfig(_AsyncMigrationMixin, DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Base class for async database configurations with connection pooling."""

    __slots__ = ("_pool_lock",)
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = True
    migration_tracker_type: "ClassVar[type[Any]]" = AsyncMigrationTracker

    def __init__(
        self,
        *,
        connection_config: "dict[str, Any] | None" = None,
        connection_instance: "PoolT | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        replicas: "ReplicasInput | None" = None,
        is_replica: bool = False,
        **kwargs: Any,
    ) -> None:
        self._reject_unexpected_kwargs(kwargs)
        self._init_config_state(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
            default_dialect="postgres",
        )
        self._pool_lock = asyncio.Lock()

    async def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        existing_pool = self.connection_instance
        if existing_pool is not None:
            await self._create_replica_pools_async()
            return existing_pool

        created_pool = await create_async_pool(
            None,
            self._pool_lock,
            lambda: self.connection_instance,
            self._create_pool,
            self.get_observability_runtime().emit_pool_create_async,
        )
        self.connection_instance = created_pool
        await self._create_replica_pools_async()
        return cast("PoolT", created_pool)

    async def close_pool(self) -> None:
        """Close the primary and every replica, preserving the first error."""
        runtime = self.get_observability_runtime()
        await self._close_pools_async(
            lambda: close_async_pool(
                self.connection_instance,
                self._close_pool,
                runtime.emit_pool_destroy_async,
                runtime.emit_pool_destroying_async,
            )
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        return await self.create_pool()

    async def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        return cast("AbstractAsyncContextManager[ConnectionT]", self._provide_connection_impl(*args, **kwargs))

    def provide_read_connection(
        self, *args: Any, bind_group: str | None = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a read connection with this config's execution mode."""
        return cast(
            "AbstractAsyncContextManager[ConnectionT]",
            super().provide_read_connection(*args, bind_group=bind_group, **kwargs),
        )

    def provide_read_session(
        self,
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a read session with this config's execution mode."""
        return cast(
            "AbstractAsyncContextManager[DriverT]",
            super().provide_read_session(
                *args,
                bind_group=bind_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            ),
        )

    def provide_routing_session(
        self: "AsyncDatabaseConfig[ConnectionT, PoolT, AsyncDriverT]",
        *args: Any,
        bind_group: str | None = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AsyncRoutingSession[AsyncDriverT]":
        """Construct a lazy session that routes each operation safely."""
        from sqlspec.driver._routing import AsyncRoutingSession

        return AsyncRoutingSession(
            self,
            *args,
            bind_group=bind_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a session, optionally binding or bypassing a tenant until cleanup completes.

        Args:
            *args: Adapter-specific session arguments.
            statement_config: Statement configuration override.
            tenant: Tenant identity or payload; None inherits the current scope.
            bypass_tenant: Explicitly bypass tenant setup for this session.
            **kwargs: Adapter-specific session options.

        Returns:
            A driver session context manager.
        """
        if bypass_tenant:
            if tenant is not None:
                msg = "provide_session() cannot combine tenant with bypass_tenant=True"
                raise ImproperConfigurationError(msg)
            return self._provide_async_tenant_session(
                TenantContextPayload.bypass(), *args, statement_config=statement_config, **kwargs
            )
        if tenant is not None:
            return self._provide_async_tenant_session(tenant, *args, statement_config=statement_config, **kwargs)
        return cast(
            "AbstractAsyncContextManager[DriverT]",
            self._provide_session_impl(*args, statement_config=statement_config, **kwargs),
        )

    @abstractmethod
    async def _create_pool(self) -> PoolT:
        """Actual async pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    async def _close_pool(self) -> None:
        """Actual async pool destruction implementation."""
        raise NotImplementedError


class _ReadSelectionReceipt:
    __slots__ = ("config",)

    def __init__(self) -> None:
        self.config: DatabaseConfigProtocol[Any, Any, Any] | None = None


class _ReadSyncContext(Generic[ConfigT]):
    __slots__ = ("_context", "_selection_receipt")

    def __init__(self, context: "AbstractContextManager[Any]", receipt: _ReadSelectionReceipt) -> None:
        self._context = context
        self._selection_receipt = receipt

    def __enter__(self) -> Any:
        return self._context.__enter__()

    def __exit__(self, *args: Any) -> Any:
        return self._context.__exit__(*args)


class _ReadAsyncContext(Generic[ConfigT]):
    __slots__ = ("_context", "_selection_receipt")

    def __init__(self, context: "AbstractAsyncContextManager[Any]", receipt: _ReadSelectionReceipt) -> None:
        self._context = context
        self._selection_receipt = receipt

    async def __aenter__(self) -> Any:
        return await self._context.__aenter__()

    async def __aexit__(self, *args: Any) -> Any:
        return await self._context.__aexit__(*args)


class ReadReplicaGroup(Generic[ConfigT]):
    """Managed homogeneous read pools with checkout-only failover."""

    __slots__ = (
        "_breaker",
        "_causal_validator",
        "_configs",
        "_fallback_to_primary",
        "_is_async",
        "_name",
        "_selector",
        "_specs",
    )

    def __init__(
        self,
        configs: "Sequence[ConfigT | ReplicaSpec]",
        *,
        strategy: SelectionStrategy | ReplicaSelector = "round_robin",
        fallback_to_primary: bool = True,
        failure_threshold: int = 3,
        recovery_seconds: float = 10.0,
        name: str = "default",
        causal_validator: "Callable[[Any, str], bool | Awaitable[bool]] | None" = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Configure replica selection, recovery, and optional causal validation."""
        if not configs or not name:
            msg = "Read replica groups need a name and at least one replica"
            raise ImproperConfigurationError(msg)
        self._specs = tuple(item if isinstance(item, ReplicaSpec) else ReplicaSpec(item) for item in configs)
        if any(not isinstance(spec.config, DatabaseConfigProtocol) for spec in self._specs):
            msg = "Read replica groups require database configurations"
            raise ImproperConfigurationError(msg)
        self._configs = tuple(cast("ConfigT", spec.config) for spec in self._specs)
        self._is_async = self._configs[0].is_async
        if any(config.is_async != self._is_async for config in self._configs):
            msg = "Read replica groups cannot mix sync and async configurations"
            raise ImproperConfigurationError(msg)
        self._name = name
        self._fallback_to_primary = fallback_to_primary
        self._causal_validator = causal_validator
        self._selector = build_selector(strategy, self._specs)
        self._breaker = ReplicaHealthBreaker(
            len(self._configs),
            failure_threshold=failure_threshold,
            recovery_seconds=recovery_seconds,
            clock=clock or time.monotonic,
        )

    @property
    def configs(self) -> tuple[ConfigT, ...]:
        """Return the group's configuration instances."""
        return self._configs

    @property
    def specs(self) -> tuple[ReplicaSpec, ...]:
        """Return weighted descriptors in their stable index order."""
        return self._specs

    @property
    def breaker(self) -> ReplicaHealthBreaker:
        """Return passive checkout health state."""
        return self._breaker

    @property
    def selector(self) -> ReplicaSelector:
        """Return the configured candidate selector."""
        return self._selector

    @property
    def fallback_to_primary(self) -> bool:
        """Whether exhaustion permits primary checkout."""
        return self._fallback_to_primary

    @property
    def name(self) -> str:
        """Return the group name."""
        return self._name

    @property
    def is_async(self) -> bool:
        """Whether all configurations use asynchronous providers."""
        return self._is_async

    def create_pools(self) -> "list[Any] | Awaitable[list[Any]]":
        """Create every replica pool."""
        if self._is_async:
            return self._create_async_pools()
        return [config.create_pool() for config in self._configs]

    async def _create_async_pools(self) -> list[Any]:
        return [await cast("Awaitable[Any]", config.create_pool()) for config in self._configs]

    def close_pools(self) -> "Awaitable[None] | None":
        """Close every pool before propagating the first close error."""
        if self._is_async:
            return self._close_async_pools()
        error: BaseException | None = None
        for config in self._configs:
            try:
                config.close_pool()
            except BaseException as exc:
                error = error or exc
        if error is not None:
            raise error
        return None

    async def _close_async_pools(self) -> None:
        error: BaseException | None = None
        for config in self._configs:
            try:
                await cast("Awaitable[None]", config.close_pool())
            except BaseException as exc:
                error = error or exc
        if error is not None:
            raise error

    def attach_observability(self, registry_config: ObservabilityConfig | None) -> None:
        """Attach the registry settings to each replica's own runtime."""
        for config in self._configs:
            config.attach_observability(registry_config)

    def provide_connection(
        self,
        *args: Any,
        primary: "DatabaseConfigProtocol[Any, Any, Any] | None" = None,
        causal_token: str | None = None,
        max_staleness_seconds: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Acquire a connection from one healthy candidate without replaying callers."""
        return self._provide(False, args, kwargs, primary, causal_token, max_staleness_seconds)

    def provide_session(
        self,
        *args: Any,
        primary: "DatabaseConfigProtocol[Any, Any, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        tenant: "str | UUID | TenantContextPayload | None" = None,
        bypass_tenant: bool = False,
        causal_token: str | None = None,
        max_staleness_seconds: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Acquire a tenant-aware session with failover limited to checkout."""
        kwargs.update(statement_config=statement_config, tenant=tenant, bypass_tenant=bypass_tenant)
        return self._provide(True, args, kwargs, primary, causal_token, max_staleness_seconds)

    def _provide(
        self,
        session: bool,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        primary: "DatabaseConfigProtocol[Any, Any, Any] | None",
        causal_token: str | None,
        staleness: float | None,
    ) -> Any:
        receipt = _ReadSelectionReceipt()
        if self._is_async:
            return _ReadAsyncContext(
                self._provide_async(session, args, kwargs, primary, causal_token, staleness, receipt), receipt
            )
        return _ReadSyncContext(
            self._provide_sync(session, args, kwargs, primary, causal_token, staleness, receipt), receipt
        )

    def _validate_sync_causal(self, value: Any, causal_token: str | None) -> bool:
        if causal_token is None or self._causal_validator is None:
            return True
        result = self._causal_validator(value, causal_token)
        if isawaitable(result):
            if callable(getattr(result, "close", None)):
                cast("Any", result).close()
            msg = "An async causal validator requires an async replica group"
            raise ImproperConfigurationError(msg)
        return result

    @contextmanager
    def _provide_sync(
        self,
        session: bool,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        primary: "DatabaseConfigProtocol[Any, Any, Any] | None",
        causal_token: str | None,
        staleness: float | None,
        receipt: _ReadSelectionReceipt,
    ) -> "Generator[Any]":
        last_error: Exception | None = None
        for index in self._selector.select_order(self._breaker, max_staleness_seconds=staleness):
            if not self._breaker.try_acquire_probe(index):
                continue
            stack = ExitStack()
            config = self._configs[index]
            provider: Callable[..., Any] = config.provide_session if session else config.provide_connection
            try:
                value = stack.enter_context(cast("AbstractContextManager[Any]", provider(*args, **kwargs)))
            except (DatabaseConnectionError, OperationalError, OSError, TimeoutError) as exc:
                stack.__exit__(*sys.exc_info())
                self._breaker.record_failure(index)
                last_error = exc
                continue
            except BaseException:
                self._breaker.release_probe(index)
                stack.__exit__(*sys.exc_info())
                raise
            try:
                caught_up = self._validate_sync_causal(value, causal_token)
            except BaseException:
                self._breaker.release_probe(index)
                stack.__exit__(*sys.exc_info())
                raise
            if not caught_up:
                self._breaker.release_probe(index)
                stack.close()
                continue
            self._breaker.record_success(index)
            receipt.config = config
            with stack:
                yield value
            return
        if self._fallback_to_primary and primary is not None:
            if last_error is not None and not primary._allow_replica_fallback(last_error):
                raise last_error
            provider = primary.provide_session if session else primary.provide_connection
            with cast("AbstractContextManager[Any]", provider(*args, **kwargs)) as value:
                receipt.config = primary
                yield value
            return
        msg = f"No healthy read replica available in group {self._name!r}"
        raise ReplicaUnavailableError(msg) from last_error

    @asynccontextmanager
    async def _provide_async(
        self,
        session: bool,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        primary: "DatabaseConfigProtocol[Any, Any, Any] | None",
        causal_token: str | None,
        staleness: float | None,
        receipt: _ReadSelectionReceipt,
    ) -> "AsyncGenerator[Any]":
        last_error: Exception | None = None
        for index in self._selector.select_order(self._breaker, max_staleness_seconds=staleness):
            if not self._breaker.try_acquire_probe(index):
                continue
            stack = AsyncExitStack()
            config = self._configs[index]
            provider: Callable[..., Any] = config.provide_session if session else config.provide_connection
            try:
                value = await stack.enter_async_context(
                    cast("AbstractAsyncContextManager[Any]", provider(*args, **kwargs))
                )
            except (DatabaseConnectionError, OperationalError, OSError, TimeoutError) as exc:
                await stack.__aexit__(*sys.exc_info())
                self._breaker.record_failure(index)
                last_error = exc
                continue
            except BaseException:
                self._breaker.release_probe(index)
                await stack.__aexit__(*sys.exc_info())
                raise
            try:
                caught_up = (
                    self._causal_validator(value, causal_token)
                    if causal_token is not None and self._causal_validator is not None
                    else True
                )
                if isawaitable(caught_up):
                    caught_up = await caught_up
            except BaseException:
                self._breaker.release_probe(index)
                await stack.__aexit__(*sys.exc_info())
                raise
            if not caught_up:
                self._breaker.release_probe(index)
                await stack.aclose()
                continue
            self._breaker.record_success(index)
            receipt.config = config
            async with stack:
                yield value
            return
        if self._fallback_to_primary and primary is not None:
            if last_error is not None and not primary._allow_replica_fallback(last_error):
                raise last_error
            provider = primary.provide_session if session else primary.provide_connection
            async with cast("AbstractAsyncContextManager[Any]", provider(*args, **kwargs)) as value:
                receipt.config = primary
                yield value
            return
        msg = f"No healthy read replica available in group {self._name!r}"
        raise ReplicaUnavailableError(msg) from last_error


class _DriverFeatureHookWrapper:
    __slots__ = ("_callback", "_context_key", "_expects_argument")

    def __init__(self, callback: "Callable[..., Any]", context_key: "str | None", expects_argument: bool) -> None:
        self._callback = callback
        self._context_key = context_key
        self._expects_argument = expects_argument

    def __call__(self, context: "dict[str, Any]") -> None:
        if not self._expects_argument:
            self._callback()
            return
        if self._context_key is None:
            self._callback(context)
            return
        self._callback(context.get(self._context_key))


def _report_unknown_keys(
    mapping: "Mapping[str, Any]", valid_keys: "frozenset[str]", prefix: str, scope: str
) -> "list[str]":
    """Describe keys a configuration scope does not declare.

    Args:
        mapping: Mapping to check.
        valid_keys: Keys the scope accepts.
        prefix: Dotted path prepended to each reported key.
        scope: Scope name used in the valid-key summary, empty for the top level.

    Returns:
        Report lines, empty when every key is recognized.
    """
    unknown = sorted(key for key in mapping if key not in valid_keys)
    if not unknown:
        return []

    lines = []
    for key in unknown:
        suggestions = get_close_matches(key, valid_keys, n=1, cutoff=0.6)
        hint = f" Did you mean {suggestions[0]!r}?" if suggestions else ""
        lines.append(f"Unknown migration_config key {f'{prefix}{key}'!r}.{hint}")
    lines.append(f"Valid {scope}keys: {', '.join(sorted(valid_keys))}.")
    return lines


def _report_template_keys(templates: Any) -> "list[str]":
    """Describe unrecognized keys nested under ``templates``.

    Args:
        templates: Value configured for the ``templates`` key.

    Returns:
        Report lines, empty when the overrides are recognized.
    """
    if not isinstance(templates, Mapping):
        return [f"migration_config key 'templates' must be a mapping, got {type(templates).__name__}."]

    lines = _report_unknown_keys(templates, MIGRATION_TEMPLATES_KEYS, "templates.", "'templates' ")
    for section, fragment_keys in _TEMPLATE_FRAGMENT_KEYS.items():
        overrides = templates.get(section)
        if overrides is None:
            continue
        path = f"templates.{section}"
        if not isinstance(overrides, Mapping):
            lines.append(f"migration_config key '{path}' must be a mapping, got {type(overrides).__name__}.")
            continue
        lines.extend(_report_unknown_keys(overrides, fragment_keys, f"{path}.", f"'{path}' "))
    return lines
