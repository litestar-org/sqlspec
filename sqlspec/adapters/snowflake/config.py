"""Snowflake database configuration with bounded connection pooling."""

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast
from urllib.parse import parse_qsl, unquote, urlsplit

from sqlspec.adapters.snowflake._typing import SnowflakeConnection, SnowflakeCursor, SnowflakeSessionContext
from sqlspec.adapters.snowflake.core import (
    apply_driver_features,
    default_statement_config,
    merge_snowflake_derive_overrides,
    normalize_connection_params,
)
from sqlspec.adapters.snowflake.driver import SnowflakeDriver
from sqlspec.adapters.snowflake.migrations import SnowflakeSyncMigrationTracker
from sqlspec.adapters.snowflake.pool import SnowflakeConnectionPool
from sqlspec.adapters.snowflake.tenant import SnowflakeSessionAttributeStrategy, SnowflakeTenantLease
from sqlspec.config import ExtensionConfigs, MigrationConfig, SyncDatabaseConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.tenant import SyncTenantStrategy, TenantStrategyShorthand
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.typing import import_optional

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("SnowflakeConfig", "SnowflakeConnectionParams", "SnowflakeDriverFeatures", "SnowflakePoolParams")


class SnowflakeConnectionParams(TypedDict):
    """Connection options forwarded to the Snowflake Python connector."""

    account: NotRequired[str]
    """Snowflake account identifier, including organization when needed."""
    user: NotRequired[str]
    """Snowflake login user name."""
    username: NotRequired[str]
    """Alias for ``user`` when the canonical key is absent."""
    password: NotRequired[str]
    """Password for password-based authentication."""
    host: NotRequired[str]
    """Explicit Snowflake endpoint host name."""
    port: NotRequired[int]
    """Endpoint port, normally ``443``."""
    protocol: NotRequired[str]
    """Endpoint transport protocol."""
    warehouse: NotRequired[str]
    """Warehouse selected for the connection."""
    database: NotRequired[str]
    """Database selected for the connection."""
    schema: NotRequired[str]
    """Schema selected for the connection."""
    role: NotRequired[str]
    """Role selected for the connection."""
    auth_type: NotRequired[str]
    """Authentication shorthand such as ``jwt``, ``pat``, ``workload_identity``, or ``oauth``."""
    authenticator: NotRequired[str]
    """Connector authenticator name or HTTPS Okta endpoint."""
    token: NotRequired[str]
    """Access token for token-based authentication."""
    token_file_path: NotRequired[str | Path]
    """Path containing a workload identity token."""
    auth_token_provider: NotRequired[Callable[[], str]]
    """Callback producing a fresh credential for each physical connection."""
    private_key: NotRequired[Any]
    """PKCS#8 DER bytes, PEM text, key path, or a private key object."""
    private_key_file: NotRequired[str | Path]
    """Path to an optionally encrypted private key file."""
    private_key_passphrase: NotRequired[str | bytes]
    """Passphrase decrypting an in-memory PEM key or private key file."""
    private_key_file_pwd: NotRequired[str | bytes]
    """Connector passphrase for an encrypted private key file."""
    workload_identity_provider: NotRequired[str]
    """Cloud identity provider used for workload identity authentication."""
    workload_identity_entra_resource: NotRequired[str]
    """Microsoft Entra resource requested during Azure token exchange."""
    workload_identity_impersonation_path: NotRequired[str | Sequence[str]]
    """Ordered identity impersonation chain."""
    connection_name: NotRequired[str]
    """Named profile loaded from the connector connections file."""
    connections_file_path: NotRequired[str | Path]
    """Custom path to the connector TOML connections file."""
    paramstyle: NotRequired[Literal["qmark", "numeric", "pyformat", "format"]]
    """Connector parameter binding mode; defaults to ``qmark``."""
    autocommit: NotRequired[bool]
    """Whether Snowflake commits statements automatically."""
    timezone: NotRequired[str]
    """Initial session time zone, unless supplied in ``session_parameters``."""
    query_tag: NotRequired[str]
    """Initial ``QUERY_TAG``, unless supplied in ``session_parameters``."""
    session_parameters: NotRequired[Mapping[str, Any]]
    """Session settings applied together when connecting."""
    login_timeout: NotRequired[int | float]
    """Maximum login duration in seconds."""
    network_timeout: NotRequired[int | float]
    """Maximum network retry duration in seconds."""
    socket_timeout: NotRequired[int | float]
    """Socket timeout in seconds."""
    application: NotRequired[str]
    """Application identifier recorded by Snowflake."""
    client_session_keep_alive: NotRequired[bool]
    """Keep the session alive while the connection is idle."""
    ocsp_fail_open: NotRequired[bool]
    """Allow connections when certificate revocation checks are unavailable."""
    insecure_mode: NotRequired[bool]
    """Disable connector certificate revocation checks."""
    validate_default_parameters: NotRequired[bool]
    """Validate database, schema, and warehouse names when connecting."""
    extra: NotRequired[Mapping[str, Any]]
    """Additional connector options, with explicit modeled options taking precedence."""


class SnowflakePoolParams(SnowflakeConnectionParams):
    """Snowflake connection options and bounded pool settings."""

    pool_min_size: NotRequired[int]
    """Number of physical sessions opened when constructing the pool; defaults to ``0``."""
    pool_max_size: NotRequired[int]
    """Maximum number of simultaneously owned sessions; defaults to ``5``."""
    pool_timeout: NotRequired[float]
    """Maximum seconds waiting for an available pool slot; defaults to ``30``."""
    pool_recycle_seconds: NotRequired[int]
    """Maximum connection lifetime in seconds; ``0`` disables recycling."""
    health_check_interval: NotRequired[float]
    """Idle seconds between connection health checks; defaults to ``60``."""


class SnowflakeDriverFeatures(TypedDict):
    """Snowflake codec, bulk transfer, and event settings."""

    json_serializer: NotRequired[Callable[[Any], str]]
    """Serialize structured parameters to JSON text."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Decode Snowflake semi-structured result values."""
    on_connection_create: NotRequired[Callable[[SnowflakeConnection], None]]
    """Callback invoked for each newly opened physical connection."""
    enable_lowercase_column_names: NotRequired[bool]
    """Lowercase implicitly uppercase result names while preserving mixed-case aliases."""
    enable_stage_parquet_load: NotRequired[bool]
    """Enable temporary-stage Parquet uploads for large Arrow loads."""
    stage_parquet_threshold_rows: NotRequired[int]
    """Minimum row count selecting stage-based loads; defaults to ``1000``."""
    enable_json_deserialization: NotRequired[bool]
    """Decode VARIANT, OBJECT, and ARRAY result values with ``json_deserializer``; defaults to ``True``."""
    enable_query_tag_correlation: NotRequired[bool]
    """Attach statement correlation information to Snowflake query tags."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec table-backed event channels."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event backend used for Snowflake table queues (``'poll_queue'``)."""
    tenant_strategy: NotRequired[SyncTenantStrategy | TenantStrategyShorthand | None]
    """Opt-in tenant strategy, or the ``"session_attributes"`` shorthand, applied to every session and connection lease."""


class SnowflakePoolConnectionContext(SyncPoolConnectionContext):
    """Acquire a pooled Snowflake connection for a context block."""

    __slots__ = ()


class SnowflakePoolSessionFactory(SyncPoolSessionFactory):
    """Acquire and release a pooled connection for a Snowflake driver session."""

    __slots__ = ()


class SnowflakeConfig(SyncDatabaseConfig[SnowflakeConnection, SnowflakeConnectionPool, SnowflakeDriver]):
    """Configure Snowflake sessions, authentication, and bounded pooling."""

    adapter_name: ClassVar[str | None] = "snowflake"
    connection_type: "ClassVar[type[SnowflakeConnection]]" = SnowflakeConnection
    driver_type: ClassVar[type[SnowflakeDriver]] = SnowflakeDriver
    migration_tracker_type: ClassVar[type[Any]] = SnowflakeSyncMigrationTracker
    supports_tenant_strategy: ClassVar[bool] = True
    tenant_strategy_shorthands: ClassVar[frozenset[str]] = frozenset({"default", "rls", "session_attributes"})
    tenant_strategy_type: ClassVar[type[Any] | None] = SnowflakeSessionAttributeStrategy
    tenant_lease_type: ClassVar[type[Any] | None] = SnowflakeTenantLease
    supports_transactional_ddl: ClassVar[bool] = False
    supports_migration_schemas: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_arrow_streaming: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    storage_partition_strategies: ClassVar[tuple[str, ...]] = ("fixed", "rows_per_chunk")
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="text"
    )
    _connection_context_class: ClassVar[type[SnowflakePoolConnectionContext]] = SnowflakePoolConnectionContext
    _session_factory_class: ClassVar[type[SnowflakePoolSessionFactory]] = SnowflakePoolSessionFactory
    _session_context_class: ClassVar[type[SnowflakeSessionContext]] = SnowflakeSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: SnowflakePoolParams | dict[str, Any] | None = None,
        connection_instance: SnowflakeConnectionPool | None = None,
        migration_config: dict[str, Any] | MigrationConfig | None = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: SnowflakeDriverFeatures | dict[str, Any] | None = None,
        bind_key: str | None = None,
        extension_config: ExtensionConfigs | None = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize independent Snowflake connection and statement settings."""
        raw = dict(connection_config or {})
        extra = raw.get("extra")
        provider = raw.get(
            "auth_token_provider", extra.get("auth_token_provider") if isinstance(extra, Mapping) else None
        )
        if provider is not None and not callable(provider):
            msg = "Snowflake auth_token_provider must be callable"
            raise ImproperConfigurationError(msg)
        static = dict(raw)
        static.pop("auth_token_provider", None)
        if isinstance(extra, Mapping):
            static["extra"] = {key: value for key, value in extra.items() if key != "auth_token_provider"}
        normalized_static = normalize_connection_params(static)
        processed_statement, features = apply_driver_features(
            statement_config, driver_features, paramstyle=normalized_static["paramstyle"]
        )
        super().__init__(
            connection_config=raw,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=processed_statement,
            driver_features=features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def _prepare_derived_connection_config(
        self, config: dict[str, Any], overrides: Mapping[str, Any]
    ) -> dict[str, Any]:
        return merge_snowflake_derive_overrides(config, overrides)

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(spec_input, str):
            return super()._build_replica_spec_overrides(spec_input)
        if spec_input.startswith("snowflake://"):
            parsed = urlsplit(spec_input)
            if not parsed.hostname:
                msg = "Snowflake replica URLs require an account"
                raise ImproperConfigurationError(msg)
            options: dict[str, Any] = {"account": parsed.hostname}
            if parsed.username is not None:
                options["user"] = unquote(parsed.username)
            if parsed.password is not None:
                options["password"] = unquote(parsed.password)
                options["auth_type"] = "password"
            parts = [unquote(part) for part in parsed.path.strip("/").split("/") if part]
            path_keys = ("database", "schema")
            if len(parts) > len(path_keys):
                msg = "Snowflake replica URLs support database and schema path segments"
                raise ImproperConfigurationError(msg)
            options.update(dict(zip(path_keys, parts, strict=False)))
            options.update(parse_qsl(parsed.query))
        else:
            if not spec_input or "://" in spec_input:
                msg = "Snowflake replicas require a warehouse name or snowflake:// URL"
                raise ImproperConfigurationError(msg)
            options = {"warehouse": spec_input}
        return {"connection_config_overrides": options}

    def create_connection(self) -> "SnowflakeConnection":
        """Open a standalone connection whose lifecycle belongs to the caller."""
        self._require_leased_tenant_connection()
        connector = import_optional("snowflake.connector")
        if connector is None:
            raise MissingDependencyError(package="snowflake-connector-python", install_package="snowflake")
        connection = cast(
            "SnowflakeConnection", connector.connect(**normalize_connection_params(self.connection_config))
        )
        try:
            self.get_observability_runtime().emit_connection_create_sync(connection)
        except BaseException:
            connection.close()
            raise
        return connection

    def _create_pool(self) -> SnowflakeConnectionPool:
        return SnowflakeConnectionPool(
            self.connection_config,
            min_size=self.connection_config.get("pool_min_size", 0),
            max_size=self.connection_config.get("pool_max_size", 5),
            timeout=self.connection_config.get("pool_timeout", 30.0),
            recycle_seconds=self.connection_config.get("pool_recycle_seconds", 3600),
            health_check_interval=self.connection_config.get("health_check_interval", 60.0),
            observability_runtime=self.get_observability_runtime(),
        )

    def _close_pool(self) -> None:
        if self.connection_instance is not None:
            self.connection_instance.close()

    def get_event_runtime_hints(self) -> EventRuntimeHints:
        """Return polling and lease defaults suitable for Snowflake warehouses."""
        return EventRuntimeHints(
            poll_interval=2.0, lease_seconds=60, retention_seconds=172_800, select_for_update=False, skip_locked=False
        )

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return Snowflake public types used by framework dependency injection."""
        namespace = super().get_signature_namespace()
        namespace.update({
            "SnowflakeConfig": SnowflakeConfig,
            "SnowflakeConnection": SnowflakeConnection,
            "SnowflakeConnectionParams": SnowflakeConnectionParams,
            "SnowflakeConnectionPool": SnowflakeConnectionPool,
            "SnowflakeCursor": SnowflakeCursor,
            "SnowflakeDriver": SnowflakeDriver,
            "SnowflakeDriverFeatures": SnowflakeDriverFeatures,
            "SnowflakePoolParams": SnowflakePoolParams,
            "SnowflakeSessionContext": SnowflakeSessionContext,
            "SnowflakePoolConnectionContext": SnowflakePoolConnectionContext,
            "SnowflakePoolSessionFactory": SnowflakePoolSessionFactory,
        })
        return namespace
