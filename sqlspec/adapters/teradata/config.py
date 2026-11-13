"""Teradata Vantage database configuration."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, Self, TypedDict, cast

from sqlspec.adapters.teradata._typing import (
    TeradataConnection,
    TeradataCursor,
    TeradataRawCursor,
    TeradataSessionContext,
)
from sqlspec.adapters.teradata.core import (
    apply_driver_features,
    build_connection_config,
    default_statement_config,
    merge_teradata_derive_overrides,
    parse_teradata_replica_endpoint,
)
from sqlspec.adapters.teradata.driver import TeradataDriver, TeradataExceptionHandler
from sqlspec.adapters.teradata.migrations import TeradataSyncMigrationTracker
from sqlspec.adapters.teradata.pool import TeradataConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.config import MigrationConfig
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "TeradataConfig",
    "TeradataConnectionContext",
    "TeradataConnectionParams",
    "TeradataDriverFeatures",
    "TeradataLogMech",
    "TeradataPoolParams",
    "TeradataSSLMode",
    "TeradataTransactionMode",
)


TeradataLogMech = Literal["TD2", "LDAP", "TDNEGO", "KRB5", "BROWSER", "JWT", "BEARER", "CODE", "CRED", "ROPC", "SECRET"]
TeradataSSLMode = Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]
TeradataTransactionMode = Literal["ANSI", "TERA", "DEFAULT"]


class TeradataConnectionParams(TypedDict, total=False):
    """Native teradatasql connection options."""

    host: str
    """Teradata server hostname."""
    user: str
    """Native authentication user."""
    username: str
    """Friendly authentication user alias."""
    password: str
    """Authentication password."""
    database: str
    """Default database."""
    dbs_port: str
    """Native server port."""
    port: int | str
    """Friendly server port alias."""
    autocommit: bool
    """Initial transaction autocommit state."""
    logmech: TeradataLogMech
    """Authentication mechanism."""
    tmode: TeradataTransactionMode
    """Transaction mode."""
    sslmode: TeradataSSLMode
    """TLS verification mode."""
    encryptdata: bool | str
    """Enable encrypted data transport."""
    account: NotRequired[str]
    """Teradata workload management account string."""
    logdata: NotRequired[str]
    """Mechanism-specific authentication payload."""
    charset: NotRequired[str]
    """Session character set."""
    sslca: NotRequired[str]
    """Path to CA certificate bundle file."""
    sslcapath: NotRequired[str]
    """Path to CA certificate directory."""
    lob_support: NotRequired[bool | str]
    """Enable LOB streaming support."""
    connect_timeout: NotRequired[int | float | str]
    """Connection establishment timeout."""
    request_timeout: NotRequired[int | float | str]
    """Statement request timeout."""
    sJson: NotRequired[str]
    """Raw teradatasql JSON connection payload."""
    cop: NotRequired[bool | str]
    """Enable COP discovery."""
    coplast: NotRequired[bool | str]
    """Retry last COP entry."""
    https_port: NotRequired[int | str]
    """HTTPS port for TLS/OAuth connections."""
    extra: dict[str, Any]
    """Additional vendor options."""


class TeradataPoolParams(TeradataConnectionParams):
    """Teradata Vantage pool parameters."""

    pool_min_size: NotRequired[int]
    """Minimum number of connections maintained in the pool."""
    pool_max_size: NotRequired[int]
    """Maximum number of connections allowed in the pool."""
    pool_timeout: NotRequired[float]
    """Seconds to wait for a connection before timing out."""
    pool_recycle_seconds: NotRequired[int]
    """Maximum age in seconds before a connection is recycled."""
    health_check_interval: NotRequired[float]
    """Interval in seconds between background health checks."""


class TeradataDriverFeatures(TypedDict):
    """Teradata Vantage driver feature flags."""

    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom callable for serializing objects to JSON strings."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom callable for deserializing JSON strings."""
    on_connection_create: "NotRequired[Callable[[TeradataConnection], None]]"
    """Callback invoked immediately when a new connection is established."""
    arraysize: NotRequired[int]
    """DB-API row fetch chunk size."""
    enable_lowercase_column_names: NotRequired[bool]
    """Fold implicit uppercase column names."""
    enable_fastexport: NotRequired[bool]
    """Attempt native FastExport for reads."""
    enable_fastload: NotRequired[bool]
    """Attempt native FastLoad for inserts."""
    query_band: NotRequired[Mapping[str, str] | str]
    """Session query-band metadata."""
    enable_events: NotRequired[bool]
    """Enable database event listening."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Backend mechanism used for dispatching events."""


class TeradataConnectionContext(SyncPoolConnectionContext):
    """Context manager for Teradata Vantage connections."""

    __slots__ = ()


class _TeradataSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


class TeradataConfig(SyncDatabaseConfig[TeradataConnection, TeradataConnectionPool, TeradataDriver]):
    """Configuration for Teradata Vantage synchronous connections over teradatasql."""

    __slots__ = (
        "_health_check_interval",
        "_pool_max_size",
        "_pool_min_size",
        "_pool_recycle_seconds",
        "_pool_timeout",
        "_session_query_band",
        "_user_connection_hook",
    )

    driver_type: "ClassVar[type[TeradataDriver]]" = TeradataDriver
    connection_type: "ClassVar[type[TeradataConnection]]" = cast("type[TeradataConnection]", TeradataConnection)
    migration_tracker_type: "ClassVar[type[TeradataSyncMigrationTracker]]" = TeradataSyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_migration_schemas: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[TeradataConnectionContext]]" = TeradataConnectionContext
    _session_factory_class: "ClassVar[type[_TeradataSessionConnectionHandler]]" = _TeradataSessionConnectionHandler
    _session_context_class: "ClassVar[type[TeradataSessionContext]]" = TeradataSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "TeradataPoolParams | dict[str, Any] | None" = None,
        connection_instance: "TeradataConnectionPool | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "TeradataDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        raw_config = dict(normalize_connection_config(connection_config))
        self._pool_min_size = raw_config.pop("pool_min_size", 1)
        self._pool_max_size = raw_config.pop("pool_max_size", 4)
        self._pool_timeout = float(raw_config.pop("pool_timeout", 30.0))
        self._pool_recycle_seconds = int(raw_config.pop("pool_recycle_seconds", 1800))
        self._health_check_interval = float(raw_config.pop("health_check_interval", 60.0))
        normalized_connection_config = build_connection_config(raw_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[TeradataConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )

        band = features_dict.get("query_band")
        self._session_query_band = band

        super().__init__(
            connection_config=normalized_connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def _prepare_derived_connection_config(
        self, config: dict[str, Any], overrides: Mapping[str, Any]
    ) -> dict[str, Any]:
        return merge_teradata_derive_overrides(config, overrides)

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(spec_input, str):
            return {"connection_config_overrides": parse_teradata_replica_endpoint(spec_input)}
        return super()._build_replica_spec_overrides(spec_input)

    def create_connection(self) -> "TeradataConnection":
        """Open a standalone Teradata Vantage connection owned by the caller."""
        return self.provide_pool().new_connection()

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "TeradataConfig": TeradataConfig,
            "TeradataConnection": TeradataConnection,
            "TeradataConnectionContext": TeradataConnectionContext,
            "TeradataConnectionParams": TeradataConnectionParams,
            "TeradataConnectionPool": TeradataConnectionPool,
            "TeradataCursor": TeradataCursor,
            "TeradataDriver": TeradataDriver,
            "TeradataDriverFeatures": TeradataDriverFeatures,
            "TeradataExceptionHandler": TeradataExceptionHandler,
            "TeradataPoolParams": TeradataPoolParams,
            "TeradataRawCursor": TeradataRawCursor,
            "TeradataSessionContext": TeradataSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.5, lease_seconds=30, select_for_update=False, skip_locked=False)

    def derive_pool(
        self,
        *,
        connection_config_overrides: Mapping[str, Any] | None = None,
        driver_features_overrides: Mapping[str, Any] | None = None,
        statement_config: "StatementConfig | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        bind_key: str | None = None,
        **overrides: Any,
    ) -> Self:
        """Derive an independent pool preserving the parent's capacity and timeouts."""
        settings = {
            "pool_min_size": self._pool_min_size,
            "pool_max_size": self._pool_max_size,
            "pool_timeout": self._pool_timeout,
            "pool_recycle_seconds": self._pool_recycle_seconds,
            "health_check_interval": self._health_check_interval,
        }
        settings.update(connection_config_overrides or {})
        return super().derive_pool(
            connection_config_overrides=settings,
            driver_features_overrides=driver_features_overrides,
            statement_config=statement_config,
            migration_config=migration_config,
            extension_config=extension_config,
            observability_config=observability_config,
            bind_key=bind_key,
            **overrides,
        )

    def _create_pool(self) -> "TeradataConnectionPool":
        return TeradataConnectionPool(
            self.connection_config,
            min_size=self._pool_min_size,
            max_size=self._pool_max_size,
            timeout=self._pool_timeout,
            recycle_seconds=self._pool_recycle_seconds,
            health_check_interval=self._health_check_interval,
            on_connection_create=self._user_connection_hook,
            session_query_band=self._session_query_band,
        )

    def _close_pool(self) -> None:
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None
