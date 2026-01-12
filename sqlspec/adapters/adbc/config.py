"""ADBC database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.adbc._typing import AdbcConnection
from sqlspec.adapters.adbc.core import (
    apply_driver_features,
    build_connection_config,
    get_statement_config,
    resolve_dialect_from_config,
    resolve_driver_connect_func,
)
from sqlspec.adapters.adbc.driver import AdbcCursor, AdbcDriver, AdbcExceptionHandler, AdbcSessionContext
from sqlspec.config import ExtensionConfigs, NoPoolSyncConfig
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.observability import ObservabilityConfig


__all__ = ("AdbcConfig", "AdbcConnectionParams", "AdbcDriverFeatures")


class AdbcConnectionParams(TypedDict):
    """ADBC connection parameters."""

    uri: NotRequired[str]
    driver_name: NotRequired[str]
    db_kwargs: NotRequired[dict[str, Any]]
    conn_kwargs: NotRequired[dict[str, Any]]
    adbc_driver_manager_entrypoint: NotRequired[str]
    autocommit: NotRequired[bool]
    isolation_level: NotRequired[str]
    batch_size: NotRequired[int]
    query_timeout: NotRequired[float]
    connection_timeout: NotRequired[float]
    ssl_mode: NotRequired[str]
    ssl_cert: NotRequired[str]
    ssl_key: NotRequired[str]
    ssl_ca: NotRequired[str]
    username: NotRequired[str]
    password: NotRequired[str]
    token: NotRequired[str]
    project_id: NotRequired[str]
    dataset_id: NotRequired[str]
    account: NotRequired[str]
    warehouse: NotRequired[str]
    database: NotRequired[str]
    schema: NotRequired[str]
    role: NotRequired[str]
    authorization_header: NotRequired[str]
    grpc_options: NotRequired[dict[str, Any]]
    gizmosql_backend: NotRequired[str]
    tls_skip_verify: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class AdbcDriverFeatures(TypedDict):
    """ADBC driver feature configuration.

    Controls optional type handling and serialization behavior for the ADBC adapter.
    These features configure how data is converted between Python and Arrow types.

    Attributes:
        json_serializer: JSON serialization function to use.
            Callable that takes Any and returns str (JSON string).
            Default: sqlspec.utils.serializers.to_json
        enable_cast_detection: Enable cast-aware parameter processing.
            When True, detects SQL casts (e.g., ::JSONB) and applies appropriate
            serialization. Currently used for PostgreSQL JSONB handling.
            Default: True
        enable_strict_type_coercion: Enforce strict type coercion rules.
            When True, raises errors for unsupported type conversions.
            When False, attempts best-effort conversion.
            Default: False
        strict_type_coercion: Alias for enable_strict_type_coercion.
        enable_arrow_extension_types: Enable PyArrow extension type support.
            When True, preserves Arrow extension type metadata when reading data.
            When False, falls back to storage types.
            Default: True
        arrow_extension_types: Alias for enable_arrow_extension_types.
        enable_events: Enable database event channel support.
            Defaults to True when extension_config["events"] is configured.
            Provides pub/sub capabilities via table-backed queue (ADBC has no native pub/sub).
            Requires extension_config["events"] for migration setup.
        events_backend: Event channel backend selection.
            Only option: "table_queue" (durable table-backed queue with retries and exactly-once delivery).
            ADBC does not have native pub/sub, so table_queue is the only backend.
            Defaults to "table_queue".
    """

    json_serializer: "NotRequired[Callable[[Any], str]]"
    enable_cast_detection: NotRequired[bool]
    enable_strict_type_coercion: NotRequired[bool]
    strict_type_coercion: NotRequired[bool]
    enable_arrow_extension_types: NotRequired[bool]
    arrow_extension_types: NotRequired[bool]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class AdbcConnectionContext:
    """Context manager for ADBC connections."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "AdbcConfig") -> None:
        self._config = config
        self._connection: AdbcConnection | None = None

    def __enter__(self) -> "AdbcConnection":
        self._connection = self._config.create_connection()
        return self._connection

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._connection:
            self._connection.close()
            self._connection = None
        return None


class _AdbcSessionConnectionHandler:
    __slots__ = ("_config", "_connection")

    def __init__(self, config: "AdbcConfig") -> None:
        self._config = config
        self._connection: AdbcConnection | None = None

    def acquire_connection(self) -> "AdbcConnection":
        self._connection = self._config.create_connection()
        return self._connection

    def release_connection(self, _conn: "AdbcConnection") -> None:
        if self._connection is None:
            return
        self._connection.close()
        self._connection = None


class AdbcConfig(NoPoolSyncConfig[AdbcConnection, AdbcDriver]):
    """ADBC configuration for Arrow Database Connectivity.

    ADBC provides an interface for connecting to multiple database systems
    with Arrow-native data transfer.

    Supports multiple database backends including PostgreSQL, SQLite, DuckDB,
    BigQuery, and Snowflake with automatic driver detection and loading.
    """

    driver_type: ClassVar[type[AdbcDriver]] = AdbcDriver
    connection_type: "ClassVar[type[AdbcConnection]]" = AdbcConnection
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    storage_partition_strategies: "ClassVar[tuple[str, ...]]" = ("fixed", "rows_per_chunk")

    def __init__(
        self,
        *,
        connection_config: "AdbcConnectionParams | dict[str, Any] | None" = None,
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: StatementConfig | None = None,
        driver_features: "AdbcDriverFeatures | dict[str, Any] | None" = None,
        bind_key: str | None = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize configuration.

        Args:
            connection_config: Connection configuration parameters
            connection_instance: Pre-created connection instance to use instead of creating new one
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Driver feature configuration (AdbcDriverFeatures)
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.
        """
        self.connection_config = normalize_connection_config(connection_config)

        if statement_config is None:
            statement_config = get_statement_config(resolve_dialect_from_config(self.connection_config))

        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        super().__init__(
            connection_config=self.connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def create_connection(self) -> AdbcConnection:
        """Create and return a new connection using the specified driver.

        Returns:
            A new connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """

        try:
            connection = resolve_driver_connect_func(
                self.connection_config.get("driver_name"), self.connection_config.get("uri")
            )(**build_connection_config(self.connection_config))
            return cast("AdbcConnection", connection)
        except Exception as e:
            driver_name = self.connection_config.get("driver_name", "Unknown")
            msg = f"Could not configure connection using driver '{driver_name}'. Error: {e}"
            raise ImproperConfigurationError(msg) from e

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AdbcConnectionContext":
        """Provide a connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            A connection context manager.
        """
        return AdbcConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "AdbcSessionContext":
        """Provide a driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            A context manager that yields an AdbcDriver instance.
        """
        statement_config = (
            statement_config
            or self.statement_config
            or get_statement_config(resolve_dialect_from_config(self.connection_config))
        )
        handler = _AdbcSessionConnectionHandler(self)

        return AdbcSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for AdbcConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "AdbcConnectionContext": AdbcConnectionContext,
            "AdbcConnection": AdbcConnection,
            "AdbcConnectionParams": AdbcConnectionParams,
            "AdbcCursor": AdbcCursor,
            "AdbcDriver": AdbcDriver,
            "AdbcDriverFeatures": AdbcDriverFeatures,
            "AdbcExceptionHandler": AdbcExceptionHandler,
            "AdbcSessionContext": AdbcSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults suitable for ADBC warehouses."""

        return EventRuntimeHints(poll_interval=2.0, lease_seconds=60, retention_seconds=172_800)
