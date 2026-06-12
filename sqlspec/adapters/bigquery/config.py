"""BigQuery database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig
from typing_extensions import NotRequired

from sqlspec.adapters.bigquery._typing import BigQueryConnection, BigQueryCursor, BigQuerySessionContext
from sqlspec.adapters.bigquery.core import apply_driver_features, build_statement_config, default_statement_config
from sqlspec.adapters.bigquery.driver import BigQueryDriver, BigQueryExceptionHandler
from sqlspec.config import ExtensionConfigs, NoPoolSyncConfig
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.typing import Empty
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("BigQueryConfig", "BigQueryConnectionParams", "BigQueryDriverFeatures")


class BigQueryConnectionParams(TypedDict):
    """Standard BigQuery connection parameters.

    Includes both official BigQuery client parameters and BigQuery-specific configuration options.
    """

    project: NotRequired[str]
    location: NotRequired[str]
    credentials: NotRequired["Credentials"]
    client_options: NotRequired["ClientOptions"]
    client_info: NotRequired["ClientInfo"]

    default_query_job_config: NotRequired[QueryJobConfig]
    default_load_job_config: NotRequired[LoadJobConfig]
    default_job_creation_mode: NotRequired[str]
    dataset_id: NotRequired[str]
    use_query_cache: NotRequired[bool]
    maximum_bytes_billed: NotRequired[int]
    enable_bigquery_ml: NotRequired[bool]
    enable_gemini_integration: NotRequired[bool]
    query_timeout_ms: NotRequired[int]
    job_timeout_ms: NotRequired[int]
    reservation_id: NotRequired[str]
    edition: NotRequired[str]
    enable_cross_cloud: NotRequired[bool]
    enable_bigquery_omni: NotRequired[bool]
    use_avro_logical_types: NotRequired[bool]
    parquet_enable_list_inference: NotRequired[bool]
    enable_column_level_security: NotRequired[bool]
    enable_row_level_security: NotRequired[bool]
    enable_dataframes: NotRequired[bool]
    dataframes_backend: NotRequired[str]
    enable_continuous_queries: NotRequired[bool]
    enable_vector_search: NotRequired[bool]
    extra: NotRequired["dict[str, Any]"]


class BigQueryDriverFeatures(TypedDict):
    """BigQuery driver-specific features configuration.

    Only non-standard BigQuery client parameters that are SQLSpec-specific extensions.

    Attributes:
        connection_instance: Pre-existing BigQuery connection instance to use.
        on_connection_create: Callback invoked when connection is created.
        json_serializer: Custom JSON serializer for dict/list parameter conversion.
            Defaults to sqlspec.utils.serializers.to_json if not provided.
        enable_uuid_conversion: Enable automatic UUID string conversion.
            When True (default), UUID strings are automatically converted to UUID objects.
            When False, UUID strings are treated as regular strings.
        enable_events: Enable database event channel support.
            Defaults to True when extension_config["events"] is configured.
            Provides pub/sub capabilities via table-backed queue (BigQuery has no native pub/sub).
            Requires extension_config["events"] for migration setup.
        events_backend: Event channel backend selection.
        Only option: "table_queue" (durable table-backed queue with retries and exactly-once delivery).
            BigQuery does not have native pub/sub, so table_queue is the only backend.
            Defaults to "table_queue".
        job_retry_deadline: Total seconds to keep retrying transient job failures. Defaults to 60.0.
            Values <= 0 disable retries entirely: API requests are not retried and ``job_retry`` is
            withheld from ``client.query()``, which also bypasses the client's built-in
            ``jobs.insert`` retry wrapper (fixed 600s deadline, retries transport timeouts).
        job_result_timeout: Timeout (seconds) for polling ``QueryJob.result()``. Defaults to the
            client polling default (waits indefinitely for the job using the API's per-call default
            timeouts). Also used as the per-request HTTP timeout when ``request_timeout`` is unset.
        request_timeout: Per-request HTTP transport timeout (seconds) for the API calls that start
            query jobs. Bounds each request so a server that accepts the request but never responds
            (e.g. a wedged emulator) raises instead of blocking indefinitely. Defaults to
            ``job_result_timeout`` when that is numeric, else 120.0.
    """

    connection_instance: NotRequired["BigQueryConnection"]
    on_connection_create: NotRequired["Callable[[Any], None]"]
    json_serializer: NotRequired["Callable[[Any], str]"]
    enable_uuid_conversion: NotRequired[bool]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]
    job_retry_deadline: NotRequired[float]
    job_result_timeout: NotRequired[float]
    request_timeout: NotRequired[float]


class BigQueryConnectionContext(SyncPoolConnectionContext):
    """Context manager for BigQuery connections."""

    __slots__ = ("_connection",)

    def __init__(self, config: "BigQueryConfig") -> None:
        super().__init__(config)
        self._connection: BigQueryConnection | None = None

    def __enter__(self) -> BigQueryConnection:
        self._connection = self._config.create_connection()
        return cast("BigQueryConnection", self._connection)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        return None


class _BigQuerySessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()

    def __init__(self, config: "BigQueryConfig") -> None:
        super().__init__(config)

    def acquire_connection(self) -> "BigQueryConnection":
        return cast("BigQueryConnection", self._config.create_connection())

    def release_connection(self, _conn: "BigQueryConnection", **kwargs: Any) -> None:
        return None


class BigQueryConfig(NoPoolSyncConfig[BigQueryConnection, BigQueryDriver]):
    """BigQuery configuration.

    Configuration for Google Cloud BigQuery connections.
    """

    driver_type: ClassVar[type[BigQueryDriver]] = BigQueryDriver
    connection_type: "ClassVar[type[BigQueryConnection]]" = BigQueryConnection
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_arrow_streaming: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    requires_staging_for_load: ClassVar[bool] = True
    staging_protocols: "ClassVar[tuple[str, ...]]" = ("gs://",)
    _connection_context_class: "ClassVar[type[BigQueryConnectionContext]]" = BigQueryConnectionContext
    _session_factory_class: "ClassVar[type[_BigQuerySessionConnectionHandler]]" = _BigQuerySessionConnectionHandler
    _session_context_class: "ClassVar[type[BigQuerySessionContext]]" = BigQuerySessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "BigQueryConnectionParams | dict[str, Any] | None" = None,
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "BigQueryDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize BigQuery configuration.

        Args:
            connection_config: Connection configuration parameters
            connection_instance: Pre-created BigQuery Client instance to use instead of creating new one
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: BigQuery-specific driver features
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.
        """

        self.connection_config = normalize_connection_config(connection_config)

        (driver_features, serializer, user_connection_hook, features_connection_instance) = apply_driver_features(
            driver_features
        )

        resolved_connection_instance = connection_instance or features_connection_instance
        self._connection_instance = resolved_connection_instance

        if "default_query_job_config" not in self.connection_config:
            self._setup_default_job_config()

        statement_config = statement_config or build_statement_config(json_serializer=serializer)

        # Fired directly in create_connection (the client-construction path) like every other adapter,
        # rather than bridged through the observability lifecycle dispatcher (which only runs under the
        # SQLSpec registry wrapper, not bare config.provide_session()).
        self._user_connection_hook = user_connection_hook

        super().__init__(
            connection_config=self.connection_config,
            connection_instance=resolved_connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

        self.driver_features = driver_features

    def _setup_default_job_config(self) -> None:
        """Set up default job configuration."""

        job_config = QueryJobConfig()

        dataset_id = self.connection_config.get("dataset_id")
        project = self.connection_config.get("project")
        if dataset_id and project and "." not in dataset_id:
            job_config.default_dataset = f"{project}.{dataset_id}"

        use_query_cache = self.connection_config.get("use_query_cache")
        if use_query_cache is not None:
            job_config.use_query_cache = use_query_cache
        else:
            job_config.use_query_cache = True

        maximum_bytes_billed = self.connection_config.get("maximum_bytes_billed")
        if maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = maximum_bytes_billed

        query_timeout_ms = self.connection_config.get("query_timeout_ms")
        if query_timeout_ms is not None:
            job_config.job_timeout_ms = query_timeout_ms

        # job_timeout_ms intentionally wins when both timeout aliases are configured.
        job_timeout_ms = self.connection_config.get("job_timeout_ms")
        if job_timeout_ms is not None:
            job_config.job_timeout_ms = job_timeout_ms

        self.connection_config["default_query_job_config"] = job_config

    def create_connection(self) -> BigQueryConnection:
        """Create and return a new BigQuery Client instance.

        Returns:
            A new BigQuery Client instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """

        if self._connection_instance is not None:
            return cast("BigQueryConnection", self._connection_instance)

        try:
            client_fields = {
                "project",
                "location",
                "credentials",
                "client_options",
                "client_info",
                "default_query_job_config",
                "default_load_job_config",
                "default_job_creation_mode",
            }
            config_dict: dict[str, Any] = {
                field: value
                for field, value in self.connection_config.items()
                if field in client_fields and value is not None and value is not Empty
            }
            connection = self.connection_type(**config_dict)

            if self._user_connection_hook is not None:
                self._user_connection_hook(connection)

            self._connection_instance = connection
        except Exception as e:
            project = self.connection_config.get("project", "Unknown")
            msg = f"Could not configure BigQuery connection for project '{project}'. Error: {e}"
            raise ImproperConfigurationError(msg) from e
        return connection

    def provide_connection(self, *_args: Any, **_kwargs: Any) -> "BigQueryConnectionContext":
        """Provide a BigQuery client within a context manager.

        Args:
            *_args: Additional arguments.
            **_kwargs: Additional keyword arguments.

        Returns:
            A BigQuery connection context manager.
        """
        return BigQueryConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "BigQuerySessionContext":
        """Provide a BigQuery driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            A BigQuery driver session context manager.
        """
        handler = _BigQuerySessionConnectionHandler(self)

        return BigQuerySessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for BigQuery types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "BigQueryConnectionContext": BigQueryConnectionContext,
            "BigQueryConnection": BigQueryConnection,
            "BigQueryConnectionParams": BigQueryConnectionParams,
            "BigQueryCursor": BigQueryCursor,
            "BigQueryDriver": BigQueryDriver,
            "BigQueryDriverFeatures": BigQueryDriverFeatures,
            "BigQueryExceptionHandler": BigQueryExceptionHandler,
            "BigQuerySessionContext": BigQuerySessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults tuned for BigQuery latency."""

        return EventRuntimeHints(poll_interval=2.0, lease_seconds=60, retention_seconds=172_800)
