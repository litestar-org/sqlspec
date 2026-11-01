"""BigQuery database configuration."""

import contextlib
import copy
import threading
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from sqlspec.adapters.bigquery._typing import (
    BigQueryConnection,
    BigQueryCursor,
    BigQuerySessionContext,
    BigQueryStorageWriteModule,
)
from sqlspec.adapters.bigquery._typing import BigQueryLoadJobConfig as LoadJobConfig
from sqlspec.adapters.bigquery._typing import BigQueryQueryJobConfig as QueryJobConfig
from sqlspec.adapters.bigquery.core import apply_driver_features, copy_job_config, default_statement_config
from sqlspec.adapters.bigquery.driver import BigQueryDriver, BigQueryExceptionHandler
from sqlspec.config import ExtensionConfigs, NoPoolSyncConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.typing import Empty
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from types import TracebackType

    from sqlspec.adapters.bigquery._typing import BigQueryClientInfo as ClientInfo
    from sqlspec.adapters.bigquery._typing import BigQueryClientOptions as ClientOptions
    from sqlspec.adapters.bigquery._typing import BigQueryCredentials as Credentials
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("BigQueryConfig", "BigQueryConnectionParams", "BigQueryDriverFeatures", "build_connection_config")

_JOB_CONFIG_OVERRIDE_KEYS: tuple[str, ...] = (
    "dataset_id",
    "use_query_cache",
    "maximum_bytes_billed",
    "query_timeout_ms",
    "job_timeout_ms",
    "labels",
    "priority",
    "reservation",
    "max_slots",
)


class BigQueryConnectionParams(TypedDict):
    """Google Cloud BigQuery client and default ``QueryJobConfig`` parameters."""

    project: NotRequired[str]
    """Billing and default job execution Google Cloud project ID."""
    project_id: NotRequired[str]
    """Alias normalized to ``project`` when ``project`` is unset."""
    location: NotRequired[str]
    """Default geographic region or multi-region (such as ``'US'`` or ``'EU'``) for jobs and datasets."""
    credentials: NotRequired["Credentials"]
    """Explicit ``google.auth.credentials.Credentials`` instance overriding ADC."""
    client_options: NotRequired["ClientOptions"]
    """GAPIC ``ClientOptions`` configuring custom endpoints, quota project, or scopes."""
    client_info: NotRequired["ClientInfo"]
    """Custom ``ClientInfo`` user-agent metadata attached to BigQuery API requests."""

    default_query_job_config: NotRequired[QueryJobConfig]
    """Base ``QueryJobConfig`` cloned and overlaid with top-level query governance fields."""
    default_load_job_config: NotRequired[LoadJobConfig]
    """Base ``LoadJobConfig`` passed to the BigQuery ``Client`` for load jobs."""
    default_job_creation_mode: NotRequired[str]
    """Client job creation mode (such as ``'JOB_CREATION_OPTIONAL'`` for stateless short queries)."""
    dataset_id: NotRequired[str]
    """Default dataset (`dataset` or `project.dataset`) used to resolve unqualified table names."""
    dataset: NotRequired[str]
    """Alias normalized to ``dataset_id`` when ``dataset_id`` is unset."""
    database: NotRequired[str]
    """Alias normalized to ``dataset_id`` when ``dataset_id`` is unset."""
    db: NotRequired[str]
    """Short alias normalized to ``dataset_id`` when ``dataset_id`` is unset."""
    use_query_cache: NotRequired[bool]
    """Read from BigQuery cached query results when unmodified (defaults to ``True``)."""
    maximum_bytes_billed: NotRequired[int]
    """Per-query cost guard failing queries estimated to scan above this byte limit."""
    query_timeout_ms: NotRequired[int]
    """Server-side query execution timeout in milliseconds applied to ``job_timeout_ms``."""
    job_timeout_ms: NotRequired[int]
    """Server-side BigQuery job execution timeout in milliseconds."""
    labels: NotRequired["dict[str, str]"]
    """Billing and governance key-value labels attached to executed query jobs."""
    priority: NotRequired[Literal["INTERACTIVE", "BATCH"]]
    """BigQuery query scheduling priority (``'INTERACTIVE'`` or ``'BATCH'``)."""
    reservation: NotRequired[str]
    """BigQuery Editions slot reservation resource path assigned to query jobs."""
    max_slots: NotRequired[int]
    """Maximum slot concurrency cap applied to individual query jobs."""
    extra: NotRequired["dict[str, Any]"]
    """Additional keyword metadata retained in normalized connection configuration."""


class BigQueryDriverFeatures(TypedDict):
    """BigQuery driver feature flags controlling query execution, Storage Write API, and ``EXPORT DATA``."""

    connection_instance: NotRequired["BigQueryConnection"]
    """Pre-created ``google.cloud.bigquery.Client`` reused without transferring lifecycle ownership."""
    on_connection_create: NotRequired["Callable[[Any], None]"]
    """Callback invoked with the ``bigquery.Client`` when instantiated by the config."""
    json_serializer: NotRequired["Callable[[Any], str]"]
    """Serializer converting dict/list parameters to JSON strings (defaults to ``sqlspec.utils.serializers.to_json``)."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Deserializer converting JSON strings to native dict/list objects."""
    enable_uuid_conversion: NotRequired[bool]
    """Automatically coerce ``UUID`` strings and parameters (defaults to ``True``)."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec table-backed event channel support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event queue backend name (``'poll_queue'``)."""
    job_retry_deadline: NotRequired[float]
    """Total seconds to retry transient query failures (defaults to ``60.0``; ``<= 0`` disables retries)."""
    job_result_timeout: NotRequired[float]
    """Maximum seconds to poll ``QueryJob.result()`` before raising a timeout error."""
    use_query_and_wait: NotRequired[bool]
    """Execute single-statement queries via low-latency ``Client.query_and_wait()`` outside transactions (defaults to ``True``)."""
    request_timeout: NotRequired[float]
    """Per-request HTTP transport timeout in seconds for job submission calls (defaults to ``job_result_timeout`` or ``120.0``)."""
    query_page_size: NotRequired[int]
    """Page size forwarded to ``QueryJob.result()`` when fetching ``SELECT`` rows."""
    query_max_results: NotRequired[int]
    """Maximum total rows fetched from ``QueryJob.result()``."""
    enable_storage_write_api: NotRequired[bool]
    """Route append ``load_from_arrow`` calls through the BigQuery Storage Write API with Parquet load fallback (defaults to ``False``)."""
    storage_write_stream_type: NotRequired[Literal["COMMITTED", "PENDING"]]
    """Storage Write API stream mode (``'PENDING'`` default atomic commit or ``'COMMITTED'`` immediate visibility)."""
    enable_native_storage: NotRequired[bool]
    """Route eligible remote object-store exports through server-side ``EXPORT DATA`` (defaults to ``True``)."""
    native_export_connection: NotRequired[str]
    """BigQuery Cloud Resource connection ID (`project.location.connection`) required for cross-cloud S3/Azure ``EXPORT DATA``."""


def build_connection_config(
    connection_config: "BigQueryConnectionParams | dict[str, Any] | Mapping[str, Any] | None",
) -> dict[str, Any]:
    """Normalize BigQuery connection configuration and map aliases."""
    config = normalize_connection_config(connection_config)
    project_alias = config.pop("project_id", None)
    if project_alias is not None and "project" not in config:
        config["project"] = project_alias
    dataset_alias = config.pop("dataset", None) or config.pop("database", None) or config.pop("db", None)
    if dataset_alias is not None and "dataset_id" not in config:
        config["dataset_id"] = dataset_alias
    return config


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
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="text"
    )
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

        self.connection_config = build_connection_config(connection_config)

        if "json_deserializer" in kwargs:
            driver_features = dict(driver_features) if driver_features else {}
            driver_features["json_deserializer"] = kwargs.pop("json_deserializer")

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        user_connection_hook = cast(
            "Callable[[BigQueryConnection], None] | None", driver_features.pop("on_connection_create", None)
        )
        features_connection_instance = driver_features.pop("connection_instance", None)

        resolved_connection_instance = connection_instance or features_connection_instance
        self._connection_instance = resolved_connection_instance
        self._owns_connection_instance = resolved_connection_instance is None
        self._storage_write_client: Any = None
        self._storage_write_lock = threading.Lock()

        self._setup_default_job_config()
        if resolved_connection_instance is not None:
            self._qualify_default_dataset(resolved_connection_instance)

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
        self.driver_features["_storage_write_client_provider"] = self._provide_storage_write_client

    def __deepcopy__(self, memo: "dict[int, Any]") -> "BigQueryConfig":
        """Create a deep copy without attempting to copy threading locks or circular method bindings."""
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for key, value in self.__dict__.items():
            if key == "_storage_write_lock":
                setattr(result, key, threading.Lock())
            elif key == "_storage_write_client":
                setattr(result, key, None)
            elif key in {"_connection_instance", "connection_instance"} and not self._owns_connection_instance:
                setattr(result, key, value)
            elif key == "driver_features" and isinstance(value, dict):
                features_copy = {
                    k: copy.deepcopy(v, memo) for k, v in value.items() if k != "_storage_write_client_provider"
                }
                features_copy["_storage_write_client_provider"] = result._provide_storage_write_client
                setattr(result, key, features_copy)
            else:
                setattr(result, key, copy.deepcopy(value, memo))
        return result

    def close_pool(self) -> None:
        """Close the clients this configuration created.

        A client supplied by the caller is left open, since its lifetime belongs
        to whoever created it.
        """
        with self._storage_write_lock:
            storage_write_client = self._storage_write_client
            self._storage_write_client = None
        if storage_write_client is not None:
            transport = getattr(storage_write_client, "transport", None)
            if transport is not None:
                transport.close()
        if self._owns_connection_instance and self._connection_instance is not None:
            self._connection_instance.close()
            self._connection_instance = None

    def create_connection(self) -> BigQueryConnection:
        """Create and return a new BigQuery Client instance.

        Returns:
            A new BigQuery Client instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """

        if self._connection_instance is not None:
            return cast("BigQueryConnection", self._connection_instance)

        with self._storage_write_lock:
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

                self._qualify_default_dataset(connection)

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

    def _provide_storage_write_client(self, connection: "BigQueryConnection") -> Any:
        """Return the shared BigQuery Storage Write client, creating it on first use.

        Args:
            connection: The BigQuery client whose credentials the write client reuses.

        Returns:
            The cached ``BigQueryWriteClient``.

        Raises:
            ImproperConfigurationError: If google-cloud-bigquery-storage is absent.
        """
        if self._storage_write_client is not None:
            return self._storage_write_client
        if BigQueryStorageWriteModule is None:
            msg = "google-cloud-bigquery-storage is required for BigQuery Storage Write API ingestion."
            raise ImproperConfigurationError(msg)
        with self._storage_write_lock:
            if self._storage_write_client is not None:
                return self._storage_write_client
            client_kwargs: dict[str, Any] = {"credentials": getattr(connection, "_credentials", None)}
            client_options = self.connection_config.get("client_options")
            if client_options is not None and client_options is not Empty:
                client_kwargs["client_options"] = client_options
            write_client_cls = cast("Any", BigQueryStorageWriteModule).BigQueryWriteClient
            self._storage_write_client = write_client_cls(**client_kwargs)
        return self._storage_write_client

    def _resolve_default_dataset(self) -> "str | None":
        """Resolve the fully qualified default dataset, if one can be determined.

        BigQuery requires ``project.dataset``; an unqualified name raises when
        assigned, so it is left unset rather than turned into a construction
        error.

        Returns:
            The qualified dataset reference, or None when no project is known.
        """
        dataset_id = self.connection_config.get("dataset_id")
        if not dataset_id:
            return None
        if "." in dataset_id:
            return cast("str", dataset_id)
        project = self.connection_config.get("project")
        if not project:
            return None
        return f"{project}.{dataset_id}"

    def _qualify_default_dataset(self, connection: "BigQueryConnection") -> None:
        """Fill in the default dataset once the client's own project is known.

        A dataset configured without a project cannot be qualified at
        construction time without a credentials lookup, so it is resolved here
        from the client instead.

        Args:
            connection: The freshly created BigQuery client.
        """
        job_config = self.connection_config.get("default_query_job_config")
        if job_config is None:
            return
        dataset_id = self.connection_config.get("dataset_id")
        if (
            dataset_id
            and "." not in dataset_id
            and not self.connection_config.get("project")
            and job_config.default_dataset is None
        ):
            project = getattr(connection, "project", None)
            if project:
                job_config.default_dataset = f"{project}.{dataset_id}"
        if getattr(connection, "_default_query_job_config", None) is None or (
            job_config.default_dataset is not None
            and getattr(connection._default_query_job_config, "default_dataset", None) is None
        ):
            with contextlib.suppress(AttributeError, TypeError):
                connection._default_query_job_config = job_config

    def _setup_default_job_config(self) -> None:
        """Set up default job configuration."""
        existing_config = self.connection_config.get("default_query_job_config")
        has_overrides = any(self.connection_config.get(key) is not None for key in _JOB_CONFIG_OVERRIDE_KEYS)
        if existing_config is not None:
            if not has_overrides:
                return
            job_config = QueryJobConfig()
            copy_job_config(existing_config, job_config)
        else:
            job_config = QueryJobConfig()

        default_dataset = self._resolve_default_dataset()
        if default_dataset is not None:
            job_config.default_dataset = default_dataset

        use_query_cache = self.connection_config.get("use_query_cache")
        if use_query_cache is not None:
            job_config.use_query_cache = use_query_cache
        elif existing_config is None:
            job_config.use_query_cache = True

        maximum_bytes_billed = self.connection_config.get("maximum_bytes_billed")
        if maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = maximum_bytes_billed

        query_timeout_ms = self.connection_config.get("query_timeout_ms")
        if query_timeout_ms is not None:
            job_config.job_timeout_ms = query_timeout_ms

        job_timeout_ms = self.connection_config.get("job_timeout_ms")
        if job_timeout_ms is not None:
            job_config.job_timeout_ms = job_timeout_ms

        labels = self.connection_config.get("labels")
        if labels is not None:
            job_config.labels = labels

        priority = self.connection_config.get("priority")
        if priority is not None:
            job_config.priority = priority.upper()

        reservation = self.connection_config.get("reservation")
        if reservation is not None:
            job_config.reservation = reservation

        max_slots = self.connection_config.get("max_slots")
        if max_slots is not None:
            job_config.max_slots = max_slots

        self.connection_config["default_query_job_config"] = job_config
