"""Spanner configuration."""

import contextlib
import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, Protocol, TypedDict, cast

from sqlspec.adapters.spanner._typing import SpannerAbstractSessionPool as AbstractSessionPool
from sqlspec.adapters.spanner._typing import SpannerAsyncAbstractSessionPool as AsyncAbstractSessionPool
from sqlspec.adapters.spanner._typing import SpannerAsyncBurstyPool as AsyncBurstyPool
from sqlspec.adapters.spanner._typing import SpannerAsyncClient as AsyncClient
from sqlspec.adapters.spanner._typing import (
    SpannerAsyncConnection,
    SpannerAsyncSessionContext,
    SpannerAsyncTransactionType,
    SpannerGoogleAPICallError,
    SpannerSyncConnection,
    SpannerSyncSessionContext,
    SpannerTransactionType,
)
from sqlspec.adapters.spanner._typing import SpannerAsyncFixedSizePool as AsyncFixedSizePool
from sqlspec.adapters.spanner._typing import SpannerAsyncPingingPool as AsyncPingingPool
from sqlspec.adapters.spanner._typing import SpannerBurstyPool as BurstyPool
from sqlspec.adapters.spanner._typing import SpannerClient as Client
from sqlspec.adapters.spanner._typing import SpannerFixedSizePool as FixedSizePool
from sqlspec.adapters.spanner._typing import SpannerPingingPool as PingingPool
from sqlspec.adapters.spanner.core import (
    apply_driver_features,
    build_session_driver_features,
    create_mapped_exception,
    default_statement_config,
    execute_partitioned_dml_async,
    execute_partitioned_dml_sync,
    resolve_transaction_completion,
    run_in_transaction_async,
    run_in_transaction_sync,
)
from sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerSyncDriver
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import (
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from logging import Logger
    from types import TracebackType

    from sqlspec.adapters.spanner._typing import SpannerAsyncDatabase as AsyncDatabase
    from sqlspec.adapters.spanner._typing import SpannerClientInfo as ClientInfo
    from sqlspec.adapters.spanner._typing import SpannerClientOptions as ClientOptions
    from sqlspec.adapters.spanner._typing import SpannerCredentials as Credentials
    from sqlspec.adapters.spanner._typing import SpannerDatabase as Database
    from sqlspec.adapters.spanner._typing import SpannerDatabaseDialect as DatabaseDialect
    from sqlspec.adapters.spanner._typing import SpannerDefaultTransactionOptions as DefaultTransactionOptions
    from sqlspec.adapters.spanner._typing import SpannerDirectedReadOptions as DirectedReadOptions
    from sqlspec.adapters.spanner._typing import SpannerEncryptionConfig as EncryptionConfig
    from sqlspec.adapters.spanner._typing import SpannerExecuteSqlRequest as ExecuteSqlRequest
    from sqlspec.adapters.spanner._typing import SpannerRequestOptions as RequestOptions
    from sqlspec.adapters.spanner._typing import SpannerRetry as Retry
    from sqlspec.config import ExtensionConfigs
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig
    from sqlspec.storage import StorageCapabilities

__all__ = (
    "SpannerAsyncConfig",
    "SpannerConnectionParams",
    "SpannerDriverFeatures",
    "SpannerPoolParams",
    "SpannerSyncConfig",
    "build_connection_config",
)

_DEFAULT_SESSION_TRANSACTION: bool = True
"""Default ``transaction`` flag for ``provide_session`` / ``provide_connection``.

``True`` yields a write-capable ``Transaction`` context; read-only
``Snapshot`` contexts are available through ``provide_read_session``."""

_CLIENT_CONFIG_FIELDS = frozenset({
    "project",
    "credentials",
    "client_info",
    "client_options",
    "query_options",
    "route_to_leader_enabled",
    "directed_read_options",
    "observability_options",
    "default_transaction_options",
    "experimental_host",
    "disable_builtin_metrics",
    "client_context",
    "use_plain_text",
    "ca_certificate",
    "client_certificate",
    "client_key",
    "instance_type",
})
_INSTANCE_CONFIG_FIELDS = frozenset({"configuration_name", "display_name", "node_count", "processing_units"})
_DATABASE_CONFIG_FIELDS = frozenset({
    "ddl_statements",
    "logger",
    "encryption_config",
    "database_dialect",
    "database_role",
    "enable_drop_protection",
    "enable_interceptors_in_tests",
    "proto_descriptors",
})


class SpannerConnectionParams(TypedDict):
    """Google Cloud Spanner client, instance, and database connection parameters."""

    project: "NotRequired[str]"
    """Google Cloud project ID owning the Spanner instance."""
    project_id: "NotRequired[str]"
    """Alias normalized to ``project`` when ``project`` is unset."""
    credentials: "NotRequired[Credentials]"
    """Explicit ``google.auth.credentials.Credentials`` instance overriding ADC."""
    client_info: "NotRequired[ClientInfo]"
    """Custom ``google.api_core.gapic_v1.client_info.ClientInfo`` user-agent metadata."""
    client_options: "NotRequired[ClientOptions | dict[str, Any]]"
    """GAPIC ``ClientOptions`` or mapping configuring API endpoint, universe domain, or quota project."""
    query_options: "NotRequired[ExecuteSqlRequest.QueryOptions]"
    """Client-wide default ``ExecuteSqlRequest.QueryOptions`` (such as ``optimizer_version``)."""
    route_to_leader_enabled: "NotRequired[bool]"
    """Route read-write transactions and Partitioned DML directly to Paxos leader replicas."""
    directed_read_options: "NotRequired[DirectedReadOptions]"
    """Client-wide replica routing rules for read-only snapshot requests."""
    observability_options: "NotRequired[Any]"
    """Google Cloud Spanner client-side OpenTelemetry tracing configuration."""
    default_transaction_options: "NotRequired[DefaultTransactionOptions]"
    """Default isolation level or read-lock mode applied to read-write transactions."""
    experimental_host: "NotRequired[str]"
    """Direct single-endpoint Spanner or emulator host address."""
    disable_builtin_metrics: "NotRequired[bool]"
    """Disable built-in Cloud Monitoring client-side metric export."""
    client_context: "NotRequired[dict[str, str]]"
    """Key-value client context attributes attached to Spanner RPC metadata."""
    use_plain_text: "NotRequired[bool]"
    """Connect over unencrypted plaintext gRPC when targeting local emulators."""
    ca_certificate: "NotRequired[str]"
    """PEM root CA certificate bundle path for custom TLS verification."""
    client_certificate: "NotRequired[str]"
    """PEM client certificate path for mTLS authentication."""
    client_key: "NotRequired[str]"
    """PEM client private key path paired with ``client_certificate``."""
    instance_type: "NotRequired[str]"
    """Spanner instance edition or provisioning type forwarded to the client."""
    instance_id: "NotRequired[str]"
    """Target Spanner instance identifier."""
    instance: "NotRequired[str]"
    """Alias normalized to ``instance_id`` when ``instance_id`` is unset."""
    configuration_name: "NotRequired[str]"
    """Regional or multi-region Spanner instance configuration resource name."""
    display_name: "NotRequired[str]"
    """Human-readable display name used when referencing or provisioning an instance."""
    node_count: "NotRequired[int]"
    """Allocated node count for the Spanner instance descriptor."""
    processing_units: "NotRequired[int]"
    """Allocated processing units for sub-node or granular Spanner instances."""
    instance_labels: "NotRequired[dict[str, str]]"
    """Resource labels associated with the Spanner instance."""
    database_id: "NotRequired[str]"
    """Target Spanner database identifier within ``instance_id``."""
    database: "NotRequired[str]"
    """Alias normalized to ``database_id`` when ``database_id`` is unset."""
    db: "NotRequired[str]"
    """Short alias normalized to ``database_id`` when ``database_id`` is unset."""
    ddl_statements: "NotRequired[tuple[str, ...] | list[str]]"
    """Initial DDL statements associated with the ``Database`` handle."""
    logger: "NotRequired[Logger]"
    """Custom Python ``logging.Logger`` attached to the Spanner ``Database`` object."""
    encryption_config: "NotRequired[EncryptionConfig | dict[str, Any]]"
    """Customer-managed Cloud KMS encryption configuration for the database."""
    database_dialect: "NotRequired[DatabaseDialect]"
    """Spanner SQL dialect enum (``GOOGLE_STANDARD_SQL`` or ``POSTGRESQL``)."""
    database_role: "NotRequired[str]"
    """Fine-grained access control database role assumed by sessions."""
    enable_drop_protection: "NotRequired[bool]"
    """Flag indicating whether database drop protection is enabled on the handle."""
    enable_interceptors_in_tests: "NotRequired[bool]"
    """Enable gRPC client interceptors when running under test harnesses."""
    proto_descriptors: "NotRequired[bytes]"
    """Serialized protobuf ``FileDescriptorSet`` bytes associated with the database."""
    extra: "NotRequired[dict[str, Any]]"
    """Additional keyword arguments retained in normalized connection configuration."""


class SpannerPoolParams(SpannerConnectionParams):
    """Session pool configuration.

    ``pool_type`` must be a sync pool class for :class:`SpannerSyncConfig`
    (default ``FixedSizePool``) and an async pool class for
    :class:`SpannerAsyncConfig` (default async ``FixedSizePool``).
    """

    pool_type: "NotRequired[type[AbstractSessionPool | AsyncAbstractSessionPool]]"
    """Session pool class (``FixedSizePool``, ``BurstyPool``, ``PingingPool``, or async equivalents)."""
    size: "NotRequired[int]"
    """Fixed session count pre-allocated by ``FixedSizePool`` or ``PingingPool``."""
    target_size: "NotRequired[int]"
    """Target warm session count maintained by ``BurstyPool``."""
    max_sessions: "NotRequired[int]"
    """Upper cap on sessions opened concurrently by custom or bursty pools."""
    default_timeout: "NotRequired[int | float]"
    """Seconds to wait when borrowing a session from the pool before timing out."""
    session_labels: "NotRequired[dict[str, str]]"
    """Key-value labels attached to sessions created by the pool."""
    labels: "NotRequired[dict[str, str]]"
    """Alias for ``session_labels`` forwarded to pool session creation."""
    ping_interval: "NotRequired[int]"
    """Background keepalive ping interval in seconds for ``PingingPool``."""
    max_age_minutes: "NotRequired[int]"
    """Maximum session age in minutes before pool rotation."""


class SpannerDriverFeatures(TypedDict):
    """Driver feature flags configuring Spanner codecs, RPC options, and Batch Write ingestion."""

    enable_uuid_conversion: "NotRequired[bool]"
    """Automatically coerce ``UUID`` parameters to strings (defaults to ``True``)."""
    json_serializer: "NotRequired[Callable[[Any], str]]"
    """Custom JSON serializer for ``JSON`` parameter payloads."""
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    """Custom JSON deserializer for ``JSON`` column values."""
    retry: "NotRequired[Retry | None]"
    """Default ``google.api_core.retry.Retry`` policy passed to ``execute_sql``, ``execute_update``, and ``batch_update``."""
    timeout: "NotRequired[float | None]"
    """Default per-RPC timeout in seconds passed to ``execute_sql``, ``execute_update``, and ``batch_update``."""
    query_options: "NotRequired[ExecuteSqlRequest.QueryOptions | dict[str, Any] | None]"
    """Default ``ExecuteSqlRequest.QueryOptions`` forwarded to SQL execution calls."""
    request_options: "NotRequired[RequestOptions | dict[str, Any] | None]"
    """Default ``RequestOptions`` (priority, request tag, transaction tag) forwarded to statement RPCs."""
    directed_read_options: "NotRequired[DirectedReadOptions | None]"
    """Default ``DirectedReadOptions`` forwarded to snapshot ``execute_sql`` reads."""
    session_labels: "NotRequired[dict[str, str]]"
    """Deprecated compatibility alias for ``connection_config['session_labels']``."""
    enable_events: "NotRequired[bool]"
    """Enable SQLSpec table-backed event channel support."""
    events_backend: "NotRequired[Literal['poll_queue']]"
    """Event queue backend name (``'poll_queue'``)."""
    enable_batch_write_api: "NotRequired[bool]"
    """Route ``load_from_arrow`` through Spanner ``batch_write`` mutation groups instead of single-transaction mutations (defaults to ``False``)."""
    on_connection_create: "NotRequired[Callable[[Any], Any | None] | None]"
    """Callback invoked after a new Spanner connection (Snapshot or Transaction) is created."""


def build_connection_config(
    connection_config: "SpannerPoolParams | dict[str, Any] | Mapping[str, Any] | None",
) -> dict[str, Any]:
    """Normalize Spanner connection configuration and map aliases."""
    config = normalize_connection_config(connection_config)
    project_alias = config.pop("project_id", None)
    if project_alias is not None and "project" not in config:
        config["project"] = project_alias
    instance_alias = config.pop("instance", None)
    if instance_alias is not None and "instance_id" not in config:
        config["instance_id"] = instance_alias
    database_alias = config.pop("database", None) or config.pop("db", None)
    if database_alias is not None and "database_id" not in config:
        config["database_id"] = database_alias
    return config


class SpannerSyncConnectionContext(SyncPoolConnectionContext):
    """Context manager for sync Spanner connections (Snapshot or Transaction)."""

    __slots__ = ("_connection", "_session", "_transaction")

    def __init__(self, config: "SpannerSyncConfig", transaction: bool = False) -> None:
        super().__init__(config)
        self._transaction = transaction
        self._connection: SpannerSyncConnection | None = None
        self._session: Any = None

    def __enter__(self) -> "SpannerSyncConnection":
        database = self._config.get_database()
        if self._transaction:
            manager = cast("Any", database).sessions_manager
            self._session = manager.get_session(SpannerTransactionType.READ_WRITE)
            try:
                txn = self._session.transaction()
                txn.__enter__()
            except BaseException:
                with contextlib.suppress(BaseException):
                    manager.put_session(self._session)
                self._session = None
                raise
            self._connection = cast("SpannerSyncConnection", txn)
        else:
            self._session = cast("Any", database).snapshot(multi_use=True)
            self._connection = cast("SpannerSyncConnection", self._session.__enter__())
        try:
            hook = getattr(self._config, "_user_connection_hook", None) or getattr(
                self._config, "on_connection_create", None
            )
            if hook:
                modified = hook(self._connection)
                if modified is not None:
                    self._connection = cast("SpannerSyncConnection", modified)
        except BaseException as exc:
            with contextlib.suppress(BaseException):
                self.__exit__(type(exc), exc, exc.__traceback__)
            self._connection = None
            self._session = None
            raise
        return self._connection

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._transaction:
            try:
                if self._connection is not None:
                    txn = cast("Any", self._connection)
                    action = resolve_transaction_completion(txn, failed=exc_type is not None)
                    if action == "commit":
                        try:
                            txn.commit()
                        except SpannerGoogleAPICallError as exc:
                            raise create_mapped_exception(exc) from exc
                    elif action == "rollback":
                        try:
                            txn.rollback()
                        except SpannerGoogleAPICallError as exc:
                            if exc_type is None:
                                raise create_mapped_exception(exc) from exc
            finally:
                if self._session is not None:
                    cast("Any", self._config.get_database()).sessions_manager.put_session(self._session)
                self._connection = None
                self._session = None
            return None

        if self._session is not None:
            self._session.__exit__(exc_type, exc_val, exc_tb)
        self._connection = None
        self._session = None
        return None


class _SpannerSyncSessionConnectionHandler(SyncPoolSessionFactory):
    """Sync session handler that uses SpannerSyncConnectionContext."""

    __slots__ = ("_transaction",)

    def __init__(self, config: "SpannerSyncConfig", transaction: bool = False) -> None:
        super().__init__(config)
        self._transaction = transaction

    def acquire_connection(self) -> "SpannerSyncConnection":
        self._ctx = SpannerSyncConnectionContext(self._config, transaction=self._transaction)
        return cast("SpannerSyncConnectionContext", self._ctx).__enter__()

    def release_connection(self, _conn: "SpannerSyncConnection", **kwargs: Any) -> None:
        if self._ctx is not None:
            context = cast("SpannerSyncConnectionContext", self._ctx)
            context._connection = _conn
            context.__exit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))
            self._ctx = None


class SpannerSyncConfig(SyncDatabaseConfig["SpannerSyncConnection", "AbstractSessionPool", SpannerSyncDriver]):
    """Spanner configuration and session management."""

    driver_type: ClassVar[type["SpannerSyncDriver"]] = SpannerSyncDriver
    connection_type: ClassVar[type["SpannerSyncConnection"]] = cast(
        "type[SpannerSyncConnection]", SpannerSyncConnection
    )
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = False
    supports_native_arrow_import: ClassVar[bool] = False
    supports_native_parquet_export: ClassVar[bool] = False
    supports_native_parquet_import: ClassVar[bool] = False
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[SpannerSyncConnectionContext]]" = SpannerSyncConnectionContext
    _session_factory_class: "ClassVar[type[_SpannerSyncSessionConnectionHandler]]" = (
        _SpannerSyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[SpannerSyncSessionContext]]" = SpannerSyncSessionContext
    _default_statement_config = default_statement_config
    _DEFAULT_SESSION_TRANSACTION: ClassVar[bool] = _DEFAULT_SESSION_TRANSACTION

    def __init__(
        self,
        *,
        connection_config: "SpannerPoolParams | dict[str, Any] | None" = None,
        connection_instance: "AbstractSessionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SpannerDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        on_connection_create: "Callable[[Any], Any | None] | None" = None,
        **kwargs: Any,
    ) -> None:
        self.connection_config, raw_driver_features = _prepare_settings(
            connection_config, driver_features, default_pool_type=FixedSizePool
        )
        user_connection_hook = on_connection_create or cast(
            "Callable[[Any], Any | None] | None", raw_driver_features.pop("on_connection_create", None)
        )
        self._user_connection_hook = user_connection_hook
        statement_config, processed_features = apply_driver_features(
            statement_config or default_statement_config, raw_driver_features
        )

        super().__init__(
            connection_config=self.connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=processed_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

        self._client: Client | None = None
        self._database: Database | None = None

    def _build_storage_capabilities(self) -> "StorageCapabilities":
        """Enable Arrow conversion and mutation ingestion when PyArrow is installed."""
        capabilities = super()._build_storage_capabilities()
        arrow_ready = self._dependency_available(ensure_pyarrow)
        capabilities["arrow_import_enabled"] = arrow_ready
        capabilities["arrow_export_enabled"] = arrow_ready
        return capabilities

    def get_database(self) -> "Database":
        """Return the configured database, creating the pool and client on first use.

        Returns:
            The Spanner database bound to the configured session pool.

        Raises:
            ImproperConfigurationError: If ``instance_id`` or ``database_id`` is missing.
        """
        instance_id, database_id = _require_database_ids(self.connection_config)

        if self.connection_instance is None:
            self.connection_instance = self.provide_pool()

        if self._database is None:
            instance = cast("Any", self._get_client()).instance(instance_id, **_instance_kwargs(self.connection_config))
            self._database = cast(
                "Database",
                instance.database(
                    database_id,
                    pool=self.connection_instance,
                    **_connection_kwargs_for(self.connection_config, _DATABASE_CONFIG_FIELDS),
                ),
            )
        return self._database

    def create_connection(self) -> "SpannerSyncConnection":
        """Return a read-only snapshot checkout owned by the caller.

        The checkout borrows a pooled session when entered and returns it on exit.

        Returns:
            A snapshot checkout to be used as a context manager.
        """
        return cast("SpannerSyncConnection", cast("Any", self.get_database()).snapshot(multi_use=True))

    def provide_connection(
        self, *args: Any, transaction: "bool" = _DEFAULT_SESSION_TRANSACTION, **kwargs: Any
    ) -> "SpannerSyncConnectionContext":
        """Yield a Transaction (default) or Snapshot context from the configured pool.

        Args:
            *args: Additional positional arguments (unused, for interface compatibility).
            transaction: If True (default), yields a Transaction context that
                supports execute_update() for DML statements. If False, yields
                a read-only Snapshot context for SELECT queries.
            **kwargs: Additional keyword arguments (unused, for interface compatibility).

        Returns:
            A Spanner connection context manager.
        """
        return SpannerSyncConnectionContext(self, transaction=transaction)

    def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        transaction: "bool" = _DEFAULT_SESSION_TRANSACTION,
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        query_options: "ExecuteSqlRequest.QueryOptions | dict[str, Any] | None" = None,
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerSyncSessionContext":
        """Provide a Spanner driver session context manager.

        Returns a write-capable Transaction session by default. Pass
        ``transaction=False`` or use :meth:`provide_read_session` to obtain a
        read-only Snapshot session.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            transaction: Whether to use a Transaction (True, default) or
                Snapshot (False).
            request_options: Session-scoped RequestOptions for Spanner statements.
            directed_read_options: Session-scoped DirectedReadOptions for reads.
            query_options: Session-scoped QueryOptions for Spanner statements.
            retry: Session-scoped retry policy for Spanner statement calls.
            timeout: Session-scoped timeout for Spanner statement calls.
            **kwargs: Additional keyword arguments.

        Returns:
            A Spanner driver session context manager.
        """
        handler = _SpannerSyncSessionConnectionHandler(self, transaction=transaction)
        return SpannerSyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=build_session_driver_features(
                self.driver_features,
                request_options=request_options,
                directed_read_options=directed_read_options,
                query_options=query_options,
                retry=retry,
                timeout=timeout,
            ),
            prepare_driver=self._prepare_driver,
        )

    def provide_write_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        query_options: "ExecuteSqlRequest.QueryOptions | dict[str, Any] | None" = None,
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerSyncSessionContext":
        """Provide a write-capable Spanner session (alias for :meth:`provide_session`)."""
        return self.provide_session(
            *args,
            statement_config=statement_config,
            transaction=True,
            request_options=request_options,
            directed_read_options=directed_read_options,
            query_options=query_options,
            retry=retry,
            timeout=timeout,
            **kwargs,
        )

    def provide_read_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        query_options: "ExecuteSqlRequest.QueryOptions | dict[str, Any] | None" = None,
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerSyncSessionContext":
        """Provide a read-only Snapshot Spanner session.

        Use for query workloads that benefit from Spanner's snapshot reads.
        For DDL/DML, use :meth:`provide_session` (write-capable by default).
        """
        return self.provide_session(
            *args,
            statement_config=statement_config,
            transaction=False,
            request_options=request_options,
            directed_read_options=directed_read_options,
            query_options=query_options,
            retry=retry,
            timeout=timeout,
            **kwargs,
        )

    def run_in_transaction(self, fn: "Callable[..., Any]", *args: Any, **kwargs: Any) -> Any:
        """Execute a callable inside Spanner's retryable transaction runner.

        ``fn`` is called as ``fn(driver, *args, **kwargs)`` with a driver bound
        to the runner's transaction. A ``DeadlockError`` caused by
        ``google.api_core.exceptions.Aborted`` is unwrapped so the runner
        retries, and a terminal ``Aborted`` is raised as ``DeadlockError``.
        """
        return run_in_transaction_sync(self.get_database(), self._transaction_driver, fn, *args, **kwargs)

    def execute_partitioned_dml(
        self,
        sql: str,
        params: "dict[str, Any] | None" = None,
        param_types: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> int:
        """Execute a Partitioned DML statement against the configured Spanner database."""
        return execute_partitioned_dml_sync(
            self.get_database(),
            sql,
            params=params,
            param_types=param_types,
            json_serializer=self.driver_features.get("json_serializer"),
            enable_uuid_conversion=self.driver_features.get("enable_uuid_conversion", True),
            **kwargs,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for SpannerSyncConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "SpannerConnectionParams": SpannerConnectionParams,
            "SpannerDriverFeatures": SpannerDriverFeatures,
            "SpannerPoolParams": SpannerPoolParams,
            "SpannerSyncConfig": SpannerSyncConfig,
            "SpannerSyncConnection": SpannerSyncConnection,
            "SpannerSyncConnectionContext": SpannerSyncConnectionContext,
            "SpannerSyncDriver": SpannerSyncDriver,
            "SpannerSyncSessionContext": SpannerSyncSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return queue defaults for Spanner JSON and transaction handling."""
        return EventRuntimeHints(cleanup_on_ack=False, use_run_in_transaction=True)

    def _get_client(self) -> "Client":
        if self._client is None:
            self._client = Client(**_connection_kwargs_for(self.connection_config, _CLIENT_CONFIG_FIELDS))
        return self._client

    def _create_pool(self) -> "AbstractSessionPool":
        _require_database_ids(self.connection_config)
        pool_type = _resolve_pool_type(
            self.connection_config, base=AbstractSessionPool, config_name="SpannerSyncConfig", expected="a sync"
        )
        if issubclass(pool_type, PingingPool):
            pool_kind = "pinging"
        elif issubclass(pool_type, FixedSizePool):
            pool_kind = "fixed"
        elif issubclass(pool_type, BurstyPool):
            pool_kind = "bursty"
        else:
            pool_kind = "custom"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return cast("AbstractSessionPool", pool_type(**_pool_kwargs(self.connection_config, pool_kind)))

    def _close_pool(self) -> None:
        """Release pooled sessions, then close the database and client."""
        pool = self.connection_instance
        if pool is not None:
            with contextlib.suppress(SpannerGoogleAPICallError):
                cast("_SyncClearableProtocol", pool).clear()
        if self._database is not None:
            with contextlib.suppress(SpannerGoogleAPICallError):
                cast("_SyncClosableProtocol", self._database).close()
        if self._client is not None:
            cast("_SyncClosableProtocol", self._client).close()
        self._client = None
        self._database = None

    def _transaction_driver(self, transaction: "SpannerSyncConnection") -> "SpannerSyncDriver":
        driver = self.driver_type(
            connection=transaction, statement_config=self.statement_config, driver_features=self.driver_features
        )
        driver.bind_external_transaction()
        return self._prepare_driver(driver)


class SpannerAsyncConnectionContext(AsyncPoolConnectionContext):
    """Context manager for async Spanner connections (Snapshot or Transaction)."""

    __slots__ = ("_session", "_transaction")

    def __init__(self, config: "SpannerAsyncConfig", transaction: bool = False) -> None:
        super().__init__(config)
        self._transaction = transaction
        self._connection: SpannerAsyncConnection | None = None
        self._session: Any = None

    async def __aenter__(self) -> "SpannerAsyncConnection":
        database = await self._config.get_database()
        if self._transaction:
            manager = cast("Any", database).sessions_manager
            self._session = await manager.get_session(SpannerAsyncTransactionType.READ_WRITE)
            try:
                txn = self._session.transaction()
                await txn.__aenter__()
            except BaseException:
                with contextlib.suppress(BaseException):
                    await manager.put_session(self._session)
                self._session = None
                raise
            self._connection = cast("SpannerAsyncConnection", txn)
        else:
            self._session = cast("Any", database).snapshot(multi_use=True)
            self._connection = cast("SpannerAsyncConnection", await self._session.__aenter__())
        try:
            hook = getattr(self._config, "_user_connection_hook", None) or getattr(
                self._config, "on_connection_create", None
            )
            if hook:
                modified = hook(self._connection)
                if modified is not None:
                    self._connection = cast("SpannerAsyncConnection", modified)
        except BaseException as exc:
            with contextlib.suppress(BaseException):
                await self.__aexit__(type(exc), exc, exc.__traceback__)
            self._connection = None
            self._session = None
            raise
        return self._connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._transaction:
            try:
                if self._connection is not None:
                    txn = cast("Any", self._connection)
                    action = resolve_transaction_completion(txn, failed=exc_type is not None)
                    if action == "commit":
                        try:
                            await txn.commit()
                        except SpannerGoogleAPICallError as exc:
                            raise create_mapped_exception(exc) from exc
                    elif action == "rollback":
                        try:
                            await txn.rollback()
                        except SpannerGoogleAPICallError as exc:
                            if exc_type is None:
                                raise create_mapped_exception(exc) from exc
            finally:
                if self._session is not None:
                    database = await self._config.get_database()
                    await cast("Any", database).sessions_manager.put_session(self._session)
                self._connection = None
                self._session = None
            return None

        if self._session is not None:
            await self._session.__aexit__(exc_type, exc_val, exc_tb)
        self._connection = None
        self._session = None
        return None


class _SpannerAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    """Async session handler that uses SpannerAsyncConnectionContext."""

    __slots__ = ("_ctx", "_transaction")

    def __init__(self, config: "SpannerAsyncConfig", transaction: bool = False) -> None:
        super().__init__(config)
        self._transaction = transaction
        self._ctx: SpannerAsyncConnectionContext | None = None

    async def acquire_connection(self) -> "SpannerAsyncConnection":
        self._ctx = SpannerAsyncConnectionContext(self._config, transaction=self._transaction)
        return await self._ctx.__aenter__()

    async def release_connection(self, _conn: "SpannerAsyncConnection", **kwargs: Any) -> None:
        if self._ctx is not None:
            self._ctx._connection = _conn
            await self._ctx.__aexit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))
            self._ctx = None


class SpannerAsyncConfig(AsyncDatabaseConfig["SpannerAsyncConnection", "AsyncAbstractSessionPool", SpannerAsyncDriver]):
    """Async Spanner configuration and session management."""

    driver_type: ClassVar[type["SpannerAsyncDriver"]] = SpannerAsyncDriver
    connection_type: ClassVar[type["SpannerAsyncConnection"]] = cast(
        "type[SpannerAsyncConnection]", SpannerAsyncConnection
    )
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = False
    supports_native_arrow_import: ClassVar[bool] = False
    supports_native_parquet_export: ClassVar[bool] = False
    supports_native_parquet_import: ClassVar[bool] = False
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[SpannerAsyncConnectionContext]]" = SpannerAsyncConnectionContext
    _session_factory_class: "ClassVar[type[_SpannerAsyncSessionConnectionHandler]]" = (
        _SpannerAsyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[SpannerAsyncSessionContext]]" = SpannerAsyncSessionContext
    _default_statement_config = default_statement_config
    _DEFAULT_SESSION_TRANSACTION: ClassVar[bool] = _DEFAULT_SESSION_TRANSACTION

    def __init__(
        self,
        *,
        connection_config: "SpannerPoolParams | dict[str, Any] | None" = None,
        connection_instance: "AsyncAbstractSessionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SpannerDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        on_connection_create: "Callable[[Any], Any | None] | None" = None,
        **kwargs: Any,
    ) -> None:
        self.connection_config, raw_driver_features = _prepare_settings(
            connection_config, driver_features, default_pool_type=AsyncFixedSizePool
        )
        user_connection_hook = on_connection_create or cast(
            "Callable[[Any], Any | None] | None", raw_driver_features.pop("on_connection_create", None)
        )
        self._user_connection_hook = user_connection_hook
        statement_config, processed_features = apply_driver_features(
            statement_config or default_statement_config, raw_driver_features
        )

        super().__init__(
            connection_config=self.connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=processed_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

        self._client: AsyncClient | None = None
        self._database: AsyncDatabase | None = None

    def _build_storage_capabilities(self) -> "StorageCapabilities":
        """Enable Arrow conversion and mutation ingestion when PyArrow is installed."""
        capabilities = super()._build_storage_capabilities()
        arrow_ready = self._dependency_available(ensure_pyarrow)
        capabilities["arrow_import_enabled"] = arrow_ready
        capabilities["arrow_export_enabled"] = arrow_ready
        return capabilities

    async def get_database(self) -> "AsyncDatabase":
        """Return the configured database, creating the pool and client on first use.

        Returns:
            The async Spanner database bound to the configured session pool.

        Raises:
            ImproperConfigurationError: If ``instance_id`` or ``database_id`` is missing.
        """
        instance_id, database_id = _require_database_ids(self.connection_config)

        if self.connection_instance is None:
            self.connection_instance = await self.provide_pool()

        if self._database is None:
            instance = cast("Any", self._get_client()).instance(instance_id, **_instance_kwargs(self.connection_config))
            self._database = cast(
                "AsyncDatabase",
                await instance.database(
                    database_id,
                    pool=self.connection_instance,
                    **_connection_kwargs_for(self.connection_config, _DATABASE_CONFIG_FIELDS),
                ),
            )
        return self._database

    async def create_connection(self) -> "SpannerAsyncConnection":
        """Return a read-only async snapshot checkout owned by the caller.

        The checkout borrows a pooled session when entered and returns it on exit.

        Returns:
            A snapshot checkout to be used as an async context manager.
        """
        database = await self.get_database()
        return cast("SpannerAsyncConnection", cast("Any", database).snapshot(multi_use=True))

    def provide_connection(
        self, *args: Any, transaction: "bool" = _DEFAULT_SESSION_TRANSACTION, **kwargs: Any
    ) -> "SpannerAsyncConnectionContext":
        """Yield a Transaction (default) or Snapshot context from the configured pool.

        Args:
            *args: Additional positional arguments (unused, for interface compatibility).
            transaction: If True (default), yields a Transaction context that
                supports execute_update() for DML statements. If False, yields
                a read-only Snapshot context for SELECT queries.
            **kwargs: Additional keyword arguments (unused, for interface compatibility).

        Returns:
            An async Spanner connection context manager.
        """
        return SpannerAsyncConnectionContext(self, transaction=transaction)

    def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        transaction: "bool" = _DEFAULT_SESSION_TRANSACTION,
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        query_options: "ExecuteSqlRequest.QueryOptions | dict[str, Any] | None" = None,
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerAsyncSessionContext":
        """Provide an async Spanner driver session context manager.

        Returns a write-capable Transaction session by default. Pass
        ``transaction=False`` or use :meth:`provide_read_session` to obtain a
        read-only Snapshot session.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            transaction: Whether to use a Transaction (True, default) or
                Snapshot (False).
            request_options: Session-scoped RequestOptions for Spanner statements.
            directed_read_options: Session-scoped DirectedReadOptions for reads.
            query_options: Session-scoped QueryOptions for Spanner statements.
            retry: Session-scoped retry policy for Spanner statement calls.
            timeout: Session-scoped timeout for Spanner statement calls.
            **kwargs: Additional keyword arguments.

        Returns:
            An async Spanner driver session context manager.
        """
        handler = _SpannerAsyncSessionConnectionHandler(self, transaction=transaction)
        return SpannerAsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=build_session_driver_features(
                self.driver_features,
                request_options=request_options,
                directed_read_options=directed_read_options,
                query_options=query_options,
                retry=retry,
                timeout=timeout,
            ),
            prepare_driver=self._prepare_driver,
        )

    def provide_write_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        query_options: "ExecuteSqlRequest.QueryOptions | dict[str, Any] | None" = None,
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerAsyncSessionContext":
        """Provide a write-capable async Spanner session (alias for :meth:`provide_session`)."""
        return self.provide_session(
            *args,
            statement_config=statement_config,
            transaction=True,
            request_options=request_options,
            directed_read_options=directed_read_options,
            query_options=query_options,
            retry=retry,
            timeout=timeout,
            **kwargs,
        )

    def provide_read_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        query_options: "ExecuteSqlRequest.QueryOptions | dict[str, Any] | None" = None,
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerAsyncSessionContext":
        """Provide a read-only Snapshot async Spanner session.

        Use for query workloads that benefit from Spanner's snapshot reads.
        For DDL/DML, use :meth:`provide_session` (write-capable by default).
        """
        return self.provide_session(
            *args,
            statement_config=statement_config,
            transaction=False,
            request_options=request_options,
            directed_read_options=directed_read_options,
            query_options=query_options,
            retry=retry,
            timeout=timeout,
            **kwargs,
        )

    async def run_in_transaction(self, fn: "Callable[..., Awaitable[Any]]", *args: Any, **kwargs: Any) -> Any:
        """Execute a coroutine function inside Spanner's async retryable transaction runner.

        ``fn`` is awaited as ``fn(driver, *args, **kwargs)`` with a driver bound
        to the runner's transaction. A ``DeadlockError`` caused by
        ``google.api_core.exceptions.Aborted`` is unwrapped so the runner
        retries, and a terminal ``Aborted`` is raised as ``DeadlockError``.
        """
        database = await self.get_database()
        return await run_in_transaction_async(database, self._transaction_driver, fn, *args, **kwargs)

    async def execute_partitioned_dml(
        self,
        sql: str,
        params: "dict[str, Any] | None" = None,
        param_types: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> int:
        """Execute a Partitioned DML statement against the configured async Spanner database."""
        database = await self.get_database()
        return await execute_partitioned_dml_async(
            database,
            sql,
            params=params,
            param_types=param_types,
            json_serializer=self.driver_features.get("json_serializer"),
            enable_uuid_conversion=self.driver_features.get("enable_uuid_conversion", True),
            **kwargs,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for SpannerAsyncConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "SpannerAsyncConfig": SpannerAsyncConfig,
            "SpannerAsyncConnection": SpannerAsyncConnection,
            "SpannerAsyncConnectionContext": SpannerAsyncConnectionContext,
            "SpannerAsyncDriver": SpannerAsyncDriver,
            "SpannerAsyncSessionContext": SpannerAsyncSessionContext,
            "SpannerConnectionParams": SpannerConnectionParams,
            "SpannerDriverFeatures": SpannerDriverFeatures,
            "SpannerPoolParams": SpannerPoolParams,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return queue defaults for Spanner JSON and transaction handling."""
        return EventRuntimeHints(cleanup_on_ack=False, use_run_in_transaction=True)

    def _get_client(self) -> "AsyncClient":
        if self._client is None:
            self._client = AsyncClient(**_connection_kwargs_for(self.connection_config, _CLIENT_CONFIG_FIELDS))
        return self._client

    async def _create_pool(self) -> "AsyncAbstractSessionPool":
        _require_database_ids(self.connection_config)
        pool_type = _resolve_pool_type(
            self.connection_config, base=AsyncAbstractSessionPool, config_name="SpannerAsyncConfig", expected="an async"
        )
        if issubclass(pool_type, AsyncPingingPool):
            pool_kind = "pinging"
        elif issubclass(pool_type, AsyncFixedSizePool):
            pool_kind = "fixed"
        elif issubclass(pool_type, AsyncBurstyPool):
            pool_kind = "bursty"
        else:
            pool_kind = "custom"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return cast("AsyncAbstractSessionPool", pool_type(**_pool_kwargs(self.connection_config, pool_kind)))

    async def _close_pool(self) -> None:
        """Release pooled sessions, then close the database and client."""
        pool = self.connection_instance
        if pool is not None:
            with contextlib.suppress(SpannerGoogleAPICallError):
                await cast("_AsyncClearableProtocol", pool).clear()
        if self._database is not None:
            with contextlib.suppress(SpannerGoogleAPICallError):
                await self._database.close()
        if self._client is not None:
            cast("_SyncClosableProtocol", self._client).close()
        self._client = None
        self._database = None

    def _transaction_driver(self, transaction: "SpannerAsyncConnection") -> "SpannerAsyncDriver":
        driver = self.driver_type(
            connection=transaction, statement_config=self.statement_config, driver_features=self.driver_features
        )
        driver.bind_external_transaction()
        return self._prepare_driver(driver)


class _SyncClearableProtocol(Protocol):
    def clear(self) -> None: ...


class _AsyncClearableProtocol(Protocol):
    async def clear(self) -> None: ...


class _SyncClosableProtocol(Protocol):
    def close(self) -> None: ...


def _prepare_settings(
    connection_config: "SpannerPoolParams | dict[str, Any] | None",
    driver_features: "SpannerDriverFeatures | dict[str, Any] | None",
    *,
    default_pool_type: type[Any],
) -> "tuple[dict[str, Any], dict[str, Any]]":
    """Normalize connection settings and move legacy session labels onto the pool settings."""
    config = build_connection_config(connection_config)
    if "min_sessions" in config:
        msg = "Spanner session pools do not support 'min_sessions'; use 'size' or 'target_size'."
        raise ImproperConfigurationError(msg)

    raw_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    legacy_session_labels = raw_driver_features.pop("session_labels", None)
    if legacy_session_labels is not None and "session_labels" not in config and "labels" not in config:
        config["session_labels"] = legacy_session_labels

    config.setdefault("size", config.pop("max_sessions", 10))
    config.setdefault("pool_type", default_pool_type)
    return config, raw_driver_features


def _require_database_ids(connection_config: "dict[str, Any]") -> "tuple[str, str]":
    instance_id = connection_config.get("instance_id")
    database_id = connection_config.get("database_id")
    if not instance_id or not database_id:
        msg = "instance_id and database_id are required."
        raise ImproperConfigurationError(msg)
    return instance_id, database_id


def _connection_kwargs_for(connection_config: "dict[str, Any]", fields: "frozenset[str] | set[str]") -> dict[str, Any]:
    return {field: connection_config[field] for field in fields if connection_config.get(field) is not None}


def _instance_kwargs(connection_config: "dict[str, Any]") -> dict[str, Any]:
    instance_kwargs = _connection_kwargs_for(connection_config, _INSTANCE_CONFIG_FIELDS)
    instance_labels = connection_config.get("instance_labels")
    if instance_labels is not None:
        instance_kwargs["labels"] = instance_labels
    return instance_kwargs


def _resolve_pool_type(
    connection_config: "dict[str, Any]", *, base: type[Any], config_name: str, expected: str
) -> type[Any]:
    pool_type = connection_config["pool_type"]
    if not isinstance(pool_type, type) or not issubclass(pool_type, base):
        msg = f"{config_name} requires {expected} session pool class; got {pool_type!r}."
        raise ImproperConfigurationError(msg)
    return pool_type


def _pool_kwargs(connection_config: "dict[str, Any]", pool_kind: str) -> dict[str, Any]:
    """Build session pool constructor arguments for a pool kind.

    Args:
        connection_config: Normalized connection settings.
        pool_kind: ``"pinging"``, ``"fixed"``, ``"bursty"``, or ``"custom"``.

    Returns:
        Keyword arguments for the pool class.
    """
    pool_kwargs: dict[str, Any] = {}
    labels = connection_config.get("session_labels", connection_config.get("labels"))
    if labels is not None:
        pool_kwargs["labels"] = labels
    database_role = connection_config.get("database_role")
    if database_role is not None:
        pool_kwargs["database_role"] = database_role
    if pool_kind == "pinging":
        pool_kwargs.update(_connection_kwargs_for(connection_config, {"size", "default_timeout"}))
        pool_kwargs["ping_interval"] = connection_config.get("ping_interval", 1800)
    elif pool_kind == "fixed":
        pool_kwargs.update(_connection_kwargs_for(connection_config, {"size", "default_timeout", "max_age_minutes"}))
    elif pool_kind == "bursty":
        target_size = connection_config.get("target_size", connection_config.get("size"))
        if target_size is not None:
            pool_kwargs["target_size"] = target_size
    else:
        pool_kwargs.update(
            _connection_kwargs_for(
                connection_config, {"size", "target_size", "default_timeout", "ping_interval", "max_age_minutes"}
            )
        )
    return pool_kwargs
