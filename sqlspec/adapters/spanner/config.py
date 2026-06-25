"""Spanner configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.pool import AbstractSessionPool, BurstyPool, FixedSizePool, PingingPool
from typing_extensions import NotRequired

from sqlspec.adapters.spanner._typing import SpannerConnection
from sqlspec.adapters.spanner.core import apply_driver_features, default_statement_config
from sqlspec.adapters.spanner.driver import SpannerSessionContext, SpannerSyncDriver
from sqlspec.config import SyncDatabaseConfig
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.type_guards import supports_close

if TYPE_CHECKING:
    from collections.abc import Callable
    from logging import Logger
    from types import TracebackType

    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.api_core.retry import Retry
    from google.auth.credentials import Credentials
    from google.cloud.spanner_admin_database_v1.types import DatabaseDialect, EncryptionConfig
    from google.cloud.spanner_v1 import DirectedReadOptions, ExecuteSqlRequest, RequestOptions
    from google.cloud.spanner_v1.database import Database
    from google.cloud.spanner_v1.transaction import DefaultTransactionOptions

    from sqlspec.config import ExtensionConfigs
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("SpannerConnectionParams", "SpannerDriverFeatures", "SpannerPoolParams", "SpannerSyncConfig")

_DEFAULT_SESSION_TRANSACTION: bool = True
"""Default ``transaction`` flag for ``provide_session`` / ``provide_connection``.

``True`` yields a write-capable :class:`Transaction` context matching every
other sqlspec adapter. Read-only :class:`Snapshot` contexts are available via
:meth:`SpannerSyncConfig.provide_read_session`. Pulled into a module-level
constant so an eventual ``SpannerAsyncConfig`` can import the same default."""

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
    """Spanner connection parameters."""

    project: "NotRequired[str]"
    credentials: "NotRequired[Credentials]"
    client_info: "NotRequired[ClientInfo]"
    client_options: "NotRequired[ClientOptions | dict[str, Any]]"
    query_options: "NotRequired[ExecuteSqlRequest.QueryOptions]"
    route_to_leader_enabled: "NotRequired[bool]"
    directed_read_options: "NotRequired[DirectedReadOptions]"
    observability_options: "NotRequired[Any]"
    default_transaction_options: "NotRequired[DefaultTransactionOptions]"
    experimental_host: "NotRequired[str]"
    disable_builtin_metrics: "NotRequired[bool]"
    client_context: "NotRequired[dict[str, str]]"
    use_plain_text: "NotRequired[bool]"
    ca_certificate: "NotRequired[str]"
    client_certificate: "NotRequired[str]"
    client_key: "NotRequired[str]"
    instance_type: "NotRequired[str]"
    instance_id: "NotRequired[str]"
    configuration_name: "NotRequired[str]"
    display_name: "NotRequired[str]"
    node_count: "NotRequired[int]"
    processing_units: "NotRequired[int]"
    instance_labels: "NotRequired[dict[str, str]]"
    database_id: "NotRequired[str]"
    ddl_statements: "NotRequired[tuple[str, ...] | list[str]]"
    logger: "NotRequired[Logger]"
    encryption_config: "NotRequired[EncryptionConfig | dict[str, Any]]"
    database_dialect: "NotRequired[DatabaseDialect]"
    database_role: "NotRequired[str]"
    enable_drop_protection: "NotRequired[bool]"
    enable_interceptors_in_tests: "NotRequired[bool]"
    proto_descriptors: "NotRequired[bytes]"
    extra: "NotRequired[dict[str, Any]]"


class SpannerPoolParams(SpannerConnectionParams):
    """Session pool configuration."""

    pool_type: "NotRequired[type[AbstractSessionPool]]"
    size: "NotRequired[int]"
    target_size: "NotRequired[int]"
    max_sessions: "NotRequired[int]"
    default_timeout: "NotRequired[int | float]"
    session_labels: "NotRequired[dict[str, str]]"
    labels: "NotRequired[dict[str, str]]"
    ping_interval: "NotRequired[int]"
    max_age_minutes: "NotRequired[int]"


class SpannerDriverFeatures(TypedDict):
    """Driver feature flags for Spanner.

    Attributes:
        enable_uuid_conversion: Enable automatic UUID string conversion.
        json_serializer: Custom JSON serializer for parameter conversion.
        json_deserializer: Custom JSON deserializer for result conversion.
        retry: Per-request retry policy passed to execute_sql(), execute_update(), and batch_update().
        timeout: Per-request timeout in seconds passed to execute_sql(), execute_update(), and batch_update().
        request_options: Default RequestOptions forwarded to execute_sql(), execute_update(),
            and batch_update(). Per-call overrides are available through normal
            driver execution methods.
        directed_read_options: Default DirectedReadOptions forwarded to execute_sql().
        session_labels: Deprecated compatibility alias for pool session labels.
            Prefer ``connection_config["session_labels"]``.
        enable_events: Enable database event channel support.
            Defaults to True when extension_config["events"] is configured.
        events_backend: Backend type for event handling.
            Spanner only supports "table_queue" (no native pub/sub).
        enable_batch_write_api: Route load_from_arrow through the Spanner Batch Write API
            (Database.mutation_groups().batch_write()) for high-throughput, independently
            committed mutation groups instead of a single in-transaction insert_or_update.
            Defaults to False.
    """

    enable_uuid_conversion: "NotRequired[bool]"
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    retry: "NotRequired[Retry | None]"
    timeout: "NotRequired[float | None]"
    request_options: "NotRequired[RequestOptions | dict[str, Any] | None]"
    directed_read_options: "NotRequired[DirectedReadOptions | None]"
    session_labels: "NotRequired[dict[str, str]]"
    enable_events: "NotRequired[bool]"
    events_backend: "NotRequired[str]"
    enable_batch_write_api: "NotRequired[bool]"


class SpannerConnectionContext(SyncPoolConnectionContext):
    """Context manager for Spanner connections."""

    __slots__ = ("_connection", "_session", "_transaction")

    def __init__(self, config: "SpannerSyncConfig", transaction: bool = False) -> None:
        super().__init__(config)
        self._transaction = transaction
        self._connection: SpannerConnection | None = None
        self._session: Any = None

    def __enter__(self) -> SpannerConnection:
        database = self._config.get_database()
        if self._transaction:
            self._session = cast("Any", database).session()
            self._session.create()
            try:
                txn = self._session.transaction()
                txn.__enter__()
                self._connection = cast("SpannerConnection", txn)
            except Exception:
                self._session.delete()
                raise
            else:
                return self._connection
        else:
            self._session = cast("Any", database).snapshot(multi_use=True)
            self._connection = cast("SpannerConnection", self._session.__enter__())
            return self._connection

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._transaction and self._connection:
            txn = cast("Any", self._connection)
            try:
                if exc_type is None:
                    try:
                        txn_id = txn._transaction_id
                    except AttributeError:
                        txn_id = None
                    try:
                        committed = txn.committed
                    except AttributeError:
                        committed = None
                    if txn_id is not None and committed is None:
                        txn.commit()
                else:
                    try:
                        rollback_txn_id = txn._transaction_id
                    except AttributeError:
                        rollback_txn_id = None
                    if rollback_txn_id is not None:
                        txn.rollback()
            finally:
                if self._session:
                    self._session.delete()
        elif self._session:
            self._session.__exit__(exc_type, exc_val, exc_tb)

        self._connection = None
        self._session = None
        return None


class _SpannerSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ("_connection_ctx",)

    def __init__(self, config: "SpannerSyncConfig", connection_ctx: "SpannerConnectionContext") -> None:
        super().__init__(config)
        self._connection_ctx = connection_ctx

    def acquire_connection(self) -> "SpannerConnection":
        return self._connection_ctx.__enter__()

    def release_connection(self, _conn: "SpannerConnection", **kwargs: Any) -> None:
        self._connection_ctx.__exit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))


class SpannerSyncConfig(SyncDatabaseConfig["SpannerConnection", "AbstractSessionPool", SpannerSyncDriver]):
    """Spanner configuration and session management."""

    driver_type: ClassVar[type["SpannerSyncDriver"]] = SpannerSyncDriver
    connection_type: ClassVar[type["SpannerConnection"]] = cast("type[SpannerConnection]", SpannerConnection)
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = False
    supports_native_parquet_import: ClassVar[bool] = False
    requires_staging_for_load: ClassVar[bool] = False
    _connection_context_class: "ClassVar[type[SpannerConnectionContext]]" = SpannerConnectionContext
    _session_factory_class: "ClassVar[type[_SpannerSessionConnectionHandler]]" = _SpannerSessionConnectionHandler
    _session_context_class: "ClassVar[type[SpannerSessionContext]]" = SpannerSessionContext
    _default_statement_config = default_statement_config

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
        **kwargs: Any,
    ) -> None:
        self.connection_config = normalize_connection_config(connection_config)
        if "min_sessions" in self.connection_config:
            msg = "Spanner session pools do not support 'min_sessions'; use 'size' or 'target_size'."
            raise ImproperConfigurationError(msg)

        raw_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        legacy_session_labels = raw_driver_features.pop("session_labels", None)
        if (
            legacy_session_labels is not None
            and "session_labels" not in self.connection_config
            and "labels" not in self.connection_config
        ):
            self.connection_config["session_labels"] = legacy_session_labels

        self.connection_config.setdefault("size", self.connection_config.pop("max_sessions", 10))
        self.connection_config.setdefault("pool_type", FixedSizePool)

        driver_features = apply_driver_features(raw_driver_features)

        statement_config = statement_config or default_statement_config

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

        self._client: Client | None = None
        self._database: Database | None = None

    def _get_client(self) -> Client:
        if self._client is None:
            client_kwargs = self._resolve_kwargs(_CLIENT_CONFIG_FIELDS)
            self._client = Client(**client_kwargs)
        return self._client

    def get_database(self) -> "Database":
        instance_id = self.connection_config.get("instance_id")
        database_id = self.connection_config.get("database_id")
        if not instance_id or not database_id:
            msg = "instance_id and database_id are required."
            raise ImproperConfigurationError(msg)

        if self.connection_instance is None:
            self.connection_instance = self.provide_pool()

        if self._database is None:
            client = self._get_client()
            instance_kwargs = self._resolve_kwargs(_INSTANCE_CONFIG_FIELDS)
            instance_labels = self.connection_config.get("instance_labels")
            if instance_labels is not None:
                instance_kwargs["labels"] = instance_labels
            database_kwargs = self._resolve_kwargs(_DATABASE_CONFIG_FIELDS)
            database_kwargs["pool"] = self.connection_instance
            self._database = client.instance(instance_id, **instance_kwargs).database(  # type: ignore[no-untyped-call]
                database_id, **database_kwargs
            )
        return self._database

    def create_connection(self) -> SpannerConnection:
        return cast("SpannerConnection", self.get_database().snapshot())  # type: ignore[no-untyped-call]

    def _create_pool(self) -> AbstractSessionPool:
        instance_id = self.connection_config.get("instance_id")
        database_id = self.connection_config.get("database_id")
        if not instance_id or not database_id:
            msg = "instance_id and database_id are required."
            raise ImproperConfigurationError(msg)

        pool_type = cast("type[AbstractSessionPool]", self.connection_config.get("pool_type", FixedSizePool))

        labels = self.connection_config.get("session_labels", self.connection_config.get("labels"))
        pool_kwargs: dict[str, Any] = self._resolve_pool_base_kwargs(labels=cast("dict[str, str] | None", labels))
        if issubclass(pool_type, PingingPool):
            pool_kwargs.update(self._resolve_kwargs({"size", "default_timeout", "ping_interval"}))
        elif issubclass(pool_type, FixedSizePool):
            pool_kwargs.update(self._resolve_kwargs({"size", "default_timeout", "max_age_minutes"}))
        elif issubclass(pool_type, BurstyPool):
            target_size = self.connection_config.get("target_size", self.connection_config.get("size"))
            if target_size is not None:
                pool_kwargs["target_size"] = target_size
        else:
            pool_kwargs.update(
                self._resolve_kwargs({"size", "target_size", "default_timeout", "ping_interval", "max_age_minutes"})
            )

        pool_factory = cast("Callable[..., AbstractSessionPool]", pool_type)
        return pool_factory(**pool_kwargs)

    def _resolve_pool_base_kwargs(self, *, labels: "dict[str, str] | None") -> dict[str, Any]:
        pool_kwargs: dict[str, Any] = {}
        if labels is not None:
            pool_kwargs["labels"] = labels
        database_role = self.connection_config.get("database_role")
        if database_role is not None:
            pool_kwargs["database_role"] = database_role
        return pool_kwargs

    def _resolve_kwargs(self, fields: "frozenset[str] | set[str]") -> dict[str, Any]:
        return {
            field: self.connection_config[field] for field in fields if self.connection_config.get(field) is not None
        }

    def _close_pool(self) -> None:
        if self.connection_instance and supports_close(self.connection_instance):
            self.connection_instance.close()
        if self._client and supports_close(self._client):
            self._client.close()
        self._client = None
        self._database = None

    def provide_connection(
        self, *args: Any, transaction: "bool" = _DEFAULT_SESSION_TRANSACTION, **kwargs: Any
    ) -> "SpannerConnectionContext":
        """Yield a Transaction (default) or Snapshot context from the configured pool.

        Args:
            *args: Additional positional arguments (unused, for interface compatibility).
            transaction: If True (default), yields a Transaction context that
                supports execute_update() for DML statements. If False, yields
                a read-only Snapshot context for SELECT queries.
            **kwargs: Additional keyword arguments (unused, for interface compatibility).
        """
        return SpannerConnectionContext(self, transaction=transaction)

    def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        transaction: "bool" = _DEFAULT_SESSION_TRANSACTION,
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerSessionContext":
        """Provide a Spanner driver session context manager.

        Returns a write-capable Transaction session by default, matching every
        other sqlspec adapter. Pass ``transaction=False`` or use
        :meth:`provide_read_session` to obtain a read-only Snapshot session.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            transaction: Whether to use a Transaction (True, default) or
                Snapshot (False).
            request_options: Session-scoped RequestOptions for Spanner statements.
            directed_read_options: Session-scoped DirectedReadOptions for reads.
            retry: Session-scoped retry policy for Spanner statement calls.
            timeout: Session-scoped timeout for Spanner statement calls.
            **kwargs: Additional keyword arguments.

        Returns:
            A Spanner driver session context manager.
        """
        connection_ctx = SpannerConnectionContext(self, transaction=transaction)
        handler = _SpannerSessionConnectionHandler(self, connection_ctx)

        return SpannerSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self._session_driver_features(
                request_options=request_options,
                directed_read_options=directed_read_options,
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
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerSessionContext":
        """Provide a write-capable Spanner session (alias for :meth:`provide_session`)."""
        return self.provide_session(
            *args,
            statement_config=statement_config,
            transaction=True,
            request_options=request_options,
            directed_read_options=directed_read_options,
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
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        **kwargs: Any,
    ) -> "SpannerSessionContext":
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
            retry=retry,
            timeout=timeout,
            **kwargs,
        )

    def _session_driver_features(
        self,
        *,
        request_options: "RequestOptions | dict[str, Any] | None",
        directed_read_options: "DirectedReadOptions | None",
        retry: "Retry | None",
        timeout: "float | None",
    ) -> "dict[str, Any]":
        if request_options is None and directed_read_options is None and retry is None and timeout is None:
            return self.driver_features
        driver_features = dict(self.driver_features)
        if request_options is not None:
            driver_features["request_options"] = request_options
        if directed_read_options is not None:
            driver_features["directed_read_options"] = directed_read_options
        if retry is not None:
            driver_features["retry"] = retry
        if timeout is not None:
            driver_features["timeout"] = timeout
        return driver_features

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for SpannerSyncConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "SpannerConnectionContext": SpannerConnectionContext,
            "SpannerConnection": SpannerConnection,
            "SpannerConnectionParams": SpannerConnectionParams,
            "SpannerDriverFeatures": SpannerDriverFeatures,
            "SpannerPoolParams": SpannerPoolParams,
            "SpannerSessionContext": SpannerSessionContext,
            "SpannerSyncConfig": SpannerSyncConfig,
            "SpannerSyncDriver": SpannerSyncDriver,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return queue defaults for Spanner JSON handling."""

        return EventRuntimeHints(json_passthrough=True)
