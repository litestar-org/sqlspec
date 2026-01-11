"""Spanner configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.pool import AbstractSessionPool, FixedSizePool
from typing_extensions import NotRequired

from sqlspec.adapters.spanner._typing import SpannerConnection
from sqlspec.adapters.spanner.core import apply_driver_features, default_statement_config
from sqlspec.adapters.spanner.driver import SpannerSessionContext, SpannerSyncDriver
from sqlspec.config import SyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config, reject_pool_aliases
from sqlspec.utils.type_guards import supports_close

if TYPE_CHECKING:
    from collections.abc import Callable

    from google.auth.credentials import Credentials
    from google.cloud.spanner_v1.database import Database

    from sqlspec.config import ExtensionConfigs
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("SpannerConnectionParams", "SpannerDriverFeatures", "SpannerPoolParams", "SpannerSyncConfig")


class SpannerConnectionParams(TypedDict):
    """Spanner connection parameters."""

    project: "NotRequired[str]"
    instance_id: "NotRequired[str]"
    database_id: "NotRequired[str]"
    credentials: "NotRequired[Credentials]"
    client_options: "NotRequired[dict[str, Any]]"
    extra: "NotRequired[dict[str, Any]]"


class SpannerPoolParams(SpannerConnectionParams):
    """Session pool configuration."""

    pool_type: "NotRequired[type[AbstractSessionPool]]"
    min_sessions: "NotRequired[int]"
    max_sessions: "NotRequired[int]"
    labels: "NotRequired[dict[str, str]]"
    ping_interval: "NotRequired[int]"


class SpannerDriverFeatures(TypedDict):
    """Driver feature flags for Spanner.

    Attributes:
        enable_uuid_conversion: Enable automatic UUID string conversion.
        json_serializer: Custom JSON serializer for parameter conversion.
        json_deserializer: Custom JSON deserializer for result conversion.
        session_labels: Labels to apply to Spanner sessions.
        enable_events: Enable database event channel support.
            Defaults to True when extension_config["events"] is configured.
        events_backend: Backend type for event handling.
            Spanner only supports "table_queue" (no native pub/sub).
    """

    enable_uuid_conversion: "NotRequired[bool]"
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    session_labels: "NotRequired[dict[str, str]]"
    enable_events: "NotRequired[bool]"
    events_backend: "NotRequired[str]"


class SpannerConnectionContext:
    """Context manager for Spanner connections."""

    __slots__ = ("_config", "_connection", "_session", "_transaction")

    def __init__(self, config: "SpannerSyncConfig", transaction: bool = False) -> None:
        self._config = config
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
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
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


class _SpannerSessionConnectionHandler:
    __slots__ = ("_connection_ctx",)

    def __init__(self, connection_ctx: "SpannerConnectionContext") -> None:
        self._connection_ctx = connection_ctx

    def acquire_connection(self) -> "SpannerConnection":
        return self._connection_ctx.__enter__()

    def release_connection(
        self,
        _conn: "SpannerConnection",
        exc_type: "type[BaseException] | None",
        exc_val: "BaseException | None",
        exc_tb: Any,
    ) -> None:
        self._connection_ctx.__exit__(exc_type, exc_val, exc_tb)


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
        reject_pool_aliases(kwargs)

        self.connection_config = normalize_connection_config(connection_config)

        self.connection_config.setdefault("min_sessions", 1)
        self.connection_config.setdefault("max_sessions", 10)
        self.connection_config.setdefault("pool_type", FixedSizePool)

        driver_features = apply_driver_features(driver_features)

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
            self._client = Client(
                project=self.connection_config.get("project"),
                credentials=self.connection_config.get("credentials"),
                client_options=self.connection_config.get("client_options"),
            )
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
            self._database = client.instance(instance_id).database(database_id, pool=self.connection_instance)  # type: ignore[no-untyped-call]
        return self._database

    def create_connection(self) -> SpannerConnection:
        instance_id = self.connection_config.get("instance_id")
        database_id = self.connection_config.get("database_id")
        if not instance_id or not database_id:
            msg = "instance_id and database_id are required."
            raise ImproperConfigurationError(msg)

        if self.connection_instance is None:
            self.connection_instance = self.provide_pool()

        client = self._get_client()
        database = client.instance(instance_id).database(database_id, pool=self.connection_instance)  # type: ignore[no-untyped-call]
        return cast("SpannerConnection", database.snapshot())

    def _create_pool(self) -> AbstractSessionPool:
        instance_id = self.connection_config.get("instance_id")
        database_id = self.connection_config.get("database_id")
        if not instance_id or not database_id:
            msg = "instance_id and database_id are required."
            raise ImproperConfigurationError(msg)

        pool_type = cast("type[AbstractSessionPool]", self.connection_config.get("pool_type", FixedSizePool))

        pool_kwargs: dict[str, Any] = {}
        if pool_type is FixedSizePool:
            if "size" in self.connection_config:
                pool_kwargs["size"] = self.connection_config["size"]
            elif "max_sessions" in self.connection_config:
                pool_kwargs["size"] = self.connection_config["max_sessions"]
            if "labels" in self.connection_config:
                pool_kwargs["labels"] = self.connection_config["labels"]
        else:
            valid_pool_keys = {"size", "labels", "ping_interval"}
            pool_kwargs = {k: v for k, v in self.connection_config.items() if k in valid_pool_keys and v is not None}
            if "size" not in pool_kwargs and "max_sessions" in self.connection_config:
                pool_kwargs["size"] = self.connection_config["max_sessions"]

        pool_factory = cast("Callable[..., AbstractSessionPool]", pool_type)
        return pool_factory(**pool_kwargs)

    def _close_pool(self) -> None:
        if self.connection_instance and supports_close(self.connection_instance):
            self.connection_instance.close()

    def provide_connection(self, *args: Any, transaction: "bool" = False, **kwargs: Any) -> "SpannerConnectionContext":
        """Yield a Snapshot (default) or Transaction context from the configured pool.

        Args:
            *args: Additional positional arguments (unused, for interface compatibility).
            transaction: If True, yields a Transaction context that supports
                execute_update() for DML statements. If False (default), yields
                a read-only Snapshot context for SELECT queries.
            **kwargs: Additional keyword arguments (unused, for interface compatibility).
        """
        return SpannerConnectionContext(self, transaction=transaction)

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, transaction: "bool" = False, **kwargs: Any
    ) -> "SpannerSessionContext":
        """Provide a Spanner driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            transaction: Whether to use a transaction.
            **kwargs: Additional keyword arguments.

        Returns:
            A Spanner driver session context manager.
        """
        connection_ctx = SpannerConnectionContext(self, transaction=transaction)
        handler = _SpannerSessionConnectionHandler(connection_ctx)

        return SpannerSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def provide_write_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "SpannerSessionContext":
        """Provide a Spanner driver write session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Returns:
            A Spanner driver write session context manager.
        """
        return self.provide_session(*args, statement_config=statement_config, transaction=True, **kwargs)

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
