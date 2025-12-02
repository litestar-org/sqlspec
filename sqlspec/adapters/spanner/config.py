"""Spanner configuration."""

from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.pool import AbstractSessionPool, FixedSizePool
from typing_extensions import NotRequired

from sqlspec.adapters.spanner._types import SpannerConnection
from sqlspec.adapters.spanner.driver import SpannerSyncDriver, spanner_statement_config
from sqlspec.config import SyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
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
    """Driver feature flags for Spanner."""

    enable_uuid_conversion: "NotRequired[bool]"
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    session_labels: "NotRequired[dict[str, str]]"


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
        pool_config: "SpannerPoolParams | dict[str, Any] | None" = None,
        pool_instance: "AbstractSessionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SpannerDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
    ) -> None:
        self.pool_config = dict(pool_config) if pool_config else {}

        self.pool_config.setdefault("min_sessions", 1)
        self.pool_config.setdefault("max_sessions", 10)
        self.pool_config.setdefault("pool_type", FixedSizePool)

        features: dict[str, Any] = dict(driver_features) if driver_features else {}
        features.setdefault("enable_uuid_conversion", True)
        features.setdefault("json_serializer", to_json)
        features.setdefault("json_deserializer", from_json)

        base_statement_config = statement_config or spanner_statement_config

        super().__init__(
            pool_config=self.pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=base_statement_config,
            driver_features=features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
        )

        self._client: Client | None = None
        self._database: Database | None = None

    def _get_client(self) -> Client:
        if self._client is None:
            self._client = Client(
                project=self.pool_config.get("project"),
                credentials=self.pool_config.get("credentials"),
                client_options=self.pool_config.get("client_options"),
            )
        return self._client

    def get_database(self) -> "Database":
        instance_id = self.pool_config.get("instance_id")
        database_id = self.pool_config.get("database_id")
        if not instance_id or not database_id:
            msg = "instance_id and database_id are required."
            raise ImproperConfigurationError(msg)

        if self.pool_instance is None:
            self.pool_instance = self.provide_pool()

        if self._database is None:
            client = self._get_client()
            self._database = client.instance(instance_id).database(database_id, pool=self.pool_instance)  # type: ignore[no-untyped-call]
        return self._database

    def create_connection(self) -> SpannerConnection:
        instance_id = self.pool_config.get("instance_id")
        database_id = self.pool_config.get("database_id")
        if not instance_id or not database_id:
            msg = "instance_id and database_id are required."
            raise ImproperConfigurationError(msg)

        if self.pool_instance is None:
            self.pool_instance = self.provide_pool()

        client = self._get_client()
        database = client.instance(instance_id).database(database_id, pool=self.pool_instance)  # type: ignore[no-untyped-call]
        return cast("SpannerConnection", database.snapshot())

    def _create_pool(self) -> AbstractSessionPool:
        instance_id = self.pool_config.get("instance_id")
        database_id = self.pool_config.get("database_id")
        if not instance_id or not database_id:
            msg = "instance_id and database_id are required."
            raise ImproperConfigurationError(msg)

        pool_type = cast("type[AbstractSessionPool]", self.pool_config.get("pool_type", FixedSizePool))

        pool_kwargs: dict[str, Any] = {}
        if pool_type is FixedSizePool:
            if "size" in self.pool_config:
                pool_kwargs["size"] = self.pool_config["size"]
            elif "max_sessions" in self.pool_config:
                pool_kwargs["size"] = self.pool_config["max_sessions"]
            if "labels" in self.pool_config:
                pool_kwargs["labels"] = self.pool_config["labels"]
        else:
            valid_pool_keys = {"size", "labels", "ping_interval"}
            pool_kwargs = {k: v for k, v in self.pool_config.items() if k in valid_pool_keys and v is not None}
            if "size" not in pool_kwargs and "max_sessions" in self.pool_config:
                pool_kwargs["size"] = self.pool_config["max_sessions"]

        pool_factory = cast("Callable[..., AbstractSessionPool]", pool_type)
        return pool_factory(**pool_kwargs)

    def _close_pool(self) -> None:
        if self.pool_instance and hasattr(self.pool_instance, "close"):
            cast("Any", self.pool_instance).close()

    @contextmanager
    def provide_connection(
        self, *args: Any, transaction: "bool" = False, **kwargs: Any
    ) -> Generator[SpannerConnection, None, None]:
        """Yield a Snapshot (default) or Batch context from the configured pool.

        Note: Spanner does not support database.transaction() as a context manager.
        For write operations requiring conditional logic, use database.run_in_transaction()
        directly. The `transaction=True` option here uses database.batch() which is
        suitable for simple insert/update/delete mutations.
        """
        database = self.get_database()
        if transaction:
            with cast("Any", database).batch() as batch:
                yield cast("SpannerConnection", batch)
        else:
            with cast("Any", database).snapshot() as snapshot:
                yield cast("SpannerConnection", snapshot)

    @contextmanager
    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, transaction: "bool" = False, **kwargs: Any
    ) -> Generator[SpannerSyncDriver, None, None]:
        with self.provide_connection(*args, transaction=transaction, **kwargs) as connection:
            driver = self.driver_type(
                connection=connection,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
            )
            yield self._prepare_driver(driver)

    @contextmanager
    def provide_write_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> Generator[SpannerSyncDriver, None, None]:
        """Convenience wrapper that always yields a write-capable transaction session."""
        with self.provide_session(*args, statement_config=statement_config, transaction=True, **kwargs) as driver:
            yield driver

    def get_signature_namespace(self) -> dict[str, Any]:
        namespace = super().get_signature_namespace()
        namespace.update({
            "SpannerSyncConfig": SpannerSyncConfig,
            "SpannerConnectionParams": SpannerConnectionParams,
            "SpannerDriverFeatures": SpannerDriverFeatures,
            "SpannerSyncDriver": SpannerSyncDriver,
        })
        return namespace
