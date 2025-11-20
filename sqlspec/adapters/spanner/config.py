"""Spanner configuration."""

import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.pool import AbstractSessionPool, FixedSizePool
from typing_extensions import NotRequired

from sqlspec.adapters.spanner._types import SpannerConnection
from sqlspec.adapters.spanner.driver import SpannerSyncDriver, spanner_statement_config
from sqlspec.base import SyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from google.auth.credentials import Credentials
    from google.cloud.spanner_v1.database import Database

    from sqlspec.config import ExtensionConfigs
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

logger = logging.getLogger(__name__)

__all__ = ("SpannerConfig", "SpannerConnectionParams", "SpannerDriverFeatures", "SpannerPoolParams")


class SpannerConnectionParams(TypedDict):
    """Spanner connection parameters."""

    project: NotRequired[str]
    instance_id: NotRequired[str]
    database_id: NotRequired[str]
    credentials: NotRequired["Credentials"]
    client_options: NotRequired[dict[str, Any]]
    extra: NotRequired[dict[str, Any]]


class SpannerPoolParams(TypedDict):
    """Spanner pool configuration."""

    pool_type: NotRequired[type[AbstractSessionPool]]
    min_sessions: NotRequired[int]
    max_sessions: NotRequired[int]
    labels: NotRequired[dict[str, str]]
    ping_interval: NotRequired[int]


class SpannerDriverFeatures(TypedDict):
    """Spanner driver features."""

    enable_uuid_conversion: NotRequired[bool]
    json_serializer: NotRequired[Callable[[Any], str]]
    json_deserializer: NotRequired[Callable[[str], Any]]
    session_labels: NotRequired[dict[str, str]]


class SpannerConfig(SyncDatabaseConfig[SpannerConnection, AbstractSessionPool, SpannerSyncDriver]):
    """Spanner configuration."""

    driver_type: ClassVar[type[SpannerSyncDriver]] = SpannerSyncDriver
    connection_type: ClassVar[type[SpannerConnection]] = SpannerConnection  # type: ignore
    supports_transactional_ddl: ClassVar[bool] = False  # Spanner DDL is separate

    def __init__(
        self,
        *,
        connection_config: "SpannerConnectionParams | dict[str, Any] | None" = None,
        pool_config: "SpannerPoolParams | dict[str, Any] | None" = None,
        pool_instance: AbstractSessionPool | None = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SpannerDriverFeatures | dict[str, Any] | None" = None,
        bind_key: str | None = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
    ) -> None:
        self.connection_config = dict(connection_config) if connection_config else {}
        self.pool_config = dict(pool_config) if pool_config else {}

        # Set defaults for pool
        if "min_sessions" not in self.pool_config:
            self.pool_config["min_sessions"] = 1
        if "max_sessions" not in self.pool_config:
            self.pool_config["max_sessions"] = 10
        if "pool_type" not in self.pool_config:
            self.pool_config["pool_type"] = FixedSizePool

        features: dict[str, Any] = dict(driver_features) if driver_features else {}
        features.setdefault("enable_uuid_conversion", True)
        serializer = features.setdefault("json_serializer", to_json)
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

    @property
    def project(self) -> str | None:
        return self.connection_config.get("project")

    @property
    def instance_id(self) -> str | None:
        return self.connection_config.get("instance_id")

    @property
    def database_id(self) -> str | None:
        return self.connection_config.get("database_id")

    @property
    def credentials(self) -> "Credentials | None":
        return self.connection_config.get("credentials")

    @property
    def client_options(self) -> dict[str, Any] | None:
        return self.connection_config.get("client_options")

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = Client(
                project=self.project, credentials=self.credentials, client_options=self.client_options
            )
        return self._client

    def create_connection(self) -> SpannerConnection:
        raise NotImplementedError("Use provide_connection() context manager for Spanner.")

    def _create_pool(self) -> AbstractSessionPool:
        if not self.instance_id or not self.database_id:
            raise ImproperConfigurationError("instance_id and database_id are required.")

        instance = self.client.instance(self.instance_id)
        database = instance.database(
            self.database_id,
            pool=None,  # We are creating the pool for the database
        )

        # Extract pool params
        pool_type = self.pool_config.get("pool_type", FixedSizePool)

        # Extract arguments for pool constructor
        # FixedSizePool etc take database as first arg, then other kwargs
        # We filter connection params
        connection_keys = {"project", "instance_id", "database_id", "credentials", "client_options", "pool_type"}
        pool_kwargs = {k: v for k, v in self.pool_config.items() if k not in connection_keys and v is not None}

        if pool_type is FixedSizePool:
            if "size" not in pool_kwargs and "max_sessions" in self.pool_config:
                pool_kwargs["size"] = self.pool_config["max_sessions"]

        return pool_type(database, **pool_kwargs)

    def _close_pool(self) -> None:
        pass

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[SpannerConnection, None, None]":
        """Provide a Spanner Snapshot/Transaction."""
        # Ensure database/pool is ready
        if self._database is None:
            self.pool_instance = self.provide_pool()
            # Re-create database with this pool
            instance = self.client.instance(self.instance_id)
            self._database = instance.database(self.database_id, pool=self.pool_instance)

        with self._database.snapshot() as snapshot:
            yield snapshot

    @contextmanager
    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "Generator[SpannerSyncDriver, None, None]":
        with self.provide_connection(*args, **kwargs) as connection:
            driver = self.driver_type(
                connection=connection,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
            )
            yield self._prepare_driver(driver)

    def get_signature_namespace(self) -> dict[str, Any]:
        namespace = super().get_signature_namespace()
        namespace.update({
            "SpannerConfig": SpannerConfig,
            "SpannerConnectionParams": SpannerConnectionParams,
            "SpannerDriverFeatures": SpannerDriverFeatures,
            "SpannerSyncDriver": SpannerSyncDriver,
        })
        return namespace
