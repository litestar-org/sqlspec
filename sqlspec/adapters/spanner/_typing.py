"""Type definitions for Spanner adapter.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from google.api_core import exceptions as spanner_exceptions
from google.api_core.client_info import ClientInfo as SpannerClientInfo
from google.api_core.client_options import ClientOptions as SpannerClientOptions
from google.api_core.exceptions import GoogleAPICallError as SpannerGoogleAPICallError
from google.api_core.exceptions import GoogleAPIError as SpannerGoogleAPIError
from google.api_core.exceptions import NotFound as SpannerNotFound
from google.api_core.retry import Retry as SpannerRetry
from google.auth.credentials import Credentials as SpannerCredentials
from google.cloud.spanner_admin_database_v1.types import DatabaseDialect as SpannerDatabaseDialect
from google.cloud.spanner_admin_database_v1.types import EncryptionConfig as SpannerEncryptionConfig
from google.cloud.spanner_v1 import Client as SpannerClient
from google.cloud.spanner_v1 import DirectedReadOptions as SpannerDirectedReadOptions
from google.cloud.spanner_v1 import ExecuteSqlRequest as SpannerExecuteSqlRequest
from google.cloud.spanner_v1 import RequestOptions as SpannerRequestOptions
from google.cloud.spanner_v1 import param_types as spanner_param_types
from google.cloud.spanner_v1._async.batch import Batch as SpannerAsyncBatch
from google.cloud.spanner_v1._async.batch import MutationGroups as SpannerAsyncMutationGroups
from google.cloud.spanner_v1._async.client import Client as SpannerAsyncClient
from google.cloud.spanner_v1._async.database import Database as SpannerAsyncDatabase
from google.cloud.spanner_v1._async.database import SnapshotCheckout as SpannerAsyncSnapshotCheckout
from google.cloud.spanner_v1._async.database_sessions_manager import TransactionType as SpannerAsyncTransactionType
from google.cloud.spanner_v1._async.pool import AbstractSessionPool as SpannerAsyncAbstractSessionPool
from google.cloud.spanner_v1._async.pool import BurstyPool as SpannerAsyncBurstyPool
from google.cloud.spanner_v1._async.pool import FixedSizePool as SpannerAsyncFixedSizePool
from google.cloud.spanner_v1._async.pool import PingingPool as SpannerAsyncPingingPool
from google.cloud.spanner_v1._async.pool import TransactionPingingPool as SpannerAsyncTransactionPingingPool
from google.cloud.spanner_v1._async.session import Session as SpannerAsyncSession
from google.cloud.spanner_v1._async.snapshot import Snapshot as SpannerAsyncSnapshot
from google.cloud.spanner_v1._async.streamed import StreamedResultSet as SpannerAsyncStreamedResultSet
from google.cloud.spanner_v1._async.transaction import Transaction as SpannerAsyncTransaction
from google.cloud.spanner_v1.batch import Batch as SpannerBatch
from google.cloud.spanner_v1.batch import MutationGroups as SpannerMutationGroups
from google.cloud.spanner_v1.data_types import Interval as SpannerInterval
from google.cloud.spanner_v1.data_types import JsonObject as SpannerJsonObject
from google.cloud.spanner_v1.database import Database as SpannerDatabase
from google.cloud.spanner_v1.database import SnapshotCheckout
from google.cloud.spanner_v1.database_sessions_manager import TransactionType as SpannerTransactionType
from google.cloud.spanner_v1.pool import AbstractSessionPool as SpannerAbstractSessionPool
from google.cloud.spanner_v1.pool import BurstyPool as SpannerBurstyPool
from google.cloud.spanner_v1.pool import FixedSizePool as SpannerFixedSizePool
from google.cloud.spanner_v1.pool import PingingPool as SpannerPingingPool
from google.cloud.spanner_v1.pool import TransactionPingingPool as SpannerTransactionPingingPool
from google.cloud.spanner_v1.transaction import DefaultTransactionOptions as SpannerDefaultTransactionOptions
from google.cloud.spanner_v1.transaction import Transaction as SpannerTransaction
from google.cloud.spanner_v1.types.type import TypeCode as SpannerTypeCode

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from google.cloud.spanner_v1.snapshot import Snapshot

    from sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerSyncDriver
    from sqlspec.core import StatementConfig

    SpannerSyncConnection: TypeAlias = Snapshot | SnapshotCheckout | SpannerTransaction | SpannerBatch
    SpannerAsyncConnection: TypeAlias = (
        SpannerAsyncSnapshot | SpannerAsyncSnapshotCheckout | SpannerAsyncTransaction | SpannerAsyncBatch
    )
else:
    SpannerSyncConnection = Any
    SpannerAsyncConnection = Any


__all__ = (
    "SpannerAbstractSessionPool",
    "SpannerAsyncAbstractSessionPool",
    "SpannerAsyncBatch",
    "SpannerAsyncBurstyPool",
    "SpannerAsyncClient",
    "SpannerAsyncConnection",
    "SpannerAsyncCursor",
    "SpannerAsyncDatabase",
    "SpannerAsyncFixedSizePool",
    "SpannerAsyncMutationGroups",
    "SpannerAsyncPingingPool",
    "SpannerAsyncSession",
    "SpannerAsyncSessionContext",
    "SpannerAsyncSnapshot",
    "SpannerAsyncStreamedResultSet",
    "SpannerAsyncTransaction",
    "SpannerAsyncTransactionPingingPool",
    "SpannerAsyncTransactionType",
    "SpannerBatch",
    "SpannerBurstyPool",
    "SpannerClient",
    "SpannerClientInfo",
    "SpannerClientOptions",
    "SpannerCredentials",
    "SpannerDatabase",
    "SpannerDatabaseDialect",
    "SpannerDefaultTransactionOptions",
    "SpannerDirectedReadOptions",
    "SpannerEncryptionConfig",
    "SpannerExecuteSqlRequest",
    "SpannerFixedSizePool",
    "SpannerGoogleAPICallError",
    "SpannerGoogleAPIError",
    "SpannerInterval",
    "SpannerJsonObject",
    "SpannerMutationGroups",
    "SpannerNotFound",
    "SpannerPingingPool",
    "SpannerRequestOptions",
    "SpannerRetry",
    "SpannerSyncConnection",
    "SpannerSyncCursor",
    "SpannerSyncSessionContext",
    "SpannerTransaction",
    "SpannerTransactionPingingPool",
    "SpannerTransactionType",
    "SpannerTypeCode",
    "spanner_exceptions",
    "spanner_param_types",
)


class SpannerSyncCursor:
    """Context manager that yields the active Spanner connection."""

    __slots__ = ("connection",)

    def __init__(self, connection: "SpannerSyncConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "SpannerSyncConnection":
        return self.connection

    def __exit__(self, *_: Any) -> None:
        return None


class SpannerAsyncCursor:
    """Async context manager that yields the active Spanner async connection."""

    __slots__ = ("connection",)

    def __init__(self, connection: "SpannerAsyncConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> "SpannerAsyncConnection":
        return self.connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        return None


class SpannerSyncSessionContext:
    """Sync context manager for Spanner sessions.

    Excluded from mypyc compilation: it receives callables from uncompiled
    config classes and instantiates compiled driver objects. The release
    callable receives exception details so the connection context can commit
    or roll back the active transaction.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[SpannerSyncDriver], SpannerSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: SpannerSyncDriver | None = None

    def __enter__(self) -> "SpannerSyncDriver":
        driver_cls = _get_sync_driver_type()
        self._connection = self._acquire_connection()
        self._driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            connection = self._driver.connection if self._driver is not None else self._connection
            self._release_connection(connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
            self._driver = None
        return None


class SpannerAsyncSessionContext:
    """Async context manager for Spanner sessions.

    Excluded from mypyc compilation: it receives callables from uncompiled
    config classes and instantiates compiled driver objects. The release
    callable receives exception details so the connection context can commit
    or roll back the active transaction.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Awaitable[Any]]",
        release_connection: "Callable[..., Awaitable[Any]]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[SpannerAsyncDriver], SpannerAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: SpannerAsyncDriver | None = None

    async def __aenter__(self) -> "SpannerAsyncDriver":
        driver_cls = _get_async_driver_type()
        self._connection = await self._acquire_connection()
        self._driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            connection = self._driver.connection if self._driver is not None else self._connection
            await self._release_connection(connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
            self._driver = None
        return None


def _get_sync_driver_type() -> "type[SpannerSyncDriver]":
    driver_module = import_module("sqlspec.adapters.spanner.driver")
    return cast("type[SpannerSyncDriver]", driver_module.SpannerSyncDriver)


def _get_async_driver_type() -> "type[SpannerAsyncDriver]":
    driver_module = import_module("sqlspec.adapters.spanner.driver")
    return cast("type[SpannerAsyncDriver]", driver_module.SpannerAsyncDriver)
