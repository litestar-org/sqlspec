"""Type definitions for Spanner adapter.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.typing import import_optional_attr


class _UnavailableSpannerGoogleAPICallError(Exception):
    """Fallback Spanner API exception when google-api-core is unavailable."""


class _UnavailableSpannerTransaction:
    """Fallback Spanner transaction class when google-cloud-spanner is unavailable."""


class _UnavailableSpannerAsyncTransaction:
    """Fallback Spanner async transaction class when google-cloud-spanner is unavailable."""


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType
    from typing import TypeAlias

    from google.api_core import exceptions as spanner_exceptions
    from google.api_core.client_info import ClientInfo as SpannerClientInfo
    from google.api_core.client_options import ClientOptions as SpannerClientOptions
    from google.api_core.exceptions import GoogleAPICallError as _SpannerGoogleAPICallError
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
    from google.cloud.spanner_v1._async.transaction import Transaction as _SpannerAsyncTransaction
    from google.cloud.spanner_v1.data_types import Interval as SpannerInterval
    from google.cloud.spanner_v1.data_types import JsonObject as SpannerJsonObject
    from google.cloud.spanner_v1.database import Database as SpannerDatabase
    from google.cloud.spanner_v1.database import SnapshotCheckout
    from google.cloud.spanner_v1.database_sessions_manager import TransactionType as SpannerSyncTransactionType
    from google.cloud.spanner_v1.pool import AbstractSessionPool as SpannerAbstractSessionPool
    from google.cloud.spanner_v1.pool import BurstyPool as SpannerBurstyPool
    from google.cloud.spanner_v1.pool import FixedSizePool as SpannerFixedSizePool
    from google.cloud.spanner_v1.pool import PingingPool as SpannerPingingPool
    from google.cloud.spanner_v1.pool import TransactionPingingPool as SpannerTransactionPingingPool
    from google.cloud.spanner_v1.snapshot import Snapshot
    from google.cloud.spanner_v1.transaction import DefaultTransactionOptions as SpannerDefaultTransactionOptions
    from google.cloud.spanner_v1.transaction import Transaction as _SpannerTransaction
    from google.cloud.spanner_v1.types.type import TypeCode as SpannerTypeCode

    from sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerSyncDriver
    from sqlspec.core import StatementConfig

    SpannerSyncConnection: TypeAlias = Snapshot | SnapshotCheckout | _SpannerTransaction
    SpannerAsyncConnection: TypeAlias = (
        SpannerAsyncSnapshot | SpannerAsyncSnapshotCheckout | _SpannerAsyncTransaction | SpannerAsyncBatch
    )
    SpannerGoogleAPICallError: TypeAlias = _SpannerGoogleAPICallError
    SpannerTransaction: TypeAlias = _SpannerTransaction
    SpannerAsyncTransaction: TypeAlias = _SpannerAsyncTransaction


if not TYPE_CHECKING:
    SpannerSyncConnection = Any
    SpannerAsyncConnection = Any
    SpannerGoogleAPICallError = (
        import_optional_attr("google.api_core.exceptions", "GoogleAPICallError")
        or _UnavailableSpannerGoogleAPICallError
    )
    SpannerTransaction = (
        import_optional_attr("google.cloud.spanner_v1.transaction", "Transaction") or _UnavailableSpannerTransaction
    )
    SpannerAsyncTransaction = (
        import_optional_attr("google.cloud.spanner_v1._async.transaction", "Transaction")
        or _UnavailableSpannerAsyncTransaction
    )


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
    "SpannerInterval",
    "SpannerJsonObject",
    "SpannerNotFound",
    "SpannerPingingPool",
    "SpannerRequestOptions",
    "SpannerRetry",
    "SpannerSyncConnection",
    "SpannerSyncCursor",
    "SpannerSyncSessionContext",
    "SpannerSyncTransactionType",
    "SpannerTransaction",
    "SpannerTransactionPingingPool",
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
        from sqlspec.adapters.spanner.driver import SpannerSyncDriver

        self._connection = self._acquire_connection()
        self._driver = SpannerSyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
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
        from sqlspec.adapters.spanner.driver import SpannerAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = SpannerAsyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None


_LAZY_DRIVER_EXPORTS: dict[str, tuple[str, str]] = {
    "SpannerAsyncTransactionType": ("google.cloud.spanner_v1._async.database_sessions_manager", "TransactionType"),
    "SpannerInterval": ("google.cloud.spanner_v1.data_types", "Interval"),
    "SpannerAsyncAbstractSessionPool": ("google.cloud.spanner_v1._async.pool", "AbstractSessionPool"),
    "SpannerAsyncBatch": ("google.cloud.spanner_v1._async.batch", "Batch"),
    "SpannerAsyncBurstyPool": ("google.cloud.spanner_v1._async.pool", "BurstyPool"),
    "SpannerAsyncClient": ("google.cloud.spanner_v1._async.client", "Client"),
    "SpannerAsyncDatabase": ("google.cloud.spanner_v1._async.database", "Database"),
    "SpannerAsyncFixedSizePool": ("google.cloud.spanner_v1._async.pool", "FixedSizePool"),
    "SpannerAsyncMutationGroups": ("google.cloud.spanner_v1._async.batch", "MutationGroups"),
    "SpannerAsyncPingingPool": ("google.cloud.spanner_v1._async.pool", "PingingPool"),
    "SpannerAsyncSession": ("google.cloud.spanner_v1._async.session", "Session"),
    "SpannerAsyncSnapshot": ("google.cloud.spanner_v1._async.snapshot", "Snapshot"),
    "SpannerAsyncStreamedResultSet": ("google.cloud.spanner_v1._async.streamed", "StreamedResultSet"),
    "SpannerAsyncTransactionPingingPool": ("google.cloud.spanner_v1._async.pool", "TransactionPingingPool"),
    "SpannerBurstyPool": ("google.cloud.spanner_v1.pool", "BurstyPool"),
    "SpannerFixedSizePool": ("google.cloud.spanner_v1.pool", "FixedSizePool"),
    "SpannerPingingPool": ("google.cloud.spanner_v1.pool", "PingingPool"),
    "SpannerTransactionPingingPool": ("google.cloud.spanner_v1.pool", "TransactionPingingPool"),
    "spanner_exceptions": ("google.api_core", "exceptions"),
    "SpannerNotFound": ("google.api_core.exceptions", "NotFound"),
    "SpannerClient": ("google.cloud.spanner_v1", "Client"),
    "spanner_param_types": ("google.cloud.spanner_v1", "param_types"),
    "SpannerJsonObject": ("google.cloud.spanner_v1.data_types", "JsonObject"),
    "SpannerSyncTransactionType": ("google.cloud.spanner_v1.database_sessions_manager", "TransactionType"),
    "SpannerTypeCode": ("google.cloud.spanner_v1.types.type", "TypeCode"),
    "SpannerClientInfo": ("google.api_core.client_info", "ClientInfo"),
    "SpannerClientOptions": ("google.api_core.client_options", "ClientOptions"),
    "SpannerRetry": ("google.api_core.retry", "Retry"),
    "SpannerCredentials": ("google.auth.credentials", "Credentials"),
    "SpannerDatabaseDialect": ("google.cloud.spanner_admin_database_v1.types", "DatabaseDialect"),
    "SpannerEncryptionConfig": ("google.cloud.spanner_admin_database_v1.types", "EncryptionConfig"),
    "SpannerDirectedReadOptions": ("google.cloud.spanner_v1", "DirectedReadOptions"),
    "SpannerExecuteSqlRequest": ("google.cloud.spanner_v1", "ExecuteSqlRequest"),
    "SpannerRequestOptions": ("google.cloud.spanner_v1", "RequestOptions"),
    "SpannerDatabase": ("google.cloud.spanner_v1.database", "Database"),
    "SpannerAbstractSessionPool": ("google.cloud.spanner_v1.pool", "AbstractSessionPool"),
    "SpannerDefaultTransactionOptions": ("google.cloud.spanner_v1.transaction", "DefaultTransactionOptions"),
}


def __getattr__(name: str) -> Any:
    """Resolve optional driver symbols only when a consumer requests them."""
    target = _LAZY_DRIVER_EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute, None)
    if value is None:
        msg = f"Cannot import {attribute!r} from {module_name!r}"
        raise ImportError(msg)
    return value
