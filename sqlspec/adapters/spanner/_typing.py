"""Type definitions for Spanner adapter.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from sqlspec.typing import import_optional_attr


class _UnavailableSpannerGoogleAPICallError(Exception):
    """Fallback Spanner API exception when google-api-core is unavailable."""


class _UnavailableSpannerTransaction:
    """Fallback Spanner transaction class when google-cloud-spanner is unavailable."""


if TYPE_CHECKING:
    from collections.abc import Callable
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
    from google.cloud.spanner_v1.data_types import JsonObject as SpannerJsonObject
    from google.cloud.spanner_v1.database import Database as SpannerDatabase
    from google.cloud.spanner_v1.database import SnapshotCheckout
    from google.cloud.spanner_v1.database_sessions_manager import TransactionType as SpannerTransactionType
    from google.cloud.spanner_v1.pool import AbstractSessionPool as SpannerAbstractSessionPool
    from google.cloud.spanner_v1.pool import BurstyPool as SpannerBurstyPool
    from google.cloud.spanner_v1.pool import FixedSizePool as SpannerFixedSizePool
    from google.cloud.spanner_v1.pool import PingingPool as SpannerPingingPool
    from google.cloud.spanner_v1.snapshot import Snapshot
    from google.cloud.spanner_v1.transaction import DefaultTransactionOptions as SpannerDefaultTransactionOptions
    from google.cloud.spanner_v1.transaction import Transaction as _SpannerTransaction
    from google.cloud.spanner_v1.types.type import TypeCode as SpannerTypeCode

    from sqlspec.adapters.spanner.driver import SpannerSyncDriver
    from sqlspec.core import StatementConfig

    SpannerConnection: TypeAlias = Snapshot | SnapshotCheckout | _SpannerTransaction
    SpannerGoogleAPICallError: TypeAlias = _SpannerGoogleAPICallError
    SpannerTransaction: TypeAlias = _SpannerTransaction


if not TYPE_CHECKING:
    SpannerConnection = Any
    SpannerGoogleAPICallError = (
        import_optional_attr("google.api_core.exceptions", "GoogleAPICallError")
        or _UnavailableSpannerGoogleAPICallError
    )
    SpannerTransaction = (
        import_optional_attr("google.cloud.spanner_v1.transaction", "Transaction") or _UnavailableSpannerTransaction
    )


__all__ = (
    "SpannerAbstractSessionPool",
    "SpannerBurstyPool",
    "SpannerClient",
    "SpannerClientInfo",
    "SpannerClientOptions",
    "SpannerConnection",
    "SpannerCredentials",
    "SpannerDatabase",
    "SpannerDatabaseDialect",
    "SpannerDefaultTransactionOptions",
    "SpannerDirectedReadOptions",
    "SpannerEncryptionConfig",
    "SpannerExecuteSqlRequest",
    "SpannerFixedSizePool",
    "SpannerGoogleAPICallError",
    "SpannerJsonObject",
    "SpannerNotFound",
    "SpannerPingingPool",
    "SpannerRequestOptions",
    "SpannerRetry",
    "SpannerSessionContext",
    "SpannerSyncCursor",
    "SpannerTransaction",
    "SpannerTransactionType",
    "SpannerTypeCode",
    "spanner_exceptions",
    "spanner_param_types",
)


class SpannerSyncCursor:
    """Context manager that yields the active Spanner connection."""

    __slots__ = ("connection",)

    def __init__(self, connection: "SpannerConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "SpannerConnection":
        return self.connection

    def __exit__(self, *_: Any) -> None:
        return None


class SpannerSessionContext:
    """Sync context manager for Spanner sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Note: This context manager receives a pre-configured connection context
    that already has the transaction flag set. The config.provide_session()
    creates the connection context with the appropriate transaction setting.

    Uses callable-based connection management to decouple from config implementation.

    Spanner requires exception info in release_connection for commit/rollback decisions.
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


_LAZY_DRIVER_EXPORTS: dict[str, tuple[str, str]] = {
    "SpannerBurstyPool": ("google.cloud.spanner_v1.pool", "BurstyPool"),
    "SpannerFixedSizePool": ("google.cloud.spanner_v1.pool", "FixedSizePool"),
    "SpannerPingingPool": ("google.cloud.spanner_v1.pool", "PingingPool"),
    "spanner_exceptions": ("google.api_core", "exceptions"),
    "SpannerNotFound": ("google.api_core.exceptions", "NotFound"),
    "SpannerClient": ("google.cloud.spanner_v1", "Client"),
    "spanner_param_types": ("google.cloud.spanner_v1", "param_types"),
    "SpannerJsonObject": ("google.cloud.spanner_v1.data_types", "JsonObject"),
    "SpannerTransactionType": ("google.cloud.spanner_v1.database_sessions_manager", "TransactionType"),
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
    value = import_optional_attr(module_name, attribute)
    if value is None:
        msg = f"Cannot import {attribute!r} from {module_name!r}"
        raise ImportError(msg)
    return value
