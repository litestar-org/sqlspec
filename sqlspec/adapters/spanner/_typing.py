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

    from google.api_core.exceptions import GoogleAPICallError as _SpannerGoogleAPICallError
    from google.cloud.spanner_v1.database import SnapshotCheckout
    from google.cloud.spanner_v1.snapshot import Snapshot
    from google.cloud.spanner_v1.transaction import Transaction as _SpannerTransaction

    from sqlspec.adapters.spanner.driver import SpannerSyncDriver
    from sqlspec.core import StatementConfig

    SpannerConnection = Snapshot | SnapshotCheckout | _SpannerTransaction
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
    "SpannerConnection",
    "SpannerGoogleAPICallError",
    "SpannerSessionContext",
    "SpannerSyncCursor",
    "SpannerTransaction",
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
