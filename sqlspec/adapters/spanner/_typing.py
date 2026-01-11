"""Type definitions for Spanner adapter.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from google.cloud.spanner_v1.database import SnapshotCheckout
    from google.cloud.spanner_v1.snapshot import Snapshot
    from google.cloud.spanner_v1.transaction import Transaction

    from sqlspec.adapters.spanner.driver import SpannerSyncDriver
    from sqlspec.core import StatementConfig

    SpannerConnection = Snapshot | SnapshotCheckout | Transaction

if not TYPE_CHECKING:
    SpannerConnection = Any


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
        release_connection: "Callable[[Any, type[BaseException] | None, BaseException | None, Any], Any]",
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
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection, exc_type, exc_val, exc_tb)
            self._connection = None
        return None


__all__ = ("SpannerConnection", "SpannerSessionContext")
