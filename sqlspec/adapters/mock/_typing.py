"""Mock adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import sqlite3
from typing import TYPE_CHECKING, Any

_MockConnection = sqlite3.Connection

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import TypeAlias

    from sqlspec.adapters.mock.driver import MockAsyncDriver, MockSyncDriver
    from sqlspec.core import StatementConfig

    MockConnection: TypeAlias = _MockConnection

if not TYPE_CHECKING:
    MockConnection = _MockConnection


class MockSyncSessionContext:
    """Sync context manager for Mock sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Uses callable-based connection management to decouple from config implementation.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
        "_target_dialect",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], MockConnection]",
        release_connection: "Callable[[MockConnection], None]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MockSyncDriver], MockSyncDriver]",
        target_dialect: str = "sqlite",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._target_dialect = target_dialect
        self._connection: MockConnection | None = None
        self._driver: MockSyncDriver | None = None

    def __enter__(self) -> "MockSyncDriver":
        from sqlspec.adapters.mock.driver import MockSyncDriver

        self._connection = self._acquire_connection()
        self._driver = MockSyncDriver(
            connection=self._connection,
            statement_config=self._statement_config,
            driver_features=self._driver_features,
            target_dialect=self._target_dialect,
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


class MockAsyncSessionContext:
    """Async context manager for Mock sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Uses callable-based connection management to decouple from config implementation.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
        "_target_dialect",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Awaitable[MockConnection]]",
        release_connection: "Callable[[MockConnection], Awaitable[None]]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MockAsyncDriver], MockAsyncDriver]",
        target_dialect: str = "sqlite",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._target_dialect = target_dialect
        self._connection: MockConnection | None = None
        self._driver: MockAsyncDriver | None = None

    async def __aenter__(self) -> "MockAsyncDriver":
        from sqlspec.adapters.mock.driver import MockAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = MockAsyncDriver(
            connection=self._connection,
            statement_config=self._statement_config,
            driver_features=self._driver_features,
            target_dialect=self._target_dialect,
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = ("MockAsyncSessionContext", "MockConnection", "MockSyncSessionContext")
