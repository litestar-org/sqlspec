"""mssql-python adapter type definitions and mypyc-excluded context managers."""

import asyncio
from typing import TYPE_CHECKING, Any

import mssql_python as _mssql_python  # pyright: ignore[reportMissingImports]
from mssql_python.connection import Connection  # pyright: ignore
from mssql_python.cursor import Cursor  # pyright: ignore

MSSQL_PYTHON_MODULE: Any = _mssql_python

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.mssql_python.driver import MssqlPythonAsyncDriver, MssqlPythonDriver
    from sqlspec.core import StatementConfig

    MssqlPythonConnection: TypeAlias = Connection
    MssqlPythonRawCursor: TypeAlias = Cursor

if not TYPE_CHECKING:
    MssqlPythonConnection = Connection
    MssqlPythonRawCursor = Cursor


class MssqlPythonCursor:
    """Sync context manager for mssql-python cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "MssqlPythonConnection") -> None:
        self.connection = connection
        self.cursor: MssqlPythonRawCursor | None = None

    def __enter__(self) -> "MssqlPythonRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
            self.cursor.close()


class MssqlPythonAsyncCursor:
    """Async context manager for mssql-python cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "MssqlPythonConnection") -> None:
        self.connection = connection
        self.cursor: MssqlPythonRawCursor | None = None

    async def __aenter__(self) -> "MssqlPythonRawCursor":
        self.cursor = await asyncio.to_thread(self.connection.cursor)
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            await asyncio.to_thread(self.cursor.close)


class MssqlPythonSessionContext:
    """Sync session context bridging compiled driver and interpreted config."""

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
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MssqlPythonDriver], MssqlPythonDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: MssqlPythonDriver | None = None

    def __enter__(self) -> "MssqlPythonDriver":
        from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver

        self._connection = self._acquire_connection()
        self._driver = MssqlPythonDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


class MssqlPythonAsyncSessionContext:
    """Async session context bridging compiled driver and interpreted config."""

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
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MssqlPythonAsyncDriver], MssqlPythonAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: MssqlPythonAsyncDriver | None = None

    async def __aenter__(self) -> "MssqlPythonAsyncDriver":
        from sqlspec.adapters.mssql_python.driver import MssqlPythonAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = MssqlPythonAsyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "MSSQL_PYTHON_MODULE",
    "MssqlPythonAsyncCursor",
    "MssqlPythonAsyncSessionContext",
    "MssqlPythonConnection",
    "MssqlPythonCursor",
    "MssqlPythonRawCursor",
    "MssqlPythonSessionContext",
)
