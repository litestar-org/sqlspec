"""mssql-python adapter type definitions and mypyc-excluded context managers."""

import contextlib
from typing import TYPE_CHECKING, Any

import mssql_python as mssql_python_module
from mssql_python import Error as MssqlPythonError
from mssql_python.connection import Connection, TokenProvider
from mssql_python.cursor import Cursor

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
    from sqlspec.core import StatementConfig

    MssqlPythonConnection: TypeAlias = Connection
    MssqlPythonRawCursor: TypeAlias = Cursor

if not TYPE_CHECKING:
    MssqlPythonConnection = Connection
    MssqlPythonRawCursor = Cursor

__all__ = (
    "MssqlPythonConnection",
    "MssqlPythonCursor",
    "MssqlPythonError",
    "MssqlPythonRawCursor",
    "MssqlPythonSessionContext",
    "TokenProvider",
    "mssql_python_module",
)


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
            with contextlib.suppress(Exception):
                self.cursor.close()


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
        acquire_connection: "Callable[[], MssqlPythonConnection]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[MssqlPythonDriver], MssqlPythonDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: MssqlPythonConnection | None = None
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
        if exc_type is not None and self._driver is not None:
            with contextlib.suppress(Exception):
                self._driver.rollback()
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None
