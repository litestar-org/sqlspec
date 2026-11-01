"""ADBC adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any, TypeAlias

from adbc_driver_manager import AdbcStatusCode
from adbc_driver_manager import Error as _AdbcNativeError
from adbc_driver_manager import NotSupportedError as AdbcNotSupportedError
from adbc_driver_manager import OperationalError as AdbcOperationalError
from adbc_driver_manager.dbapi import Connection
from adbc_driver_manager.dbapi import Cursor as _AdbcRawCursor

from sqlspec.adapters.adbc.core import resolve_dialect_name
from sqlspec.typing import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from adbc_driver_flightsql import DatabaseOptions as AdbcFlightSqlDatabaseOptions

    from sqlspec.adapters.adbc.driver import AdbcDriver
    from sqlspec.core import StatementConfig

AdbcConnection: TypeAlias = Connection
AdbcRawCursor: TypeAlias = _AdbcRawCursor
AdbcNativeError: TypeAlias = _AdbcNativeError

__all__ = (
    "AdbcConnection",
    "AdbcCursor",
    "AdbcFlightSqlDatabaseOptions",
    "AdbcNativeError",
    "AdbcNotSupportedError",
    "AdbcOperationalError",
    "AdbcRawCursor",
    "AdbcSessionContext",
    "AdbcStatusCode",
)


class AdbcCursor:
    """Context manager for cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AdbcConnection") -> None:
        self.connection = connection
        self.cursor: AdbcRawCursor | None = None

    def __enter__(self) -> "AdbcRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class AdbcSessionContext:
    """Sync context manager for ADBC sessions.

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
        "_driver_class",
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
        prepare_driver: "Callable[[AdbcDriver], AdbcDriver]",
        driver_class: "type[AdbcDriver] | None" = None,
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_class = driver_class
        self._connection: Any = None
        self._driver: AdbcDriver | None = None

    def __enter__(self) -> "AdbcDriver":
        driver_class = self._driver_class
        if driver_class is None:
            from sqlspec.adapters.adbc.driver import AdbcDriver

            driver_class = AdbcDriver

        self._connection = self._acquire_connection()
        try:
            resolved_dialect = resolve_dialect_name(self._statement_config.dialect) or None
            self._driver = driver_class(
                connection=self._connection,
                statement_config=self._statement_config,
                driver_features=self._driver_features,
                dialect=resolved_dialect,
            )
            return self._prepare_driver(self._driver)
        except BaseException as exc:
            connection = self._connection
            self._connection = None
            self._driver = None
            with contextlib.suppress(BaseException):
                self._release_connection(connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__)
            raise

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            connection = self._connection
            self._connection = None
            self._driver = None
            self._release_connection(connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
        return None


def __getattr__(name: str) -> Any:
    """Load Flight SQL options only when that optional driver is used."""
    if name != "AdbcFlightSqlDatabaseOptions":
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    value = import_optional_attr("adbc_driver_flightsql", "DatabaseOptions")
    if value is None:
        msg = "Cannot import DatabaseOptions from adbc_driver_flightsql"
        raise ImportError(msg)
    return value
