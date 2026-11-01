"""Sybase ASE adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr

from sqlspec.utils.module_loader import import_optional_attr

PYMSSQL_MODULE: Any = None
_DRIVER_CLS: Any = None


class _MissingSybaseDriverError(Exception):
    """Placeholder base used only when pymssql is not installed."""


_NATIVE_ERROR: Any = import_optional_attr("pymssql", "Error") or _MissingSybaseDriverError
SybaseError: Any = _NATIVE_ERROR


if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from pymssql import Connection as _SybaseConnection
    from pymssql import Cursor as _SybaseRawCursor
    from pymssql._pymssql import QueryParams as SybaseQueryParams

    from sqlspec.adapters.sybase.driver import SybaseDriver
    from sqlspec.core import StatementConfig

    SybaseConnection: TypeAlias = _SybaseConnection
    SybaseRawCursor: TypeAlias = _SybaseRawCursor

if not TYPE_CHECKING:
    SybaseQueryParams = Any
    SybaseConnection = Any
    SybaseRawCursor = Any

__all__ = (
    "PYMSSQL_MODULE",
    "SybaseConnection",
    "SybaseCursor",
    "SybaseError",
    "SybaseQueryParams",
    "SybaseRawCursor",
    "SybaseSessionContext",
)


class SybaseCursor:
    """Context manager for Sybase pymssql cursor operations."""

    __slots__ = ("_as_dict", "connection", "cursor")

    def __init__(self, connection: "SybaseConnection", *, as_dict: bool | None = None) -> None:
        self.connection = connection
        self.cursor: SybaseRawCursor | None = None
        self._as_dict = as_dict

    def __enter__(self) -> "SybaseRawCursor":
        if self._as_dict is None:
            self.cursor = self.connection.cursor()
        else:
            try:
                raw_cursor: Any = (
                    self.connection.cursor(as_dict=True) if self._as_dict else self.connection.cursor(as_dict=False)
                )
                self.cursor = raw_cursor
            except TypeError:
                self.cursor = self.connection.cursor()
        assert self.cursor is not None
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SybaseSessionContext:
    """Sync context manager for Sybase ASE sessions."""

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
        prepare_driver: "Callable[[SybaseDriver], SybaseDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: SybaseDriver | None = None

    def __enter__(self) -> "SybaseDriver":
        global _DRIVER_CLS
        if _DRIVER_CLS is None:
            _DRIVER_CLS = import_optional_attr("sqlspec.adapters.sybase.driver", "SybaseDriver")
        self._connection = self._acquire_connection()
        self._driver = _DRIVER_CLS(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        assert self._driver is not None
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._driver is not None and (exc_type is not None or self._driver._connection_in_transaction()):
            with contextlib.suppress(Exception):
                self._driver.rollback()
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
            self._driver = None
        return None
