"""SAP SQL Anywhere adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr

from sqlspec.utils.module_loader import import_optional_attr

SQLANYDB_MODULE: Any = None
_DRIVER_CLS: Any = None


class SqlanywhereError(Exception):
    """SAP SQL Anywhere driver exception base."""


if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.sqlanywhere.driver import SqlanywhereDriver
    from sqlspec.core import StatementConfig

    SqlanywhereConnection: TypeAlias = Any
    SqlanywhereRawCursor: TypeAlias = Any
    SqlanywhereQueryParams: TypeAlias = Any

if not TYPE_CHECKING:
    SqlanywhereConnection = Any
    SqlanywhereRawCursor = Any
    SqlanywhereQueryParams = Any

__all__ = (
    "SQLANYDB_MODULE",
    "SqlanywhereConnection",
    "SqlanywhereCursor",
    "SqlanywhereError",
    "SqlanywhereQueryParams",
    "SqlanywhereRawCursor",
    "SqlanywhereSessionContext",
)


class SqlanywhereCursor:
    """Context manager for SQL Anywhere sqlanydb cursor operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "SqlanywhereConnection") -> None:
        self.connection = connection
        self.cursor: SqlanywhereRawCursor | None = None

    def __enter__(self) -> "SqlanywhereRawCursor":
        self.cursor = self.connection.cursor()
        assert self.cursor is not None
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None and hasattr(self.cursor, "close"):
            with contextlib.suppress(Exception):
                self.cursor.close()


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SqlanywhereSessionContext:
    """Sync context manager for SAP SQL Anywhere sessions."""

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
        prepare_driver: "Callable[[SqlanywhereDriver], SqlanywhereDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: SqlanywhereDriver | None = None

    def __enter__(self) -> "SqlanywhereDriver":
        global _DRIVER_CLS
        if _DRIVER_CLS is None:
            _DRIVER_CLS = import_optional_attr("sqlspec.adapters.sqlanywhere.driver", "SqlanywhereDriver")
        self._connection = self._acquire_connection()
        self._driver = _DRIVER_CLS(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        assert self._driver is not None
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is None:
            return None
        try:
            if self._driver is not None:
                self._driver._release_open_work()
        finally:
            self._release_connection(self._connection)
            self._connection = None
            self._driver = None
        return None
