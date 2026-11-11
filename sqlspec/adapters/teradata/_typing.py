"""Teradata Vantage adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import importlib
from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.teradata.driver import TeradataDriver
    from sqlspec.core import StatementConfig

    TeradataConnection: TypeAlias = Any
    TeradataRawCursor: TypeAlias = Any
    TeradataQueryParams: TypeAlias = Any
else:
    TeradataConnection = object
    TeradataRawCursor = object
    TeradataQueryParams = Any

__all__ = ("TeradataConnection", "TeradataCursor", "TeradataQueryParams", "TeradataRawCursor", "TeradataSessionContext")


class TeradataCursor:
    """Context manager for Teradata teradatasql cursor operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "TeradataConnection") -> None:
        self.connection = connection
        self.cursor: TeradataRawCursor | None = None

    def __enter__(self) -> "TeradataRawCursor":
        self.cursor = self.connection.cursor()
        assert self.cursor is not None
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None and hasattr(self.cursor, "close"):
            with contextlib.suppress(Exception):
                self.cursor.close()


_DRIVER_CLS: "type[TeradataDriver] | None" = None


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class TeradataSessionContext:
    """Sync context manager for Teradata Vantage sessions."""

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
        prepare_driver: "Callable[[TeradataDriver], TeradataDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: TeradataDriver | None = None

    def __enter__(self) -> "TeradataDriver":
        global _DRIVER_CLS
        if _DRIVER_CLS is None:
            module = importlib.import_module("sqlspec.adapters.teradata.driver")
            _DRIVER_CLS = module.TeradataDriver
        assert _DRIVER_CLS is not None
        self._connection = self._acquire_connection()
        try:
            self._driver = _DRIVER_CLS(
                connection=self._connection,
                statement_config=self._statement_config,
                driver_features=self._driver_features,
            )
            return self._prepare_driver(self._driver)
        except BaseException as exc:
            self._release_connection(self._connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__)
            self._connection = None
            self._driver = None
            raise

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        try:
            if self._driver is not None:
                self._driver._release_open_work()
        except BaseException as exc:
            if self._connection is not None:
                self._release_connection(self._connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__)
            raise
        else:
            if self._connection is not None:
                self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
        finally:
            self._connection = None
            self._driver = None
        return None
