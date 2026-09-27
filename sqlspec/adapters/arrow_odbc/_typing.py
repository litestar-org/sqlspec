"""arrow-odbc adapter type definitions and mypyc-excluded context managers."""

from typing import TYPE_CHECKING, Any

from arrow_odbc import Connection as ArrowOdbcConnection
from arrow_odbc import Connection as ArrowOdbcRawCursor
from arrow_odbc import Error as ArrowOdbcError
from arrow_odbc import TextEncoding, enable_odbc_connection_pooling
from arrow_odbc import connect as arrow_odbc_connect

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver
    from sqlspec.core import StatementConfig

__all__ = (
    "ArrowOdbcConnection",
    "ArrowOdbcCursor",
    "ArrowOdbcError",
    "ArrowOdbcRawCursor",
    "ArrowOdbcSessionContext",
    "TextEncoding",
    "arrow_odbc_connect",
    "enable_odbc_connection_pooling",
)


class ArrowOdbcCursor:
    """Context manager yielding the arrow-odbc connection as its execution cursor."""

    __slots__ = ("connection",)

    def __init__(self, connection: "ArrowOdbcConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "ArrowOdbcRawCursor":
        return self.connection

    def __exit__(self, *_: object) -> None:
        return None


class ArrowOdbcSessionContext:
    """Sync session context bridging interpreted config and compiled driver code."""

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
        prepare_driver: "Callable[[ArrowOdbcDriver], ArrowOdbcDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: ArrowOdbcDriver | None = None

    def __enter__(self) -> "ArrowOdbcDriver":
        from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver

        self._connection = self._acquire_connection()
        self._driver = ArrowOdbcDriver(
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
