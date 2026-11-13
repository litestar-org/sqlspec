"""arrow-odbc adapter type definitions and mypyc-excluded context managers."""

from typing import TYPE_CHECKING, Any

import arrow_odbc as _arrow_odbc
from arrow_odbc import Error as ArrowOdbcError
from arrow_odbc import TextEncoding, enable_odbc_connection_pooling
from arrow_odbc import connect as arrow_odbc_connect

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver
    from sqlspec.core import StatementConfig

    ArrowOdbcConnection: TypeAlias = _arrow_odbc.Connection
    ArrowOdbcRawCursor: TypeAlias = _arrow_odbc.Connection
else:
    ArrowOdbcConnection = _arrow_odbc.Connection
    ArrowOdbcRawCursor = _arrow_odbc.Connection

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
        driver = self._driver
        rollback_exc: BaseException | None = None
        driver_rolled_back = False
        try:
            if self._driver is not None and self._driver._connection_in_transaction():
                driver_rolled_back = True
                try:
                    self._driver.rollback()
                except BaseException as exc:
                    rollback_exc = exc
                    if exc_type is None:
                        raise
        finally:
            try:
                self._driver = None
                if self._connection is not None:
                    connection, self._connection = self._connection, None
                    release_type = exc_type if exc_type is not None else (type(rollback_exc) if rollback_exc else None)
                    release_val = exc_val if exc_val is not None else rollback_exc
                    release_tb = (
                        exc_tb if exc_tb is not None else (rollback_exc.__traceback__ if rollback_exc else None)
                    )
                    self._release_connection(
                        connection,
                        exc_type=release_type,
                        exc_val=release_val,
                        exc_tb=release_tb,
                        already_rolled_back=driver_rolled_back,
                    )
            finally:
                self._driver = None
                if driver is not None:
                    driver._detach_connection()
        return None
