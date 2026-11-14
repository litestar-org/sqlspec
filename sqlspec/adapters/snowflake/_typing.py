"""Snowflake cursor and session context types."""

import contextlib
from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any, TypeAlias

from mypy_extensions import mypyc_attr

if TYPE_CHECKING:
    from snowflake.connector.connection import SnowflakeConnection as _SnowflakeConnection
    from snowflake.connector.cursor import SnowflakeCursor as _SnowflakeRawCursor

    from sqlspec.adapters.snowflake.driver import SnowflakeDriver
    from sqlspec.core import StatementConfig

    SnowflakeConnection: TypeAlias = _SnowflakeConnection
    SnowflakeRawCursor: TypeAlias = _SnowflakeRawCursor
else:
    SnowflakeConnection = Any
    SnowflakeRawCursor = Any

__all__ = ("SnowflakeConnection", "SnowflakeCursor", "SnowflakeRawCursor", "SnowflakeSessionContext")


class SnowflakeCursor:
    """Context manager closing a Snowflake cursor after use."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "SnowflakeConnection") -> None:
        self.connection = connection
        self.cursor: SnowflakeRawCursor | None = None

    def __enter__(self) -> "SnowflakeRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        cursor, self.cursor = self.cursor, None
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SnowflakeSessionContext:
    """Acquire, prepare, and release a synchronous Snowflake driver session."""

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
        acquire_connection: "Callable[[], SnowflakeConnection]",
        release_connection: Callable[..., Any],
        statement_config: "StatementConfig",
        driver_features: dict[str, Any],
        prepare_driver: "Callable[[SnowflakeDriver], SnowflakeDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: SnowflakeConnection | None = None
        self._driver: SnowflakeDriver | None = None

    def __enter__(self) -> "SnowflakeDriver":
        from sqlspec.adapters.snowflake.driver import SnowflakeDriver

        connection = self._acquire_connection()
        self._connection = connection
        try:
            self._driver = SnowflakeDriver(
                connection=connection, statement_config=self._statement_config, driver_features=self._driver_features
            )
            return self._prepare_driver(self._driver)
        except BaseException as exc:
            driver = self._driver
            try:
                if driver is not None:
                    with contextlib.suppress(Exception):
                        driver.close()
            finally:
                self._driver = None
                self._connection = None
                try:
                    self._release_connection(connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__)
                finally:
                    if driver is not None:
                        driver._detach_connection()
            raise

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        driver, self._driver = self._driver, None
        connection, self._connection = self._connection, None
        try:
            if driver is not None:
                driver.close()
        except BaseException as exc:
            if connection is not None:
                self._release_connection(connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__)
            raise
        else:
            if connection is not None:
                self._release_connection(connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
        finally:
            if driver is not None:
                driver._detach_connection()
