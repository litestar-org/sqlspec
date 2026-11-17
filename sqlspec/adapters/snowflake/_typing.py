"""Snowflake cursor and session context types."""

import contextlib
from typing import TYPE_CHECKING, Any, TypeAlias

from mypy_extensions import mypyc_attr

from sqlspec.driver._session import SyncDriverSessionContext

if TYPE_CHECKING:
    from snowflake.connector.connection import SnowflakeConnection as _SnowflakeConnection
    from snowflake.connector.cursor import SnowflakeCursor as _SnowflakeRawCursor

    from sqlspec.adapters.snowflake.driver import SnowflakeDriver

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


@mypyc_attr(allow_interpreted_subclasses=True)
class SnowflakeSessionContext(SyncDriverSessionContext):
    """Acquire, prepare, and release a synchronous Snowflake driver session."""

    __slots__ = ()

    def _resolve_driver_class(self) -> "type[SnowflakeDriver]":
        from sqlspec.adapters.snowflake.driver import SnowflakeDriver

        return SnowflakeDriver

    def _cleanup_driver(self, driver: Any, *, error: bool) -> None:
        driver._close_open_streams(error=error)
        driver.close()
