"""AsyncMy adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import os
from typing import TYPE_CHECKING, Any

import asyncmy as _asyncmy
from asyncmy import Connection
from asyncmy import errors as _asyncmy_errors
from asyncmy.connection import LoadLocalFile
from asyncmy.connection import MySQLResult as _AsyncmyResult
from asyncmy.constants import FIELD_TYPE as _ASYNCMY_FIELD_TYPE
from asyncmy.cursors import RE_INSERT_VALUES as ASYNCMY_INSERT_VALUES_PATTERN
from asyncmy.cursors import Cursor as _AsyncmyCursor
from asyncmy.cursors import DictCursor as _AsyncmyDictCursor
from asyncmy.cursors import SSCursor as AsyncmySSCursor
from asyncmy.errors import ProgrammingError as _AsyncmyProgrammingError
from asyncmy.pool import Pool as _AsyncmyPool
from asyncmy.protocol import LoadLocalPacketWrapper as _LoadLocalPacketWrapper

from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Protocol, TypeAlias

    class AsyncmyMySQLError(Exception):
        errno: int

    class AsyncmyError(AsyncmyMySQLError):
        pass

    class AsyncmyProgrammingError(AsyncmyError):
        pass

    class AsyncmyConnectionProtocol(Protocol):
        server_status: int

        def cursor(self, cursor: "type[Any] | None" = None) -> "AsyncmyRawCursor": ...

        async def begin(self) -> Any: ...

        async def commit(self) -> object: ...

        async def rollback(self) -> object: ...

        def close(self) -> None: ...

    class AsyncmyModuleProtocol(Protocol):
        async def connect(self, *args: Any, **kwargs: Any) -> "AsyncmyConnection": ...

        async def create_pool(self, **kwargs: Any) -> "AsyncmyPool": ...

    class AsyncmyFieldTypeProtocol(Protocol):
        JSON: int

    AsyncmyConnection: TypeAlias = AsyncmyConnectionProtocol
    AsyncmyDictCursor: TypeAlias = _AsyncmyDictCursor
    AsyncmyFieldType: TypeAlias = AsyncmyFieldTypeProtocol
    AsyncmyModule: TypeAlias = AsyncmyModuleProtocol
    AsyncmyPool: TypeAlias = _AsyncmyPool
    AsyncmyRawCursor: TypeAlias = _AsyncmyCursor
else:
    AsyncmyConnection = Connection
    AsyncmyDictCursor = _AsyncmyDictCursor
    AsyncmyError = _asyncmy_errors.Error
    AsyncmyFieldType = _ASYNCMY_FIELD_TYPE
    AsyncmyMySQLError = _asyncmy_errors.MySQLError
    AsyncmyProgrammingError = _AsyncmyProgrammingError
    AsyncmyModule = _asyncmy
    AsyncmyPool = _AsyncmyPool
    AsyncmyRawCursor = _AsyncmyCursor

__all__ = (
    "ASYNCMY_INSERT_VALUES_PATTERN",
    "AsyncmyConnection",
    "AsyncmyCursor",
    "AsyncmyDictCursor",
    "AsyncmyError",
    "AsyncmyFieldType",
    "AsyncmyModule",
    "AsyncmyMySQLError",
    "AsyncmyPool",
    "AsyncmyProgrammingError",
    "AsyncmyRawCursor",
    "AsyncmySSCursor",
    "asyncmy_local_infile",
)


class AsyncmyCursor:
    """Context manager for AsyncMy cursor operations.

    Provides automatic cursor acquisition and cleanup for database operations.
    """

    __slots__ = ("connection", "cursor", "cursor_class")

    def __init__(self, connection: "AsyncmyConnection", cursor_class: "type[AsyncmyRawCursor] | None" = None) -> None:
        self.connection = connection
        self.cursor_class = cursor_class
        self.cursor: AsyncmyRawCursor | None = None

    async def __aenter__(self) -> "AsyncmyRawCursor":
        if self.cursor_class is None:
            self.cursor = self.connection.cursor()
        else:
            try:
                self.cursor = self.connection.cursor(self.cursor_class)
            except TypeError:
                self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                raw_cursor: Any = self.cursor
                if hasattr(raw_cursor, "close"):
                    await raw_cursor.close()
                elif hasattr(raw_cursor, "__aexit__"):
                    await raw_cursor.__aexit__(None, None, None)


@contextlib.contextmanager
def asyncmy_local_infile(connection: "AsyncmyConnection", filename: str) -> "Iterator[None]":
    """Scope the asyncmy 0.2.13/0.2.14 filename handoff fix to one native load.

    Args:
        connection: Physical connection exclusively held by this operation.
        filename: Owned payload path expected in the server's file request.

    Yields:
        Control while native result reading uses the filename adapter.
    """
    raw: Any = connection
    missing = object()
    previous = raw.__dict__.get("_read_query_result", missing)

    async def read_result(unbuffered: bool = False) -> None:
        raw._result = None
        result: Any = _AsyncmyLocalInfileResult(raw, filename)
        if unbuffered:
            try:
                await result.init_unbuffered_query()
            except BaseException:
                result.unbuffered_active = False
                result.connection = None
                raise
        else:
            await result.read()
        raw._result = result
        raw._affected_rows = result.affected_rows
        if result.server_status:
            raw.server_status = result.server_status

    raw._read_query_result = read_result
    try:
        yield
    except BaseException:
        with contextlib.suppress(Exception):
            raw.close()
        raw._connected = False
        raise
    finally:
        if previous is missing:
            del raw._read_query_result
        else:
            raw._read_query_result = previous


class _AsyncmyLocalInfileResult(_AsyncmyResult):
    """Normalize the upstream filename handoff while retaining its native sender."""

    __slots__ = ("_filename",)

    def __init__(self, connection: Any, filename: str) -> None:
        super().__init__(connection)
        self._filename = filename

    async def _read_load_local_packet(self, first_packet: Any) -> None:
        request = _LoadLocalPacketWrapper(first_packet).filename
        if not self.connection._local_infile or os.fsdecode(request) != self._filename:
            msg = "MySQL requested an unexpected LOCAL INFILE payload."
            raise SQLSpecError(msg)
        sender: Any = LoadLocalFile(self._filename, self.connection)
        await sender.send_data()
        packet = await self.connection.read_packet()
        if not packet.is_ok_packet():
            msg = "MySQL did not acknowledge the LOCAL INFILE payload."
            raise SQLSpecError(msg)
        raw_self: Any = self
        raw_self._read_ok_packet(packet)
