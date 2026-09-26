"""AsyncMy adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import os
from typing import TYPE_CHECKING, Any, cast

import asyncmy
import asyncmy.constants
import asyncmy.cursors
import asyncmy.errors
import asyncmy.pool
from asyncmy.connection import LoadLocalFile, MySQLResult
from asyncmy.protocol import LoadLocalPacketWrapper

from sqlspec.exceptions import SQLSpecError

ASYNCMY_INSERT_VALUES_PATTERN = asyncmy.cursors.RE_INSERT_VALUES

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator
    from types import TracebackType
    from typing import Protocol, TypeAlias

    from sqlspec.adapters.asyncmy.driver import AsyncmyDriver
    from sqlspec.core import StatementConfig

    class AsyncmyConnectionProtocol(Protocol):
        def cursor(self) -> "AsyncmyRawCursor": ...

        async def commit(self) -> object: ...

        async def rollback(self) -> object: ...

        def close(self) -> None: ...

    class AsyncmyModuleProtocol(Protocol):
        async def connect(self, *args: Any, **kwargs: Any) -> "AsyncmyConnection": ...

        async def create_pool(self, **kwargs: Any) -> "AsyncmyPool": ...

    class AsyncmyFieldTypeProtocol(Protocol):
        JSON: int

    AsyncmyConnection: TypeAlias = AsyncmyConnectionProtocol
    AsyncmyDictCursor: TypeAlias = asyncmy.cursors.DictCursor
    AsyncmyError: TypeAlias = asyncmy.errors.Error
    AsyncmyFieldType: TypeAlias = AsyncmyFieldTypeProtocol
    AsyncmyMySQLError: TypeAlias = asyncmy.errors.MySQLError
    AsyncmyModule: TypeAlias = AsyncmyModuleProtocol
    AsyncmyPool: TypeAlias = asyncmy.pool.Pool
    AsyncmyProgrammingError: TypeAlias = asyncmy.errors.ProgrammingError
    AsyncmyRawCursor: TypeAlias = asyncmy.cursors.Cursor
    AsyncmySSCursor: TypeAlias = asyncmy.cursors.SSCursor

if not TYPE_CHECKING:
    AsyncmyConnection = asyncmy.Connection
    AsyncmyDictCursor = asyncmy.cursors.DictCursor
    AsyncmyError = asyncmy.errors.Error
    AsyncmyFieldType = asyncmy.constants.FIELD_TYPE
    AsyncmyMySQLError = asyncmy.errors.MySQLError
    AsyncmyModule = asyncmy
    AsyncmyPool = asyncmy.pool.Pool
    AsyncmyProgrammingError = asyncmy.errors.ProgrammingError
    AsyncmyRawCursor = asyncmy.cursors.Cursor
    AsyncmySSCursor = asyncmy.cursors.SSCursor

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
    "AsyncmySessionContext",
    "asyncmy_local_infile",
)


class AsyncmyCursor:
    """Context manager for AsyncMy cursor operations.

    Provides automatic cursor acquisition and cleanup for database operations.
    """

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AsyncmyConnection") -> None:
        self.connection = connection
        self.cursor: AsyncmyRawCursor | None = None

    async def __aenter__(self) -> "AsyncmyRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                await self.cursor.close()


class AsyncmySessionContext:
    """Async context manager for AsyncMy sessions.

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
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Awaitable[AsyncmyConnection]]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[AsyncmyDriver], AsyncmyDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: AsyncmyConnection | None = None
        self._driver: AsyncmyDriver | None = None

    async def __aenter__(self) -> "AsyncmyDriver":
        from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

        self._connection = await self._acquire_connection()
        self._driver = AsyncmyDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None


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
        setattr(raw, "_result", None)
        result = _AsyncmyLocalInfileResult(raw, filename)
        if unbuffered:
            try:
                init_fn = cast("Callable[[], Awaitable[None]]", result.init_unbuffered_query)
                await init_fn()
            except BaseException:
                setattr(result, "unbuffered_active", False)
                setattr(result, "connection", None)
                raise
        else:
            read_fn = cast("Callable[[], Awaitable[None]]", result.read)
            await read_fn()
        setattr(raw, "_result", result)
        setattr(raw, "_affected_rows", result.affected_rows)
        if result.server_status:
            setattr(raw, "server_status", result.server_status)

    setattr(raw, "_read_query_result", read_result)
    try:
        yield
    except BaseException:
        with contextlib.suppress(Exception):
            raw.close()
        setattr(raw, "_connected", False)
        raise
    finally:
        if previous is missing:
            if "_read_query_result" in raw.__dict__:
                del raw._read_query_result
        else:
            setattr(raw, "_read_query_result", previous)


class _AsyncmyLocalInfileResult(MySQLResult):
    """Normalize the upstream filename handoff while retaining its native sender."""

    __slots__ = ("_filename",)

    def __init__(self, connection: Any, filename: str) -> None:
        super().__init__(connection)
        self._filename = filename

    async def _read_load_local_packet(self, first_packet: Any) -> None:
        request = LoadLocalPacketWrapper(first_packet).filename
        if not self.connection._local_infile or os.fsdecode(request) != self._filename:
            msg = "MySQL requested an unexpected LOCAL INFILE payload."
            raise SQLSpecError(msg)
        sender = LoadLocalFile(self._filename, self.connection)
        send_data = cast("Callable[[], Awaitable[None]]", sender.send_data)
        await send_data()
        packet = await self.connection.read_packet()
        if not packet.is_ok_packet():
            msg = "MySQL did not acknowledge the LOCAL INFILE payload."
            raise SQLSpecError(msg)
        read_ok_fn: Callable[[Any], None] | None = getattr(self, "_read_ok_packet", None)
        if read_ok_fn is not None:
            read_ok_fn(packet)
