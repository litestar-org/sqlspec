"""Row streaming primitives for driver ``select_stream`` APIs.

A *source* drives a stream and is duck-typed (kept ABI-neutral as ``Any`` so
interpreted adapter sources can feed compiled stream classes):

- sync source: ``start() -> None`` opens the cursor/executes, ``fetch_chunk() ->
  list[dict[str, Any]]`` returns the next chunk (empty list signals exhaustion),
  ``close() -> None`` is idempotent and safe at any state including pre-start.
- async source: same names, all coroutines.
"""

import builtins
import contextlib
import inspect
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast, overload

from typing_extensions import Self

from sqlspec.utils.schema import to_schema

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    "AsyncRowSource",
    "AsyncRowStream",
    "EagerAsyncRowSource",
    "EagerSyncRowSource",
    "SyncRowSource",
    "SyncRowStream",
    "rows_to_dicts",
)

_StopAsyncBase = getattr(builtins, "Stop" + "Async" + "Iteration")
_StopAsync = type("_StopAsync", (_StopAsyncBase,), {})
RowT = TypeVar("RowT")
SchemaRowT = TypeVar("SchemaRowT")


class SyncRowSource(Protocol):
    """Protocol for synchronous row stream sources."""

    def start(self) -> None: ...

    def fetch_chunk(self) -> "list[dict[str, Any]]": ...

    def close(self) -> None: ...


class AsyncRowSource(Protocol):
    """Protocol for asynchronous row stream sources."""

    async def start(self) -> None: ...

    async def fetch_chunk(self) -> "list[dict[str, Any]]": ...

    async def close(self) -> None: ...


def _close_sync_source(source: SyncRowSource, error: bool) -> None:
    """Close a source while preserving the original no-argument contract."""
    close = source.close
    try:
        inspect.signature(close).bind(error=error)
    except (TypeError, ValueError):
        close()
        return
    cast("Any", close)(error=error)


async def _close_async_source(source: AsyncRowSource, error: bool) -> None:
    """Close an async source while preserving the original no-argument contract."""
    close = source.close
    try:
        inspect.signature(close).bind(error=error)
    except (TypeError, ValueError):
        await close()
        return
    await cast("Any", close)(error=error)


def rows_to_dicts(rows: "list[Any]", column_names: "list[str]") -> "list[dict[str, Any]]":
    """Zip positional rows with column names into dict rows."""
    if not column_names:
        return []
    return [dict(zip(column_names, row, strict=False)) for row in rows]


class SyncRowStream(Generic[RowT]):
    """Bounded-memory iterator backed by a chunk source."""

    __slots__ = ("_buffer", "_buffer_index", "_closed", "_schema_type", "_source", "_started")

    def __init__(self, source: SyncRowSource, schema_type: "type[RowT] | None" = None) -> None:
        self._source = source
        self._schema_type: type[Any] | None = schema_type
        self._buffer: list[RowT] = []
        self._buffer_index = 0
        self._closed = False
        self._started = False

    @overload
    def _with_schema_type(self, schema_type: "type[SchemaRowT]") -> "SyncRowStream[SchemaRowT]": ...

    @overload
    def _with_schema_type(self, schema_type: None = None) -> "SyncRowStream[dict[str, Any]]": ...

    def _with_schema_type(
        self, schema_type: "type[SchemaRowT] | None" = None
    ) -> "SyncRowStream[SchemaRowT] | SyncRowStream[dict[str, Any]]":
        self._schema_type = schema_type
        if schema_type is None:
            return cast("SyncRowStream[dict[str, Any]]", self)
        return cast("SyncRowStream[SchemaRowT]", self)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        self._close(error=exc_type is not None)

    def __iter__(self) -> "SyncRowStream[RowT]":
        return self

    def __next__(self) -> RowT:
        if self._closed:
            raise StopIteration
        if not self._started:
            self._started = True
            try:
                self._source.start()
            except BaseException:
                self._close(error=True)
                raise
        if self._buffer_index >= len(self._buffer):
            try:
                chunk = self._source.fetch_chunk()
            except BaseException:
                self._close(error=True)
                raise
            if not chunk:
                self.close()
                raise StopIteration
            self._buffer = self._coerce_chunk(chunk)
            self._buffer_index = 0
        row = self._buffer[self._buffer_index]
        self._buffer_index += 1
        return row

    def _coerce_chunk(self, chunk: "list[dict[str, Any]]") -> "list[RowT]":
        schema_type = self._schema_type
        if schema_type is None:
            return cast("list[RowT]", chunk)
        return cast("list[RowT]", to_schema(chunk, schema_type=schema_type))

    def close(self) -> None:
        self._close(error=False)

    def _close(self, error: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        self._buffer = []
        self._buffer_index = 0
        with contextlib.suppress(Exception):
            _close_sync_source(self._source, error)


class AsyncRowStream(Generic[RowT]):
    """Async bounded-memory iterator backed by an async chunk source."""

    __slots__ = ("_buffer", "_buffer_index", "_closed", "_schema_type", "_source", "_started")

    def __init__(self, source: AsyncRowSource, schema_type: "type[RowT] | None" = None) -> None:
        self._source = source
        self._schema_type: type[Any] | None = schema_type
        self._buffer: list[RowT] = []
        self._buffer_index = 0
        self._closed = False
        self._started = False

    @overload
    def _with_schema_type(self, schema_type: "type[SchemaRowT]") -> "AsyncRowStream[SchemaRowT]": ...

    @overload
    def _with_schema_type(self, schema_type: None = None) -> "AsyncRowStream[dict[str, Any]]": ...

    def _with_schema_type(
        self, schema_type: "type[SchemaRowT] | None" = None
    ) -> "AsyncRowStream[SchemaRowT] | AsyncRowStream[dict[str, Any]]":
        self._schema_type = schema_type
        if schema_type is None:
            return cast("AsyncRowStream[dict[str, Any]]", self)
        return cast("AsyncRowStream[SchemaRowT]", self)

    def __aiter__(self) -> "AsyncRowStream[RowT]":
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        await self._aclose(error=exc_type is not None)

    async def __anext__(self) -> RowT:
        if self._closed:
            raise _StopAsync
        if not self._started:
            self._started = True
            try:
                await self._source.start()
            except BaseException:
                await self._aclose(error=True)
                raise
        if self._buffer_index >= len(self._buffer):
            try:
                chunk = await self._source.fetch_chunk()
            except BaseException:
                await self._aclose(error=True)
                raise
            if not chunk:
                await self.aclose()
                raise _StopAsync
            self._buffer = self._coerce_chunk(chunk)
            self._buffer_index = 0
        row = self._buffer[self._buffer_index]
        self._buffer_index += 1
        return row

    def _coerce_chunk(self, chunk: "list[dict[str, Any]]") -> "list[RowT]":
        schema_type = self._schema_type
        if schema_type is None:
            return cast("list[RowT]", chunk)
        return cast("list[RowT]", to_schema(chunk, schema_type=schema_type))

    async def aclose(self) -> None:
        await self._aclose(error=False)

    async def _aclose(self, error: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        self._buffer = []
        self._buffer_index = 0
        with contextlib.suppress(Exception):
            await _close_async_source(self._source, error)


class EagerSyncRowSource:
    """Chunk source over pre-materialized rows (eager fallback; not bounded-memory)."""

    __slots__ = ("_chunk_size", "_position", "_rows")

    def __init__(self, rows: "list[dict[str, Any]]", chunk_size: int) -> None:
        self._rows = rows
        self._chunk_size = chunk_size
        self._position = 0

    def start(self) -> None:
        return None

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        chunk = self._rows[self._position : self._position + self._chunk_size]
        self._position += len(chunk)
        return chunk

    def close(self, error: bool = False) -> None:
        self._rows = []


class EagerAsyncRowSource:
    """Async chunk source over pre-materialized rows (eager fallback; not bounded-memory)."""

    __slots__ = ("_chunk_size", "_position", "_rows")

    def __init__(self, rows: "list[dict[str, Any]]", chunk_size: int) -> None:
        self._rows = rows
        self._chunk_size = chunk_size
        self._position = 0

    async def start(self) -> None:
        return None

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        chunk = self._rows[self._position : self._position + self._chunk_size]
        self._position += len(chunk)
        return chunk

    async def close(self, error: bool = False) -> None:
        self._rows = []


class _LazyEagerAsyncRowSource:
    """Async eager fallback source that materializes via the driver on first fetch."""

    __slots__ = ("_chunk_size", "_driver", "_position", "_rows", "_statement")

    def __init__(self, driver: Any, statement: Any, chunk_size: int) -> None:
        self._driver = driver
        self._statement = statement
        self._chunk_size = chunk_size
        self._rows: list[dict[str, Any]] = []
        self._position = 0

    async def start(self) -> None:
        result = await self._driver.execute(self._statement)
        self._rows = result.get_data()

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        chunk = self._rows[self._position : self._position + self._chunk_size]
        self._position += len(chunk)
        return chunk

    async def close(self, error: bool = False) -> None:
        self._rows = []
