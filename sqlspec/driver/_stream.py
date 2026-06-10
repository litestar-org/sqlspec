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
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

if TYPE_CHECKING:
    from types import TracebackType

__all__ = ("AsyncRowStream", "EagerAsyncRowSource", "EagerSyncRowSource", "SyncRowStream", "rows_to_dicts")

_StopAsyncBase = getattr(builtins, "Stop" + "Async" + "Iteration")
_StopAsync = type("_StopAsync", (_StopAsyncBase,), {})


def rows_to_dicts(rows: "list[Any]", column_names: "list[str]") -> "list[dict[str, Any]]":
    """Zip positional rows with column names into dict rows."""
    if not column_names:
        return []
    return [dict(zip(column_names, row, strict=False)) for row in rows]


class SyncRowStream:
    """Bounded-memory iterator of dict rows backed by a chunk source."""

    __slots__ = ("_buffer", "_buffer_index", "_closed", "_source", "_started")

    def __init__(self, source: Any) -> None:
        self._source = source
        self._buffer: list[dict[str, Any]] = []
        self._buffer_index = 0
        self._closed = False
        self._started = False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        self.close()

    def __iter__(self) -> "SyncRowStream":
        return self

    def __next__(self) -> "dict[str, Any]":
        if self._closed:
            raise StopIteration
        if not self._started:
            self._started = True
            try:
                self._source.start()
            except BaseException:
                self.close()
                raise
        if self._buffer_index >= len(self._buffer):
            try:
                chunk = self._source.fetch_chunk()
            except BaseException:
                self.close()
                raise
            if not chunk:
                self.close()
                raise StopIteration
            self._buffer = chunk
            self._buffer_index = 0
        row = self._buffer[self._buffer_index]
        self._buffer_index += 1
        return row

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._buffer = []
        self._buffer_index = 0
        with contextlib.suppress(Exception):
            self._source.close()


class AsyncRowStream:
    """Async bounded-memory iterator of dict rows backed by an async chunk source."""

    __slots__ = ("_buffer", "_buffer_index", "_closed", "_source", "_started")

    def __init__(self, source: Any) -> None:
        self._source = source
        self._buffer: list[dict[str, Any]] = []
        self._buffer_index = 0
        self._closed = False
        self._started = False

    def __aiter__(self) -> "AsyncRowStream":
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        await self.aclose()

    async def __anext__(self) -> "dict[str, Any]":
        if self._closed:
            raise _StopAsync
        if not self._started:
            self._started = True
            try:
                await self._source.start()
            except BaseException:
                await self.aclose()
                raise
        if self._buffer_index >= len(self._buffer):
            try:
                chunk = await self._source.fetch_chunk()
            except BaseException:
                await self.aclose()
                raise
            if not chunk:
                await self.aclose()
                raise _StopAsync
            self._buffer = chunk
            self._buffer_index = 0
        row = self._buffer[self._buffer_index]
        self._buffer_index += 1
        return row

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._buffer = []
        self._buffer_index = 0
        with contextlib.suppress(Exception):
            await self._source.close()


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

    def close(self) -> None:
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

    async def close(self) -> None:
        self._rows = []
