"""Base class for storage backends."""

# ruff: noqa: RSE102
import asyncio
import builtins
import contextlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, cast

from mypy_extensions import mypyc_attr
from typing_extensions import Self

from sqlspec.typing import ArrowRecordBatch, ArrowTable
from sqlspec.utils.sync_tools import CapacityLimiter

if TYPE_CHECKING:
    from types import TracebackType

_StopAsyncBase = getattr(builtins, "Stop" + "Async" + "Iteration")
_StopAsync = type("_StopAsync", (_StopAsyncBase,), {})
storage_limiter = CapacityLimiter(100)


class _ExhaustedSentinel:
    """Sentinel value to signal iterator exhaustion across thread boundaries.

    StopIteration cannot be raised into asyncio Futures, so we use this sentinel
    to signal iterator exhaustion from the thread pool back to the async context.
    """

    __slots__ = ()


_EXHAUSTED = _ExhaustedSentinel()


def _next_or_sentinel(iterator: "Iterator[Any]") -> "Any":
    """Get next item or return sentinel if exhausted."""
    try:
        return next(iterator)
    except StopIteration:
        return _EXHAUSTED


def _read_chunk_or_sentinel(file_obj: Any, chunk_size: int) -> Any:
    """Read a chunk from a file-like object or return sentinel if exhausted."""
    try:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            return _EXHAUSTED
    except EOFError:
        return _EXHAUSTED
    return chunk


class AsyncArrowBatchIterator:
    """Async iterator wrapper for sync Arrow batch iterators."""

    __slots__ = ("_sync_iter",)

    def __init__(self, sync_iterator: "Iterator[ArrowRecordBatch]") -> None:
        self._sync_iter = sync_iterator

    def __aiter__(self) -> "AsyncArrowBatchIterator":
        return self

    def _sync_next(self) -> "ArrowRecordBatch":
        result = _next_or_sentinel(self._sync_iter)
        if result is _EXHAUSTED:
            raise _StopAsync()
        return cast("ArrowRecordBatch", result)

    def __anext__(self) -> Any:
        # Returning a Future avoids mypyc coroutine state machine bugs entirely.
        return asyncio.get_running_loop().run_in_executor(None, self._sync_next)


class AsyncBytesIterator:
    """Async iterator wrapper for sync bytes iterators."""

    __slots__ = ("_sync_iter",)

    def __init__(self, sync_iterator: "Iterator[bytes]") -> None:
        self._sync_iter = sync_iterator

    def __aiter__(self) -> "AsyncBytesIterator":
        return self

    def _sync_next(self) -> bytes:
        try:
            return next(self._sync_iter)
        except StopIteration:
            raise _StopAsync() from None

    def __anext__(self) -> Any:
        return asyncio.get_running_loop().run_in_executor(None, self._sync_next)


class AsyncChunkedBytesIterator:
    """Async iterator that yields pre-loaded bytes data in chunks."""

    __slots__ = ("_chunk_size", "_data", "_offset")

    def __init__(self, data: bytes, chunk_size: int = 65536) -> None:
        self._data = data
        self._chunk_size = chunk_size
        self._offset = 0

    def __aiter__(self) -> "AsyncChunkedBytesIterator":
        return self

    def _get_next_chunk(self) -> bytes:
        if self._offset >= len(self._data):
            raise _StopAsync()
        chunk = self._data[self._offset : self._offset + self._chunk_size]
        self._offset += self._chunk_size
        return chunk

    def __anext__(self) -> Any:
        # We use a Future even for memory-only data to satisfy the protocol safely.
        return asyncio.get_running_loop().run_in_executor(None, self._get_next_chunk)


class AsyncObStoreStreamIterator:
    """Async iterator wrapper for obstore streaming."""

    __slots__ = ("_buffer", "_chunk_size", "_stream", "_stream_exhausted")

    def __init__(self, stream: Any, chunk_size: "int | None" = None) -> None:
        self._stream = stream
        self._buffer = bytearray()
        self._chunk_size = chunk_size if chunk_size is not None and chunk_size > 0 else None
        self._stream_exhausted = False

    def __aiter__(self) -> "AsyncObStoreStreamIterator":
        return self

    def __anext__(self) -> Any:
        # For obstore, we MUST be async. To avoid mypyc's async def bugs,
        # we return the coroutine object directly from the underlying stream
        # when possible, or use a hand-rolled coroutine that doesn't use 'await'
        # in a way that triggers the buggy generator-helper optimization.

        if self._chunk_size is None:
            # Delegate directly to the underlying coroutine object.
            # Mypyc handles this safely because it's a simple function return.
            return self._stream.__anext__()

        # For re-chunking, we use a module-level helper to avoid class-level state
        # machine issues if possible, but for now we'll stick to delegating
        # when chunk_size is None as that is the common case.
        return self._stream.__anext__()


class AsyncThreadedBytesIterator:
    """Async iterator that reads from a synchronous file-like object in a thread pool."""

    __slots__ = ("_chunk_size", "_closed", "_file_obj")

    def __init__(self, file_obj: Any, chunk_size: int = 65536) -> None:
        self._file_obj = file_obj
        self._chunk_size = chunk_size
        self._closed = False

    def __aiter__(self) -> "AsyncThreadedBytesIterator":
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            self._file_obj.close()

    def _sync_read(self) -> bytes:
        if self._closed:
            raise _StopAsync()
        result = _read_chunk_or_sentinel(self._file_obj, self._chunk_size)
        if result is _EXHAUSTED:
            self._closed = True
            with contextlib.suppress(Exception):
                self._file_obj.close()
            raise _StopAsync()
        return cast("bytes", result)

    def __anext__(self) -> Any:
        return asyncio.get_running_loop().run_in_executor(None, self._sync_read)


@mypyc_attr(allow_interpreted_subclasses=True)
class ObjectStoreBase(ABC):
    """Base class for storage backends.

    All synchronous methods follow the *_sync naming convention for consistency
    with their async counterparts.
    """

    __slots__ = ()

    @abstractmethod
    def read_bytes_sync(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def write_bytes_sync(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def stream_read_sync(self, path: str, chunk_size: "int | None" = None, **kwargs: Any) -> Iterator[bytes]:
        """Stream bytes from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def read_text_sync(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def write_text_sync(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects in storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def exists_sync(self, path: str, **kwargs: Any) -> bool:
        """Check if object exists in storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def delete_sync(self, path: str, **kwargs: Any) -> None:
        """Delete object from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def copy_sync(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object within storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def move_sync(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object within storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def glob_sync(self, pattern: str, **kwargs: Any) -> "list[str]":
        """Find objects matching pattern synchronously."""
        raise NotImplementedError

    @abstractmethod
    def get_metadata_sync(self, path: str, **kwargs: Any) -> "dict[str, object]":
        """Get object metadata from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def is_object_sync(self, path: str) -> bool:
        """Check if path points to an object synchronously."""
        raise NotImplementedError

    @abstractmethod
    def is_path_sync(self, path: str) -> bool:
        """Check if path points to a directory synchronously."""
        raise NotImplementedError

    @abstractmethod
    def read_arrow_sync(self, path: str, **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def write_arrow_sync(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def stream_arrow_sync(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def stream_read_async(
        self, path: str, chunk_size: "int | None" = None, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects in storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Check if object exists in storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Delete object from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object within storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object within storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def get_metadata_async(self, path: str, **kwargs: Any) -> "dict[str, object]":
        """Get object metadata from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def read_arrow_async(self, path: str, **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def write_arrow_async(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        """Stream Arrow record batches from storage asynchronously."""
        raise NotImplementedError


__all__ = (
    "AsyncArrowBatchIterator",
    "AsyncBytesIterator",
    "AsyncChunkedBytesIterator",
    "AsyncObStoreStreamIterator",
    "AsyncThreadedBytesIterator",
    "ObjectStoreBase",
)
