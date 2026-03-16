"""Async iterator classes for storage backends.

This module is intentionally excluded from mypyc compilation because
async __anext__ methods that use asyncio.to_thread cause segfaults
when compiled — the C coroutine state machine cannot survive the
suspend/resume cycle across thread boundaries.
"""

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, cast

from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from sqlspec.typing import ArrowRecordBatch

__all__ = (
    "AsyncArrowBatchIterator",
    "AsyncBytesIterator",
    "AsyncChunkedBytesIterator",
    "AsyncObStoreStreamIterator",
    "AsyncThreadedBytesIterator",
)


class _ExhaustedSentinel:
    """Sentinel value to signal iterator exhaustion across thread boundaries.

    StopIteration cannot be raised into asyncio Futures, so we use this sentinel
    to signal iterator exhaustion from the thread pool back to the async context.
    """

    __slots__ = ()


_EXHAUSTED = _ExhaustedSentinel()


def _next_or_sentinel(iterator: "Iterator[Any]") -> "Any":
    """Get next item or return sentinel if exhausted.

    This helper wraps next() to catch StopIteration in the thread,
    since StopIteration cannot propagate through asyncio Futures.
    """
    try:
        return next(iterator)
    except StopIteration:
        return _EXHAUSTED


def _read_chunk_or_sentinel(file_obj: Any, chunk_size: int) -> Any:
    """Read a chunk from a file-like object or return sentinel if exhausted.

    This helper is used by AsyncThreadedBytesIterator to offload blocking reads.
    """
    try:
        chunk = file_obj.read(chunk_size)
    except EOFError:
        return _EXHAUSTED
    if not chunk:
        return _EXHAUSTED
    return chunk


class AsyncArrowBatchIterator:
    """Async iterator wrapper for sync Arrow batch iterators.

    This class implements the async iterator protocol without using async generators,
    allowing it to be compiled by mypyc (which doesn't support async generators).

    The class wraps a synchronous iterator and exposes it as an async iterator,
    enabling usage with `async for` syntax.
    """

    __slots__ = ("_sync_iter",)

    def __init__(self, sync_iterator: "Iterator[ArrowRecordBatch]") -> None:
        """Initialize the async iterator wrapper.

        Args:
            sync_iterator: The synchronous iterator to wrap.

        """
        self._sync_iter = sync_iterator

    def __aiter__(self) -> "AsyncArrowBatchIterator":
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> "ArrowRecordBatch":
        """Get the next item from the iterator asynchronously.

        Uses asyncio.to_thread to offload the blocking next() call
        to a thread pool, preventing event loop blocking.

        Returns:
            The next Arrow record batch.

        Raises:
            StopAsyncIteration: When the iterator is exhausted.

        """
        result = await asyncio.to_thread(_next_or_sentinel, self._sync_iter)
        if result is _EXHAUSTED:
            raise StopAsyncIteration
        return cast("ArrowRecordBatch", result)


class AsyncBytesIterator:
    """Async iterator wrapper for sync bytes iterators.

    This class implements the async iterator protocol without using async generators,
    allowing it to be compiled by mypyc (which doesn't support async generators).

    The class wraps a synchronous iterator and exposes it as an async iterator,
    enabling usage with `async for` syntax.

    Note: This class blocks the event loop during I/O. For non-blocking streaming,
    use AsyncChunkedBytesIterator with pre-loaded data instead.
    """

    __slots__ = ("_sync_iter",)

    def __init__(self, sync_iterator: "Iterator[bytes]") -> None:
        """Initialize the async iterator wrapper.

        Args:
            sync_iterator: The synchronous iterator to wrap.

        """
        self._sync_iter = sync_iterator

    def __aiter__(self) -> "AsyncBytesIterator":
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> bytes:
        """Get the next item from the iterator asynchronously.

        Returns:
            The next chunk of bytes.

        Raises:
            StopAsyncIteration: When the iterator is exhausted.

        """
        try:
            return next(self._sync_iter)
        except StopIteration:
            raise StopAsyncIteration from None


class AsyncChunkedBytesIterator:
    """Async iterator that yields pre-loaded bytes data in chunks.

    This class implements the async iterator protocol without using async generators,
    allowing it to be compiled by mypyc (which doesn't support async generators).

    Unlike AsyncBytesIterator, this class works with pre-loaded data and yields
    control to the event loop between chunks via asyncio.sleep(0), ensuring
    the event loop is not blocked during iteration.

    Usage pattern:
        # Load data in thread pool to avoid blocking
        data = await asyncio.to_thread(read_bytes, path)
        # Stream chunks without blocking event loop
        return AsyncChunkedBytesIterator(data, chunk_size=65536)
    """

    __slots__ = ("_chunk_size", "_data", "_offset")

    def __init__(self, data: bytes, chunk_size: int = 65536) -> None:
        """Initialize the chunked bytes iterator.

        Args:
            data: The bytes data to iterate over in chunks.
            chunk_size: Size of each chunk to yield (default: 65536 bytes).

        """
        self._data = data
        self._chunk_size = chunk_size
        self._offset = 0

    def __aiter__(self) -> "AsyncChunkedBytesIterator":
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> bytes:
        """Get the next chunk of bytes asynchronously.

        Yields control to the event loop via asyncio.sleep(0) before returning
        each chunk, ensuring other tasks can run during iteration.

        Returns:
            The next chunk of bytes.

        Raises:
            StopAsyncIteration: When all data has been yielded.

        """
        if self._offset >= len(self._data):
            raise StopAsyncIteration

        # Yield to event loop to allow other tasks to run
        await asyncio.sleep(0)

        chunk = self._data[self._offset : self._offset + self._chunk_size]
        self._offset += self._chunk_size
        return chunk


class AsyncObStoreStreamIterator:
    """Async iterator wrapper for obstore streaming.

    This class wraps obstore's native async stream and ensures it yields
    bytes objects while remaining compatible with mypyc.
    """

    __slots__ = ("_buffer", "_chunk_size", "_stream", "_stream_exhausted")

    def __init__(self, stream: Any, chunk_size: "int | None" = None) -> None:
        """Initialize the obstore stream wrapper.

        Args:
            stream: The native obstore async stream to wrap.
            chunk_size: Optional chunk size to re-chunk streamed data.

        """
        self._stream = stream
        self._buffer = bytearray()
        self._chunk_size = chunk_size if chunk_size is not None and chunk_size > 0 else None
        self._stream_exhausted = False

    def __aiter__(self) -> "AsyncObStoreStreamIterator":
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> bytes:
        """Get the next chunk from the obstore stream asynchronously.

        Returns:
            The next chunk of bytes.

        Raises:
            StopAsyncIteration: When the stream is exhausted.

        """
        if self._chunk_size is None:
            try:
                chunk = await self._stream.__anext__()
                return bytes(chunk)
            except StopAsyncIteration:
                raise StopAsyncIteration from None

        while not self._stream_exhausted and len(self._buffer) < self._chunk_size:
            try:
                chunk = await self._stream.__anext__()
            except StopAsyncIteration:
                self._stream_exhausted = True
                break
            self._buffer.extend(bytes(chunk))

        if self._buffer:
            if len(self._buffer) >= self._chunk_size:
                data = bytes(self._buffer[: self._chunk_size])
                del self._buffer[: self._chunk_size]
                return data
            if self._stream_exhausted:
                data = bytes(self._buffer)
                self._buffer.clear()
                return data

        raise StopAsyncIteration from None


class AsyncThreadedBytesIterator:
    """Async iterator that reads from a synchronous file-like object in a thread pool.

    This class implements the async iterator protocol without using async generators,
    allowing it to be compiled by mypyc. It offloads blocking read/close calls
    to a thread pool to avoid blocking the event loop.

    NOTE: We specifically avoid __del__ here as it causes segmentation faults
    in mypyc compiled mode during GC teardown.
    """

    __slots__ = ("_chunk_size", "_closed", "_file_obj")

    def __init__(self, file_obj: Any, chunk_size: int = 65536) -> None:
        self._file_obj = file_obj
        self._chunk_size = chunk_size
        self._closed = False

    def __aiter__(self) -> "AsyncThreadedBytesIterator":
        return self

    async def __aenter__(self) -> Self:
        """Return the iterator for async context manager usage."""
        return self

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        """Close the underlying file when exiting a context."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying file object."""
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            await asyncio.to_thread(self._file_obj.close)

    async def __anext__(self) -> bytes:
        if self._closed:
            raise StopAsyncIteration

        # Offload blocking read to a thread pool
        result = await asyncio.to_thread(_read_chunk_or_sentinel, self._file_obj, self._chunk_size)

        if result is _EXHAUSTED:
            await self.aclose()
            raise StopAsyncIteration

        return cast("bytes", result)
