"""Base class for storage backends."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import Any, cast

from mypy_extensions import mypyc_attr

from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = (
    "AsyncArrowBatchIterator",
    "AsyncBytesIterator",
    "AsyncChunkedBytesIterator",
    "AsyncObStoreStreamIterator",
    "AsyncThreadedBytesIterator",
    "ObjectStoreBase",
)


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

        Returns:
            The next Arrow record batch.

        Raises:
            StopAsyncIteration: When the iterator is exhausted.
        """
        try:
            return next(self._sync_iter)
        except StopIteration:
            raise StopAsyncIteration from None


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
        import asyncio

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

    __slots__ = ("_stream",)

    def __init__(self, stream: Any) -> None:
        """Initialize the obstore stream wrapper.

        Args:
            stream: The native obstore async stream to wrap.
        """
        self._stream = stream

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
        try:
            chunk = await self._stream.__anext__()
            return bytes(chunk)
        except StopAsyncIteration:
            raise StopAsyncIteration from None


class AsyncThreadedBytesIterator:
    """Async iterator that reads from a synchronous file-like object in a thread pool.

    This class implements the async iterator protocol without using async generators,
    allowing it to be compiled by mypyc. It offloads blocking read/close calls
    to a thread pool to avoid blocking the event loop.
    """

    __slots__ = ("_chunk_size", "_file_obj")

    def __init__(self, file_obj: Any, chunk_size: int = 65536) -> None:
        """Initialize the threaded bytes iterator.

        Args:
            file_obj: Synchronous file-like object supporting read() and close().
            chunk_size: Size of each chunk to read (default: 65536 bytes).
        """
        self._file_obj = file_obj
        self._chunk_size = chunk_size

    def __aiter__(self) -> "AsyncThreadedBytesIterator":
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> bytes:
        """Read the next chunk of bytes in a thread pool.

        Returns:
            The next chunk of bytes.

        Raises:
            StopAsyncIteration: When the file is fully read.
        """
        import asyncio

        try:
            chunk = await asyncio.to_thread(self._file_obj.read, self._chunk_size)
            if not chunk:
                await asyncio.to_thread(self._file_obj.close)
                raise StopAsyncIteration
            return cast("bytes", chunk)
        except EOFError:
            await asyncio.to_thread(self._file_obj.close)
            raise StopAsyncIteration from None
        except Exception:
            await asyncio.to_thread(self._file_obj.close)
            raise


@mypyc_attr(allow_interpreted_subclasses=True)
class ObjectStoreBase(ABC):
    """Base class for storage backends."""

    __slots__ = ()

    @abstractmethod
    def read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from storage."""
        raise NotImplementedError

    @abstractmethod
    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage."""
        raise NotImplementedError

    @abstractmethod
    def stream_read(self, path: str, chunk_size: "int | None" = None, **kwargs: Any) -> Iterator[bytes]:
        """Stream bytes from storage."""
        raise NotImplementedError

    @abstractmethod
    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage."""
        raise NotImplementedError

    @abstractmethod
    def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage."""
        raise NotImplementedError

    @abstractmethod
    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects in storage."""
        raise NotImplementedError

    @abstractmethod
    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if object exists in storage."""
        raise NotImplementedError

    @abstractmethod
    def delete(self, path: str, **kwargs: Any) -> None:
        """Delete object from storage."""
        raise NotImplementedError

    @abstractmethod
    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object within storage."""
        raise NotImplementedError

    @abstractmethod
    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object within storage."""
        raise NotImplementedError

    @abstractmethod
    def glob(self, pattern: str, **kwargs: Any) -> "list[str]":
        """Find objects matching pattern."""
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, path: str, **kwargs: Any) -> "dict[str, object]":
        """Get object metadata from storage."""
        raise NotImplementedError

    @abstractmethod
    def is_object(self, path: str) -> bool:
        """Check if path points to an object."""
        raise NotImplementedError

    @abstractmethod
    def is_path(self, path: str) -> bool:
        """Check if path points to a directory."""
        raise NotImplementedError

    @abstractmethod
    def read_arrow(self, path: str, **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage."""
        raise NotImplementedError

    @abstractmethod
    def write_arrow(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage."""
        raise NotImplementedError

    @abstractmethod
    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches from storage."""
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
    def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
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
    def get_metadata_async(self, path: str, **kwargs: Any) -> "dict[str, object]":
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
