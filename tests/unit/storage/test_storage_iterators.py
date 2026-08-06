"""Tests for storage iterator helpers."""

import io

import pytest

from sqlspec.storage.backends.base import AsyncObStoreStreamIterator, AsyncThreadedBytesIterator


class FakeAsyncStream:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = iter(chunks)

    def __aiter__(self) -> "FakeAsyncStream":
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self._chunks)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


@pytest.mark.anyio
async def test_async_threaded_bytes_iterator_closes_file() -> None:
    """Ensure AsyncThreadedBytesIterator closes the wrapped file object via context manager."""
    file_obj = io.BytesIO(b"abcdef")

    async with AsyncThreadedBytesIterator(file_obj, chunk_size=2) as iterator:
        chunk = await iterator.__anext__()
        assert chunk == b"ab"
        assert not file_obj.closed

        # Exhaust the iterator
        async for _ in iterator:
            pass

    # Verified: Explicit cleanup or loop exit closes the file
    assert file_obj.closed


@pytest.mark.anyio
async def test_async_threaded_bytes_iterator_early_exit_closes_file() -> None:
    """Ensure AsyncThreadedBytesIterator closes the wrapped file object on early exit."""
    file_obj = io.BytesIO(b"abcdef")

    async with AsyncThreadedBytesIterator(file_obj, chunk_size=2) as iterator:
        async for chunk in iterator:
            assert chunk == b"ab"
            break

    # Verified: Context manager ensures closure even on break/early exit
    assert file_obj.closed


def test_async_obstore_stream_iterator_slots_are_minimal() -> None:
    iterator = AsyncObStoreStreamIterator(FakeAsyncStream([b"abc"]), chunk_size=2)

    assert AsyncObStoreStreamIterator.__slots__ == ("_chunk_size", "_stream")
    assert not hasattr(iterator, "_buffer")
    assert not hasattr(iterator, "_stream_exhausted")


@pytest.mark.anyio
async def test_async_obstore_stream_iterator_delegates_to_stream() -> None:
    iterator = AsyncObStoreStreamIterator(FakeAsyncStream([b"abc"]), chunk_size=2)

    assert await iterator.__anext__() == b"abc"
