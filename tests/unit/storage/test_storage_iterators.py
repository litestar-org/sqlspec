"""Tests for storage iterator helpers."""

import io

from sqlspec.storage.backends.base import AsyncThreadedBytesIterator


async def test_async_threaded_bytes_iterator_aclose_closes_file() -> None:
    """Ensure aclose closes the wrapped file object."""
    file_obj = io.BytesIO(b"abcdef")
    iterator = AsyncThreadedBytesIterator(file_obj, chunk_size=2)

    await iterator.__anext__()
    assert not file_obj.closed

    await iterator.aclose()
    assert file_obj.closed


async def test_async_threaded_bytes_iterator_context_manager_closes_file() -> None:
    """Ensure async context manager closes the wrapped file object."""
    file_obj = io.BytesIO(b"abcdef")

    async with AsyncThreadedBytesIterator(file_obj, chunk_size=2) as iterator:
        chunk = await iterator.__anext__()
        assert chunk == b"ab"

    assert file_obj.closed
