"""Tests for storage iterator helpers."""

import io

import pytest

from sqlspec.storage.backends.base import AsyncThreadedBytesIterator


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
