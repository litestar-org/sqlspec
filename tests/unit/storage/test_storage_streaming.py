# pyright: reportPrivateUsage=false
"""Tests for storage streaming fix."""

import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED

if OBSTORE_INSTALLED:
    from sqlspec.storage.backends.obstore import ObStoreBackend

if FSSPEC_INSTALLED:
    from sqlspec.storage.backends.fsspec import FSSpecBackend


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.asyncio
async def test_obstore_streaming_does_not_buffer() -> None:
    """Verify that ObStoreBackend now uses native streaming and does not buffer."""
    with patch("obstore.store.LocalStore", MagicMock()):
        store = ObStoreBackend("file:///tmp")
        
        # Mock the store's get_async method
        mock_result = MagicMock()
        mock_stream = MagicMock()
        # Mocking an async iterator: it needs __aiter__ returning self, and __anext__ being an async method
        mock_stream.__aiter__.return_value = mock_stream
        mock_stream.__anext__ = AsyncMock(side_effect=[b"chunk1", b"chunk2", StopAsyncIteration])
        mock_result.stream.return_value = mock_stream
        
        store.store.get_async = AsyncMock(return_value=mock_result)
        
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend.read_bytes") as mock_read:
            # Call stream_read_async
            stream = await store.stream_read_async("test.txt")
            
            # Iterate over the stream
            chunks = []
            async for chunk in stream:
                chunks.append(chunk)
            
            assert chunks == [b"chunk1", b"chunk2"]
            assert not mock_read.called, "ObStoreBackend should NOT call read_bytes anymore"
            assert store.store.get_async.called


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.asyncio
async def test_fsspec_streaming_does_not_buffer() -> None:
    """Verify that FSSpecBackend now uses chunked reading and does not buffer."""
    with patch("fsspec.filesystem") as mock_fs_factory:
        mock_fs = MagicMock()
        mock_fs_factory.return_value = mock_fs
        
        store = FSSpecBackend("file")
        
        # Mock fs.open
        mock_file = MagicMock()
        mock_file.read.side_effect = [b"chunk1", b"chunk2", b""]
        mock_fs.open.return_value = mock_file
        
        with patch("sqlspec.storage.backends.fsspec.FSSpecBackend.read_bytes") as mock_read:
            stream = await store.stream_read_async("test.txt", chunk_size=6)
            
            # Iterate
            chunks = []
            async for chunk in stream:
                chunks.append(chunk)
            
            assert chunks == [b"chunk1", b"chunk2"]
            assert not mock_read.called, "FSSpecBackend should NOT call read_bytes anymore"
            assert mock_fs.open.called
            assert mock_file.read.called