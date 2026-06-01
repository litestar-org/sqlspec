"""Regression tests for ObjectStoreProtocol stream_arrow_async conformance."""

import inspect
from pathlib import Path

from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.backends.local import LocalStore


def test_protocol_stream_arrow_async_is_not_coroutine_function() -> None:
    """ObjectStoreProtocol.stream_arrow_async is a plain def returning AsyncIterator."""
    assert not inspect.iscoroutinefunction(ObjectStoreProtocol.stream_arrow_async)


def test_local_store_satisfies_protocol(tmp_path: Path) -> None:
    """LocalStore structurally satisfies ObjectStoreProtocol."""
    assert isinstance(LocalStore(base_path=tmp_path), ObjectStoreProtocol)


def test_local_store_stream_arrow_async_is_not_coroutine_function() -> None:
    """Concrete backends should not expose stream_arrow_async as async def."""
    assert not inspect.iscoroutinefunction(LocalStore.stream_arrow_async)


def test_stream_arrow_async_notes_are_present() -> None:
    """Source comments document why stream_arrow_async is not async def."""
    assert "Returns AsyncIterator directly" in Path("sqlspec/protocols.py").read_text()
    assert "Returns AsyncIterator directly" in Path("sqlspec/storage/backends/base.py").read_text()
