"""Regression tests for shared ADK service dispatch helpers."""

from typing import Any, cast

import pytest

from sqlspec.extensions.adk.artifact.service import _call_storage_backend
from sqlspec.extensions.adk.memory.service import SQLSpecMemoryService


class _SyncMemoryStore:
    def insert_memory_entries(self, records: list[object]) -> int:
        return len(records)


class _AsyncMemoryStore:
    async def insert_memory_entries(self, records: list[object]) -> int:
        return len(records)


@pytest.mark.anyio
@pytest.mark.parametrize("store", [_SyncMemoryStore(), _AsyncMemoryStore()])
async def test_memory_service_call_store_dispatches_sync_and_async_methods(store: object) -> None:
    service = SQLSpecMemoryService(cast("Any", store))

    result = await service._call_store("insert_memory_entries", [object(), object()])

    assert result == 2


class _AsyncStorageBackend:
    async def read_bytes_async(self, path: str) -> bytes:
        return path.encode()


class _SyncStorageBackend:
    def read_bytes_sync(self, path: str) -> bytes:
        return path.encode()


@pytest.mark.anyio
@pytest.mark.parametrize("backend", [_AsyncStorageBackend(), _SyncStorageBackend()])
async def test_artifact_backend_dispatch_uses_available_capability(backend: object) -> None:
    result = await _call_storage_backend(cast("Any", backend), "read_bytes_async", "read_bytes_sync", "payload")

    assert result == b"payload"
