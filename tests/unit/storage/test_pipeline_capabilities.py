# pyright: reportPrivateUsage=false
"""Tests for storage pipeline backend capability caching."""

from typing import Any, cast

import pytest

from sqlspec.storage.pipeline import AsyncStoragePipeline, StagedArtifact


class _CapabilityCacheBackend:
    backend_type = "capability-cache"

    def __init__(self) -> None:
        self.deleted_paths: list[str] = []
        self.payloads: dict[str, bytes] = {}

    async def delete_async(self, path: str) -> None:
        self.deleted_paths.append(path)
        self.payloads.pop(path, None)

    async def read_bytes_async(self, path: str) -> bytes:
        return self.payloads[path]

    async def write_bytes_async(self, path: str, payload: bytes) -> None:
        self.payloads[path] = payload


class _CapabilityCacheRegistry:
    def __init__(self, backend: _CapabilityCacheBackend) -> None:
        self.backend = backend
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, destination: str, **options: Any) -> _CapabilityCacheBackend:
        self.calls.append((destination, dict(options)))
        return self.backend


async def test_async_pipeline_caches_backend_capability_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = _CapabilityCacheBackend()
    registry = _CapabilityCacheRegistry(backend)
    pipeline = AsyncStoragePipeline(registry=cast(Any, registry))
    capability_calls = {"delete": 0, "read": 0, "write": 0}

    def _supports_async_delete(_: Any) -> bool:
        capability_calls["delete"] += 1
        return True

    def _supports_async_read_bytes(_: Any) -> bool:
        capability_calls["read"] += 1
        return True

    def _supports_async_write_bytes(_: Any) -> bool:
        capability_calls["write"] += 1
        return True

    monkeypatch.setattr("sqlspec.storage.pipeline.supports_async_delete", _supports_async_delete)
    monkeypatch.setattr("sqlspec.storage.pipeline.supports_async_read_bytes", _supports_async_read_bytes)
    monkeypatch.setattr("sqlspec.storage.pipeline.supports_async_write_bytes", _supports_async_write_bytes)

    artifact: StagedArtifact = {
        "partition_id": "0",
        "uri": "file://tmp/payload.jsonl",
        "cleanup_token": "cleanup::0",
        "ttl_seconds": 0,
        "expires_at": 0.0,
        "correlation_id": "cleanup",
    }

    await pipeline.write_rows([{"id": 1}], "file://tmp/payload.jsonl")
    await pipeline.write_rows([{"id": 2}], "file://tmp/payload.jsonl")
    await pipeline.read_arrow_async("file://tmp/payload.jsonl", file_format="jsonl")
    await pipeline.read_arrow_async("file://tmp/payload.jsonl", file_format="jsonl")
    await pipeline.cleanup_staging_artifacts([artifact])
    await pipeline.cleanup_staging_artifacts([artifact])

    assert capability_calls == {"delete": 1, "read": 1, "write": 1}

    pipeline.clear_cache()
    registry.backend = _CapabilityCacheBackend()
    await pipeline.write_rows([{"id": 3}], "file://tmp/payload.jsonl")

    assert capability_calls == {"delete": 2, "read": 2, "write": 2}
