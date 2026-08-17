"""Unit tests for SQLSpec artifact service content coordination."""

import logging
from datetime import datetime, timezone
from typing import Any

import pytest

from sqlspec.extensions.adk.artifact._types import StoredArtifact
from sqlspec.extensions.adk.artifact.service import SQLSpecArtifactService

STORAGE_URI = "file:///artifacts"


def _record(version: int, filename: str = "report.txt") -> StoredArtifact:
    return StoredArtifact(
        app_name="agent_app",
        user_id="user-1",
        session_id=None,
        filename=filename,
        version=version,
        mime_type="text/plain",
        canonical_uri=f"{STORAGE_URI}/apps/agent_app/users/user-1/artifacts/{filename}/v{version}",
        custom_metadata=None,
        created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


class _RecordingBackend:
    """Storage backend double recording delete attempts."""

    def __init__(self, failing_paths: "set[str] | None" = None) -> None:
        self.deleted: list[str] = []
        self.failing_paths = failing_paths or set()

    async def delete_async(self, path: str) -> None:
        self.deleted.append(path)
        if path in self.failing_paths:
            msg = f"backend refused {path}"
            raise RuntimeError(msg)


class _RecordingRegistry:
    """Storage registry double counting backend resolutions."""

    def __init__(self, backend: _RecordingBackend) -> None:
        self.backend = backend
        self.resolved: list[str] = []

    def get(self, uri: str, **_kwargs: Any) -> _RecordingBackend:
        self.resolved.append(uri)
        return self.backend


class _RecordingStore:
    """Artifact metadata store double."""

    def __init__(self, batches: "list[list[StoredArtifact]] | None" = None) -> None:
        self.artifact_table = "adk_artifact"
        self.retention_calls: list[tuple[datetime, str | None]] = []
        self.delete_calls: list[tuple[str, str, str, str | None]] = []
        self.batches = batches if batches is not None else [[]]

    async def delete_artifacts_older_than(
        self, before: datetime, app_name: "str | None" = None
    ) -> "list[StoredArtifact]":
        self.retention_calls.append((before, app_name))
        return self.batches[min(len(self.retention_calls) - 1, len(self.batches) - 1)]

    async def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[StoredArtifact]":
        self.delete_calls.append((app_name, user_id, filename, session_id))
        return self.batches[0]


class _FailingStore(_RecordingStore):
    """Artifact metadata store double that fails the retention query."""

    async def delete_artifacts_older_than(
        self, before: datetime, app_name: "str | None" = None
    ) -> "list[StoredArtifact]":
        self.retention_calls.append((before, app_name))
        msg = "metadata delete failed"
        raise RuntimeError(msg)


def _service(store: Any, backend: _RecordingBackend) -> "tuple[SQLSpecArtifactService, _RecordingRegistry]":
    registry = _RecordingRegistry(backend)
    service = SQLSpecArtifactService(store, STORAGE_URI, registry=registry)  # type: ignore[arg-type]
    return service, registry


async def test_delete_artifacts_older_than_forwards_cutoff_and_app_scope() -> None:
    store = _RecordingStore([[_record(0), _record(1)]])
    backend = _RecordingBackend()
    service, _registry = _service(store, backend)
    cutoff = datetime(2024, 6, 1, tzinfo=timezone.utc)

    deleted = await service.delete_artifacts_older_than(cutoff, app_name="agent_app")

    assert deleted == 2
    assert store.retention_calls == [(cutoff, "agent_app")]


async def test_delete_artifacts_older_than_deletes_content_for_every_version() -> None:
    store = _RecordingStore([[_record(0), _record(1), _record(0, filename="notes.txt")]])
    backend = _RecordingBackend()
    service, registry = _service(store, backend)

    deleted = await service.delete_artifacts_older_than(datetime(2024, 6, 1, tzinfo=timezone.utc))

    assert deleted == 3
    assert backend.deleted == [
        "apps/agent_app/users/user-1/artifacts/report.txt/v0",
        "apps/agent_app/users/user-1/artifacts/report.txt/v1",
        "apps/agent_app/users/user-1/artifacts/notes.txt/v0",
    ]
    assert registry.resolved == [STORAGE_URI]


async def test_delete_artifacts_older_than_without_matches_skips_storage() -> None:
    store = _RecordingStore([[]])
    backend = _RecordingBackend()
    service, registry = _service(store, backend)

    deleted = await service.delete_artifacts_older_than(datetime(2024, 6, 1, tzinfo=timezone.utc))

    assert deleted == 0
    assert registry.resolved == []
    assert backend.deleted == []


async def test_delete_artifacts_older_than_continues_after_content_failure(caplog: pytest.LogCaptureFixture) -> None:
    failing_path = "apps/agent_app/users/user-1/artifacts/report.txt/v0"
    store = _RecordingStore([[_record(0), _record(1)]])
    backend = _RecordingBackend(failing_paths={failing_path})
    service, _registry = _service(store, backend)

    with caplog.at_level(logging.WARNING, logger="sqlspec.extensions.adk.artifact.service"):
        deleted = await service.delete_artifacts_older_than(datetime(2024, 6, 1, tzinfo=timezone.utc))

    assert deleted == 2
    assert backend.deleted == [failing_path, "apps/agent_app/users/user-1/artifacts/report.txt/v1"]
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    fields = warnings[0].__dict__["extra_fields"]
    assert fields["canonical_uri"] == f"{STORAGE_URI}/{failing_path}"
    assert fields["version"] == 0


async def test_second_prune_does_not_retry_content_for_deleted_metadata() -> None:
    store = _RecordingStore([[_record(0)], []])
    backend = _RecordingBackend(failing_paths={"apps/agent_app/users/user-1/artifacts/report.txt/v0"})
    service, _registry = _service(store, backend)
    cutoff = datetime(2024, 6, 1, tzinfo=timezone.utc)

    first = await service.delete_artifacts_older_than(cutoff)
    second = await service.delete_artifacts_older_than(cutoff)

    assert (first, second) == (1, 0)
    assert backend.deleted == ["apps/agent_app/users/user-1/artifacts/report.txt/v0"]


async def test_delete_artifacts_older_than_propagates_metadata_failure() -> None:
    store = _FailingStore([[_record(0)]])
    backend = _RecordingBackend()
    service, registry = _service(store, backend)

    with pytest.raises(RuntimeError, match="metadata delete failed"):
        await service.delete_artifacts_older_than(datetime(2024, 6, 1, tzinfo=timezone.utc))

    assert registry.resolved == []
    assert backend.deleted == []


async def test_delete_artifact_cleans_up_content_for_every_version() -> None:
    store = _RecordingStore([[_record(0), _record(1)]])
    backend = _RecordingBackend()
    service, _registry = _service(store, backend)

    await service.delete_artifact(app_name="agent_app", user_id="user-1", filename="report.txt")

    assert store.delete_calls == [("agent_app", "user-1", "report.txt", None)]
    assert backend.deleted == [
        "apps/agent_app/users/user-1/artifacts/report.txt/v0",
        "apps/agent_app/users/user-1/artifacts/report.txt/v1",
    ]
