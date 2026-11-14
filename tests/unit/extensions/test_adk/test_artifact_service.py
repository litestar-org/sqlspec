"""Unit tests for SQLSpec artifact service content coordination."""

import logging
from datetime import UTC, datetime
from typing import Any

import pytest
from google.adk.errors.input_validation_error import InputValidationError
from google.genai import types

from sqlspec.extensions.adk.artifact._types import StoredArtifact
from sqlspec.extensions.adk.artifact.service import _REWIND_TOMBSTONE, SQLSpecArtifactService

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
        created_at=datetime(2020, 1, 1, tzinfo=UTC),
    )


class _RecordingBackend:
    """Storage backend double recording read, write, and delete attempts."""

    def __init__(self, failing_paths: "set[str] | None" = None) -> None:
        self.deleted: list[str] = []
        self.blobs: dict[str, bytes] = {}
        self.failing_paths = failing_paths or set()

    async def write_bytes_async(self, path: str, data: bytes) -> None:
        self.blobs[path] = data

    async def read_bytes_async(self, path: str) -> bytes:
        return self.blobs[path]

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
        self.saved_records: dict[tuple[str, str, str | None, str], list[StoredArtifact]] = {}
        self.batches = batches if batches is not None else [[]]

    async def get_next_version(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> int:
        key = (app_name, user_id, session_id, filename)
        return len(self.saved_records.get(key, []))

    async def insert_artifact(self, record: StoredArtifact) -> None:
        key = (record["app_name"], record["user_id"], record["session_id"], record["filename"])
        self.saved_records.setdefault(key, []).append(record)

    async def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None, version: "int | None" = None
    ) -> "StoredArtifact | None":
        key = (app_name, user_id, session_id, filename)
        versions = self.saved_records.get(key, [])
        if not versions:
            return None
        if version is None:
            return versions[-1]
        for rec in versions:
            if rec["version"] == version:
                return rec
        return None

    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[StoredArtifact]":
        key = (app_name, user_id, session_id, filename)
        return list(self.saved_records.get(key, []))

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


def _service(
    store: Any, backend: _RecordingBackend, uri: str = STORAGE_URI
) -> "tuple[SQLSpecArtifactService, _RecordingRegistry]":
    registry: Any = _RecordingRegistry(backend)
    service = SQLSpecArtifactService(store, uri, registry=registry)
    return service, registry


async def test_delete_artifacts_older_than_forwards_cutoff_and_app_scope() -> None:
    store = _RecordingStore([[_record(0), _record(1)]])
    backend = _RecordingBackend()
    service, _registry = _service(store, backend)
    cutoff = datetime(2024, 6, 1, tzinfo=UTC)

    deleted = await service.delete_artifacts_older_than(cutoff, app_name="agent_app")

    assert deleted == 2
    assert store.retention_calls == [(cutoff, "agent_app")]


async def test_delete_artifacts_older_than_deletes_content_for_every_version() -> None:
    store = _RecordingStore([[_record(0), _record(1), _record(0, filename="notes.txt")]])
    backend = _RecordingBackend()
    service, registry = _service(store, backend)

    deleted = await service.delete_artifacts_older_than(datetime(2024, 6, 1, tzinfo=UTC))

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

    deleted = await service.delete_artifacts_older_than(datetime(2024, 6, 1, tzinfo=UTC))

    assert deleted == 0
    assert registry.resolved == []
    assert backend.deleted == []


async def test_delete_artifacts_older_than_continues_after_content_failure(caplog: pytest.LogCaptureFixture) -> None:
    failing_path = "apps/agent_app/users/user-1/artifacts/report.txt/v0"
    store = _RecordingStore([[_record(0), _record(1)]])
    backend = _RecordingBackend(failing_paths={failing_path})
    service, _registry = _service(store, backend)

    with caplog.at_level(logging.WARNING, logger="sqlspec.extensions.adk.artifact.service"):
        deleted = await service.delete_artifacts_older_than(datetime(2024, 6, 1, tzinfo=UTC))

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
    cutoff = datetime(2024, 6, 1, tzinfo=UTC)

    first = await service.delete_artifacts_older_than(cutoff)
    second = await service.delete_artifacts_older_than(cutoff)

    assert (first, second) == (1, 0)
    assert backend.deleted == ["apps/agent_app/users/user-1/artifacts/report.txt/v0"]


async def test_delete_artifacts_older_than_propagates_metadata_failure() -> None:
    store = _FailingStore([[_record(0)]])
    backend = _RecordingBackend()
    service, registry = _service(store, backend)

    with pytest.raises(RuntimeError, match="metadata delete failed"):
        await service.delete_artifacts_older_than(datetime(2024, 6, 1, tzinfo=UTC))

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


async def test_user_namespace_filename_auto_scopes_session_id_to_none() -> None:
    """Filenames prefixed with user: automatically store, load, list, and delete under session_id=None."""
    store = _RecordingStore()
    backend = _RecordingBackend()
    service, _registry = _service(store, backend)

    part = types.Part.from_text(text="user profile note")
    ver = await service.save_artifact(
        app_name="agent_app", user_id="user-1", filename="user:profile.txt", artifact=part, session_id="sess-1"
    )
    assert ver == 0
    assert ("agent_app", "user-1", None, "user:profile.txt") in store.saved_records

    loaded = await service.load_artifact(
        app_name="agent_app", user_id="user-1", filename="user:profile.txt", session_id="sess-2"
    )
    assert loaded is not None
    assert loaded.text == "user profile note"

    versions = await service.list_versions(
        app_name="agent_app", user_id="user-1", filename="user:profile.txt", session_id="sess-2"
    )
    assert versions == [0]

    meta = await service.get_artifact_version(
        app_name="agent_app", user_id="user-1", filename="user:profile.txt", session_id="sess-2"
    )
    assert meta is not None
    assert meta.version == 0

    await service.delete_artifact(
        app_name="agent_app", user_id="user-1", filename="user:profile.txt", session_id="sess-2"
    )
    assert store.delete_calls[-1] == ("agent_app", "user-1", "user:profile.txt", None)


async def test_session_id_validation_raises_input_validation_error() -> None:
    """Missing session_id on session-scoped save or reserved user* session_id raises InputValidationError."""
    store = _RecordingStore()
    backend = _RecordingBackend()
    service, _registry = _service(store, backend)

    with pytest.raises(InputValidationError, match="Session ID must be provided"):
        await service.save_artifact(
            app_name="agent_app",
            user_id="user-1",
            filename="report.txt",
            artifact=types.Part.from_text(text="hello"),
            session_id=None,
        )

    with pytest.raises(InputValidationError, match="reserved value 'user'"):
        await service.save_artifact(
            app_name="agent_app",
            user_id="user-1",
            filename="report.txt",
            artifact=types.Part.from_text(text="hello"),
            session_id="user",
        )

    with pytest.raises(InputValidationError, match="traversal segments"):
        await service.load_artifact(
            app_name="agent_app", user_id="user-1", filename="report.txt", session_id="../escape"
        )


async def test_artifact_reference_validation_and_recursive_resolution() -> None:
    """artifact:// references are scope-validated on save and recursively resolved on load."""
    store = _RecordingStore()
    backend = _RecordingBackend()
    service, _registry = _service(store, backend)

    await service.save_artifact(
        app_name="agent_app",
        user_id="user-1",
        filename="target.txt",
        artifact=types.Part.from_text(text="resolved payload"),
        session_id="sess-1",
    )

    valid_ref = types.Part(
        file_data=types.FileData(
            file_uri="artifact://apps/agent_app/users/user-1/sessions/sess-1/artifacts/target.txt/versions/0"
        )
    )
    await service.save_artifact(
        app_name="agent_app", user_id="user-1", filename="alias.txt", artifact=valid_ref, session_id="sess-1"
    )

    resolved = await service.load_artifact(
        app_name="agent_app", user_id="user-1", filename="alias.txt", session_id="sess-1"
    )
    assert resolved is not None
    assert resolved.text == "resolved payload"

    cross_user_ref = types.Part(
        file_data=types.FileData(
            file_uri="artifact://apps/agent_app/users/other-user/sessions/sess-1/artifacts/target.txt/versions/0"
        )
    )
    with pytest.raises(InputValidationError, match="same app and user scope"):
        await service.save_artifact(
            app_name="agent_app", user_id="user-1", filename="bad_ref.txt", artifact=cross_user_ref, session_id="sess-1"
        )

    self_ref = types.Part(
        file_data=types.FileData(
            file_uri="artifact://apps/agent_app/users/user-1/sessions/sess-1/artifacts/cycle.txt/versions/0"
        )
    )
    await service.save_artifact(
        app_name="agent_app", user_id="user-1", filename="cycle.txt", artifact=self_ref, session_id="sess-1"
    )
    with pytest.raises(InputValidationError, match="Exceeded maximum recursion depth"):
        await service.load_artifact(app_name="agent_app", user_id="user-1", filename="cycle.txt", session_id="sess-1")


async def test_load_artifact_returns_none_for_rewind_tombstone() -> None:
    """load_artifact returns None when the stored Part equals _REWIND_TOMBSTONE or empty Part()."""
    store = _RecordingStore()
    backend = _RecordingBackend()
    service, _registry = _service(store, backend)

    await service.save_artifact(
        app_name="agent_app", user_id="user-1", filename="rewound.bin", artifact=_REWIND_TOMBSTONE, session_id="sess-1"
    )
    loaded = await service.load_artifact(
        app_name="agent_app", user_id="user-1", filename="rewound.bin", session_id="sess-1"
    )
    assert loaded is None


@pytest.mark.parametrize(
    ("uri", "expected_base_uri", "expected_prefix"),
    [
        ("s3://bucket/tenant-a", "s3://bucket/tenant-a", "tenant-a"),
        ("s3://bucket/tenant-a/", "s3://bucket/tenant-a", "tenant-a"),
        ("gs://bucket/prefix/nested", "gs://bucket/prefix/nested", "prefix/nested"),
        ("az://container/tenant-b", "az://container/tenant-b", "tenant-b"),
    ],
)
async def test_cloud_uri_prefix_preserved_across_save_load_and_delete(
    uri: str, expected_base_uri: str, expected_prefix: str
) -> None:
    """Cloud URIs with non-empty path prefixes write, read, and delete under that prefix matching canonical_uri."""
    store = _RecordingStore()
    backend = _RecordingBackend()
    service, registry = _service(store, backend, uri)

    version = await service.save_artifact(
        app_name="agent_app",
        user_id="user-1",
        filename="report.txt",
        artifact=types.Part.from_text(text="scoped payload"),
        session_id="sess-1",
    )

    assert version == 0
    expected_relative = "apps/agent_app/users/user-1/sessions/sess-1/artifacts/report.txt/v0"
    expected_key = f"{expected_prefix}/{expected_relative}"
    assert list(backend.blobs) == [expected_key]
    saved_record = store.saved_records[("agent_app", "user-1", "sess-1", "report.txt")][0]
    assert saved_record["canonical_uri"] == f"{expected_base_uri}/{expected_relative}"

    loaded = await service.load_artifact(
        app_name="agent_app", user_id="user-1", filename="report.txt", session_id="sess-1"
    )
    assert loaded is not None
    assert loaded.text == "scoped payload"

    store.batches = [[saved_record]]
    await service.delete_artifact(app_name="agent_app", user_id="user-1", filename="report.txt", session_id="sess-1")
    assert backend.deleted == [expected_key]
    assert registry.resolved == [expected_base_uri, expected_base_uri, expected_base_uri]


@pytest.mark.parametrize("uri", ["s3://bucket", "s3://bucket/", "file:///artifacts", "artifact-alias"])
async def test_root_cloud_file_and_alias_uris_keep_unprefixed_keys(uri: str) -> None:
    """Root cloud bucket URIs, file:// URIs, and aliases keep keys relative to apps/."""
    store = _RecordingStore()
    backend = _RecordingBackend()
    service, _registry = _service(store, backend, uri)

    await service.save_artifact(
        app_name="agent_app",
        user_id="user-1",
        filename="report.txt",
        artifact=types.Part.from_text(text="root payload"),
        session_id="sess-1",
    )

    assert list(backend.blobs) == ["apps/agent_app/users/user-1/sessions/sess-1/artifacts/report.txt/v0"]


@pytest.mark.parametrize(
    ("app_name", "user_id", "filename"),
    [
        ("agent_app/", "user-1", "report.txt"),
        ("agent_app", "bob/", "report.txt"),
        ("agent_app", "/bob", "report.txt"),
        ("agent_app", ".", "report.txt"),
        ("agent_app", "user-1", "*.txt"),
        ("agent_app", "user-1", "report?.txt"),
        ("agent_app", "user-1", "report[1].txt"),
    ],
)
async def test_save_artifact_rejects_unsafe_path_components(app_name: str, user_id: str, filename: str) -> None:
    """save_artifact rejects slash-padded, dot, and glob path components before writing bytes."""
    store = _RecordingStore()
    backend = _RecordingBackend()
    service, _registry = _service(store, backend)

    with pytest.raises(ValueError, match="Unsafe path component"):
        await service.save_artifact(
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            artifact=types.Part.from_text(text="rejected"),
            session_id="sess-1",
        )
    assert backend.blobs == {}
