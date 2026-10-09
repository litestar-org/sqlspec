"""Unit tests for SpannerSyncADKArtifactStore and SpannerAsyncADKArtifactStore."""

from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

from google.api_core.exceptions import NotFound
from google.cloud.spanner_v1 import param_types

from sqlspec.adapters.spanner.adk import SpannerAsyncADKArtifactStore, SpannerSyncADKArtifactStore
from sqlspec.adapters.spanner.adk.artifact_store import USER_SCOPED_SESSION_ID
from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.extensions.adk.artifact._types import StoredArtifact


class _FakeAsyncIterator:
    """Async iterator wrapper for fake Spanner async result sets."""

    def __init__(self, items: list[Any]) -> None:
        self._iter = iter(items)

    def __aiter__(self) -> "_FakeAsyncIterator":
        return self

    async def __anext__(self) -> Any:
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


def _make_sync_config(adk_ext: dict[str, Any] | None = None) -> SpannerSyncConfig:
    ext = {"adk": adk_ext} if adk_ext else {}
    return SpannerSyncConfig(
        connection_config={"project": "test-project", "instance_id": "test-instance", "database_id": "test-db"},
        extension_config=cast("Any", ext),
    )


def _make_async_config(adk_ext: dict[str, Any] | None = None) -> SpannerAsyncConfig:
    ext = {"adk": adk_ext} if adk_ext else {}
    return SpannerAsyncConfig(
        connection_config={"project": "test-project", "instance_id": "test-instance", "database_id": "test-db"},
        extension_config=cast("Any", ext),
    )


def _sample_record(
    version: int = 0,
    session_id: str | None = "sess-1",
    filename: str = "report.pdf",
    custom_metadata: dict[str, Any] | None = None,
) -> StoredArtifact:
    return StoredArtifact(
        app_name="app-1",
        user_id="user-1",
        session_id=session_id,
        filename=filename,
        version=version,
        mime_type="application/pdf",
        canonical_uri=f"gs://bucket/app-1/user-1/{filename}/v{version}",
        custom_metadata=custom_metadata if custom_metadata is not None else {"author": "agent"},
        created_at=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
    )


def test_default_table_name_and_user_scoped_sentinel() -> None:
    """Default artifact table is adk_artifact and user-scoped sentinel is empty string."""
    store = SpannerSyncADKArtifactStore(_make_sync_config())
    assert store.artifact_table == "adk_artifact"
    assert USER_SCOPED_SESSION_ID == ""


def test_artifact_table_ddl_and_ttl_policy() -> None:
    """_artifact_table_ddl emits Spanner composite PK and optional ROW DELETION POLICY."""
    store = SpannerSyncADKArtifactStore(_make_sync_config())
    ddl = store._artifact_table_ddl()
    assert len(ddl) == 1
    stmt = ddl[0]
    assert "CREATE TABLE IF NOT EXISTS adk_artifact" in stmt
    assert "app_name STRING(128) NOT NULL" in stmt
    assert "user_id STRING(128) NOT NULL" in stmt
    assert "session_id STRING(128) NOT NULL" in stmt
    assert "filename STRING(512) NOT NULL" in stmt
    assert "version INT64 NOT NULL" in stmt
    assert "mime_type STRING(256)" in stmt
    assert "canonical_uri STRING(2048) NOT NULL" in stmt
    assert "custom_metadata JSON" in stmt
    assert "created_at TIMESTAMP NOT NULL" in stmt
    assert "PRIMARY KEY (app_name, user_id, session_id, filename, version)" in stmt
    assert "ROW DELETION POLICY" not in stmt

    ttl_store = SpannerSyncADKArtifactStore(_make_sync_config({"retention": {"artifact_ttl_seconds": 86_400}}))
    ttl_ddl = ttl_store._artifact_table_ddl()[0]
    assert "ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 1 DAY))" in ttl_ddl


def test_sync_create_and_drop_table_idempotency(mocker: Any) -> None:
    """create_table skips DDL when table exists and drop_table drops only when present."""
    store = SpannerSyncADKArtifactStore(_make_sync_config())
    existing_table = MagicMock()
    existing_table.table_id = "adk_artifact"

    mock_db = MagicMock()
    mock_db.list_tables.return_value = [existing_table]
    mocker.patch.object(SpannerSyncADKArtifactStore, "_database", return_value=mock_db)

    store.create_table()
    mock_db.update_ddl.assert_not_called()

    store.drop_table()
    mock_db.update_ddl.assert_called_once_with(["DROP TABLE adk_artifact"])

    mock_db.reset_mock()
    mock_db.list_tables.return_value = []
    op = MagicMock()
    mock_db.update_ddl.return_value = op

    store.create_table()
    mock_db.update_ddl.assert_called_once()
    op.result.assert_called_once()

    mock_db.reset_mock()
    store.drop_table()
    mock_db.update_ddl.assert_not_called()


def test_sync_insert_and_get_artifact_user_and_session_scoped(mocker: Any) -> None:
    """insert_artifact normalizes session_id=None to '' and get_artifact restores None."""
    store = SpannerSyncADKArtifactStore(_make_sync_config())
    tx = MagicMock()
    mock_db = MagicMock()
    mock_db.run_in_transaction.side_effect = lambda fn: fn(tx)
    mocker.patch.object(SpannerSyncADKArtifactStore, "_database", return_value=mock_db)

    user_record = _sample_record(version=0, session_id=None)
    store.insert_artifact(user_record)

    tx.execute_update.assert_called_once()
    insert_sql = tx.execute_update.call_args.args[0]
    insert_params = tx.execute_update.call_args.kwargs["params"]
    assert "INSERT INTO adk_artifact" in insert_sql
    assert insert_params["session_id"] == ""
    assert insert_params["version"] == 0

    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    fake_row = (
        "app-1",
        "user-1",
        "",
        "report.pdf",
        0,
        "application/pdf",
        "gs://bucket/app-1/user-1/report.pdf/v0",
        '{"author":"agent"}',
        now,
    )
    read_mock = mocker.patch.object(SpannerSyncADKArtifactStore, "_run_read", return_value=[fake_row])

    loaded_latest = store.get_artifact("app-1", "user-1", "report.pdf", session_id=None, version=None)
    assert loaded_latest is not None
    assert loaded_latest["session_id"] is None
    assert loaded_latest["custom_metadata"] == {"author": "agent"}
    assert "ORDER BY version DESC LIMIT 1" in read_mock.call_args.args[0]
    assert read_mock.call_args.args[1]["session_id"] == ""

    loaded_exact = store.get_artifact("app-1", "user-1", "report.pdf", session_id="sess-1", version=2)
    assert loaded_exact is not None
    assert "AND version = @version" in read_mock.call_args.args[0]
    assert read_mock.call_args.args[1]["version"] == 2
    assert read_mock.call_args.args[1]["session_id"] == "sess-1"

    mocker.patch.object(
        SpannerSyncADKArtifactStore, "_run_read", side_effect=cast("Any", NotFound)("Table adk_artifact not found")
    )
    assert store.get_artifact("app-1", "user-1", "report.pdf") is None


def test_sync_list_versions_keys_and_next_version(mocker: Any) -> None:
    """list_artifact_versions, list_artifact_keys, and get_next_version build expected Spanner queries."""
    store = SpannerSyncADKArtifactStore(_make_sync_config())
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    rows = [
        ("app-1", "user-1", "sess-1", "report.pdf", 0, "application/pdf", "gs://b/v0", None, now),
        ("app-1", "user-1", "sess-1", "report.pdf", 1, "application/pdf", "gs://b/v1", {"k": 1}, now),
    ]
    read_mock = mocker.patch.object(SpannerSyncADKArtifactStore, "_run_read", return_value=rows)

    versions = store.list_artifact_versions("app-1", "user-1", "report.pdf", session_id="sess-1")
    assert [v["version"] for v in versions] == [0, 1]
    assert versions[0]["custom_metadata"] is None
    assert versions[1]["custom_metadata"] == {"k": 1}
    assert "ORDER BY version ASC" in read_mock.call_args.args[0]

    read_mock.return_value = [("a.txt",), ("b.txt",)]
    keys_with_session = store.list_artifact_keys("app-1", "user-1", session_id="sess-1")
    assert keys_with_session == ["a.txt", "b.txt"]
    assert "session_id IN ('', @session_id)" in read_mock.call_args.args[0]

    keys_user_only = store.list_artifact_keys("app-1", "user-1", session_id=None)
    assert keys_user_only == ["a.txt", "b.txt"]
    assert "session_id = ''" in read_mock.call_args.args[0]

    read_mock.return_value = [(3,)]
    next_ver = store.get_next_version("app-1", "user-1", "report.pdf", session_id="sess-1")
    assert next_ver == 3
    assert "COALESCE(MAX(version) + 1, 0)" in read_mock.call_args.args[0]


def test_sync_delete_artifact_and_delete_older_than(mocker: Any) -> None:
    """delete_artifact and delete_artifacts_older_than atomically query and delete inside run_in_transaction."""
    store = SpannerSyncADKArtifactStore(_make_sync_config())
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    row = ("app-1", "user-1", "sess-1", "report.pdf", 0, "application/pdf", "gs://b/v0", None, now)

    tx = MagicMock()
    tx.execute_sql.return_value = [row]
    tx.execute_update.return_value = 1

    mock_db = MagicMock()
    mock_db.run_in_transaction.side_effect = lambda fn: fn(tx)
    mocker.patch.object(SpannerSyncADKArtifactStore, "_database", return_value=mock_db)

    deleted = store.delete_artifact("app-1", "user-1", "report.pdf", session_id="sess-1")
    assert len(deleted) == 1
    assert deleted[0]["canonical_uri"] == "gs://b/v0"
    tx.execute_sql.assert_called_once()
    tx.execute_update.assert_called_once()
    assert "DELETE FROM adk_artifact" in tx.execute_update.call_args.args[0]

    tx.reset_mock()
    tx.execute_sql.return_value = [row]
    cutoff = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    swept = store.delete_artifacts_older_than(cutoff, app_name="app-1")
    assert len(swept) == 1
    assert "created_at < @before" in tx.execute_sql.call_args.args[0]
    assert "app_name = @app_name" in tx.execute_sql.call_args.args[0]
    assert tx.execute_sql.call_args.kwargs["param_types"]["before"] == param_types.TIMESTAMP
    assert (
        "DELETE FROM adk_artifact WHERE created_at < @before AND app_name = @app_name"
        in tx.execute_update.call_args.args[0]
    )


async def test_async_artifact_store_operations(mocker: Any) -> None:
    """SpannerAsyncADKArtifactStore executes async snapshot reads and transactional writes."""
    store = SpannerAsyncADKArtifactStore(_make_async_config())
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    row = ("app-1", "user-1", "", "notes.md", 0, "text/markdown", "gs://b/notes/v0", '{"tag":"x"}', now)

    tx = MagicMock()
    tx.execute_sql = AsyncMock(return_value=_FakeAsyncIterator([row]))
    tx.execute_update = AsyncMock(return_value=1)

    snapshot = MagicMock()
    snapshot.execute_sql = AsyncMock(return_value=_FakeAsyncIterator([row]))
    snapshot_cm = MagicMock()
    snapshot_cm.__aenter__ = AsyncMock(return_value=snapshot)
    snapshot_cm.__aexit__ = AsyncMock(return_value=None)

    mock_db = MagicMock()
    mock_db.snapshot.return_value = snapshot_cm

    async def _run_in_tx(fn: Any) -> Any:
        return await fn(tx)

    mock_db.run_in_transaction = AsyncMock(side_effect=_run_in_tx)
    mocker.patch.object(SpannerAsyncADKArtifactStore, "_database", AsyncMock(return_value=mock_db))

    record = _sample_record(version=0, session_id=None, filename="notes.md")
    await store.insert_artifact(record)
    tx.execute_update.assert_awaited_once()

    snapshot.execute_sql = AsyncMock(return_value=_FakeAsyncIterator([row]))
    fetched = await store.get_artifact("app-1", "user-1", "notes.md", session_id=None)
    assert fetched is not None
    assert fetched["session_id"] is None
    assert fetched["custom_metadata"] == {"tag": "x"}

    snapshot.execute_sql = AsyncMock(return_value=_FakeAsyncIterator([row]))
    versions = await store.list_artifact_versions("app-1", "user-1", "notes.md", session_id=None)
    assert len(versions) == 1

    snapshot.execute_sql = AsyncMock(return_value=_FakeAsyncIterator([("notes.md",)]))
    keys = await store.list_artifact_keys("app-1", "user-1", session_id=None)
    assert keys == ["notes.md"]

    snapshot.execute_sql = AsyncMock(return_value=_FakeAsyncIterator([(1,)]))
    next_ver = await store.get_next_version("app-1", "user-1", "notes.md", session_id=None)
    assert next_ver == 1

    tx.execute_update.reset_mock()
    tx.execute_sql = AsyncMock(return_value=_FakeAsyncIterator([row]))
    deleted = await store.delete_artifact("app-1", "user-1", "notes.md", session_id=None)
    assert len(deleted) == 1
    tx.execute_update.assert_awaited_once()

    tx.execute_update.reset_mock()
    tx.execute_sql = AsyncMock(return_value=_FakeAsyncIterator([row]))
    swept = await store.delete_artifacts_older_than(now)
    assert len(swept) == 1
    tx.execute_update.assert_awaited_once()
