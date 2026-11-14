"""Regression tests for common ADK store state mixins."""

import inspect
from datetime import UTC, datetime, timedelta

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.extensions.adk.artifact._types import StoredArtifact
from sqlspec.extensions.adk.artifact.store import (
    BaseAsyncADKArtifactStore,
    BaseSyncADKArtifactStore,
    _ADKArtifactStoreCommon,
)
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore, _ADKMemoryStoreCommon
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore, _ADKStoreCommon


class _ConcreteArtifactStore(BaseSyncADKArtifactStore[SqliteConfig]):
    __slots__ = ()

    def insert_artifact(self, record: StoredArtifact) -> None:
        return None

    def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None, version: int | None = None
    ) -> StoredArtifact | None:
        return None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        return []

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        return []

    def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        return []

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        return 0

    def create_table(self) -> None:
        return None


class _ConcreteAsyncArtifactStore(BaseAsyncADKArtifactStore[SqliteConfig]):
    __slots__ = ()

    async def insert_artifact(self, record: StoredArtifact) -> None:
        return None

    async def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None, version: int | None = None
    ) -> StoredArtifact | None:
        return None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        return []

    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        return []

    async def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        return []

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        return 0

    async def create_table(self) -> None:
        return None


def _aged_record(version: int) -> StoredArtifact:
    return StoredArtifact(
        app_name="agent_app",
        user_id="user-1",
        session_id=None,
        filename="report.txt",
        version=version,
        mime_type="text/plain",
        canonical_uri=f"file:///base/apps/agent_app/users/user-1/artifacts/report.txt/v{version}",
        custom_metadata=None,
        created_at=datetime(2020, 1, 1, tzinfo=UTC),
    )


class _RetainingArtifactStore(_ConcreteAsyncArtifactStore):
    __slots__ = ("calls",)

    def __init__(self, config: SqliteConfig) -> None:
        super().__init__(config)
        self.calls: list[tuple[datetime, str | None]] = []

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        self.calls.append((before, app_name))
        return [_aged_record(0), _aged_record(1)]


def _sqlite_config() -> SqliteConfig:
    return SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={
            "adk": {
                "session_table": "agent_sessions",
                "events_table": "agent_events",
                "app_state_table": "agent_app_state",
                "user_state_table": "agent_user_state",
                "metadata_table": "agent_metadata",
                "memory_table": "agent_memory",
                "artifact_table": "agent_artifact",
                "owner_id_column": "tenant_id INTEGER",
            }
        },
    )


def test_session_store_bases_share_slotted_common_state() -> None:
    assert issubclass(BaseAsyncADKStore, _ADKStoreCommon)
    assert issubclass(BaseSyncADKStore, _ADKStoreCommon)
    assert BaseAsyncADKStore.__slots__ == ()
    assert BaseSyncADKStore.__slots__ == ()


def test_memory_store_bases_share_slotted_common_state() -> None:
    assert issubclass(BaseAsyncADKMemoryStore, _ADKMemoryStoreCommon)
    assert issubclass(BaseSyncADKMemoryStore, _ADKMemoryStoreCommon)
    assert BaseAsyncADKMemoryStore.__slots__ == ()
    assert BaseSyncADKMemoryStore.__slots__ == ()


def test_artifact_store_bases_share_slotted_common_state() -> None:
    assert issubclass(BaseAsyncADKArtifactStore, _ADKArtifactStoreCommon)
    assert issubclass(BaseSyncADKArtifactStore, _ADKArtifactStoreCommon)
    assert BaseAsyncADKArtifactStore.__slots__ == ()
    assert BaseSyncADKArtifactStore.__slots__ == ()


@pytest.mark.parametrize(
    "store",
    [
        pytest.param(SqliteADKStore(_sqlite_config()), id="session"),
        pytest.param(SqliteADKMemoryStore(_sqlite_config()), id="memory"),
        pytest.param(_ConcreteArtifactStore(_sqlite_config()), id="artifact"),
    ],
)
def test_concrete_adk_stores_reject_undeclared_attributes(store: object) -> None:
    with pytest.raises(AttributeError):
        setattr(store, "undeclared", True)


def test_artifact_store_subclasses_stay_instantiable_without_retention_override() -> None:
    config = _sqlite_config()

    assert _ConcreteArtifactStore(config).artifact_table == "agent_artifact"
    assert _ConcreteAsyncArtifactStore(config).artifact_table == "agent_artifact"


def test_sync_artifact_store_retention_hook_defaults_to_not_implemented() -> None:
    store = _ConcreteArtifactStore(_sqlite_config())

    with pytest.raises(NotImplementedError, match="does not support artifact retention"):
        store.delete_artifacts_older_than(datetime(2024, 1, 1, tzinfo=UTC))


async def test_async_artifact_store_retention_hook_defaults_to_not_implemented() -> None:
    store = _ConcreteAsyncArtifactStore(_sqlite_config())

    with pytest.raises(NotImplementedError, match="does not support artifact retention"):
        await store.delete_artifacts_older_than(datetime(2024, 1, 1, tzinfo=UTC))


async def test_artifact_store_retention_override_receives_cutoff_and_app_name() -> None:
    store = _RetainingArtifactStore(_sqlite_config())
    cutoff = datetime(2024, 6, 1, tzinfo=UTC)

    records = await store.delete_artifacts_older_than(cutoff, app_name="agent_app")

    assert store.calls == [(cutoff, "agent_app")]
    assert [record["version"] for record in records] == [0, 1]
    assert records[0]["canonical_uri"].endswith("/report.txt/v0")


def test_concrete_session_store_exposes_all_shared_properties() -> None:
    config = _sqlite_config()
    store = SqliteADKStore(config)

    assert {
        "config": store.config,
        "session_table": store.session_table,
        "events_table": store.events_table,
        "app_state_table": store.app_state_table,
        "user_state_table": store.user_state_table,
        "metadata_table": store.metadata_table,
        "owner_id_column_ddl": store.owner_id_column_ddl,
        "owner_id_column_name": store.owner_id_column_name,
    } == {
        "config": config,
        "session_table": "agent_sessions",
        "events_table": "agent_events",
        "app_state_table": "agent_app_state",
        "user_state_table": "agent_user_state",
        "metadata_table": "agent_metadata",
        "owner_id_column_ddl": "tenant_id INTEGER",
        "owner_id_column_name": "tenant_id",
    }


@pytest.mark.parametrize(
    ("expires_in", "expected_kind", "expected_delta_seconds"),
    [
        pytest.param(None, "none", 0.0, id="none"),
        pytest.param(timedelta(milliseconds=250), "future", 0.25, id="subsecond-timedelta"),
        pytest.param(5, "future", 5.0, id="positive-int"),
        pytest.param(0, "past", 0.0, id="zero-int"),
        pytest.param(-5, "past", 0.0, id="negative-int"),
        pytest.param(timedelta(0), "past", 0.0, id="zero-timedelta"),
        pytest.param(timedelta(seconds=-1), "past", 0.0, id="negative-timedelta"),
    ],
)
def test_adk_store_common_calculate_expires_at_precision_and_non_positive_expiry(
    expires_in: int | timedelta | None, expected_kind: str, expected_delta_seconds: float
) -> None:
    store = SqliteADKStore(_sqlite_config())
    before = datetime.now(UTC)
    result = store._calculate_expires_at(expires_in)
    after = datetime.now(UTC)

    if expected_kind == "none":
        assert result is None
    elif expected_kind == "past":
        assert result is not None
        assert before - timedelta(microseconds=1) <= result <= after - timedelta(microseconds=1)
    else:
        assert result is not None
        lower = before + timedelta(seconds=expected_delta_seconds) - timedelta(milliseconds=25)
        upper = after + timedelta(seconds=expected_delta_seconds) + timedelta(milliseconds=25)
        assert lower <= result <= upper


@pytest.mark.parametrize(
    ("renew_for", "expected"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero-int"),
        pytest.param(-1, False, id="negative-int"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=-5), False, id="negative-timedelta"),
        pytest.param(1, True, id="positive-int"),
        pytest.param(timedelta(milliseconds=1), True, id="subsecond-timedelta"),
    ],
)
def test_adk_store_common_should_renew_requires_positive_duration(
    renew_for: int | timedelta | None, expected: bool
) -> None:
    store = SqliteADKStore(_sqlite_config())
    assert store._should_renew(renew_for) is expected


@pytest.mark.parametrize(
    "base_cls",
    [
        pytest.param(BaseAsyncADKMemoryStore, id="async-memory-base"),
        pytest.param(BaseSyncADKMemoryStore, id="sync-memory-base"),
    ],
)
def test_memory_store_base_delete_entries_by_session_signature(base_cls: type) -> None:
    """Both abstract memory bases expose optional app_name and user_id owner filters."""
    params = inspect.signature(base_cls.delete_entries_by_session).parameters
    assert list(params) == ["self", "session_id", "app_name", "user_id"]
    assert params["app_name"].default is None
    assert params["user_id"].default is None


def test_concrete_memory_store_accepts_owner_filters_through_base_reference() -> None:
    """A concrete memory store called via a BaseSyncADKMemoryStore reference accepts owner filters."""
    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {"enable_memory": True}})
    base_ref: BaseSyncADKMemoryStore[SqliteConfig] = SqliteADKMemoryStore(config)
    base_ref.create_tables()
    assert base_ref.delete_entries_by_session("missing-session", app_name="app", user_id="user") == 0


@pytest.mark.parametrize(
    "invalid_ddl",
    [
        pytest.param("", id="empty"),
        pytest.param("   ", id="whitespace"),
        pytest.param("owner_id TEXT DEFAULT 'anon'", id="single-quoted-literal"),
    ],
)
def test_standalone_memory_store_validates_owner_id_column_ddl(invalid_ddl: str) -> None:
    """Standalone memory stores normalize and validate owner_id_column identically to session stores."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"adk": {"enable_memory": True, "owner_id_column": invalid_ddl}},
    )
    with pytest.raises(ValueError, match="owner_id_column"):
        SqliteADKMemoryStore(config)
