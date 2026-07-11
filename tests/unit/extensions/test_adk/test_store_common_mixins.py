"""Regression tests for common ADK store state mixins."""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.extensions.adk.artifact._types import ArtifactRecord
from sqlspec.extensions.adk.artifact.store import (
    BaseAsyncADKArtifactStore,
    BaseSyncADKArtifactStore,
    _ADKArtifactStoreCommon,
)
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore, _ADKMemoryStoreCommon
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore, _ADKStoreCommon


class _ConcreteArtifactStore(BaseSyncADKArtifactStore[SqliteConfig]):
    __slots__ = ()

    def insert_artifact(self, record: ArtifactRecord) -> None:
        return None

    def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None, version: int | None = None
    ) -> ArtifactRecord | None:
        return None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        return []

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[ArtifactRecord]:
        return []

    def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[ArtifactRecord]:
        return []

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        return 0

    def create_table(self) -> None:
        return None


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
