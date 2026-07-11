"""Regression tests for common ADK store state mixins."""

from sqlspec.extensions.adk.artifact.store import (
    BaseAsyncADKArtifactStore,
    BaseSyncADKArtifactStore,
    _ADKArtifactStoreCommon,
)
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore, _ADKMemoryStoreCommon
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore, _ADKStoreCommon


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
