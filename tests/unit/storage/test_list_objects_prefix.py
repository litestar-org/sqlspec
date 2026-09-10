"""Prefix filtering must work for a local-backed store that has a base_path.

The base path is baked into the store root at construction, so resolving a
caller-supplied prefix against it again looks for the segment twice.
"""

from pathlib import Path

import pytest

from sqlspec.typing import OBSTORE_INSTALLED

pytestmark = pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")

LAYOUT = ("nested/data/a.parquet", "nested/data/b.parquet", "nested/other/c.parquet", "nested/top.parquet")


@pytest.fixture
def based_root(tmp_path: Path) -> Path:
    for rel in LAYOUT:
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x")
    return tmp_path


def test_local_store_with_base_path_filters_by_prefix(based_root: Path) -> None:
    """A prefix must scope the listing rather than returning nothing."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{based_root}", base_path="nested")

    assert store.list_objects_sync(prefix="data/") == ["data/a.parquet", "data/b.parquet"]


def test_local_store_with_base_path_lists_everything_without_prefix(based_root: Path) -> None:
    """The no-prefix path already worked and must keep working."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{based_root}", base_path="nested")

    assert store.list_objects_sync() == ["data/a.parquet", "data/b.parquet", "other/c.parquet", "top.parquet"]


def test_local_store_without_base_path_filters_by_prefix(tmp_path: Path) -> None:
    """A store with no base_path is unaffected."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    for rel in ("data/a.parquet", "top.parquet"):
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x")
    store = ObStoreBackend(f"file://{tmp_path}")

    assert store.list_objects_sync(prefix="data/") == ["data/a.parquet"]


def test_prefix_is_segment_aligned(based_root: Path) -> None:
    """A partial segment must not match, because obstore prefixes are per segment."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{based_root}", base_path="nested")

    assert store.list_objects_sync(prefix="dat") == []


@pytest.mark.anyio
async def test_local_store_with_base_path_filters_by_prefix_async(based_root: Path) -> None:
    """The async twin has the same defect and must be fixed with it."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{based_root}", base_path="nested")

    assert await store.list_objects_async(prefix="data/") == ["data/a.parquet", "data/b.parquet"]
