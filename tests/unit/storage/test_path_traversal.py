"""Security tests for storage path containment.

A caller-supplied object key must never resolve outside its backend's root.
"""

from pathlib import Path

import pytest

from sqlspec.exceptions import StoragePathTraversalError
from sqlspec.storage._paths import resolve_storage_path
from sqlspec.storage.backends.local import LocalStore
from sqlspec.typing import OBSTORE_INSTALLED

ESCAPING_PATHS = ["../../etc/passwd", "../outside.txt", "nested/../../outside.txt", "..", "../"]


@pytest.mark.parametrize("candidate", ESCAPING_PATHS)
def test_local_store_rejects_escaping_relative_path(tmp_path: Path, candidate: str) -> None:
    """A relative key containing ``..`` must not resolve outside the store root."""
    store = LocalStore(str(tmp_path / "root"), base_path="workspaces")

    with pytest.raises(StoragePathTraversalError):
        store.resolve_uri(candidate)


def test_local_store_rejects_absolute_path_outside_root(tmp_path: Path) -> None:
    """An absolute key outside the root must be refused, not silently honoured."""
    store = LocalStore(str(tmp_path / "root"))

    with pytest.raises(StoragePathTraversalError):
        store.resolve_uri("/etc/passwd")


def test_local_store_allows_nested_path(tmp_path: Path) -> None:
    """Legitimate nested keys keep working."""
    store = LocalStore(str(tmp_path / "root"), base_path="workspaces")

    resolved = store.resolve_uri(Path("a/b/data.parquet"))

    assert resolved == str((tmp_path / "root" / "workspaces" / "a" / "b" / "data.parquet").resolve())


def test_local_store_allows_absolute_path_inside_root(tmp_path: Path) -> None:
    """An absolute key already inside the root is accepted."""
    root = tmp_path / "root"
    store = LocalStore(str(root))

    resolved = store.resolve_uri(str(root / "data.parquet"))

    assert resolved == str((root / "data.parquet").resolve())


@pytest.mark.parametrize("candidate", ESCAPING_PATHS)
def test_resolve_storage_path_rejects_escaping_relative_path(candidate: str) -> None:
    """The shared resolver refuses traversal for local destinations."""
    with pytest.raises(StoragePathTraversalError):
        resolve_storage_path(candidate, base_path="workspaces")


def test_resolve_storage_path_allows_nested_path() -> None:
    """The shared resolver still joins ordinary relative keys."""
    assert resolve_storage_path("a/b/data.parquet", base_path="workspaces") == "workspaces/a/b/data.parquet"


def test_percent_encoded_dots_are_a_literal_name() -> None:
    """``%2e%2e`` is a filename, not traversal, because nothing here decodes it."""
    assert resolve_storage_path("%2e%2e/data.parquet", base_path="ws") == "ws/%2e%2e/data.parquet"


def test_backslash_traversal_is_rejected() -> None:
    """Windows-style separators must not smuggle a parent reference past the check."""
    with pytest.raises(StoragePathTraversalError):
        resolve_storage_path("..\\..\\outside.txt", base_path="ws")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize("candidate", ESCAPING_PATHS)
def test_obstore_local_backend_rejects_escaping_path(tmp_path: Path, candidate: str) -> None:
    """The obstore backend's local path must stay inside the store root."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path / 'root'}")

    with pytest.raises(StoragePathTraversalError):
        store.resolve_uri(candidate)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_local_backend_allows_nested_path(tmp_path: Path) -> None:
    """Legitimate nested keys keep working on the obstore local backend."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    root = tmp_path / "root"
    store = ObStoreBackend(f"file://{root}")

    assert store.resolve_uri("a/b/data.parquet") == str((root / "a" / "b" / "data.parquet").resolve())
