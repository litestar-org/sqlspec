"""Unit tests for storage path containment and local root confinement.

A caller-supplied object key must never resolve outside its backend's root.
"""

from pathlib import Path

import pytest

from sqlspec.exceptions import StoragePathTraversalError
from sqlspec.storage._paths import parse_file_uri_path, resolve_storage_path
from sqlspec.storage.backends.local import LocalStore
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED, PYARROW_INSTALLED

ESCAPING_PATHS = ["../../etc/passwd", "../outside.txt", "nested/../../outside.txt", "..", "../"]
ENCODED_CONTROL_PATHS = [
    "%2e%2e/data.parquet",
    "%2E%2E/data.parquet",
    "nested/%2foutside.txt",
    "nested/%5coutside.txt",
    "data%00.parquet",
    "data\x00.parquet",
]


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


@pytest.mark.parametrize("candidate", ENCODED_CONTROL_PATHS)
def test_resolve_storage_path_rejects_encoded_controls_and_nul(candidate: str) -> None:
    """Percent-encoded dots, slashes, backslashes, and NUL bytes are rejected consistently."""
    with pytest.raises(StoragePathTraversalError):
        resolve_storage_path(candidate, base_path="ws")


@pytest.mark.parametrize(
    "uri",
    [
        "file:///tmp/%2e%2e/etc/passwd",
        "file:///tmp/%2E%2E/etc/passwd",
        "file:///tmp/nested%2foutside",
        "file:///tmp/nested%5coutside",
        "file:///tmp/item%00.parquet",
        "file:///tmp/item\x00.parquet",
    ],
)
def test_parse_file_uri_path_rejects_encoded_traversal_and_nul(uri: str) -> None:
    """parse_file_uri_path rejects encoded traversal and NUL sequences before and after decoding."""
    with pytest.raises(StoragePathTraversalError):
        parse_file_uri_path(uri)


def test_parse_file_uri_path_returns_empty_for_bare_file_scheme(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bare file:// URIs resolve to the current working directory rather than filesystem root."""
    monkeypatch.chdir(tmp_path)
    expected_cwd = tmp_path.resolve()

    assert parse_file_uri_path("file://") == ""
    assert LocalStore("file://").base_path == expected_cwd

    if FSSPEC_INSTALLED:
        from sqlspec.storage.backends.fsspec import FSSpecBackend

        assert FSSpecBackend("file://").base_path == str(expected_cwd)

    if OBSTORE_INSTALLED:
        from sqlspec.storage.backends.obstore import ObStoreBackend

        assert ObStoreBackend("file://")._local_store_root == str(expected_cwd)


def test_backslash_traversal_is_rejected() -> None:
    """Windows-style separators must not smuggle a parent reference past the check."""
    with pytest.raises(StoragePathTraversalError):
        resolve_storage_path("..\\..\\outside.txt", base_path="ws")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize("candidate", [*ESCAPING_PATHS, *ENCODED_CONTROL_PATHS])
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


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.parametrize("candidate", [*ESCAPING_PATHS, *ENCODED_CONTROL_PATHS])
def test_fsspec_local_backend_rejects_escaping_path(tmp_path: Path, candidate: str) -> None:
    """FSSpecBackend on local file protocol refuses escaping and encoded control paths."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path / "root"))

    with pytest.raises(StoragePathTraversalError):
        store.resolve_uri(candidate)


@pytest.mark.parametrize("backend_kind", ["local", "fsspec", "obstore"])
def test_local_backends_confine_symlinks_outside_root(tmp_path: Path, backend_kind: str) -> None:
    """Local backends exclude out-of-root symlinks during listing/globbing and reject direct reads or Arrow streams."""
    if backend_kind == "fsspec" and not FSSPEC_INSTALLED:
        pytest.skip("fsspec missing")
    if backend_kind == "obstore" and not OBSTORE_INSTALLED:
        pytest.skip("obstore missing")

    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()

    inside_file = root / "inside.txt"
    inside_file.write_bytes(b"inside")
    outside_file = outside / "secret.txt"
    outside_file.write_bytes(b"outside")
    escape_link = root / "escape.txt"
    escape_link.symlink_to(outside_file)

    if PYARROW_INSTALLED:
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.table({"id": [1]}), root / "inside.parquet")
        pq.write_table(pa.table({"id": [99]}), outside / "outside.parquet")
        (root / "escape.parquet").symlink_to(outside / "outside.parquet")

    if backend_kind == "local":
        store: object = LocalStore(str(root))
    elif backend_kind == "fsspec":
        from sqlspec.storage.backends.fsspec import FSSpecBackend

        store = FSSpecBackend("file", base_path=str(root))
    else:
        from sqlspec.storage.backends.obstore import ObStoreBackend

        store = ObStoreBackend(f"file://{root}")

    typed_store = store
    assert isinstance(typed_store, LocalStore) or hasattr(typed_store, "resolve_uri")

    with pytest.raises(StoragePathTraversalError):
        typed_store.resolve_uri("escape.txt")  # type: ignore[attr-defined]
    with pytest.raises(StoragePathTraversalError):
        typed_store.read_bytes_sync("escape.txt")  # type: ignore[attr-defined]

    listed = typed_store.list_objects_sync()  # type: ignore[attr-defined]
    assert not any("escape.txt" in item for item in listed)
    assert any("inside.txt" in item for item in listed)

    globbed = typed_store.glob_sync("*.txt")  # type: ignore[attr-defined]
    assert not any("escape.txt" in item for item in globbed)
    assert any("inside.txt" in item for item in globbed)

    if PYARROW_INSTALLED:
        batches = list(typed_store.stream_arrow_sync("*.parquet"))  # type: ignore[attr-defined]
        import pyarrow as pa

        assert pa.Table.from_batches(batches).to_pydict() == {"id": [1]}


def test_local_store_rejects_outside_file_uri_and_resolves_inside_file_uri(tmp_path: Path) -> None:
    """LocalStore rejects escaping file:// URIs and resolves in-root file:// URIs and globs consistently."""
    root = tmp_path / "root"
    root.mkdir()
    target = root / "nested" / "data.txt"
    target.parent.mkdir()
    target.write_bytes(b"ok")

    store = LocalStore(f"file://{root}")
    assert store.resolve_uri(f"file://{target}") == str(target.resolve())
    assert store.read_bytes_sync(f"file://{target}") == b"ok"
    assert store.glob_sync(f"file://{root}/nested/*.txt") == ["nested/data.txt"]

    with pytest.raises(StoragePathTraversalError):
        store.resolve_uri("file:///etc/passwd")
    with pytest.raises(StoragePathTraversalError):
        store.glob_sync("file:///etc/*.txt")
