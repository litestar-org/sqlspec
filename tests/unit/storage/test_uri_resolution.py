"""Unit tests for backend-neutral storage URI resolution."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.backends.base import ObjectStoreBase
from sqlspec.storage.backends.local import LocalStore
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED


def test_object_store_contract_exposes_uri_resolution() -> None:
    assert hasattr(ObjectStoreProtocol, "resolve_uri")
    assert getattr(ObjectStoreBase.resolve_uri, "__isabstractmethod__", False)


def test_local_store_resolves_nonexistent_path(tmp_path: Path) -> None:
    store = LocalStore(str(tmp_path), base_path="workspaces")

    assert store.resolve_uri(Path("data.parquet")) == str((tmp_path / "workspaces" / "data.parquet").resolve())


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_resolves_prefixed_s3_uri() -> None:
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("s3://bucket/prefix/", base_path="workspaces/")

    assert store.resolve_uri(Path("data.parquet")) == "s3://bucket/prefix/workspaces/data.parquet"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_protocol_only_config_resolves_bucket_prefix() -> None:
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend.from_config({"protocol": "s3", "base_path": "bucket/prefix", "fs_config": {}})

    assert store.resolve_uri("data.parquet") == "s3://bucket/prefix/data.parquet"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_resolves_memory_uri_with_empty_base_path() -> None:
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("memory://")

    assert store.resolve_uri("data.parquet") == "memory://data.parquet"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_resolves_prefixed_s3_uri() -> None:
    store = _remote_obstore("s3://bucket/prefix/", base_path="workspaces/")

    assert store.resolve_uri(Path("data.parquet")) == "s3://bucket/prefix/workspaces/data.parquet"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_preserves_query_and_fragment() -> None:
    store = _remote_obstore("s3://bucket/prefix?version=1#section", base_path="workspaces")

    assert store.resolve_uri("data.parquet") == "s3://bucket/prefix/workspaces/data.parquet?version=1#section"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_resolves_memory_uri_without_corrupting_empty_authority() -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://")

    assert store.resolve_uri("data.parquet") == "memory://data.parquet"


@pytest.mark.skipif(not FSSPEC_INSTALLED or not OBSTORE_INSTALLED, reason="storage backends missing")
def test_file_backends_resolve_equivalent_absolute_paths(tmp_path: Path) -> None:
    from sqlspec.storage.backends.fsspec import FSSpecBackend
    from sqlspec.storage.backends.obstore import ObStoreBackend

    expected = str((tmp_path / "workspaces" / "data.parquet").resolve())
    backends = (
        LocalStore(str(tmp_path), base_path="workspaces"),
        FSSpecBackend(f"file://{tmp_path}", base_path="workspaces"),
        ObStoreBackend(f"file://{tmp_path}", base_path="workspaces"),
    )

    assert [backend.resolve_uri("data.parquet") for backend in backends] == [expected, expected, expected]


def _remote_obstore(store_uri: str, base_path: str = "") -> Any:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend.__new__(ObStoreBackend)
    store._is_local_store = False
    store._local_store_root = ""
    store.base_path = base_path.rstrip("/")
    store.protocol = store_uri.split("://", maxsplit=1)[0]
    store.store = object()
    store.store_options = {}
    store.store_uri = store_uri
    return store
