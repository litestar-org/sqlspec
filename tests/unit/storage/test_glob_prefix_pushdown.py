"""Static prefix extraction and listing pushdown for obstore globbing."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.storage._paths import extract_glob_static_prefix
from sqlspec.typing import OBSTORE_INSTALLED

pytestmark = pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")

_FIXTURE_KEYS = ("data/a.parquet", "data/sub/b.parquet", "data/x/y/z.parquet", "data_other/c.parquet", "top.parquet")


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("data/*.parquet", "data/"),
        ("data/**/*.parquet", "data/"),
        ("*.parquet", ""),
        ("data/2024/file.parquet", "data/2024/"),
        ("data/[abc]/x.parquet", "data/"),
        ("data/2024_*/x.parquet", "data/"),
        ("/abs/data/*.parquet", "abs/data/"),
        ("", ""),
        ("a//b/*.txt", "a/"),
        ("data/?/x.txt", "data/"),
    ],
)
def test_extract_glob_static_prefix(pattern: str, expected: str) -> None:
    result = extract_glob_static_prefix(pattern)

    assert result == expected
    assert result == "" or result.endswith("/")
    assert "//" not in result


def _populated(uri: str, base_path: str = "") -> Any:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(uri, base_path=base_path)
    for key in _FIXTURE_KEYS:
        store.write_bytes_sync(key, b"1")
    return store


def _stores(tmp_path: Path) -> list[Any]:
    return [
        _populated(f"file://{tmp_path / 'plain'}"),
        _populated(f"file://{tmp_path / 'based'}", base_path="tenant"),
        _populated("memory://"),
        _populated("memory://", base_path="tenant"),
    ]


def _strip(store: Any, keys: list[str]) -> list[str]:
    prefix = store.base_path.rstrip("/") + "/" if store.base_path and not store._is_local_store else ""
    return [key.removeprefix(prefix) for key in keys]


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("data/*.parquet", ["data/a.parquet"]),
        ("data/**/*.parquet", ["data/a.parquet", "data/sub/b.parquet", "data/x/y/z.parquet"]),
        ("top.parquet", ["top.parquet"]),
        ("data/sub/b.parquet", ["data/sub/b.parquet"]),
        ("missing/*.parquet", []),
        ("", []),
        ("*.parquet", ["top.parquet"]),
    ],
)
def test_glob_results_with_prefix_pushdown(tmp_path: Path, pattern: str, expected: list[str]) -> None:
    for store in _stores(tmp_path):
        assert _strip(store, store.glob_sync(pattern)) == expected, store.store_uri


class _RecordingListStore:
    def __init__(self) -> None:
        self.list_paths: list[str] = []

    def list(self, path: str) -> Any:
        self.list_paths.append(path)
        return iter([[{"path": "data/a.parquet"}, {"path": "data_other/c.parquet"}]])


def test_glob_passes_static_prefix_to_store_list() -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://", base_path="tenant")
    recorder = _RecordingListStore()
    store.store = recorder

    store.glob_sync("data/*.parquet")
    store.glob_sync("*.parquet")

    assert recorder.list_paths == ["tenant/data/", "tenant/"]
