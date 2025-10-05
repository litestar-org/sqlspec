"""Unit tests for storage utilities."""

from typing import Any

import pytest
from typing_extensions import Self

from sqlspec.exceptions import MissingDependencyError
from sqlspec.storage._utils import AsyncIteratorWrapper, ensure_pyarrow, resolve_storage_path
from sqlspec.typing import PYARROW_INSTALLED


def test_ensure_pyarrow_succeeds_when_installed() -> None:
    """Test ensure_pyarrow succeeds when pyarrow is available."""
    if not PYARROW_INSTALLED:
        pytest.skip("pyarrow not installed")

    ensure_pyarrow()


def test_ensure_pyarrow_raises_when_not_installed() -> None:
    """Test ensure_pyarrow raises error when pyarrow not available."""
    if PYARROW_INSTALLED:
        pytest.skip("pyarrow is installed")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        ensure_pyarrow()


def test_resolve_storage_path_file_protocol_absolute() -> None:
    """Test path resolution for file protocol with absolute path."""
    result = resolve_storage_path("/data/file.txt", base_path="", protocol="file")
    assert result == "data/file.txt"


def test_resolve_storage_path_file_protocol_with_base() -> None:
    """Test path resolution for file protocol with base_path."""
    result = resolve_storage_path("file.txt", base_path="/base", protocol="file")
    assert result == "/base/file.txt"


def test_resolve_storage_path_file_scheme_stripping() -> None:
    """Test file:// scheme stripping."""
    result = resolve_storage_path("file:///data/file.txt", base_path="", protocol="file", strip_file_scheme=True)
    assert result == "data/file.txt"


def test_resolve_storage_path_file_scheme_preserved() -> None:
    """Test file:// scheme preserved when strip_file_scheme=False."""
    result = resolve_storage_path("file:///data/file.txt", base_path="", protocol="file", strip_file_scheme=False)
    assert result == "file:///data/file.txt"


def test_resolve_storage_path_cloud_protocol() -> None:
    """Test path resolution for cloud protocols."""
    result = resolve_storage_path("data/file.txt", base_path="bucket/prefix", protocol="s3")
    assert result == "bucket/prefix/data/file.txt"


def test_resolve_storage_path_cloud_absolute() -> None:
    """Test absolute path handling for cloud protocols."""
    result = resolve_storage_path("/data/file.txt", base_path="", protocol="s3")
    assert result == "/data/file.txt"


def test_resolve_storage_path_no_base_path() -> None:
    """Test path resolution without base_path."""
    result = resolve_storage_path("data/file.txt", base_path="", protocol="s3")
    assert result == "data/file.txt"


def test_resolve_storage_path_pathlib_input() -> None:
    """Test path resolution with pathlib.Path input."""
    from pathlib import Path

    result = resolve_storage_path(Path("data") / "file.txt", base_path="", protocol="file")
    assert result == "data/file.txt"


@pytest.mark.asyncio
async def test_async_iterator_wrapper_normal() -> None:
    """Test AsyncIteratorWrapper with normal iteration."""
    sync_iter = iter(range(5))
    async_iter = AsyncIteratorWrapper(sync_iter)

    results = [item async for item in async_iter]

    assert results == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_async_iterator_wrapper_empty() -> None:
    """Test AsyncIteratorWrapper with empty iterator."""
    sync_iter = iter([])  # type: ignore
    async_iter = AsyncIteratorWrapper(sync_iter)

    results = [item async for item in async_iter]

    assert results == []


@pytest.mark.asyncio
async def test_async_iterator_wrapper_single_item() -> None:
    """Test AsyncIteratorWrapper with single item."""
    sync_iter = iter([42])
    async_iter = AsyncIteratorWrapper(sync_iter)

    results = [item async for item in async_iter]

    assert results == [42]


@pytest.mark.asyncio
async def test_async_iterator_wrapper_type_preservation() -> None:
    """Test AsyncIteratorWrapper preserves item types."""
    data = [{"key": "value"}, [1, 2, 3], "string"]
    sync_iter = iter(data)
    async_iter = AsyncIteratorWrapper(sync_iter)

    results = [item async for item in async_iter]

    assert results == data
    assert isinstance(results[0], dict)
    assert isinstance(results[1], list)
    assert isinstance(results[2], str)


@pytest.mark.asyncio
async def test_async_iterator_wrapper_aclose() -> None:
    """Test AsyncIteratorWrapper aclose() method."""

    class CloseableIterator:
        def __init__(self, items: list) -> None:
            self.items = iter(items)
            self.closed = False

        def __iter__(self) -> Self:
            return self

        def __next__(self) -> Any:
            return next(self.items)

        def close(self) -> None:
            self.closed = True

    closeable = CloseableIterator([1, 2, 3])
    async_iter = AsyncIteratorWrapper(closeable)

    result = await async_iter.__anext__()
    assert result == 1
    assert not closeable.closed

    await async_iter.aclose()
    assert closeable.closed
