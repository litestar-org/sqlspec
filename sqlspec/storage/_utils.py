"""Shared utilities for storage backends."""

from typing import TYPE_CHECKING, Generic, TypeVar

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PYARROW_INSTALLED
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

T = TypeVar("T")

__all__ = ("AsyncIteratorWrapper", "ensure_pyarrow", "resolve_storage_path")


class AsyncIteratorWrapper(Generic[T]):
    """Wrap sync iterator with async_(next) calls.

    Prevents event loop blocking by offloading blocking next() calls
    to a thread pool while keeping iterator creation on main thread.

    Args:
        sync_iter: Synchronous iterator to wrap.

    Examples:
        >>> sync_iter = iter([1, 2, 3])
        >>> async_iter = AsyncIteratorWrapper(sync_iter)
        >>> async for item in async_iter:
        ...     print(item)
    """

    __slots__ = ("sync_iter",)

    def __init__(self, sync_iter: "Iterator[T]") -> None:
        self.sync_iter = sync_iter

    def __aiter__(self) -> "AsyncIteratorWrapper[T]":
        return self

    async def __anext__(self) -> T:
        def _safe_next() -> T:
            try:
                return next(self.sync_iter)
            except StopIteration as e:
                raise StopAsyncIteration from e

        return await async_(_safe_next)()

    async def aclose(self) -> None:
        """Close underlying iterator if it supports close()."""
        try:
            close_method = self.sync_iter.close  # type: ignore[attr-defined]
            await async_(close_method)()  # pyright: ignore
        except AttributeError:
            pass


def ensure_pyarrow() -> None:
    """Ensure PyArrow is available for Arrow operations.

    Raises:
        MissingDependencyError: If pyarrow is not installed.
    """
    if not PYARROW_INSTALLED:
        raise MissingDependencyError(package="pyarrow", install_package="pyarrow")


def resolve_storage_path(
    path: "str | Path", base_path: str = "", protocol: str = "file", strip_file_scheme: bool = True
) -> str:
    """Resolve path relative to base_path with protocol-specific handling.

    Args:
        path: Path to resolve (may include file:// scheme).
        base_path: Base path to prepend if path is relative.
        protocol: Storage protocol (file, s3, gs, etc.).
        strip_file_scheme: Whether to strip file:// prefix.

    Returns:
        Resolved path string suitable for the storage backend.

    Examples:
        >>> resolve_storage_path("/data/file.txt", protocol="file")
        'data/file.txt'

        >>> resolve_storage_path(
        ...     "file.txt", base_path="/base", protocol="file"
        ... )
        '/base/file.txt'

        >>> resolve_storage_path(
        ...     "file:///data/file.txt", strip_file_scheme=True
        ... )
        'data/file.txt'
    """
    path_str = str(path)

    if strip_file_scheme and path_str.startswith("file://"):
        path_str = path_str.removeprefix("file://")

    if protocol == "file" and path_str.startswith("/"):
        return path_str.lstrip("/")

    if not base_path:
        return path_str

    clean_base = base_path.rstrip("/")
    clean_path = path_str.lstrip("/")
    return f"{clean_base}/{clean_path}"
