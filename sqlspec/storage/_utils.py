"""Shared utilities for storage backends."""

from typing import TYPE_CHECKING

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ("ensure_pyarrow", "resolve_storage_path")


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
