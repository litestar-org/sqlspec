"""Shared utilities for storage backends."""

from pathlib import Path
from typing import Any, Final

from sqlspec.utils.module_loader import ensure_pyarrow

FILE_PROTOCOL: Final[str] = "file"
FILE_SCHEME_PREFIX: Final[str] = "file://"

__all__ = ("FILE_PROTOCOL", "FILE_SCHEME_PREFIX", "import_pyarrow", "import_pyarrow_parquet", "resolve_storage_path")


def import_pyarrow() -> "Any":
    """Import PyArrow with optional dependency guard.

    Returns:
        PyArrow module.
    """

    ensure_pyarrow()
    import pyarrow as pa

    return pa


def import_pyarrow_parquet() -> "Any":
    """Import PyArrow parquet module with optional dependency guard.

    Returns:
        PyArrow parquet module.
    """

    ensure_pyarrow()
    import pyarrow.parquet as pq

    return pq


def resolve_storage_path(
    path: "str | Path", base_path: str = "", protocol: str = FILE_PROTOCOL, strip_file_scheme: bool = True
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
        'base/file.txt'

        >>> resolve_storage_path(
        ...     "file:///data/file.txt", strip_file_scheme=True
        ... )
        'data/file.txt'

        >>> resolve_storage_path(
        ...     "/data/subdir/file.txt",
        ...     base_path="/data",
        ...     protocol="file",
        ... )
        'subdir/file.txt'
    """

    path_str = str(path)

    if strip_file_scheme and path_str.startswith(FILE_SCHEME_PREFIX):
        path_str = path_str.removeprefix(FILE_SCHEME_PREFIX)

    if protocol == FILE_PROTOCOL:
        path_obj = Path(path_str)

        if path_obj.is_absolute():
            if base_path:
                base_obj = Path(base_path)
                try:
                    relative = path_obj.relative_to(base_obj)
                    return f"{base_path.rstrip('/')}/{relative}"
                except ValueError:
                    return path_str.lstrip("/")
            return path_str.lstrip("/")

        if base_path:
            return f"{base_path.rstrip('/')}/{path_str}"

        return path_str

    if not base_path:
        return path_str

    clean_base = base_path.rstrip("/")
    clean_path = path_str.lstrip("/")
    return f"{clean_base}/{clean_path}"
