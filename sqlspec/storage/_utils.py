"""Shared utilities for storage backends."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ("resolve_storage_path",)


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
    from pathlib import Path as PathlibPath

    path_str = str(path)

    if strip_file_scheme and path_str.startswith("file://"):
        path_str = path_str.removeprefix("file://")

    # For local file protocol
    if protocol == "file":
        path_obj = PathlibPath(path_str)

        # Absolute path handling
        if path_obj.is_absolute():
            if base_path:
                base_obj = PathlibPath(base_path)
                # Try to make path relative to base_path
                try:
                    relative = path_obj.relative_to(base_obj)
                    # Return joined path for FSSpec-style backends
                    return f"{base_path.rstrip('/')}/{relative}"
                except ValueError:
                    # Path is outside base_path
                    return path_str.lstrip("/")
            # No base_path - strip leading /
            return path_str.lstrip("/")

        # Relative path with base_path - join them
        if base_path:
            return f"{base_path.rstrip('/')}/{path_str}"

        # Relative path without base_path
        return path_str

    # For cloud storage protocols (s3, gs, etc.), join with base_path
    if not base_path:
        return path_str

    clean_base = base_path.rstrip("/")
    clean_path = path_str.lstrip("/")
    return f"{clean_base}/{clean_path}"
