"""Pure storage path helpers safe for mypyc compilation."""

from pathlib import Path
from typing import Final

__all__ = ("FILE_PROTOCOL", "FILE_SCHEME_PREFIX", "resolve_storage_path")


FILE_PROTOCOL: Final[str] = "file"
FILE_SCHEME_PREFIX: Final[str] = "file://"


def resolve_storage_path(
    path: "str | Path", base_path: str = "", protocol: str = FILE_PROTOCOL, strip_file_scheme: bool = True
) -> str:
    """Resolve path relative to base_path with protocol-specific handling.

    Args:
        path: Path to resolve.
        base_path: Base path to prepend if path is relative.
        protocol: Storage protocol.
        strip_file_scheme: Whether to strip ``file://`` prefixes.

    Returns:
        Resolved path string suitable for the storage backend.
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
                    if str(relative) == ".":
                        return base_path
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
