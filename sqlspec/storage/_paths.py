"""Pure storage path helpers safe for mypyc compilation."""

import re
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Final

from sqlspec.exceptions import StoragePathTraversalError

__all__ = (
    "FILE_PROTOCOL",
    "FILE_SCHEME_PREFIX",
    "ensure_path_within_root",
    "extract_glob_static_prefix",
    "glob_to_regex",
    "is_file_destination",
    "reject_parent_traversal",
    "resolve_storage_path",
    "strip_windows_drive_prefix",
)


FILE_PROTOCOL: Final[str] = "file"
FILE_SCHEME_PREFIX: Final[str] = "file://"


def reject_parent_traversal(path: "str | Path") -> None:
    """Raise if any segment of ``path`` is a parent reference.

    Both separators are considered, so a Windows-style key cannot smuggle a
    parent reference past a POSIX-only split.

    Args:
        path: Caller-supplied storage path.

    Raises:
        StoragePathTraversalError: If a ``..`` segment is present.
    """
    path_str = str(path)
    posix_parts = PurePosixPath(path_str).parts
    windows_parts = PureWindowsPath(path_str).parts
    if ".." in posix_parts or ".." in windows_parts:
        raise StoragePathTraversalError(path_str)


def ensure_path_within_root(path: "str | Path", root: "str | Path") -> str:
    """Return ``path`` as a ``root``-relative POSIX path, refusing anything outside ``root``.

    A relative path is joined to ``root``; an absolute path must already be inside
    it. Parent references are rejected before any filesystem call, and the
    resolved result is re-checked so a symlink cannot escape.

    Args:
        path: Caller-supplied storage path.
        root: Directory the path must stay within.

    Returns:
        The path relative to ``root``, or ``""`` when it is ``root`` itself.

    Raises:
        StoragePathTraversalError: If the path escapes ``root``.
    """
    reject_parent_traversal(path)

    root_obj = Path(str(root)).resolve()
    path_obj = Path(str(path))
    candidate = path_obj if path_obj.is_absolute() else root_obj / path_obj
    resolved = candidate.resolve()

    if resolved != root_obj and root_obj not in resolved.parents:
        raise StoragePathTraversalError(str(path), str(root_obj))

    if resolved == root_obj:
        return ""
    return resolved.relative_to(root_obj).as_posix()


_GLOB_MAGIC: Final = re.compile(r"[*?\[]")


def _glob_segment_regex(segment: str) -> str:
    """Translate one glob path segment to regex source that never crosses ``/``."""
    out: list[str] = []
    index = 0
    length = len(segment)
    while index < length:
        char = segment[index]
        if char == "*":
            out.append("[^/]*")
            index += 1
        elif char == "?":
            out.append("[^/]")
            index += 1
        elif char == "[":
            close = segment.find("]", index + 1)
            body = segment[index + 1 : close] if close != -1 else ""
            negated = body.startswith(("!", "^"))
            if negated:
                body = body[1:]
            if close == -1 or not body:
                out.append(re.escape(char))
                index += 1
                continue
            escaped = body.replace("\\", "\\\\").replace("[", "\\[")
            prefix = "^" if negated else ""
            out.append(f"[{prefix}{escaped}]")
            index = close + 1
        else:
            out.append(re.escape(char))
            index += 1
    return "".join(out)


def extract_glob_static_prefix(pattern: str) -> str:
    """Return the literal directory prefix of a glob pattern.

    The result is either ``""`` or a string ending in ``/`` made of whole path
    segments that contain no glob metacharacters. A pattern with no wildcards
    yields its directory portion, never the full key.

    Args:
        pattern: Glob pattern in POSIX form.

    Returns:
        Static directory prefix suitable for a listing API.
    """
    segments = pattern.lstrip("/").split("/")
    static: list[str] = []
    for segment in segments[:-1]:
        if not segment or _GLOB_MAGIC.search(segment):
            break
        static.append(segment)
    return "/".join(static) + "/" if static else ""


def glob_to_regex(pattern: str) -> "re.Pattern[str]":
    """Compile a glob pattern to an anchored regex with pathlib semantics.

    ``*`` and ``?`` match within a single path segment. A ``**`` segment matches
    zero or more whole segments. Matching is anchored at both ends, so a pattern
    describes the entire object key rather than a suffix of it.

    The honored magic characters are ``*``, ``?`` and ``[``. There is no brace
    expansion and no escape syntax, matching what the local and fsspec backends
    accept. Compiled patterns are cached by :mod:`re` itself, so repeated calls
    with the same pattern do not recompile.

    Args:
        pattern: Glob pattern.

    Returns:
        A compiled, anchored regex. An empty pattern matches nothing.
    """
    if not pattern:
        return re.compile(r"(?!)")

    parts = pattern.split("/")
    pieces: list[str] = []
    last_index = len(parts) - 1
    for index, part in enumerate(parts):
        if part == "**":
            pieces.append(".*" if index == last_index else "(?:[^/]+/)*")
            continue
        pieces.append(_glob_segment_regex(part))
        if index != last_index:
            pieces.append("/")

    return re.compile(f"(?s:{''.join(pieces)})\\Z")


def strip_windows_drive_prefix(path: str) -> str:
    """Drop a leading slash from a urlparse'd Windows drive path (``/C:/x`` -> ``C:/x``)."""
    if path and len(path) > 2 and path[2] == ":":  # noqa: PLR2004
        return path[1:]
    return path


def is_file_destination(path: "str | Path") -> bool:
    """Classify a local path as a file (vs directory) destination.

    Resolves the file-vs-directory ambiguity for paths that may not exist yet, so
    writes and reads agree. A trailing separator or an existing directory is a
    directory; a path with a filename suffix is a file; otherwise it is a directory.
    """
    path_str = str(path)
    if path_str.endswith(("/", "\\")):
        return False
    path_obj = Path(path_str)
    if path_obj.is_dir():
        return False
    return bool(path_obj.suffix)


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

    Raises:
        StoragePathTraversalError: If the path contains a parent reference.
    """

    reject_parent_traversal(path)

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
