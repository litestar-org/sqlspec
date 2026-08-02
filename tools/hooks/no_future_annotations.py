"""Disallow postponed evaluation of annotations in SQLSpec package code."""

import ast
import sys
from pathlib import Path


def main(paths: list[str]) -> int:
    """Scan Python files for forbidden future-annotations imports.

    Args:
        paths: File paths supplied by the hook runner.

    Returns:
        A nonzero status when any file imports ``__future__.annotations``.
    """
    offending_paths = [path for raw_path in paths if (path := Path(raw_path)).suffix == ".py" and _is_offending(path)]
    if not offending_paths:
        return 0

    sys.stderr.write("Disallowed future import found. Remove `from __future__ import annotations` from:\n")
    for path in offending_paths:
        sys.stderr.write(f" - {path}\n")
    return 1


def _is_offending(path: Path) -> bool:
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return False

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return "from __future__ import annotations" in source

    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in tree.body
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
