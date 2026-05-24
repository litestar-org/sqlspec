"""Clean-break guard tests for the ADK extension.

Two forbidden patterns are policed here:

1. ``event_json`` — Revision 6 Q2 renamed every ADK adapter's events payload
   column to ``event_data`` to match the upstream Google ADK schema. Any
   re-introduction of the older name in shipped code is treated as a
   regression.
2. Compatibility shim markers — PRD Global Constraint #1 prohibits shims in
   the ADK clean break. Patterns like ``backwards_compat``, ``legacy_``, and
   ``# DEPRECATED`` must not leak into ``sqlspec/extensions/adk/`` or
   per-adapter ``adk/`` modules.
"""

import re
from pathlib import Path

ADK_ROOTS = (
    Path(__file__).parents[4] / "sqlspec" / "extensions" / "adk",
    Path(__file__).parents[4] / "sqlspec" / "adapters",
)


def _iter_adk_sources() -> "list[Path]":
    files: list[Path] = []
    for root in ADK_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            parts = path.parts
            if "adk" not in parts:
                continue
            if "__pycache__" in parts:
                continue
            files.append(path)
    return files


def test_no_event_json_references() -> None:
    """The clean-break events payload column is named event_data everywhere."""
    pattern = re.compile(r"\bevent_json\b")
    offenders: list[str] = []
    for path in _iter_adk_sources():
        contents = path.read_text(encoding="utf-8")
        if pattern.search(contents):
            offenders.append(str(path))
    assert not offenders, (
        "event_json column name reintroduced in ADK sources — rename to event_data.\n"
        "Offending files:\n  - " + "\n  - ".join(offenders)
    )


def test_no_compat_shim_markers() -> None:
    """The ADK clean break forbids backwards-compatibility shims and deprecation markers."""
    forbidden_patterns = (
        re.compile(r"backwards?_compat", re.IGNORECASE),
        re.compile(r"\blegacy_"),
        re.compile(r"#\s*DEPRECATED"),
    )
    offenders: list[str] = []
    for path in _iter_adk_sources():
        contents = path.read_text(encoding="utf-8")
        for pattern in forbidden_patterns:
            match = pattern.search(contents)
            if match:
                offenders.append(f"{path} (matched {match.group(0)!r})")
                break
    assert not offenders, (
        "Compat shim markers detected in ADK sources — PRD Global Constraint #1 forbids them.\n"
        "Offending files:\n  - " + "\n  - ".join(offenders)
    )
