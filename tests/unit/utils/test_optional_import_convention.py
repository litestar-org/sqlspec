"""Enforcement guard for the optional-dependency import convention.

The project routes all optional third-party module acquisition through the
``module_loader`` engine (``import_optional`` / ``import_optional_attr``) and the
``sqlspec.typing`` facade. Static ``import uuid_utils`` / ``import pgvector``
statements bypass that convention and must not reappear in ``sqlspec/`` source.

See ``.agents/patterns.md`` section 2.4 for the decision tree.
"""

import re
from pathlib import Path

import sqlspec

# Optional modules that must only be acquired via the engine, never imported statically.
_GUARDED_MODULES = ("uuid_utils", "pgvector")
# Matches `import uuid_utils`, `import uuid_utils as x`, `import pgvector.psycopg`,
# and `from pgvector import ...` — but not substrings inside identifiers.
_STATIC_IMPORT = re.compile(
    r"^\s*(?:import\s+(?:" + "|".join(_GUARDED_MODULES) + r")\b"
    r"|from\s+(?:" + "|".join(_GUARDED_MODULES) + r")\b)",
    re.MULTILINE,
)


def test_no_static_optional_imports_in_sqlspec_source() -> None:
    """No static import of a guarded optional module may exist in sqlspec/."""
    source_root = Path(sqlspec.__file__).parent
    offenders: list[str] = []
    for path in source_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for match in _STATIC_IMPORT.finditer(text):
            line_no = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(source_root.parent)}:{line_no}: {match.group().strip()}")

    assert not offenders, (
        "Static optional-module imports bypass the import_optional convention "
        "(see .agents/patterns.md 2.4):\n" + "\n".join(offenders)
    )
