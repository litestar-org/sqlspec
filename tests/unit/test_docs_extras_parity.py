"""Installation docs parity tests for optional extras."""

import re
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _normalize_dependency_name(requirement: str) -> str:
    requirement = requirement.split(";", 1)[0].strip()
    return re.split(r"[<>=!~]", requirement, maxsplit=1)[0].strip()


def _package_groups_table() -> str:
    docs = (PROJECT_ROOT / "docs/getting_started/installation.rst").read_text()
    _, table = docs.split("Package groups\n--------------", maxsplit=1)
    return table.split("Multiple extras\n---------------", maxsplit=1)[0]


def _documented_extras() -> dict[str, set[str]]:
    table = _package_groups_table()
    rows: dict[str, set[str]] = {}
    matches = list(re.finditer(r"^\s+\* - ``([^`]+)``\s*$", table, flags=re.MULTILINE))

    for index, match in enumerate(matches):
        extra_name = match.group(1)
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(table)
        row = table[match.end() : next_start]
        includes_match = re.search(r"^\s+- (.+)$", row, flags=re.MULTILINE)
        if includes_match is None:
            rows[extra_name] = set()
            continue
        rows[extra_name] = {
            _normalize_dependency_name(dependency) for dependency in re.findall(r"``([^`]+)``", includes_match.group(1))
        }

    return rows


def _documented_extra_names() -> list[str]:
    return re.findall(r"^\s+\* - ``([^`]+)``\s*$", _package_groups_table(), flags=re.MULTILINE)


def _pyproject_extras() -> dict[str, set[str]]:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    return {
        extra_name: {_normalize_dependency_name(dependency) for dependency in dependencies}
        for extra_name, dependencies in pyproject["project"]["optional-dependencies"].items()
    }


def test_installation_docs_extras_match_pyproject_optional_dependencies() -> None:
    """Every public optional extra should appear exactly once in installation docs."""
    documented_names = _documented_extra_names()
    assert len(documented_names) == len(set(documented_names))
    assert _documented_extras() == _pyproject_extras()
