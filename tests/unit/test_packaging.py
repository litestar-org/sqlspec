"""Tests for packaging configuration and optional extras."""

import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def test_db2_optional_dependency_registered() -> None:
    """Verify db2 optional dependency extra is declared in pyproject.toml."""
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)

    optional_deps = data.get("project", {}).get("optional-dependencies", {})
    assert "db2" in optional_deps
    assert any("ibm_db" in dep for dep in optional_deps["db2"])


def test_db2_pytest_marker_registered() -> None:
    """Verify db2 pytest marker is defined in pytest configuration."""
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)

    markers = data.get("tool", {}).get("pytest", {}).get("ini_options", {}).get("markers", [])
    assert any(m.startswith("db2:") for m in markers)
