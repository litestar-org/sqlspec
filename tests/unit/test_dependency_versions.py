"""Compatibility-floor tests for behavior-sensitive dependencies."""

import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_sqlglot_30_13_is_the_declared_and_locked_minimum() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    dependencies = pyproject["project"]["dependencies"]
    mypyc_dependencies = pyproject["project"]["optional-dependencies"]["mypyc"]
    lockfile = (PROJECT_ROOT / "uv.lock").read_text()

    assert "sqlglot>=30.13.0" in dependencies
    assert "sqlglot[c]>=30.13.0" in mypyc_dependencies
    assert 'name = "sqlglot"' in lockfile
    assert 'version = "30.13.0"' in lockfile
    assert 'specifier = ">=30.13.0"' in lockfile
