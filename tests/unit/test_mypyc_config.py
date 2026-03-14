"""Tests for mypyc build configuration."""

from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib


def test_mypyc_include_set_covers_safe_storage_runtime_modules() -> None:
    """Safe storage runtime modules should be in the mypyc include set."""
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text())
    mypyc_config = config["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]["mypyc"]

    include = set(mypyc_config["include"])
    exclude = set(mypyc_config["exclude"])

    assert "sqlspec/storage/registry.py" in include
    assert "sqlspec/storage/errors.py" in include
    assert "sqlspec/storage/backends/base.py" in include
    assert "sqlspec/utils/arrow_helpers.py" in exclude
