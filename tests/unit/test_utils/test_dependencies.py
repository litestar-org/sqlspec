"""Unit tests for dependency checking utilities."""

import shutil
import sys
from pathlib import Path

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PANDAS_INSTALLED, POLARS_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils import module_loader as dependencies
from sqlspec.utils.module_loader import ensure_pandas, ensure_polars, ensure_pyarrow


def test_ensure_pyarrow_succeeds_when_installed() -> None:
    """Test ensure_pyarrow succeeds when pyarrow is available."""
    if not PYARROW_INSTALLED:
        pytest.skip("pyarrow not installed")

    ensure_pyarrow()


def test_ensure_pyarrow_raises_when_not_installed() -> None:
    """Test ensure_pyarrow raises error when pyarrow not available."""
    if PYARROW_INSTALLED:
        pytest.skip("pyarrow is installed")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        ensure_pyarrow()


def test_ensure_pandas_succeeds_when_installed() -> None:
    """Test ensure_pandas succeeds when pandas is available."""
    if not PANDAS_INSTALLED:
        pytest.skip("pandas not installed")

    ensure_pandas()


def test_ensure_pandas_raises_when_not_installed() -> None:
    """Test ensure_pandas raises error when pandas not available."""
    if PANDAS_INSTALLED:
        pytest.skip("pandas is installed")

    with pytest.raises(MissingDependencyError, match="pandas"):
        ensure_pandas()


def test_ensure_polars_succeeds_when_installed() -> None:
    """Test ensure_polars succeeds when polars is available."""
    if not POLARS_INSTALLED:
        pytest.skip("polars not installed")

    ensure_polars()


def test_ensure_polars_raises_when_not_installed() -> None:
    """Test ensure_polars raises error when polars not available."""
    if POLARS_INSTALLED:
        pytest.skip("polars is installed")

    with pytest.raises(MissingDependencyError, match="polars"):
        ensure_polars()


def _write_dummy_package(root: Path, package_name: str) -> None:
    pkg_path = root / package_name
    pkg_path.mkdir()
    (pkg_path / "__init__.py").write_text("__all__ = ()\n", encoding="utf-8")


@pytest.mark.usefixtures("monkeypatch")
def test_dependency_detection_recomputes_after_cache_reset(tmp_path, monkeypatch) -> None:
    """Ensure module availability reflects runtime environment changes."""

    module_name = "sqlspec_optional_dummy_pkg"
    dependencies.reset_dependency_cache(module_name)
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    assert dependencies.module_available(module_name) is False

    _write_dummy_package(tmp_path, module_name)
    monkeypatch.syspath_prepend(str(tmp_path))
    dependencies.reset_dependency_cache(module_name)
    assert dependencies.module_available(module_name) is True

    flag = dependencies.dependency_flag(module_name)
    dependencies.reset_dependency_cache(module_name)
    assert bool(flag) is True


@pytest.mark.usefixtures("monkeypatch")
def test_dependency_flag_handles_module_removal(tmp_path, monkeypatch) -> None:
    """OptionalDependencyFlag should respond to missing modules after cache reset."""

    module_name = "sqlspec_optional_dummy_pkg_removed"
    dependencies.reset_dependency_cache(module_name)
    monkeypatch.delitem(sys.modules, module_name, raising=False)

    _write_dummy_package(tmp_path, module_name)
    monkeypatch.syspath_prepend(str(tmp_path))
    dependencies.reset_dependency_cache(module_name)
    flag = dependencies.dependency_flag(module_name)
    assert bool(flag) is True

    # Remove package and ensure detection flips back to False once cache clears.
    dependencies.reset_dependency_cache(module_name)
    shutil.rmtree(tmp_path / module_name, ignore_errors=True)
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    dependencies.reset_dependency_cache(module_name)
    assert bool(flag) is False
