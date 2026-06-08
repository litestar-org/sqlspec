"""Unit tests for dependency checking utilities."""

import shutil
import sys
from pathlib import Path

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PANDAS_INSTALLED, POLARS_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils import module_loader as dependencies
from sqlspec.utils.module_loader import ensure_pandas, ensure_polars, ensure_pyarrow, import_string, module_to_os_path


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


def test_facade_re_exports_import_optional() -> None:
    """sqlspec.typing exposes import_optional/import_optional_attr identically."""
    from sqlspec import typing as public_typing

    assert public_typing.import_optional is dependencies.import_optional
    assert public_typing.import_optional_attr is dependencies.import_optional_attr
    assert "import_optional" in public_typing.__all__
    assert "import_optional_attr" in public_typing.__all__


def test_import_optional_returns_module_when_available() -> None:
    """import_optional returns the imported module object when present."""
    import json

    assert dependencies.import_optional("json") is json


def test_import_optional_returns_none_when_missing() -> None:
    """import_optional returns None (silent) when the module is absent."""
    module_name = "sqlspec_optional_import_missing_pkg"
    dependencies.reset_dependency_cache(module_name)
    monkeypatch_free_delete(module_name)

    assert dependencies.import_optional(module_name) is None


def monkeypatch_free_delete(module_name: str) -> None:
    """Drop a module from sys.modules if present (helper for cache tests)."""
    sys.modules.pop(module_name, None)


@pytest.mark.usefixtures("monkeypatch")
def test_import_optional_recomputes_after_cache_reset(tmp_path, monkeypatch) -> None:
    """import_optional reflects environment changes once its cache is reset."""
    module_name = "sqlspec_optional_import_dummy_pkg"
    dependencies.reset_dependency_cache(module_name)
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    assert dependencies.import_optional(module_name) is None

    _write_dummy_package(tmp_path, module_name)
    monkeypatch.syspath_prepend(str(tmp_path))
    dependencies.reset_dependency_cache(module_name)
    resolved = dependencies.import_optional(module_name)
    assert resolved is not None
    assert resolved.__name__ == module_name


@pytest.mark.usefixtures("monkeypatch")
def test_import_optional_caches_resolved_module(tmp_path, monkeypatch) -> None:
    """import_optional caches the resolved module until the cache is cleared."""
    module_name = "sqlspec_optional_import_cached_pkg"
    dependencies.reset_dependency_cache(module_name)
    monkeypatch.delitem(sys.modules, module_name, raising=False)

    _write_dummy_package(tmp_path, module_name)
    monkeypatch.syspath_prepend(str(tmp_path))
    dependencies.reset_dependency_cache(module_name)
    first = dependencies.import_optional(module_name)
    assert first is not None

    # Removing the package from disk must NOT change the cached result.
    shutil.rmtree(tmp_path / module_name, ignore_errors=True)
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    assert dependencies.import_optional(module_name) is first

    # After an explicit reset, detection flips back to None.
    dependencies.reset_dependency_cache(module_name)
    assert dependencies.import_optional(module_name) is None


def test_import_optional_attr_returns_attribute_when_present() -> None:
    """import_optional_attr returns the attribute from an available module."""
    assert dependencies.import_optional_attr("json", "dumps") is __import__("json").dumps


def test_import_optional_attr_returns_none_when_module_missing() -> None:
    """import_optional_attr returns None when the module is absent."""
    module_name = "sqlspec_optional_attr_missing_pkg"
    dependencies.reset_dependency_cache(module_name)
    monkeypatch_free_delete(module_name)
    assert dependencies.import_optional_attr(module_name, "anything") is None


def test_import_optional_attr_returns_none_when_attr_missing() -> None:
    """import_optional_attr returns None when the attribute is absent."""
    assert dependencies.import_optional_attr("json", "definitely_not_an_attr") is None


def test_import_string_basic_module() -> None:
    """Test import_string with basic module import."""
    sys_module = import_string("sys")
    assert sys_module is sys


def test_import_string_module_attribute() -> None:
    """Test import_string with module attribute."""
    path_class = import_string("pathlib.Path")
    assert path_class is Path


def test_import_string_nested_attribute() -> None:
    """Test import_string with nested attributes."""
    result = import_string("sys.version_info.major")
    assert isinstance(result, int)


def test_import_string_invalid_module() -> None:
    """Test import_string with invalid module."""
    with pytest.raises(ImportError, match="doesn't look like a module path"):
        import_string("nonexistent.module.path")


def test_import_string_invalid_attribute() -> None:
    """Test import_string with invalid attribute."""
    with pytest.raises(ImportError, match="has no attribute"):
        import_string("sys.nonexistent_attribute")


def test_import_string_partial_module_path() -> None:
    """Test import_string handles partial module paths correctly."""
    json_module = import_string("json")
    assert json_module.__name__ == "json"


def test_import_string_exception_handling() -> None:
    """Test import_string exception handling."""
    with pytest.raises(ImportError, match="Could not import"):
        import_string("this.will.definitely.fail")


def test_module_to_os_path_basic() -> None:
    """Test module_to_os_path with basic module."""
    path = module_to_os_path("pathlib")
    assert isinstance(path, Path)
    assert path.exists()


def test_module_to_os_path_current_package() -> None:
    """Test module_to_os_path with sqlspec package."""
    path = module_to_os_path("sqlspec")
    assert isinstance(path, Path)
    assert path.exists()
    assert path.is_dir()


def test_module_to_os_path_nonexistent() -> None:
    """Test module_to_os_path with nonexistent module."""
    with pytest.raises(TypeError, match="Couldn't find the path"):
        module_to_os_path("definitely.nonexistent.module")


def test_module_to_os_path_file_module() -> None:
    """Test module_to_os_path returns parent for file modules."""
    path = module_to_os_path("sqlspec.exceptions")
    assert isinstance(path, Path)
    assert path.exists()


def test_complex_module_import_scenarios() -> None:
    """Test complex module import scenarios."""
    pathlib_module = import_string("pathlib")
    assert pathlib_module.__name__ == "pathlib"

    path_class = import_string("pathlib.Path")
    assert path_class.__name__ == "Path"

    path_instance = path_class("/tmp")
    assert isinstance(path_instance, Path)
