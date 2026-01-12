"""Unit tests for dependency checking utilities."""

import shutil
import sys
import threading
from pathlib import Path

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PANDAS_INSTALLED, POLARS_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils import module_loader as dependencies
from sqlspec.utils.module_loader import ensure_pandas, ensure_polars, ensure_pyarrow, import_string, module_to_os_path
from sqlspec.utils.singleton import SingletonMeta

pytestmark = pytest.mark.xdist_group("utils")


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


def test_singleton_single_instance() -> None:
    """Test singleton pattern creates only one instance."""

    class SingletonTestClass(metaclass=SingletonMeta):
        """Test singleton class."""

        def __init__(self, value: str = "default") -> None:
            self.value = value

    instance1 = SingletonTestClass("test1")
    instance2 = SingletonTestClass("test2")

    assert instance1 is instance2
    assert instance1.value == "test1"
    assert instance2.value == "test1"


def test_singleton_different_classes() -> None:
    """Test different singleton classes have separate instances."""

    class SingletonTestClass(metaclass=SingletonMeta):
        """Test singleton class."""

        def __init__(self, value: str = "default") -> None:
            self.value = value

    class AnotherSingletonClass(metaclass=SingletonMeta):
        """Another test singleton class."""

        def __init__(self, data: int = 42) -> None:
            self.data = data

    singleton1 = SingletonTestClass("test")
    singleton2 = AnotherSingletonClass(100)

    assert singleton1 is not singleton2  # type: ignore[comparison-overlap]  # pyright: ignore[reportUnnecessaryComparison]
    assert isinstance(singleton1, SingletonTestClass)
    assert isinstance(singleton2, AnotherSingletonClass)


def test_singleton_thread_safety() -> None:
    """Test singleton pattern is thread-safe."""

    class ThreadSafeSingleton(metaclass=SingletonMeta):
        """Thread-safe singleton class."""

        def __init__(self, value: str = "default") -> None:
            self.value = value

    instances = []

    def create_instance() -> None:
        instance = ThreadSafeSingleton("thread_test")
        instances.append(instance)

    threads = [threading.Thread(target=create_instance) for _ in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len({id(instance) for instance in instances}) == 1
    assert all(instance is instances[0] for instance in instances)


def test_singleton_with_args() -> None:
    """Test singleton pattern with constructor arguments."""

    class SingletonWithArgs(metaclass=SingletonMeta):
        """Singleton class for argument handling."""

        def __init__(self, value: str = "default") -> None:
            self.value = value

    instance1 = SingletonWithArgs("first")
    instance2 = SingletonWithArgs("second")

    assert instance1 is instance2
    assert instance1.value == "first"


def test_singleton_metaclass_edge_cases() -> None:
    """Test singleton metaclass with edge cases."""

    class SingletonOne(metaclass=SingletonMeta):
        """Singleton class instance one."""

        def __init__(self, value: str = "default") -> None:
            self.value = value

    class SingletonTwo(metaclass=SingletonMeta):
        """Singleton class instance two."""

        def __init__(self, value: str = "default") -> None:
            self.value = value

    instance1 = SingletonOne("first")
    instance2 = SingletonTwo("second")

    assert type(instance1) is not type(instance2)  # type: ignore[comparison-overlap,unused-ignore]
    assert instance1.value == "first"
    assert instance2.value == "second"
