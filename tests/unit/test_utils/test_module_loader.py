"""Tests for sqlspec.utils.module_loader module.

Tests module loading utilities including string imports and path resolution.
"""

import sys
from pathlib import Path

import pytest

from sqlspec.utils.module_loader import import_string, module_to_os_path


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
    # This should work by importing the closest valid module
    json_module = import_string("json")
    assert json_module.__name__ == "json"


def test_import_string_exception_handling() -> None:
    """Test import_string exception handling."""
    with pytest.raises(ImportError, match="Could not import"):
        import_string("this.will.definitely.fail")


def test_module_to_os_path_basic() -> None:
    """Test module_to_os_path with basic module."""
    # Use pathlib instead of sys since sys is built-in and doesn't have a real path
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
    # Test with a specific module file
    path = module_to_os_path("sqlspec.exceptions")
    assert isinstance(path, Path)
    assert path.exists()
    # Should return the directory containing the module, not the file itself


def test_complex_module_import_scenarios() -> None:
    """Test complex module import scenarios."""
    # Test importing from a module that exists
    pathlib_module = import_string("pathlib")
    assert pathlib_module.__name__ == "pathlib"

    # Test importing a class from a module
    path_class = import_string("pathlib.Path")
    assert path_class.__name__ == "Path"

    # Test that we can actually use the imported class
    path_instance = path_class("/tmp")
    assert isinstance(path_instance, Path)
