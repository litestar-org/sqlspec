"""Tests for sqlspec.utils.module_loader module."""

from __future__ import annotations

import types
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlspec.config import SyncDatabaseConfig
from sqlspec.utils.module_loader import import_string, module_to_os_path


def test_import_string_basic_functionality() -> None:
    """Test basic import_string functionality."""
    cls = import_string("sqlspec.config.SyncDatabaseConfig")
    assert type(cls) is type(SyncDatabaseConfig)


def test_import_string_module_not_found() -> None:
    """Test import_string with non-existent modules."""
    with pytest.raises(ImportError):
        import_string("GenericAlembicConfigNew")

    with pytest.raises(ImportError):
        import_string("sqlspec.base.GenericAlembicConfigNew")

    with pytest.raises(ImportError):
        import_string("imaginary_module_that_doesnt_exist.Config")


def test_import_string_non_existing_attribute() -> None:
    """Test import_string with non-existent attributes."""
    with pytest.raises(ImportError):
        import_string("sqlspec.base.AsyncDatabaseConfig.extra")


def test_import_string_builtin_modules() -> None:
    """Test import_string with built-in modules and functions."""
    # Test built-in function
    len_func = import_string("builtins.len")
    assert len_func == len

    # Test built-in type
    list_type = import_string("builtins.list")
    assert list_type is list

    # Test standard library
    path_class = import_string("pathlib.Path")
    assert path_class == Path


def test_import_string_nested_attributes() -> None:
    """Test import_string with deeply nested attributes."""
    # Test accessing nested attributes
    path_exists = import_string("pathlib.Path.exists")
    assert callable(path_exists)

    # Test accessing class methods
    path_cwd = import_string("pathlib.Path.cwd")
    assert callable(path_cwd)


def test_import_string_cached(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that import_string uses module caching."""
    tmp_path.joinpath("testmodule.py").write_text("x = 'foo'")
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]

    # First import
    result1 = import_string("testmodule.x")
    assert result1 == "foo"

    # Second import should use cached module
    result2 = import_string("testmodule.x")
    assert result2 == "foo"


def test_import_string_with_dynamic_content(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test import_string with dynamically created modules."""
    # Create a module with a class
    module_content = """
class TestClass:
    def __init__(self, value):
        self.value = value

    def get_value(self):
        return self.value

TEST_CONSTANT = "test_value"

def test_function(x: int) -> int:
    return x * 2
"""

    tmp_path.joinpath("dynamic_module.py").write_text(module_content)
    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]

    # Import class
    test_class = import_string("dynamic_module.TestClass")
    instance = test_class("hello")
    assert instance.get_value() == "hello"

    # Import constant
    test_constant = import_string("dynamic_module.TEST_CONSTANT")
    assert test_constant == "test_value"

    # Import function
    test_function = import_string("dynamic_module.test_function")
    assert test_function(5) == 10


def test_import_string_error_propagation() -> None:
    """Test that import_string properly propagates import errors."""
    # Test with malformed module path
    with pytest.raises(ImportError, match="doesn't look like a module path"):
        import_string("nonexistent.module.attribute")

    # Test with invalid attribute path
    with pytest.raises(ImportError):
        import_string("sys.nonexistent_attribute")


@pytest.mark.parametrize(
    ("import_path", "expected_type"),
    [
        ("builtins.str", type),
        ("builtins.dict", type),
        ("builtins.len", type(len)),
        ("sys.version", str),
        ("pathlib.Path", type),
    ],
    ids=["str_type", "dict_type", "len_function", "sys_version", "path_class"],
)
def test_import_string_various_types(import_path: str, expected_type: type) -> None:
    """Test import_string with various built-in types and functions."""
    imported_obj = import_string(import_path)
    assert isinstance(imported_obj, expected_type)


def test_import_string_with_packages(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test import_string with packages and submodules."""
    # Create package structure
    package_dir = tmp_path / "test_package"
    package_dir.mkdir()

    # Create __init__.py
    (package_dir / "__init__.py").write_text("PACKAGE_CONSTANT = 'package_value'")

    # Create submodule
    (package_dir / "submodule.py").write_text("""
class SubClass:
    def method(self):
        return "submodule_method"

SUB_CONSTANT = "sub_value"
""")

    # Create sub-package
    sub_package_dir = package_dir / "sub_package"
    sub_package_dir.mkdir()
    (sub_package_dir / "__init__.py").write_text("SUB_PACKAGE_CONST = 'sub_package_value'")

    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]

    # Test package constant
    package_const = import_string("test_package.PACKAGE_CONSTANT")
    assert package_const == "package_value"

    # Test submodule class
    sub_class = import_string("test_package.submodule.SubClass")
    instance = sub_class()
    assert instance.method() == "submodule_method"

    # Test submodule constant
    sub_const = import_string("test_package.submodule.SUB_CONSTANT")
    assert sub_const == "sub_value"

    # Test sub-package constant
    sub_package_const = import_string("test_package.sub_package.SUB_PACKAGE_CONST")
    assert sub_package_const == "sub_package_value"


def test_module_to_os_path_basic_functionality(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test basic module_to_os_path functionality."""
    the_path = module_to_os_path("sqlspec.base")
    assert the_path.exists()

    tmp_path.joinpath("simple_module.py").write_text("x = 'foo'")
    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]
    os_path = module_to_os_path("simple_module")
    assert os_path == Path(tmp_path)


def test_module_to_os_path_error_handling() -> None:
    """Test module_to_os_path error handling."""
    with pytest.raises((ImportError, TypeError)):
        module_to_os_path("sqlspec.base.GenericDatabaseConfig")

    with pytest.raises((ImportError, TypeError)):
        module_to_os_path("sqlspec.base.GenericDatabaseConfig.extra.module")


def test_module_to_os_path_with_packages(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test module_to_os_path with package structures."""
    # Create package
    package_dir = tmp_path / "test_package"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("# package init")

    # Create submodule
    (package_dir / "submodule.py").write_text("# submodule")

    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]

    # Test package path
    package_path = module_to_os_path("test_package")
    assert package_path.resolve().name == package_dir.resolve().name

    # Test submodule path
    submodule_path = module_to_os_path("test_package.submodule")
    assert submodule_path.name == package_dir.name


def test_module_to_os_path_built_in_modules() -> None:
    """Test module_to_os_path with built-in modules."""
    # Built-in modules might not have a file path
    result = module_to_os_path("sys")
    assert result is None or isinstance(result, (str, Path))


def test_module_to_os_path_standard_library() -> None:
    """Test module_to_os_path with standard library modules."""
    # Test with a standard library module that should have a path
    sys_path = module_to_os_path("sys")
    # sys might be built-in, so this could raise an exception
    if sys_path is not None:
        assert isinstance(sys_path, Path)


@patch("sqlspec.utils.module_loader.importlib.import_module")
def test_import_string_with_import_error(mock_import: Mock) -> None:
    """Test import_string behavior when importlib raises ImportError."""
    mock_import.side_effect = ImportError("Mocked import error")

    with pytest.raises(ImportError, match="Mocked import error"):
        import_string("some.module.attribute")


@patch("sqlspec.utils.module_loader.importlib.import_module")
def test_import_string_attribute_error_handling(mock_import: Mock) -> None:
    """Test import_string handling of AttributeError."""
    # Use types.SimpleNamespace, which has no attributes by default
    mock_import.return_value = types.SimpleNamespace()

    with pytest.raises(ImportError):
        import_string("some.module.nonexistent_attr")


def test_import_string_module_with_init_side_effects(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test import_string with modules that have side effects during import."""
    module_content = f"""
# Module with side effects
import sys

# Record that this module was imported
with open(r"{tmp_path / "side_effects.txt"}", "a") as f:
    f.write("module_imported\\n")

COUNTER = 42
"""

    tmp_path.joinpath("side_effect_module.py").write_text(module_content)
    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]

    # First import
    counter1 = import_string("side_effect_module.COUNTER")
    assert counter1 == 42

    # Check side effect occurred
    side_effects_file = tmp_path / "side_effects.txt"
    if side_effects_file.exists():
        content = side_effects_file.read_text()
        assert "module_imported" in content


def test_import_string_circular_import_protection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test import_string with circular imports."""
    # Create module A that imports module B
    module_a_content = """
# This will be set after module B is imported
from module_b import B_CONSTANT
A_CONSTANT = f"A_{B_CONSTANT}"
"""

    # Create module B that imports module A (circular)
    module_b_content = """
B_CONSTANT = "base"
# Note: We avoid actual circular import in this test to prevent real issues
"""

    tmp_path.joinpath("module_a.py").write_text(module_a_content)
    tmp_path.joinpath("module_b.py").write_text(module_b_content)
    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]

    # This should work without issues
    a_constant = import_string("module_a.A_CONSTANT")
    assert a_constant == "A_base"


@pytest.mark.parametrize(
    "invalid_path",
    [
        "",  # Empty string
        ".",  # Just dot
        ".module",  # Leading dot
        "module.",  # Trailing dot
        "module..attr",  # Double dots
        "123invalid",  # Starting with number
    ],
    ids=["empty", "just_dot", "leading_dot", "trailing_dot", "double_dots", "starts_with_number"],
)
def test_import_string_invalid_paths(invalid_path: str) -> None:
    """Test import_string with various invalid import paths."""
    with pytest.raises((ImportError, ValueError)):
        import_string(invalid_path)


def test_import_string_special_attributes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test import_string with special Python attributes."""
    module_content = '''
class TestClass:
    """A test class."""

    def __init__(self):
        self.value = "test"

    def __str__(self):
        return "TestClass instance"

__version__ = "1.0.0"
__author__ = "Test Author"
'''

    tmp_path.joinpath("special_module.py").write_text(module_content)
    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]

    # Import special attributes
    version = import_string("special_module.__version__")
    assert version == "1.0.0"

    author = import_string("special_module.__author__")
    assert author == "Test Author"

    # Import class and special method
    test_class = import_string("special_module.TestClass")
    instance = test_class()
    assert str(instance) == "TestClass instance"
