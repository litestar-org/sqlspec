"""Migration package exports retain the defining objects and import behavior."""

from importlib import import_module
from types import ModuleType

import pytest

from sqlspec import migrations


def test_migration_exports_resolve_to_defining_objects() -> None:
    """Facade resolution returns real classes and functions, including star imports."""
    namespace: dict[str, object] = {}
    exec("from sqlspec.migrations import *", namespace)
    for name in migrations.__all__:
        value = getattr(migrations, name)
        assert namespace[name] is value
        assert value is getattr(import_module(value.__module__), name)
        assert migrations.__getattr__(name) is value
    assert set(migrations.__all__).issubset(dir(migrations))


@pytest.mark.parametrize("name", ["commands", "loaders", "runner", "schema", "squash", "tracker", "utils"])
def test_migration_submodule_exports(name: str) -> None:
    """Previously available package module attributes remain importable."""
    module = getattr(migrations, name)
    assert isinstance(module, ModuleType)
    assert module is import_module(f"sqlspec.migrations.{name}")


def test_unknown_migration_export() -> None:
    """Unknown names report the normal module attribute error."""
    with pytest.raises(AttributeError, match=r"sqlspec\.migrations.*missing_export"):
        getattr(migrations, "missing_export")
