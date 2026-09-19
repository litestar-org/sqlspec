"""Tests for names promoted to the public package surfaces."""

import importlib
import pickle
import subprocess
import sys

import pytest

import sqlspec
from sqlspec.utils import uuids


def test_top_level_uuid_exports() -> None:
    """The top-level package re-exports the identifier generators from ``sqlspec.utils.uuids``."""
    assert sqlspec.uuid4 is uuids.uuid4
    assert sqlspec.uuid6 is uuids.uuid6
    assert sqlspec.uuid7 is uuids.uuid7
    assert sqlspec.nanoid is uuids.nanoid


def test_top_level_exports_resolve() -> None:
    """Every name in ``sqlspec.__all__`` resolves, including lazily loaded names."""
    for name in sqlspec.__all__:
        getattr(sqlspec, name)


def test_builder_exports_retain_defining_objects() -> None:
    """All builder exports remain real objects through normal and star imports."""
    from sqlspec import builder
    from sqlspec.builder._factory import sql

    namespace: dict[str, object] = {}
    exec("from sqlspec.builder import *", namespace)
    for name in builder.__all__:
        value = getattr(builder, name)
        assert namespace[name] is value
        if name == "sql":
            assert value is sql
        else:
            assert value is getattr(importlib.import_module(value.__module__), value.__name__)
    assert set(builder.__all__).issubset(dir(builder))
    with pytest.raises(AttributeError, match="has no attribute 'unknown_builder'"):
        getattr(builder, "unknown_builder")


def test_exported_builder_class_supports_subclasses_and_pickle() -> None:
    from sqlspec.builder import Select

    class AppSelect(Select):
        pass

    assert AppSelect("1").build().sql == Select("1").build().sql
    assert pickle.loads(pickle.dumps(Select)) is Select


@pytest.mark.parametrize("defining_first", [False, True])
def test_builder_import_order_and_concurrent_access(defining_first: bool) -> None:
    script = f"""
import importlib
from concurrent.futures import ThreadPoolExecutor

if {defining_first!r}:
    importlib.import_module('sqlspec.builder._factory')
builder = importlib.import_module('sqlspec.builder')
with ThreadPoolExecutor(max_workers=4) as pool:
    factories = list(pool.map(lambda _: builder.sql, range(8)))
from sqlspec.builder._factory import sql
assert all(factory is sql for factory in factories)
assert builder.Select('1').build().sql == sql.select('1').build().sql
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
