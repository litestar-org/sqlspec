"""Dead-code regression tests for the adapter type converter sweep."""

import importlib

import pytest

import sqlspec.core as core
import sqlspec.core.type_converter as type_converter
from sqlspec.adapters.adbc.type_converter import ADBCOutputConverter
from sqlspec.adapters.duckdb.type_converter import DuckDBOutputConverter
from sqlspec.adapters.oracledb.type_converter import OracleOutputConverter
from sqlspec.adapters.spanner.type_converter import SpannerOutputConverter
from sqlspec.core import BaseTypeConverter


def test_core_type_converter_module_no_longer_exposes_cached_path() -> None:
    """The shared converter module should no longer export the retired cache path."""
    assert not hasattr(core, "CachedOutputConverter")
    assert not hasattr(core, "DEFAULT_CACHE_SIZE")
    assert not hasattr(core, "DEFAULT_SPECIAL_CHARS")

    assert not hasattr(type_converter, "CachedOutputConverter")
    assert not hasattr(type_converter, "_CachedConverter")
    assert not hasattr(type_converter, "_make_cached_converter")
    assert not hasattr(type_converter, "DEFAULT_CACHE_SIZE")
    assert not hasattr(type_converter, "DEFAULT_SPECIAL_CHARS")
    assert "CachedOutputConverter" not in type_converter.__all__
    assert "DEFAULT_CACHE_SIZE" not in type_converter.__all__
    assert "DEFAULT_SPECIAL_CHARS" not in type_converter.__all__


@pytest.mark.parametrize(
    ("converter_cls",),
    [(ADBCOutputConverter,), (DuckDBOutputConverter,), (OracleOutputConverter,), (SpannerOutputConverter,)],
)
def test_output_converters_inherit_base_type_converter_directly(converter_cls: type[BaseTypeConverter]) -> None:
    """Adapter output converters should be attached directly to the shared base class."""
    assert converter_cls.__mro__[1] is BaseTypeConverter


def test_bigquery_type_converter_module_is_removed() -> None:
    """The dead BigQuery output converter module should no longer be importable."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("sqlspec.adapters.bigquery.type_converter")
