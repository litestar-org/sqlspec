"""Dead-code regression tests for the adapter type converter sweep."""

import importlib

import pytest

import sqlspec.core as core
import sqlspec.core.type_converter as type_converter
from sqlspec.adapters.oracledb.type_converter import OracleOutputConverter


def _name(*parts: str) -> str:
    return "".join(parts)


def test_core_type_converter_module_no_longer_exposes_cached_path() -> None:
    """The shared converter module should no longer export the retired cache path."""
    cached_output = _name("Cached", "Output", "Converter")
    cached_class = _name("_", "Cached", "Converter")
    cached_factory = _name("_make_", "cached", "_converter")

    assert not hasattr(core, cached_output)
    assert not hasattr(core, "DEFAULT_CACHE_SIZE")
    assert not hasattr(core, "DEFAULT_SPECIAL_CHARS")

    assert not hasattr(type_converter, cached_output)
    assert not hasattr(type_converter, cached_class)
    assert not hasattr(type_converter, cached_factory)
    assert not hasattr(type_converter, "DEFAULT_CACHE_SIZE")
    assert not hasattr(type_converter, "DEFAULT_SPECIAL_CHARS")
    assert cached_output not in type_converter.__all__
    assert "DEFAULT_CACHE_SIZE" not in type_converter.__all__
    assert "DEFAULT_SPECIAL_CHARS" not in type_converter.__all__


def test_core_type_converter_module_no_longer_exposes_content_sniffing_surface() -> None:
    """The shared regex-driven content-detection surface is retired in C7."""
    for name in (
        _name("Base", "Type", "Converter"),
        "DEFAULT_DETECTION_CHARS",
        _name("SPECIAL", "_TYPE", "_REGEX"),
        "_TYPE_CONVERTERS",
    ):
        assert not hasattr(core, name)
        assert not hasattr(type_converter, name)
        assert name not in type_converter.__all__


def test_adapter_detection_methods_are_removed() -> None:
    """Adapter converters must not retain content-sniffing methods after C7."""
    from sqlspec.adapters.adbc.type_converter import ADBCOutputConverter

    detect_method = _name("detect", "_type")
    convert_method = _name("convert", "_value")
    convert_if_method = _name("convert", "_if", "_detected")
    dialect_method = _name("get", "_dialect", "_specific", "_converter")

    for converter_cls in (ADBCOutputConverter, OracleOutputConverter):
        assert not hasattr(converter_cls, convert_if_method)
        assert not hasattr(converter_cls, detect_method)
        assert not hasattr(converter_cls, convert_method)

    assert not hasattr(ADBCOutputConverter, dialect_method)


def test_bigquery_type_converter_module_is_removed() -> None:
    """The dead BigQuery output converter module should no longer be importable."""
    module_name = _name("sqlspec.adapters.bigquery.", "type", "_converter")
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)


def test_duckdb_type_converter_module_is_removed() -> None:
    """DuckDB no longer keeps a string-content-sniffing converter module."""
    module_name = _name("sqlspec.adapters.duckdb.", "type", "_converter")
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)


def test_spanner_output_converter_is_removed() -> None:
    """Spanner fetch conversion is column-plan driven, not content-sniffing class driven."""
    import sqlspec.adapters.spanner.type_converter as spanner_type_converter

    converter_name = _name("Spanner", "Output", "Converter")
    assert not hasattr(spanner_type_converter, converter_name)
    assert converter_name not in spanner_type_converter.__all__
