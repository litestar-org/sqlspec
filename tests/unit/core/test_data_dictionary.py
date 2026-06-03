"""Unit tests for the data dictionary registry and loader."""

from typing import cast

import pytest

import sqlspec.data_dictionary._registry as registry
from sqlspec.core import SQL
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    DialectConfig,
    get_data_dictionary_loader,
    get_dialect_config,
    list_registered_dialects,
    normalize_dialect_name,
)
from sqlspec.exceptions import SQLFileNotFoundError


def test_data_dictionary_loader_lists_known_dialects() -> None:
    """Ensure loader lists bundled dialect directories."""
    loader = DataDictionaryLoader()
    dialects = loader.list_dialects()
    assert "postgres" in dialects
    assert "sqlite" in dialects


def test_data_dictionary_loader_get_query_text() -> None:
    """Ensure loader returns SQL text for named queries."""
    loader = DataDictionaryLoader()
    query_text = loader.get_query_text("postgres", "tables_by_schema")
    assert "dependency_tree" in query_text
    assert "information_schema" in query_text


def test_data_dictionary_loader_get_query() -> None:
    """Ensure loader returns SQL objects for named queries."""
    loader = DataDictionaryLoader()
    query = loader.get_query("postgres", "tables_by_schema")
    assert isinstance(query, SQL)
    assert query.raw_sql is not None


def test_data_dictionary_loader_unknown_dialect_raises() -> None:
    """Ensure missing dialect paths raise SQLFileNotFoundError."""
    loader = DataDictionaryLoader()
    with pytest.raises(SQLFileNotFoundError):
        loader.get_query_text("not-a-dialect", "tables_by_schema")


def test_get_data_dictionary_loader_singleton() -> None:
    """Ensure the loader singleton returns the same instance."""
    first = get_data_dictionary_loader()
    second = get_data_dictionary_loader()
    assert first is second


def test_registry_normalizes_aliases() -> None:
    """Ensure dialect aliases normalize to canonical names."""
    assert normalize_dialect_name("PostgreSQL") == "postgres"
    assert normalize_dialect_name("mariadb") == "mysql"


def test_registry_lists_registered_dialects() -> None:
    """Ensure default dialects are registered."""
    dialects = list_registered_dialects()
    assert "postgres" in dialects
    assert "sqlite" in dialects


def test_get_dialect_config_unknown_raises() -> None:
    """Ensure unknown dialects raise ValueError."""
    with pytest.raises(ValueError, match="Unknown dialect"):
        get_dialect_config("not-a-dialect")


def test_get_dialect_config_features() -> None:
    """Ensure dialect configs expose feature flags and types."""
    config = get_dialect_config("postgres")
    assert config.get_feature_flag("supports_transactions") is True
    assert config.get_feature_version("supports_json") is not None
    assert config.get_optimal_type("json") == "JSONB"


def test_registry_dialects_loaded_annotation_is_bool() -> None:
    """_DIALECTS_LOADED should be annotated to avoid Literal[False] narrowing."""
    assert registry.__annotations__["_DIALECTS_LOADED"] is bool


def test_registry_load_default_dialects_sets_loaded_flag(monkeypatch) -> None:
    """_load_default_dialects sets the loaded flag after importing dialects."""
    calls: list[str] = []
    monkeypatch.setattr(registry, "_DIALECTS_LOADED", False)
    monkeypatch.setattr(registry.importlib, "import_module", lambda name: calls.append(name))
    registry._load_default_dialects()
    assert calls == ["sqlspec.data_dictionary.dialects"]
    assert registry._DIALECTS_LOADED is True


def test_registry_load_default_dialects_is_idempotent(monkeypatch) -> None:
    """_load_default_dialects should not import again after the flag is set."""
    monkeypatch.setattr(registry, "_DIALECTS_LOADED", True)

    def fail_import(name: str) -> None:
        raise AssertionError(name)

    monkeypatch.setattr(registry.importlib, "import_module", fail_import)
    registry._load_default_dialects()


def test_registry_get_dialect_config_triggers_load(monkeypatch) -> None:
    """get_dialect_config calls the default dialect loader before lookup."""
    calls = 0
    config = cast("DialectConfig", object())
    monkeypatch.setitem(registry._DIALECT_CONFIGS, "example", config)

    def fake_load() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(registry, "_load_default_dialects", fake_load)
    assert registry.get_dialect_config("example") is config
    assert calls == 1
