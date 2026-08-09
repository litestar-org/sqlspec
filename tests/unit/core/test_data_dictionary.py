"""Unit tests for the data dictionary registry and loader."""

import re
from importlib import resources
from typing import Any, cast

import pytest

import sqlspec.data_dictionary._registry as registry
from sqlspec.core import SQL
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    DialectConfig,
    VersionInfo,
    get_data_dictionary_loader,
    get_dialect_config,
    list_registered_dialects,
    normalize_dialect_name,
)


def test_data_dictionary_loader_lists_known_dialects() -> None:
    """Ensure loader lists bundled dialect directories."""
    loader = DataDictionaryLoader()
    dialects = loader.list_dialects()
    assert "postgres" in dialects
    assert "sqlite" in dialects


def test_data_dictionary_loader_get_domain_query_text() -> None:
    """Ensure loader returns SQL text for named queries."""
    loader = DataDictionaryLoader()
    query_text = loader.get_domain_query_text("postgres", "tables", "by_schema")
    assert query_text is not None
    assert "dependency_tree" in query_text
    assert "pg_catalog" in query_text


def test_data_dictionary_loader_get_domain_query() -> None:
    """Ensure loader returns SQL objects for named queries."""
    loader = DataDictionaryLoader()
    query = loader.get_domain_query("postgres", "tables", "by_schema")
    assert isinstance(query.sql, SQL)
    assert query.sql.raw_sql is not None


def test_data_dictionary_loader_unknown_dialect_is_unsupported() -> None:
    """Ensure missing dialect paths return an unsupported domain query."""
    loader = DataDictionaryLoader()
    query = loader.get_domain_query("not-a-dialect", "tables", "by_schema")
    assert query.is_supported is False


def test_data_dictionary_query_names_are_domain_relative() -> None:
    """Ensure query names do not repeat the domain supplied by their file."""
    dialects_root = resources.files("sqlspec.data_dictionary.dialects")
    violations: list[str] = []
    for dialect_path in dialects_root.iterdir():
        sql_root = dialect_path.joinpath("sql")
        if not sql_root.is_dir():
            continue
        for sql_file in _iter_sql_resources(sql_root):
            domain = sql_file.name.removesuffix(".sql")
            violations.extend(
                f"{dialect_path.name}/{domain}/{query_name}"
                for query_name in re.findall(r"^-- name: (\S+)", sql_file.read_text(), re.MULTILINE)
                if query_name.startswith(f"{domain}_")
            )
    assert violations == []


def _iter_sql_resources(root: Any) -> list[Any]:
    """Return SQL resources below a traversable root."""
    resources_to_visit = [root]
    sql_files = []
    while resources_to_visit:
        current = resources_to_visit.pop()
        for child in current.iterdir():
            if child.is_dir():
                resources_to_visit.append(child)
            elif child.name.endswith(".sql"):
                sql_files.append(child)
    return sql_files


def test_get_data_dictionary_loader_singleton() -> None:
    """Ensure the loader singleton returns the same instance."""
    first = get_data_dictionary_loader()
    second = get_data_dictionary_loader()
    assert first is second


def test_registry_normalizes_aliases() -> None:
    """Ensure dialect aliases normalize to canonical names."""
    assert normalize_dialect_name("PostgreSQL") == "postgres"
    assert normalize_dialect_name("mysql") == "mysql"
    assert normalize_dialect_name("mariadb") == "mariadb"


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


def test_row_locking_capability_flags_are_introspectable() -> None:
    """Dialect configs expose row-locking support for queue claim logic."""
    expected_for_update = {
        "bigquery": False,
        "cockroachdb": True,
        "duckdb": False,
        "mssql": False,
        "mysql": True,
        "oracle": True,
        "postgres": True,
        "spanner": False,
        "sqlite": False,
    }
    expected_static_skip_locked = {
        "bigquery": False,
        "cockroachdb": True,
        "duckdb": False,
        "mssql": False,
        "oracle": True,
        "spanner": False,
        "sqlite": False,
    }

    for dialect, expected in expected_for_update.items():
        assert get_dialect_config(dialect).get_feature_flag("supports_for_update") is expected

    for dialect, expected in expected_static_skip_locked.items():
        assert get_dialect_config(dialect).get_feature_flag("supports_skip_locked") is expected

    assert get_dialect_config("postgres").get_feature_version("supports_skip_locked") == VersionInfo(9, 5, 0)
    assert get_dialect_config("mysql").get_feature_version("supports_skip_locked") == VersionInfo(8, 0, 1)


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
