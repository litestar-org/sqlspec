"""Unit tests for Sybase ASE and SAP SQL Anywhere data dictionary configurations and SQL packs."""

import pytest

from sqlspec.data_dictionary import DataDictionaryLoader, TableMetadata, VersionInfo, get_dialect_config
from sqlspec.data_dictionary.dialects import SQLANYWHERE_CONFIG, SYBASE_CONFIG
from sqlspec.data_dictionary.dialects.sqlanywhere import (
    SQLANYWHERE_METADATA_DOMAINS,
    SqlanywhereVersionInfo,
    build_sqlanywhere_metadata_capability_profile,
    extract_sqlanywhere_version_value,
    list_sqlanywhere_available_features,
    merge_sqlanywhere_table_lists,
    resolve_sqlanywhere_feature_flag,
)
from sqlspec.data_dictionary.dialects.sybase import (
    SYBASE_METADATA_DOMAINS,
    SybaseVersionInfo,
    build_sybase_metadata_capability_profile,
    extract_sybase_version_value,
    list_sybase_available_features,
    merge_sybase_table_lists,
    resolve_sybase_feature_flag,
)


def test_sybase_and_sqlanywhere_dialect_configs_registered() -> None:
    """Both sybase and sqlanywhere dialect configs should resolve by canonical name and alias."""
    assert get_dialect_config("sybase") is SYBASE_CONFIG
    assert get_dialect_config("ase") is SYBASE_CONFIG
    assert get_dialect_config("sqlanywhere") is SQLANYWHERE_CONFIG
    assert get_dialect_config("sa") is SQLANYWHERE_CONFIG

    assert SYBASE_CONFIG.name == "sybase"
    assert SYBASE_CONFIG.default_schema == "dbo"
    assert SYBASE_CONFIG.get_feature_flag("supports_cte") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_window_functions") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_merge") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_returning") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_transactional_ddl") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_transactions") is True
    assert SYBASE_CONFIG.get_optimal_type("boolean") == "BIT"
    assert SYBASE_CONFIG.get_optimal_type("timestamp") == "BIGDATETIME"
    assert SYBASE_CONFIG.get_optimal_type("blob") == "IMAGE"

    assert SQLANYWHERE_CONFIG.name == "sqlanywhere"
    assert SQLANYWHERE_CONFIG.default_schema == "DBA"
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_cte") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_window_functions") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_merge") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_for_update") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_skip_locked") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_transactional_ddl") is False
    assert SQLANYWHERE_CONFIG.get_optimal_type("uuid") == "UNIQUEIDENTIFIER"
    assert SQLANYWHERE_CONFIG.get_optimal_type("text") == "LONG VARCHAR"
    assert SQLANYWHERE_CONFIG.get_optimal_type("blob") == "LONG BINARY"


def test_sybase_and_sqlanywhere_version_patterns() -> None:
    """Version regexes should extract major, minor, and patch components for ASE and SQL Anywhere."""
    ase_banner = (
        "Adaptive Server Enterprise/16.0 SP04 PL04/EBF 30626 SMP/P/x86_64/"
        "Enterprise Linux/ase160sp04pl04x/0/64-bit/FBO/Thu"
    )
    ase_match = SYBASE_CONFIG.version_pattern.search(ase_banner)
    assert ase_match is not None
    assert tuple(int(part or 0) for part in ase_match.groups()) == (16, 0, 4)

    sa_banner = "17.0.11.7458"
    sa_match = SQLANYWHERE_CONFIG.version_pattern.search(sa_banner)
    assert sa_match is not None
    assert tuple(int(part or 0) for part in sa_match.groups()) == (17, 0, 11)


def test_sybase_and_sqlanywhere_reflection_helpers() -> None:
    """Reflection helpers should extract version values, resolve features, and merge table lists."""
    ase_version = SybaseVersionInfo(16, 0, 4, "SP04")
    assert str(ase_version) == "16.0.4 (SP04)"
    assert extract_sybase_version_value({"version_string": "Adaptive Server Enterprise/16.0 SP04"}) == (
        "Adaptive Server Enterprise/16.0 SP04"
    )
    assert resolve_sybase_feature_flag("supports_transactions", ase_version) is True
    assert resolve_sybase_feature_flag("supports_cte", ase_version) is False
    assert "supports_transactions" in list_sybase_available_features()

    sa_version = SqlanywhereVersionInfo(17, 0, 11, "7458")
    assert str(sa_version) == "17.0.11 (7458)"
    assert extract_sqlanywhere_version_value({"version_string": "17.0.11.7458"}) == "17.0.11.7458"
    assert resolve_sqlanywhere_feature_flag("supports_cte", sa_version) is True
    assert resolve_sqlanywhere_feature_flag("supports_window_functions", VersionInfo(12, 0, 0)) is True
    assert "supports_cte" in list_sqlanywhere_available_features()

    ordered: list[TableMetadata] = [{"schema_name": "dbo", "table_name": "users"}]
    all_rows: list[TableMetadata] = [
        {"schema_name": "dbo", "table_name": "posts"},
        {"schema_name": "dbo", "table_name": "users"},
    ]
    assert merge_sybase_table_lists(ordered, all_rows) == [
        {"schema_name": "dbo", "table_name": "users"},
        {"schema_name": "dbo", "table_name": "posts"},
    ]
    assert merge_sqlanywhere_table_lists(ordered, all_rows) == [
        {"schema_name": "dbo", "table_name": "users"},
        {"schema_name": "dbo", "table_name": "posts"},
    ]

    sybase_profile = build_sybase_metadata_capability_profile("sybase")
    assert sybase_profile.dialect == "sybase"
    assert len(sybase_profile.capabilities) == len(SYBASE_METADATA_DOMAINS)

    sa_profile = build_sqlanywhere_metadata_capability_profile("sqlanywhere")
    assert sa_profile.dialect == "sqlanywhere"
    assert len(sa_profile.capabilities) == len(SQLANYWHERE_METADATA_DOMAINS)


@pytest.mark.parametrize("dialect", ["sybase", "sqlanywhere"])
@pytest.mark.parametrize(
    ("domain", "query_names"),
    [
        ("version", ("current",)),
        ("schemas", ("by_schema", "current")),
        ("tables", ("by_schema", "all_by_schema", "names_by_schema")),
        ("columns", ("by_schema", "by_table")),
        ("indexes", ("by_schema", "by_table")),
        ("foreign_keys", ("by_schema", "by_table")),
        ("constraints", ("by_schema", "by_table")),
        ("objects", ("by_schema",)),
        ("views", ("by_schema",)),
    ],
)
def test_all_domain_queries_load_for_sybase_and_sqlanywhere(
    dialect: str, domain: str, query_names: tuple[str, ...]
) -> None:
    """DataDictionaryLoader should load all 9 domains and named queries for sybase and sqlanywhere."""
    loader = DataDictionaryLoader()
    assert loader.has_domain_queries(dialect, domain) is True
    for query_name in query_names:
        query = loader.get_domain_query(dialect, domain, query_name)
        assert query.is_supported is True
        assert query.query_text is not None
        assert query.query_text.strip() != ""


def test_sybase_and_sqlanywhere_catalog_tables_referenced() -> None:
    """Sybase queries should use dbo.sys* catalogs and SQL Anywhere queries should use SYS.SYS* catalogs."""
    loader = DataDictionaryLoader()

    sybase_tables = loader.get_domain_query_text("sybase", "tables", "by_schema") or ""
    sybase_columns = loader.get_domain_query_text("sybase", "columns", "by_table") or ""
    sybase_fks = loader.get_domain_query_text("sybase", "foreign_keys", "by_table") or ""
    assert "dbo.sysobjects" in sybase_tables
    assert "dbo.syscolumns" in sybase_columns
    assert "dbo.systypes" in sybase_columns
    assert "dbo.sysreferences" in sybase_fks

    sa_tables = loader.get_domain_query_text("sqlanywhere", "tables", "by_schema") or ""
    sa_columns = loader.get_domain_query_text("sqlanywhere", "columns", "by_table") or ""
    sa_fks = loader.get_domain_query_text("sqlanywhere", "foreign_keys", "by_table") or ""
    assert "SYS.SYSTAB" in sa_tables
    assert "SYS.SYSTABCOL" in sa_columns
    assert "SYS.SYSDOMAIN" in sa_columns
    assert "SYS.SYSFKEY" in sa_fks
