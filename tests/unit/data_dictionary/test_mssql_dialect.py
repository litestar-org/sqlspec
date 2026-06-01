"""Unit tests for MSSQL data dictionary dialect helpers."""

import pytest

from sqlspec.data_dictionary import TableMetadata
from sqlspec.data_dictionary.dialects.mssql import (
    MSSQL_CONFIG,
    is_mssql_azure_sql,
    list_mssql_available_features,
    merge_mssql_table_lists,
    mssql_supports_greatest_least,
    mssql_supports_json_functions,
    mssql_supports_native_json,
    mssql_supports_string_agg,
    parse_mssql_engine_edition,
    parse_mssql_version_components,
    resolve_mssql_feature_flag,
)


@pytest.mark.parametrize(
    ("version_string", "expected"),
    [
        ("Microsoft SQL Server 2022 (RTM-CU13) (KB5036432) - 16.0.4131.2 (X64)", (16, 0, 4131, 2)),
        ("Microsoft SQL Server 2019 (RTM-CU25) (KB5033688) - 15.0.4360.2 (X64)", (15, 0, 4360, 2)),
        ("Microsoft SQL Server 2017 (RTM-CU31) (KB5016884) - 14.0.3456.2 (X64)", (14, 0, 3456, 2)),
        ("Microsoft SQL Azure (RTM) - 12.0.2000.8", (12, 0, 2000, 8)),
    ],
)
def test_parse_mssql_version_components(version_string: str, expected: tuple[int, int, int, int]) -> None:
    """MSSQL version strings should parse into four integer components."""
    assert parse_mssql_version_components(version_string) == expected


def test_feature_predicates_sql_server_2016() -> None:
    """SQL Server 2016 should expose JSON functions but not later features."""
    assert mssql_supports_json_functions(13) is True
    assert mssql_supports_string_agg(13) is False
    assert mssql_supports_greatest_least(13) is False
    assert mssql_supports_native_json(13) is False


def test_feature_predicates_sql_server_2022() -> None:
    """SQL Server 2022 should expose STRING_AGG and GREATEST/LEAST."""
    assert mssql_supports_json_functions(16) is True
    assert mssql_supports_string_agg(16) is True
    assert mssql_supports_greatest_least(16) is True
    assert mssql_supports_native_json(16) is False


def test_feature_predicates_sql_server_2025() -> None:
    """SQL Server 2025 should expose the native JSON type."""
    assert mssql_supports_native_json(17) is True


def test_azure_sql_native_json_via_feature_probe() -> None:
    """Azure SQL can opt into native JSON even with evergreen major versions."""
    assert mssql_supports_native_json(12, is_azure_sql=True) is True


def test_list_mssql_available_features_includes_dynamic_flags() -> None:
    """The feature list should include static and MSSQL-specific dynamic flags."""
    features = list_mssql_available_features()

    assert "supports_json_functions" in features
    assert "supports_string_agg" in features
    assert "supports_transactions" in features


def test_resolve_mssql_feature_flag_uses_version_predicates() -> None:
    """Dynamic flags should resolve from SQL Server major version information."""
    assert resolve_mssql_feature_flag("supports_greatest_least", major=16) is True
    assert resolve_mssql_feature_flag("supports_greatest_least", major=15) is False


@pytest.mark.parametrize(("value", "expected"), [(5, 5), ("8", 8), (b"11", 11), (None, None), ("not-an-edition", None)])
def test_parse_mssql_engine_edition(value: object, expected: int | None) -> None:
    """EngineEdition values should parse defensively from driver-returned scalars."""
    assert parse_mssql_engine_edition(value) == expected


@pytest.mark.parametrize("edition", [5, 8, 11])
def test_is_mssql_azure_sql(edition: int) -> None:
    """Azure SQL engine editions should be recognized."""
    assert is_mssql_azure_sql(edition) is True


def test_mssql_default_schema_is_dbo() -> None:
    """MSSQL should default introspection to dbo."""
    mssql_module = __import__("sqlspec.data_dictionary.dialects.mssql", fromlist=["resolve_mssql_default_schema"])

    assert not hasattr(mssql_module, "resolve_mssql_default_schema")
    assert MSSQL_CONFIG.default_schema == "dbo"


def test_merge_mssql_table_lists_preserves_dependency_order_and_appends_orphans() -> None:
    """Dependency-ordered rows should win and catalog-only rows should be appended."""
    ordered: list[TableMetadata] = [
        {"schema_name": "dbo", "table_name": "parent"},
        {"schema_name": "dbo", "table_name": "child"},
    ]
    all_rows: list[TableMetadata] = [
        {"schema_name": "dbo", "table_name": "child"},
        {"schema_name": "dbo", "table_name": "orphan"},
        {"schema_name": "dbo", "table_name": "parent"},
    ]

    assert merge_mssql_table_lists(ordered, all_rows) == [
        {"schema_name": "dbo", "table_name": "parent"},
        {"schema_name": "dbo", "table_name": "child"},
        {"schema_name": "dbo", "table_name": "orphan"},
    ]
