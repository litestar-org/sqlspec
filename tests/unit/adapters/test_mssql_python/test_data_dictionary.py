"""Unit tests for the mssql_python data dictionary."""

from pathlib import Path
from typing import Any, cast

from sqlspec.adapters.mssql_python.data_dictionary import MssqlPythonSyncDataDictionary, MssqlVersionInfo

MSSQL_QUERY_DIR = Path("sqlspec/data_dictionary/sql/mssql")
EXPECTED_MSSQL_QUERY_FILES = {"columns.sql", "foreign_keys.sql", "indexes.sql", "tables.sql", "version.sql"}


class FakeSyncDriver:
    """Minimal sync driver for data dictionary tests."""

    def __init__(self) -> None:
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []

    def select_one_or_none(self, _statement: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            "version_string": "Microsoft SQL Server 2022 - 16.0.4131.2 (X64)",
            "product_version": "16.0.4131.2",
            "edition": "Developer Edition",
            "engine_edition": 3,
        }

    def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        if kwargs.get("table_name") == "app":
            return [{"column_name": "id"}]
        if len(self.select_calls) == 1:
            return [{"schema_name": "dbo", "table_name": "parent"}]
        if len(self.select_calls) == 2:
            return [{"schema_name": "dbo", "table_name": "parent"}, {"schema_name": "dbo", "table_name": "orphan"}]
        return []


def test_mssql_query_files_follow_dialect_category_layout() -> None:
    """MSSQL data-dictionary SQL should use the existing per-category file layout."""
    assert {path.name for path in MSSQL_QUERY_DIR.glob("*.sql")} == EXPECTED_MSSQL_QUERY_FILES


def test_sync_data_dictionary_builds_version_info() -> None:
    """The sync dictionary should parse product version and edition metadata."""
    driver = FakeSyncDriver()
    data_dictionary = MssqlPythonSyncDataDictionary()

    version = data_dictionary.get_version(cast(Any, driver))

    assert isinstance(version, MssqlVersionInfo)
    assert version.major == 16
    assert version.build == 4131
    assert version.revision == 2
    assert version.edition == "Developer Edition"
    assert version.is_azure_sql is False
    assert data_dictionary.get_feature_flag(cast(Any, driver), "supports_greatest_least") is True
    assert data_dictionary.get_feature_flag(cast(Any, driver), "supports_native_json") is False


def test_mssql_version_info_uses_build_in_version_tuple_not_patch() -> None:
    version = MssqlVersionInfo(16, 0, 5050)

    assert version.patch == 0
    assert version.build == 5050
    assert version.version_tuple == (16, 0, 5050)


def test_sync_data_dictionary_merges_table_lists_with_default_schema() -> None:
    """The sync dictionary should query dbo by default and append unordered tables."""
    driver = FakeSyncDriver()
    data_dictionary = MssqlPythonSyncDataDictionary()

    tables = data_dictionary.get_tables(cast(Any, driver))

    assert tables == [{"schema_name": "dbo", "table_name": "parent"}, {"schema_name": "dbo", "table_name": "orphan"}]
    assert driver.select_calls[0][1]["schema_name"] == "dbo"
    assert driver.select_calls[1][1]["schema_name"] == "dbo"


def test_sync_data_dictionary_selects_columns_by_table() -> None:
    """Table-scoped metadata calls should bind schema and table parameters."""
    driver = FakeSyncDriver()
    data_dictionary = MssqlPythonSyncDataDictionary()

    columns = data_dictionary.get_columns(cast(Any, driver), table="app", schema="custom")

    assert columns == [{"column_name": "id"}]
    assert driver.select_calls[0][1]["schema_name"] == "custom"
    assert driver.select_calls[0][1]["table_name"] == "app"
