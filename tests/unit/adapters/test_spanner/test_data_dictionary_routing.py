"""Tests for SpannerDataDictionary query routing."""

from typing import Any, cast

from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary
from sqlspec.data_dictionary import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata


class MockSpannerDriver:
    """Mock SpannerSyncDriver to capture query execution."""

    def __init__(self) -> None:
        self.last_query: Any = None
        self.last_params: dict[str, Any] = {}

    def select(self, query: Any, **params: Any) -> list[Any]:
        """Capture query and parameters."""
        self.last_query = query
        self.last_params = params
        schema_type = params.get("schema_type")
        if schema_type is TableMetadata:
            return [TableMetadata(name="users", schema="public", table_type="BASE TABLE")]
        if schema_type is ColumnMetadata:
            return [ColumnMetadata(name="id", table="users", schema="public", data_type="INT64", ordinal_position=1)]
        if schema_type is IndexMetadata:
            return [IndexMetadata(name="users_by_name", table="users", schema="public", columns=["name"])]
        if schema_type is ForeignKeyMetadata:
            return [
                ForeignKeyMetadata(
                    table_name="users",
                    column_name="org_id",
                    referenced_table="orgs",
                    referenced_column="id",
                    constraint_name="fk_users_org",
                    schema="public",
                    referenced_schema="public",
                )
            ]
        return []


def test_spanner_data_dictionary_default_mode() -> None:
    """Default mode is googlesql."""
    dictionary = SpannerDataDictionary()
    assert dictionary.mode == "googlesql"


def test_get_tables_routes_to_googlesql() -> None:
    """get_tables executes query containing GoogleSQL fields."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    tables = dictionary.get_tables(cast(Any, driver))

    assert len(tables) == 1
    query_sql = str(driver.last_query)
    assert "ROW_DELETION_POLICY_EXPRESSION" in query_sql
    assert "SPANNER_STATE" in query_sql


def test_get_columns_routes_to_googlesql() -> None:
    """get_columns executes query containing GoogleSQL column extensions."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    columns = dictionary.get_columns(cast(Any, driver), table="users")

    assert len(columns) == 1
    query_sql = str(driver.last_query)
    assert "SPANNER_TYPE" in query_sql
    assert "IS_STORED" in query_sql


def test_get_indexes_routes_to_googlesql() -> None:
    """get_indexes executes query containing GoogleSQL index extensions."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    indexes = dictionary.get_indexes(cast(Any, driver), table="users")

    assert len(indexes) == 1
    query_sql = str(driver.last_query)
    assert "INDEX_STATE" in query_sql
    assert "IS_NULL_FILTERED" in query_sql


def test_get_foreign_keys_routes_to_googlesql() -> None:
    """get_foreign_keys executes query containing GoogleSQL CTEs."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    fks = dictionary.get_foreign_keys(cast(Any, driver), table="users")

    assert len(fks) == 1
    query_sql = str(driver.last_query)
    assert "fk_columns" in query_sql
    assert "pk_columns" in query_sql


def test_get_query_explicit_mode_override() -> None:
    """Explicit mode override routes to PostgreSQL query templates."""
    dictionary = SpannerDataDictionary()
    query = dictionary.get_query("tables", "by_schema", mode="postgresql")

    assert "table_catalog" in query.raw_sql
    assert ":schema_name::text" in query.raw_sql
