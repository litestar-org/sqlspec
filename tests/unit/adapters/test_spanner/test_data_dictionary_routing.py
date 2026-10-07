"""Tests for SpannerSyncDataDictionary query routing."""

from typing import Any, cast

import pytest

from sqlspec.adapters.spanner.data_dictionary import SpannerAsyncDataDictionary, SpannerSyncDataDictionary
from sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerSyncDriver
from sqlspec.core import StatementConfig
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
            return [TableMetadata(table_name="users", schema_name="public", table_type="BASE TABLE")]
        if schema_type is ColumnMetadata:
            return [
                ColumnMetadata(
                    column_name="id", table_name="users", schema_name="public", data_type="INT64", ordinal_position=1
                )
            ]
        if schema_type is IndexMetadata:
            return [
                IndexMetadata(index_name="users_by_name", table_name="users", schema_name="public", columns=["name"])
            ]
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
    dictionary = SpannerSyncDataDictionary()
    assert dictionary.mode == "googlesql"


def test_get_tables_routes_to_googlesql() -> None:
    """get_tables executes query containing GoogleSQL fields and TableMetadata aliases."""
    dictionary = SpannerSyncDataDictionary()
    driver = MockSpannerDriver()
    tables = dictionary.get_tables(cast(Any, driver))

    assert len(tables) == 1
    query_sql = str(driver.last_query)
    assert "TABLE_SCHEMA AS schema_name" in query_sql
    assert "TABLE_NAME AS table_name" in query_sql
    assert "TABLE_TYPE AS table_type" in query_sql
    assert "ROW_DELETION_POLICY_EXPRESSION" in query_sql
    assert "SPANNER_STATE" in query_sql


def test_get_columns_routes_to_googlesql() -> None:
    """get_columns executes query containing GoogleSQL column extensions and ColumnMetadata aliases."""
    dictionary = SpannerSyncDataDictionary()
    driver = MockSpannerDriver()
    columns = dictionary.get_columns(cast(Any, driver), table="users")

    assert len(columns) == 1
    query_sql = str(driver.last_query)
    assert "TABLE_SCHEMA AS schema_name" in query_sql
    assert "COLUMN_NAME AS column_name" in query_sql
    assert "SPANNER_TYPE AS data_type" in query_sql
    assert "IS_STORED" in query_sql


def test_get_indexes_routes_to_googlesql() -> None:
    """get_indexes executes query containing GoogleSQL index extensions and IndexMetadata aliases."""
    dictionary = SpannerSyncDataDictionary()
    driver = MockSpannerDriver()
    indexes = dictionary.get_indexes(cast(Any, driver), table="users")

    assert len(indexes) == 1
    query_sql = str(driver.last_query)
    assert "i.INDEX_NAME AS index_name" in query_sql
    assert "i.TABLE_NAME AS table_name" in query_sql
    assert "i.IS_UNIQUE AS is_unique" in query_sql
    assert "AS columns" in query_sql
    assert "INDEX_STATE" in query_sql
    assert "IS_NULL_FILTERED" in query_sql


def test_get_foreign_keys_routes_to_googlesql() -> None:
    """get_foreign_keys executes query containing GoogleSQL CTEs."""
    dictionary = SpannerSyncDataDictionary()
    driver = MockSpannerDriver()
    fks = dictionary.get_foreign_keys(cast(Any, driver), table="users")

    assert len(fks) == 1
    query_sql = str(driver.last_query)
    assert "fk_columns" in query_sql
    assert "pk_columns" in query_sql


def test_get_query_explicit_mode_override() -> None:
    """Explicit mode override routes to PostgreSQL query templates with expected aliases."""
    dictionary = SpannerSyncDataDictionary()
    query = dictionary.get_query("tables", "by_schema", mode="postgresql")

    assert "table_catalog" in query.raw_sql
    assert "table_schema AS schema_name" in query.raw_sql
    assert ":schema_name::text" in query.raw_sql

    index_query = dictionary.get_query("indexes", "by_schema", mode="postgresql")
    assert "i.index_name AS index_name" in index_query.raw_sql
    assert "AS columns" in index_query.raw_sql


@pytest.mark.parametrize("mode", ["googlesql", "postgresql"])
@pytest.mark.parametrize("domain", ["tables", "columns", "indexes"])
def test_schema_metadata_binds_only_schema(mode: str, domain: str) -> None:
    dictionary = SpannerSyncDataDictionary(mode=mode)
    driver = MockSpannerDriver()
    getattr(dictionary, f"get_{domain}")(cast("Any", driver), schema="public")

    statement = driver.last_query.copy(parameters={"schema_name": driver.last_params["schema_name"]})
    _sql, parameters = statement.compile()
    assert parameters == ("public", "public")


class MockAsyncSpannerDriver:
    """Mock SpannerAsyncDriver to capture async query execution."""

    def __init__(self) -> None:
        self.last_query: Any = None
        self.last_params: dict[str, Any] = {}

    async def select(self, query: Any, **params: Any) -> list[Any]:
        self.last_query = query
        self.last_params = params
        schema_type = params.get("schema_type")
        if schema_type is TableMetadata:
            return [TableMetadata(table_name="users", schema_name="public", table_type="BASE TABLE")]
        if schema_type is ColumnMetadata:
            return [
                ColumnMetadata(
                    column_name="id", table_name="users", schema_name="public", data_type="INT64", ordinal_position=1
                )
            ]
        if schema_type is IndexMetadata:
            return [
                IndexMetadata(index_name="users_by_name", table_name="users", schema_name="public", columns=["name"])
            ]
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


async def test_async_data_dictionary_routing_and_sync_alias() -> None:
    """Verify SpannerAsyncDataDictionary routes GoogleSQL and PostgreSQL queries and SpannerSyncDataDictionary is aliased."""
    assert SpannerSyncDataDictionary is SpannerSyncDataDictionary

    dictionary = SpannerAsyncDataDictionary()
    assert dictionary.mode == "googlesql"
    driver = MockAsyncSpannerDriver()

    tables = await dictionary.get_tables(cast("Any", driver))
    assert len(tables) == 1
    assert "TABLE_SCHEMA AS schema_name" in str(driver.last_query)

    columns = await dictionary.get_columns(cast("Any", driver), table="users")
    assert len(columns) == 1
    assert "COLUMN_NAME AS column_name" in str(driver.last_query)

    indexes = await dictionary.get_indexes(cast("Any", driver), table="users")
    assert len(indexes) == 1
    assert "i.INDEX_NAME AS index_name" in str(driver.last_query)

    fks = await dictionary.get_foreign_keys(cast("Any", driver), table="users")
    assert len(fks) == 1
    assert "fk_columns" in str(driver.last_query)

    pg_dictionary = SpannerAsyncDataDictionary(mode="postgresql")
    assert pg_dictionary.mode == "postgresql"
    await pg_dictionary.get_tables(cast("Any", driver), schema="public")
    assert "table_catalog" in str(driver.last_query)


def test_driver_data_dictionary_dialect_mode_routing() -> None:
    """Verify SpannerSyncDriver and SpannerAsyncDriver instantiate data dictionaries with dialect-matched mode."""
    async_driver_gsql = SpannerAsyncDriver(connection=cast("Any", object()))
    assert isinstance(async_driver_gsql.data_dictionary, SpannerAsyncDataDictionary)
    assert async_driver_gsql.data_dictionary.mode == "googlesql"

    async_driver_pg = SpannerAsyncDriver(
        connection=cast("Any", object()), statement_config=StatementConfig(dialect="spangres")
    )
    assert isinstance(async_driver_pg.data_dictionary, SpannerAsyncDataDictionary)
    assert async_driver_pg.data_dictionary.mode == "postgresql"

    sync_driver_pg = SpannerSyncDriver(
        connection=cast("Any", object()), statement_config=StatementConfig(dialect="spangres")
    )
    assert isinstance(sync_driver_pg.data_dictionary, SpannerSyncDataDictionary)
    assert sync_driver_pg.data_dictionary.mode == "postgresql"
