"""Snowflake public catalog reflection and native DDL behavior."""

from typing import Any, cast

import pytest

from sqlspec.adapters.snowflake import SnowflakeDriver
from sqlspec.data_dictionary import MetadataSupport, VersionInfo


def _result(columns: list[str], rows: list[tuple[Any, ...]]) -> dict[str, Any]:
    return {"description": [(name, 2, None, None, None, None, True) for name in columns], "rows": rows}


def test_snowflake_dictionary_types_capabilities_and_version(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = _result(["version"], [("9.2.1",)])
    driver = SnowflakeDriver(snowflake_connection)
    dictionary = driver.data_dictionary
    assert dictionary.get_optimal_type(driver, "json") == "VARIANT"
    version = dictionary.get_version(driver)
    assert version == VersionInfo(9, 2, 1)
    assert dictionary.get_version(driver) is version
    assert len(snowflake_connection.calls) == 1
    profile = dictionary.get_metadata_capabilities(driver, ["tables", "indexes"])
    assert profile.get("tables").support == MetadataSupport.SUPPORTED
    assert profile.get("indexes").support == MetadataSupport.UNSUPPORTED
    assert dictionary.get_indexes(driver, "orders") == []


def test_snowflake_dictionary_columns_bind_exact_case(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = _result(
        ["table_name", "column_name", "data_type", "is_nullable", "ordinal_position"],
        [("Orders", "PAYLOAD", "VARIANT", True, 1)],
    )
    driver = SnowflakeDriver(snowflake_connection)
    columns = driver.data_dictionary.get_columns(driver, '"Orders"', schema="sales")
    assert columns[0]["column_name"] == "PAYLOAD"
    assert columns[0]["data_type"] == "VARIANT"
    assert list(snowflake_connection.calls[-1][1]) == ["SALES", "Orders"]


def test_imported_keys_use_show_scope_and_lowercase_sdk_fields(snowflake_connection: Any) -> None:
    snowflake_connection.results['SHOW IMPORTED KEYS IN TABLE "SALES"."Orders"'] = _result(
        [
            "fk_schema_name",
            "fk_table_name",
            "fk_column_name",
            "fk_name",
            "pk_schema_name",
            "pk_table_name",
            "pk_column_name",
            "key_sequence",
            "delete_rule",
            "update_rule",
        ],
        [("SALES", "Orders", "ACCOUNT_ID", "FK_ORDERS", "SALES", "ACCOUNTS", "ID", 1, "CASCADE", "NO ACTION")],
    )
    driver = SnowflakeDriver(snowflake_connection)
    keys = driver.data_dictionary.get_foreign_keys(driver, '"Orders"', "sales")
    assert [(key.table_name, key.column_name, key.referenced_table, key.referenced_column) for key in keys] == [
        ("Orders", "ACCOUNT_ID", "ACCOUNTS", "ID")
    ]
    assert keys[0].on_delete == "CASCADE"
    assert snowflake_connection.calls[-1][0] == 'SHOW IMPORTED KEYS IN TABLE "SALES"."Orders"'


def test_native_ddl_uses_bound_get_ddl_arguments(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = _result(["ddl"], [('CREATE TABLE "SALES"."Orders" (ID NUMBER)',)])
    driver = SnowflakeDriver(snowflake_connection)
    result = driver.data_dictionary.get_ddl(driver, '"Orders"', "sales", include_dependencies=False)
    assert result.status == MetadataSupport.SUPPORTED
    assert result.ddl == 'CREATE TABLE "SALES"."Orders" (ID NUMBER)'
    assert list(snowflake_connection.calls[-1][1]) == ["TABLE", '"SALES"."Orders"']


@pytest.mark.parametrize("method_name", ["get_schemas", "get_objects", "get_views", "get_sequences", "get_routines"])
def test_snowflake_metadata_domains_use_shared_catalog_queries(snowflake_connection: Any, method_name: str) -> None:
    snowflake_connection.default_result = _result(["object_name", "schema_name"], [("ORDERS", "PUBLIC")])
    driver = SnowflakeDriver(snowflake_connection)
    metadata = getattr(driver.data_dictionary, method_name)(driver)
    assert metadata.items == ({"object_name": "ORDERS", "schema_name": "PUBLIC"},)
    assert metadata.capability.support == MetadataSupport.SUPPORTED
    assert "information_schema" in snowflake_connection.calls[-1][0].lower()


def test_snowflake_catalog_tables_schema_columns_and_feature_flags(snowflake_connection: Any) -> None:
    driver = SnowflakeDriver(snowflake_connection)
    snowflake_connection.default_result = _result(["table_name", "schema_name"], [("ORDERS", "PUBLIC")])
    assert driver.data_dictionary.get_tables(driver) == [{"table_name": "ORDERS", "schema_name": "PUBLIC"}]
    snowflake_connection.default_result = _result(
        ["table_name", "column_name", "data_type"], [("ORDERS", "ID", "NUMBER")]
    )
    assert driver.data_dictionary.get_columns(driver) == [
        {"table_name": "ORDERS", "column_name": "ID", "data_type": "NUMBER"}
    ]
    snowflake_connection.default_result = _result(["version"], [("9.2.1",)])
    assert driver.data_dictionary.get_feature_flag(driver, "supports_savepoints") is False
    assert "supports_json" in driver.data_dictionary.list_available_features()


@pytest.mark.parametrize("table", [None, '"Orders"'])
def test_constraints_merge_catalog_and_ordered_show_key_columns(snowflake_connection: Any, table: str | None) -> None:
    snowflake_connection.default_result = _result(
        ["table_name", "constraint_name", "constraint_type"],
        [("Orders", "PK_ORDERS", "PRIMARY KEY"), ("Orders", "CK_AMOUNT", "CHECK")],
    )
    scope = 'IN TABLE "SALES"."Orders"' if table else 'IN SCHEMA "SALES"'
    snowflake_connection.results[f"SHOW PRIMARY KEYS {scope}"] = _result(
        ["database_name", "schema_name", "table_name", "constraint_name", "column_name", "key_sequence"],
        [("DB", "SALES", "Orders", "PK_ORDERS", "ID", 1)],
    )
    snowflake_connection.results[f"SHOW UNIQUE KEYS {scope}"] = _result(
        ["database_name", "schema_name", "table_name", "constraint_name", "column_name", "key_sequence"],
        [("DB", "SALES", "Orders", "UQ_ORDER", "CODE", 2), ("DB", "SALES", "Orders", "UQ_ORDER", "REGION", 1)],
    )
    driver = SnowflakeDriver(snowflake_connection)
    metadata = driver.data_dictionary.get_constraints(driver, table, "sales")
    constraints = {row["constraint_name"]: row for row in cast("tuple[dict[str, Any], ...]", metadata.items)}
    assert list(constraints) == ["CK_AMOUNT", "PK_ORDERS", "UQ_ORDER"]
    assert constraints["PK_ORDERS"]["column_names"] == ["ID"]
    assert constraints["UQ_ORDER"]["column_names"] == ["REGION", "CODE"]


@pytest.mark.parametrize(
    ("table", "schema", "expected"),
    [
        (None, None, "SHOW IMPORTED KEYS IN SCHEMA"),
        (None, "sales", 'SHOW IMPORTED KEYS IN SCHEMA "SALES"'),
        ('"sales"."Orders"', "ignored", 'SHOW IMPORTED KEYS IN TABLE "sales"."Orders"'),
    ],
)
def test_imported_keys_scope_handles_schema_and_qualified_table(
    snowflake_connection: Any, table: str | None, schema: str | None, expected: str
) -> None:
    snowflake_connection.default_result = _result(["fk_name"], [])
    driver = SnowflakeDriver(snowflake_connection)
    assert driver.data_dictionary.get_foreign_keys(driver, table, schema) == []
    assert snowflake_connection.calls[-1][0] == expected


def test_missing_view_ddl_returns_explicit_unsupported_result(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = _result(["ddl"], [])
    driver = SnowflakeDriver(snowflake_connection)
    result = driver.data_dictionary.get_ddl(driver, "missing", object_type="view", include_dependencies=False)
    assert result.status == MetadataSupport.UNSUPPORTED
    assert result.warnings == ("GET_DDL returned no definition",)
