"""Exercise native reflection against recorded Unicode DBC catalog results."""

from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.teradata.data_dictionary import TeradataDataDictionary
from sqlspec.exceptions import OperationalError


def test_version_cache_features_and_type_selection() -> None:
    driver = MagicMock()
    driver.select_one_or_none.return_value = {"version_string": "20.00.18.29"}
    dictionary = TeradataDataDictionary()
    assert str(dictionary.get_version(driver)) == "20.00.18.29"
    assert dictionary.get_feature_flag(driver, "supports_json") is True
    assert dictionary.get_feature_flag(driver, "supports_returning") is False
    assert dictionary.get_optimal_type(driver, "json") == "JSON"
    assert "supports_cte" in dictionary.list_available_features()
    assert dictionary.get_metadata_capabilities(driver).dialect == "teradata"
    driver.select_one_or_none.assert_called_once()


def test_missing_version_is_cached() -> None:
    dictionary = TeradataDataDictionary()
    driver = MagicMock()
    driver.select_one_or_none.return_value = None
    assert dictionary.get_version(driver) is None
    assert dictionary.get_version(driver) is None
    driver.select_one_or_none.assert_called_once()


def test_tables_keep_dependency_order_and_append_views() -> None:
    dictionary = TeradataDataDictionary()
    driver = MagicMock()
    parent = {"schema_name": "mart", "table_name": "parent"}
    child = {"schema_name": "mart", "table_name": "child"}
    view = {"schema_name": "mart", "table_name": "view"}
    driver.select.side_effect = [[parent, child], [child, view, parent]]
    assert dictionary.get_tables(driver, "mart") == [parent, child, view]


@pytest.mark.parametrize("table", [None, "orders"])
def test_columns_indexes_foreign_keys_and_constraints(table: str | None) -> None:
    dictionary = TeradataDataDictionary()
    driver = MagicMock()
    driver.select.return_value = [
        {
            "schema_name": "mart",
            "table_name": "orders",
            "column_name": "id",
            "data_type": "INTEGER",
            "is_nullable": 0,
            "ordinal_position": 1,
            "column_default": "1",
            "is_identity": 1,
            "identity_generation": "BY DEFAULT",
            "numeric_precision": 10,
            "numeric_scale": 0,
            "generation_expression": "id+1",
        }
    ]
    column = dictionary.get_columns(driver, table, "mart")[0]
    assert column.get("is_nullable") is False
    assert column.get("identity_generation") == "BY DEFAULT"
    assert column.get("numeric_precision") == 10
    assert column.get("column_default") == "1"
    driver.select.return_value = [
        {
            "schema_name": "mart",
            "table_name": "orders",
            "index_name": "ix_pair",
            "column_name": "tenant",
            "is_unique": 1,
        },
        {"schema_name": "mart", "table_name": "orders", "index_name": "ix_pair", "column_name": "id", "is_unique": 1},
    ]
    assert dictionary.get_indexes(driver, table, "mart")[0].get("columns") == ["tenant", "id"]
    driver.select.return_value = [
        {
            "schema_name": "mart",
            "table_name": "orders",
            "column_name": "id",
            "constraint_name": "fk_order",
            "referenced_table_name": "customers",
            "referenced_column_name": "id",
            "referenced_schema": "mart",
        }
    ]
    fk = dictionary.get_foreign_keys(driver, table, "mart")[0]
    assert (fk.referenced_table, fk.referenced_column, fk.schema) == ("customers", "id", "mart")
    driver.select.return_value = [{"constraint_name": "ck_id", "check_clause": "id > 0"}]
    assert dictionary.get_constraints(driver, table, "mart").items == (
        {"constraint_name": "ck_id", "check_clause": "id > 0"},
    )


def test_view_type_probe_is_scoped_and_permission_failure_keeps_columns() -> None:
    dictionary = TeradataDataDictionary()
    driver = MagicMock()
    row = {"schema_name": "mart", "table_name": "v_orders", "column_name": "amount", "data_type": None}
    driver.select.side_effect = [
        [row],
        [
            {
                "column_name": "amount",
                "raw_type_code": "D",
                "numeric_precision": 18,
                "numeric_scale": 4,
                "is_nullable": "Y",
            }
        ],
    ]
    column = dictionary.get_columns(driver, "v_orders", "mart")[0]
    assert column.get("data_type") == "DECIMAL"
    assert column.get("numeric_scale") == 4
    assert column.get("is_nullable") is True
    query, schema, table = driver.select.call_args.args
    assert "FROM DBC.ColumnsQV" in query
    assert (schema, table) == ("mart", "v_orders")
    driver.select.side_effect = [[row], OperationalError("[Error 3523] Permission denied")]
    assert dictionary.get_columns(driver, "v_orders", "mart")[0].get("column_name") == "amount"
    driver.select.side_effect = [[row]]
    assert dictionary.get_columns(driver, schema="mart")[0].get("column_name") == "amount"


def test_views_reconstruct_ordered_overflow_chunks() -> None:
    dictionary = TeradataDataDictionary()
    driver = MagicMock()
    driver.select.return_value = [
        {"schema_name": "mart", "view_name": "v", "definition": "SELECT 1", "line_no": 2},
        {"schema_name": "mart", "view_name": "v", "definition": "CREATE VIEW v AS ", "line_no": 1},
    ]
    result = dictionary.get_views(driver, "mart")
    assert len(result.items) == 1
    item = result.items[0]
    assert isinstance(item, dict)
    assert item["definition"] == "CREATE VIEW v AS SELECT 1"


@pytest.mark.parametrize("operation", ["get_objects", "get_schemas"])
def test_catalog_domains_preserve_rows(operation: str) -> None:
    dictionary = TeradataDataDictionary()
    driver = MagicMock()
    driver.select.return_value = [{"schema_name": "mart", "object_name": "orders"}]
    result = getattr(dictionary, operation)(driver)
    assert result.items == ({"schema_name": "mart", "object_name": "orders"},)


@pytest.mark.parametrize(
    "code,expected",
    [
        ("CV", "VARCHAR"),
        ("TZ", "TIME WITH TIME ZONE"),
        ("PT", "PERIOD(TIME)"),
        ("PM", "PERIOD(TIMESTAMP WITH TIME ZONE)"),
        ("N", "NUMBER"),
        ("XM", "XML"),
    ],
)
def test_view_fallback_enriches_type_and_unicode_length(code: str, expected: str) -> None:
    driver = MagicMock()
    driver.select.side_effect = [
        [{"column_name": "value", "data_type": None}],
        [{"column_name": "value", "raw_type_code": code, "max_length": 200, "char_type": 2, "is_nullable": "N"}],
    ]
    column = TeradataDataDictionary().get_columns(driver, "v_data", "mart")[0]
    assert column.get("data_type") == expected
    assert column.get("is_nullable") is False
    if code == "CV":
        assert column.get("max_length") == 100
