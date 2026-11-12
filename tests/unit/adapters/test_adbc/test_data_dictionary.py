"""Unit tests for ADBC native metadata normalization and fallback."""

from typing import Any, cast
from unittest.mock import Mock

import pyarrow as pa
import pytest

pytest.importorskip("adbc_driver_manager")

from adbc_driver_manager import NotSupportedError as AdbcNotSupportedError

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.data_dictionary import (
    AdbcDataDictionary,
    _arrow_type_to_sql,
    _normalize_native_columns,
    _normalize_native_foreign_keys,
    _normalize_native_statistics,
    _normalize_native_tables,
)
from sqlspec.data_dictionary import DDLResult, MetadataFidelity, MetadataSource, MetadataSupport
from sqlspec.exceptions import OperationalError

SQLITE_OBJECTS_PAYLOAD: list[dict[str, Any]] = [
    {
        "catalog_name": "main",
        "catalog_db_schemas": [
            {
                "db_schema_name": "",
                "db_schema_tables": [
                    {
                        "table_name": "t1",
                        "table_type": "table",
                        "table_columns": [
                            {
                                "column_name": "id",
                                "ordinal_position": 1,
                                "xdbc_type_name": "INTEGER",
                                "xdbc_is_nullable": "YES",
                            },
                            {
                                "column_name": "name",
                                "ordinal_position": 2,
                                "xdbc_type_name": "TEXT",
                                "xdbc_is_nullable": "YES",
                            },
                            {
                                "column_name": "ref_id",
                                "ordinal_position": 3,
                                "xdbc_type_name": "INTEGER",
                                "xdbc_is_nullable": "YES",
                            },
                        ],
                        "table_constraints": [
                            {
                                "constraint_name": None,
                                "constraint_type": "PRIMARY KEY",
                                "constraint_column_names": ["id"],
                                "constraint_column_usage": None,
                            },
                            {
                                "constraint_name": None,
                                "constraint_type": "FOREIGN KEY",
                                "constraint_column_names": ["ref_id"],
                                "constraint_column_usage": [
                                    {"fk_catalog": "main", "fk_db_schema": "", "fk_table": "t1", "fk_column_name": "id"}
                                ],
                            },
                        ],
                    },
                    {"table_name": "idx_name", "table_type": "index", "table_columns": [], "table_constraints": []},
                ],
            }
        ],
    }
]

DUCKDB_OBJECTS_PAYLOAD: list[dict[str, Any]] = [
    {
        "catalog_name": "memory",
        "catalog_db_schemas": [
            {
                "db_schema_name": "main",
                "db_schema_tables": [
                    {
                        "table_name": "t2",
                        "table_type": "BASE TABLE",
                        "table_columns": [
                            {
                                "column_name": "id",
                                "ordinal_position": 1,
                                "xdbc_is_nullable": "YES",
                                "xdbc_type_name": None,
                            },
                            {
                                "column_name": "val",
                                "ordinal_position": 2,
                                "xdbc_is_nullable": "YES",
                                "xdbc_type_name": None,
                            },
                        ],
                        "table_constraints": [],
                    }
                ],
            }
        ],
    }
]


def _make_reader(payload: list[dict[str, Any]]) -> Mock:
    reader = Mock()
    reader.read_all.return_value.to_pylist.return_value = payload
    return reader


def test_normalize_native_tables_filters_index_rows() -> None:
    """Native table normalization should ignore index leak rows and keep schema fallback."""
    tables = _normalize_native_tables(SQLITE_OBJECTS_PAYLOAD)

    assert len(tables) == 1
    assert tables[0]["table_name"] == "t1"
    assert tables[0]["schema_name"] == "main"
    assert all(entry["table_name"] != "idx_name" for entry in tables)


def test_normalize_native_columns_marks_primary_key() -> None:
    """Native column normalization should mark primary-key columns and preserve types."""
    columns = _normalize_native_columns(SQLITE_OBJECTS_PAYLOAD, table_name_exact="t1")
    by_name = {entry["column_name"]: entry for entry in columns}

    assert by_name["id"]["is_primary"] is True
    assert by_name["id"]["data_type"] == "INTEGER"
    assert by_name["id"]["ordinal_position"] == 1
    assert "is_primary" not in by_name["name"]


def test_normalize_native_columns_exact_table_filter() -> None:
    """Native column normalization should respect exact table-name filtering."""
    assert _normalize_native_columns(SQLITE_OBJECTS_PAYLOAD, table_name_exact="other") == []


def test_normalize_native_foreign_keys() -> None:
    """Native foreign-key normalization should preserve table and schema linkage."""
    keys = _normalize_native_foreign_keys(SQLITE_OBJECTS_PAYLOAD, table_name_exact="t1")

    assert len(keys) == 1
    key = keys[0]
    assert key.table_name == "t1"
    assert key.column_name == "ref_id"
    assert key.referenced_table == "t1"
    assert key.referenced_column == "id"
    assert key.constraint_name is None
    assert key.schema == "main"
    assert key.referenced_schema == "main"


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        (pa.bool_(), "BOOLEAN"),
        (pa.int16(), "SMALLINT"),
        (pa.int32(), "INTEGER"),
        (pa.int64(), "BIGINT"),
        (pa.float32(), "REAL"),
        (pa.float64(), "DOUBLE"),
        (pa.decimal128(10, 2), "DECIMAL(10,2)"),
        (pa.string(), "VARCHAR"),
        (pa.binary(), "VARBINARY"),
        (pa.date32(), "DATE"),
        (pa.time64("us"), "TIME"),
        (pa.timestamp("us"), "TIMESTAMP"),
    ],
)
def test_arrow_type_to_sql_mapping(data_type: pa.DataType, expected: str) -> None:
    """Arrow field types should map to SQL type strings for schema probing."""
    assert _arrow_type_to_sql(data_type) == expected


def test_get_tables_falls_back_to_sql_on_not_supported() -> None:
    """Native GetObjects failures should fall back to the SQL table query path."""
    driver = Mock()
    driver.dialect = "sqlite"
    driver.connection.adbc_get_objects.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")
    driver.select.return_value = [{"table_name": "fallback"}]

    result = AdbcDataDictionary().get_tables(driver)

    assert result == [{"table_name": "fallback"}]
    driver.select.assert_called_once()


def test_get_foreign_keys_empty_native_result_falls_back() -> None:
    """Native foreign-key discovery should fall back when drivers omit constraint metadata."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_objects.return_value = _make_reader([
        {
            "catalog_name": "memory",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "main",
                    "db_schema_tables": [{"table_name": "t2", "table_type": "table", "table_columns": []}],
                }
            ],
        }
    ])
    fallback: list[Any] = [Mock()]
    driver.select.return_value = fallback

    result = AdbcDataDictionary().get_foreign_keys(driver, table="t2")

    assert result == fallback
    driver.select.assert_called_once()


def test_empty_foreign_keys_not_failed() -> None:
    """Tables with empty constraints list should return empty list without falling back."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_objects.return_value = _make_reader([
        {
            "catalog_name": "memory",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "main",
                    "db_schema_tables": [
                        {"table_name": "t2", "table_type": "table", "table_columns": [], "table_constraints": []}
                    ],
                }
            ],
        }
    ])

    result = AdbcDataDictionary().get_foreign_keys(driver, table="t2")

    assert result == []
    driver.select.assert_not_called()


def test_snowflake_indexes_fallback() -> None:
    """Snowflake dialect returns empty indexes without raising SQLFileNotFoundError."""
    driver = Mock()
    driver.dialect = "snowflake"
    result = AdbcDataDictionary().get_indexes(driver, table="t1")
    assert result == []


def test_get_columns_schema_wide_incomplete_falls_back() -> None:
    """Schema-wide native column discovery should fall back when type names are incomplete."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_objects.return_value = _make_reader(DUCKDB_OBJECTS_PAYLOAD)
    driver.select.return_value = [{"column_name": "fallback"}]

    result = AdbcDataDictionary().get_columns(driver)

    assert result == [{"column_name": "fallback"}]
    driver.connection.adbc_get_table_schema.assert_not_called()
    driver.select.assert_called_once()


def test_get_columns_missing_native_nullability_falls_back() -> None:
    """Native column discovery should fall back when nullability metadata is incomplete."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_objects.return_value = _make_reader([
        {
            "catalog_name": "memory",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "main",
                    "db_schema_tables": [
                        {
                            "table_name": "t2",
                            "table_type": "table",
                            "table_columns": [{"column_name": "id", "xdbc_type_name": "INTEGER"}],
                            "table_constraints": [],
                        }
                    ],
                }
            ],
        }
    ])
    driver.select.return_value = [{"column_name": "fallback"}]

    result = AdbcDataDictionary().get_columns(driver, table="t2")

    assert result == [{"column_name": "fallback"}]
    driver.connection.adbc_get_table_schema.assert_not_called()
    driver.select.assert_called_once()


def test_get_columns_single_table_enriched_from_table_schema() -> None:
    """Single-table native column discovery should enrich missing types from the Arrow schema."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_objects.return_value = _make_reader(DUCKDB_OBJECTS_PAYLOAD)
    schema_fields: list[pa.Field[Any]] = [pa.field("id", pa.int32()), pa.field("val", pa.float64())]
    driver.connection.adbc_get_table_schema.return_value = pa.schema(schema_fields)

    result = AdbcDataDictionary().get_columns(driver, table="t2")
    by_name = {entry["column_name"]: entry for entry in result}

    assert by_name["id"]["data_type"] == "INTEGER"
    assert by_name["val"]["data_type"] == "DOUBLE"
    driver.select.assert_not_called()


def test_normalize_native_statistics() -> None:
    """Native statistics normalization should keep catalog and schema context."""
    payload = [
        {
            "catalog_name": "db",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "public",
                    "db_schema_statistics": [
                        {
                            "table_name": "items",
                            "column_name": None,
                            "statistic_key": 6,
                            "statistic_value": 42,
                            "statistic_is_approximate": True,
                        },
                        {
                            "table_name": "items",
                            "column_name": "name",
                            "statistic_key": 5,
                            "statistic_value": 0,
                            "statistic_is_approximate": False,
                        },
                    ],
                }
            ],
        }
    ]

    stats = _normalize_native_statistics(payload)

    assert len(stats) == 2
    assert stats[0]["catalog_name"] == "db"
    assert stats[0]["schema_name"] == "public"
    assert stats[0]["statistic_name"] == "adbc.statistic.row_count"
    assert stats[0]["column_name"] is None
    assert stats[0]["is_approximate"] is True
    assert stats[1]["statistic_name"] == "adbc.statistic.null_count"
    assert stats[1]["column_name"] == "name"


def test_normalize_native_statistics_unknown_key() -> None:
    """Unknown statistics keys should preserve the numeric key as a string name."""
    payload = [
        {
            "catalog_name": "db",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "public",
                    "db_schema_statistics": [
                        {
                            "table_name": "items",
                            "column_name": None,
                            "statistic_key": 1100,
                            "statistic_value": 1,
                            "statistic_is_approximate": False,
                        }
                    ],
                }
            ],
        }
    ]

    stats = _normalize_native_statistics(payload)

    assert stats[0]["statistic_name"] == "1100"


def test_get_statistics_raises_operational_error_when_unsupported() -> None:
    """Unsupported native statistics should raise sqlspec OperationalError."""
    driver = Mock()
    driver.dialect = "sqlite"
    driver.connection.adbc_get_statistics.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")

    with pytest.raises(OperationalError, match="does not support native table statistics"):
        AdbcDataDictionary().get_statistics(driver, "items")


def test_get_statistics_filters_exact_table() -> None:
    """Native statistics should be filtered back to the exact requested table name."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_statistics.return_value = _make_reader([
        {
            "catalog_name": "memory",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "main",
                    "db_schema_statistics": [
                        {
                            "table_name": "items",
                            "column_name": None,
                            "statistic_key": 6,
                            "statistic_value": 3,
                            "statistic_is_approximate": True,
                        },
                        {
                            "table_name": "items_archive",
                            "column_name": None,
                            "statistic_key": 6,
                            "statistic_value": 7,
                            "statistic_is_approximate": True,
                        },
                    ],
                }
            ],
        }
    ])

    statistics = AdbcDataDictionary().get_statistics(driver, "items")

    assert len(statistics) == 1
    assert statistics[0]["table_name"] == "items"


def test_adbc_capabilities_include_get_info_table_types_statistics() -> None:
    """ADBC capability profiles should report explicit transport metadata support."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_info.return_value = {"vendor_name": "duckdb", "driver_name": "duckdb"}
    driver.connection.adbc_get_table_types.return_value = ["BASE TABLE", "VIEW"]
    driver.connection.adbc_get_objects.return_value = _make_reader([])
    driver.connection.adbc_get_statistics.return_value = _make_reader([])
    driver.connection.adbc_get_statistic_names.return_value = _make_reader([
        {"statistic_key": 6, "statistic_name": "row_count"}
    ])

    profile = AdbcDataDictionary().get_metadata_capabilities(
        driver, domains=("tables", "columns", "constraints", "statistics", "ddl")
    )

    assert profile.dialect == "duckdb"
    assert profile.adapter == "adbc"
    assert profile.get("tables").support == MetadataSupport.SUPPORTED
    assert profile.get("tables").fidelity == MetadataFidelity.TRANSPORT_FALLBACK
    assert profile.get("tables").source == MetadataSource.DRIVER_METADATA
    assert profile.get("columns").support == MetadataSupport.SUPPORTED
    assert profile.get("statistics").support == MetadataSupport.SUPPORTED
    assert profile.get("statistics").risks
    assert profile.get("ddl").support == MetadataSupport.UNSUPPORTED
    driver.connection.adbc_get_info.assert_called_once()
    driver.connection.adbc_get_table_types.assert_called_once()
    driver.connection.adbc_get_objects.assert_called()
    driver.connection.adbc_get_statistics.assert_called_once()
    driver.connection.adbc_get_statistic_names.assert_called_once()


def test_adbc_get_objects_unique_check_constraints_are_lossy() -> None:
    """ADBC GetObjects constraint shells should be surfaced with lossy fidelity."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_objects.return_value = _make_reader([
        {
            "catalog_name": "memory",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "main",
                    "db_schema_tables": [
                        {
                            "table_name": "items",
                            "table_type": "BASE TABLE",
                            "table_columns": [],
                            "table_constraints": [
                                {
                                    "constraint_name": "uq_items_name",
                                    "constraint_type": "UNIQUE",
                                    "constraint_column_names": ["name"],
                                    "constraint_column_usage": None,
                                },
                                {
                                    "constraint_name": "ck_items_qty",
                                    "constraint_type": "CHECK",
                                    "constraint_column_names": ["quantity"],
                                    "constraint_column_usage": None,
                                },
                            ],
                        }
                    ],
                }
            ],
        }
    ])

    result = AdbcDataDictionary().get_constraints(driver, table="items")

    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.capability.fidelity == MetadataFidelity.LOSSY
    assert result.capability.source == MetadataSource.DRIVER_METADATA
    assert result.warnings
    constraint_items = [cast("Any", item) for item in result.items]
    assert [item.identity.name for item in constraint_items] == ["uq_items_name", "ck_items_qty"]
    assert [item.attributes["constraint_type"] for item in constraint_items] == ["UNIQUE", "CHECK"]
    assert all(item.attributes["is_lossy"] is True for item in constraint_items)


def test_adbc_view_ddl_is_explicitly_unsupported() -> None:
    """ADBC transport metadata should not claim lossless view DDL."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite"})
    with config.provide_session() as driver:
        result = driver.data_dictionary.get_ddl(
            driver,
            "items",
            schema="main",
            object_type="view",
            include_dependencies=False,
            prefer_native=False,
            redact=False,
        )

    assert result.identity.name == "items"
    assert result.identity.object_type == "view"
    assert result.identity.schema == "main"
    assert result.ddl is None
    assert result.status == MetadataSupport.UNSUPPORTED
    assert result.fidelity == MetadataFidelity.UNSUPPORTED
    assert result.source == MetadataSource.DRIVER_METADATA
    assert result.warnings


@pytest.mark.parametrize("target_dialect", [None, "postgres"])
def test_adbc_table_and_schema_ddl_preserve_detected_dialect_and_quoted_names(target_dialect: str | None) -> None:
    """DDL orchestration uses the active ADBC dialect for quoted catalog names and target rendering."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite"})
    with config.provide_session() as driver:
        driver.execute('CREATE TABLE "Mixed Items" ("Mixed ID" INTEGER PRIMARY KEY, "Label" TEXT)')
        dictionary = driver.data_dictionary
        table = dictionary.get_ddl(
            driver,
            '"Mixed Items"',
            schema='"main"',
            include_dependencies=False,
            prefer_native=False,
            target_dialect=target_dialect,
        )
        schema = dictionary.get_schema_ddl(
            driver,
            schema='"main"',
            include_domains=("tables",),
            include_dependencies=False,
            prefer_native=False,
            target_dialect=target_dialect,
        )
    dialect = target_dialect or "sqlite"
    assert table.status == MetadataSupport.SUPPORTED
    assert (table.identity.name, table.identity.schema, table.identity.dialect) == ("Mixed Items", "main", dialect)
    assert isinstance(table.ddl, str)
    assert '"Mixed Items"' in table.ddl
    assert '"Mixed ID"' in table.ddl
    assert len(schema.items) == 1
    item = schema.items[0]
    assert isinstance(item, DDLResult)
    assert (item.identity.name, item.identity.schema, item.identity.dialect, item.status) == (
        "Mixed Items",
        "main",
        dialect,
        MetadataSupport.SUPPORTED,
    )
    assert item.ddl == table.ddl


def test_get_statistics_uses_native_statistic_names_when_available() -> None:
    """ADBC statistic-name metadata should replace built-in fallback labels."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_statistics.return_value = _make_reader([
        {
            "catalog_name": "memory",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "main",
                    "db_schema_statistics": [
                        {
                            "table_name": "items",
                            "column_name": None,
                            "statistic_key": 777,
                            "statistic_value": 3,
                            "statistic_is_approximate": False,
                        }
                    ],
                }
            ],
        }
    ])
    driver.connection.adbc_get_statistic_names.return_value = _make_reader([
        {"statistic_key": 777, "statistic_name": "vendor.custom_count"}
    ])

    statistics = AdbcDataDictionary().get_statistics(driver, "items", approximate=False)

    assert statistics[0]["statistic_name"] == "vendor.custom_count"
    assert statistics[0]["is_approximate"] is False
    driver.connection.adbc_get_statistic_names.assert_called_once()


@pytest.mark.parametrize(
    ("version_str", "expected_type"),
    [("23.4.0.24.05", "JSON"), ("21.3.0.0.0", "JSON"), ("19.3.0.0.0", "BLOB"), ("11.2.0.4.0", "CLOB")],
)
def test_oracle_optimal_json_type_resolution(version_str: str, expected_type: str) -> None:
    driver = Mock()
    driver.dialect = "oracle"
    driver.select_value_or_none.return_value = f"Oracle Database {version_str}"

    dictionary = AdbcDataDictionary()
    assert dictionary.get_optimal_type(driver, "json") == expected_type


def test_adbc_sqlite_get_columns_fallback_passes_bind_parameters() -> None:
    """SQLite fallback column queries should bind table_name and schema_name."""
    driver = Mock()
    driver.dialect = "sqlite"
    driver.connection.adbc_get_objects.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")
    driver.select.return_value = [{"column_name": "id", "data_type": "INTEGER", "is_primary": 1}]

    result = AdbcDataDictionary().get_columns(driver, table="users", schema="main")

    assert result == [{"column_name": "id", "data_type": "INTEGER", "is_primary": 1}]
    driver.select.assert_called_once()
    assert driver.select.call_args.kwargs["table_name"] == "users"
    assert driver.select.call_args.kwargs["schema_name"] == "main"


def test_normalize_native_columns_marks_autoincrement_and_generated() -> None:
    """Native column normalization should populate is_identity and is_generated from xdbc flags."""
    payload: list[dict[str, Any]] = [
        {
            "catalog_name": "main",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "public",
                    "db_schema_tables": [
                        {
                            "table_name": "items",
                            "table_type": "BASE TABLE",
                            "table_columns": [
                                {
                                    "column_name": "id",
                                    "ordinal_position": 1,
                                    "xdbc_type_name": "INTEGER",
                                    "xdbc_is_nullable": "NO",
                                    "xdbc_is_autoincrement": "YES",
                                },
                                {
                                    "column_name": "slug",
                                    "ordinal_position": 2,
                                    "xdbc_type_name": "TEXT",
                                    "xdbc_is_nullable": "YES",
                                    "xdbc_is_generatedcolumn": "YES",
                                },
                            ],
                            "table_constraints": [],
                        }
                    ],
                }
            ],
        }
    ]
    columns = _normalize_native_columns(payload, table_name_exact="items")
    by_name = {entry["column_name"]: entry for entry in columns}

    assert by_name["id"]["is_identity"] is True
    assert by_name["slug"]["is_generated"] is True


def test_snowflake_tables_and_columns_fallback_uses_information_schema() -> None:
    """Snowflake fallback queries information_schema and normalizes uppercase row keys."""
    dictionary = AdbcDataDictionary()
    driver = Mock()
    driver.dialect = "snowflake"
    driver.connection.adbc_get_objects.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")
    driver.select_one_or_none.return_value = {"DATABASE_NAME": "app_db", "SCHEMA_NAME": "app"}
    driver.select.side_effect = [
        [{"SCHEMA_NAME": "PUBLIC", "TABLE_NAME": "LS_SESS", "TABLE_TYPE": "BASE TABLE"}],
        [
            {
                "SCHEMA_NAME": "PUBLIC",
                "TABLE_NAME": "LS_SESS",
                "COLUMN_NAME": "SESSION_ID",
                "DATA_TYPE": "TEXT",
                "ORDINAL_POSITION": 1,
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
            }
        ],
    ]

    tables = dictionary.get_tables(driver)
    columns = dictionary.get_columns(driver, table="ls_sess", schema="PUBLIC")

    assert tables == [{"schema_name": "PUBLIC", "table_name": "LS_SESS", "table_type": "BASE TABLE"}]
    tables_call = driver.select.call_args_list[0]
    assert tables_call.kwargs == {"schema_name": "app"}
    assert '"app_db".information_schema.tables' in tables_call.args[0].raw_sql
    assert driver.select.call_args_list[1].kwargs == {"schema_name": "PUBLIC", "table_name": "LS_SESS"}
    assert columns[0]["column_name"] == "SESSION_ID"
    assert columns[0]["data_type"] == "TEXT"


def test_oracle_and_db2_index_fallback_normalizes_rows() -> None:
    """Oracle and Db2 SQL index fallbacks normalize uppercase keys and column lists."""
    dictionary = AdbcDataDictionary()

    oracle_driver = Mock()
    oracle_driver.dialect = "oracle"
    oracle_driver.select.return_value = [
        {
            "SCHEMA_NAME": "APP",
            "INDEX_NAME": "IDX_SESS_EXP",
            "TABLE_NAME": "LS_SESS",
            "IS_UNIQUE": 0,
            "IS_PRIMARY": 0,
            "COLUMNS": "EXPIRES_AT,SESSION_ID",
        }
    ]
    oracle_indexes = dictionary.get_indexes(oracle_driver, table="ls_sess")
    assert oracle_indexes[0]["index_name"] == "IDX_SESS_EXP"
    assert oracle_indexes[0]["columns"] == ["EXPIRES_AT", "SESSION_ID"]

    db2_driver = Mock()
    db2_driver.dialect = "db2"
    db2_driver.select.return_value = [
        {
            "SCHEMA_NAME": "DB2INST1",
            "TABLE_NAME": "ADK_SESS",
            "INDEX_NAME": "IDX_ADK_SESS_APP_USER",
            "COLUMN_NAME": "APP_NAME",
            "COLUMN_POSITION": 1,
            "IS_UNIQUE": 0,
            "IS_PRIMARY": 0,
        },
        {
            "SCHEMA_NAME": "DB2INST1",
            "TABLE_NAME": "ADK_SESS",
            "INDEX_NAME": "IDX_ADK_SESS_APP_USER",
            "COLUMN_NAME": "USER_ID",
            "COLUMN_POSITION": 2,
            "IS_UNIQUE": 0,
            "IS_PRIMARY": 0,
        },
    ]
    db2_indexes = dictionary.get_indexes(db2_driver, table="adk_sess")
    assert len(db2_indexes) == 1
    assert db2_indexes[0]["index_name"] == "IDX_ADK_SESS_APP_USER"
    assert db2_indexes[0]["columns"] == ["APP_NAME", "USER_ID"]


def test_snowflake_tables_and_columns_filter_permutations() -> None:
    """Snowflake SQL catalog fallback supports schema-scoped, table-scoped, and unfiltered queries."""
    dictionary = AdbcDataDictionary()
    driver = Mock()
    driver.dialect = "snowflake"
    driver.connection.adbc_get_objects.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")
    driver.select_one_or_none.return_value = {"DATABASE_NAME": "APP_DB", "SCHEMA_NAME": "APP"}
    driver.select.return_value = [{"SCHEMA_NAME": "PUBLIC", "TABLE_NAME": "SESS", "COLUMN_NAME": "ID"}]

    schema_tables = dictionary.get_tables(driver, schema="PUBLIC")
    table_cols = dictionary.get_columns(driver, table="sess")
    schema_cols = dictionary.get_columns(driver, schema="PUBLIC")
    all_cols = dictionary.get_columns(driver)

    assert schema_tables[0]["table_name"] == "SESS"
    assert table_cols[0]["column_name"] == "ID"
    assert schema_cols[0]["column_name"] == "ID"
    assert all_cols[0]["column_name"] == "ID"


def test_teradata_native_metadata_padding() -> None:
    dictionary = AdbcDataDictionary()
    driver = Mock()
    driver.dialect = "vantage"
    driver.connection.adbc_get_objects.return_value = _make_reader([
        {
            "catalog_name": "analytics   ",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "analytics   ",
                    "db_schema_tables": [
                        {
                            "table_name": "events   ",
                            "table_type": "TABLE   ",
                            "table_columns": [
                                {
                                    "column_name": "event_id   ",
                                    "xdbc_type_name": "VARCHAR   ",
                                    "xdbc_nullable": 0,
                                    "ordinal_position": 1,
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ])
    tables = dictionary.get_tables(driver)
    assert tables[0]["table_name"] == "events"
    assert tables[0]["schema_name"] == "analytics"
    columns = dictionary.get_columns(driver, table="events", schema="analytics")
    assert columns[0]["column_name"] == "event_id"
    assert columns[0]["data_type"] == "VARCHAR"
    assert columns[0]["is_nullable"] == "NO"


@pytest.mark.parametrize("flag,expected", [("N", False), ("0", False), ("U", True), ("Y", True), ("1", True)])
def test_teradata_sql_index_flags(flag: str, expected: bool) -> None:
    driver = Mock()
    driver.dialect = "teradata"
    driver.connection.adbc_get_objects.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")
    driver.select.return_value = [
        {"index_name": "idx_event   ", "table_name": "events   ", "column_name": "event_id   ", "is_unique": flag}
    ]
    result = AdbcDataDictionary().get_indexes(driver, table="events")
    assert result == [
        {
            "schema_name": "",
            "table_name": "events",
            "index_name": "idx_event",
            "columns": ["event_id"],
            "is_unique": expected,
            "is_primary": False,
        }
    ]


@pytest.mark.parametrize("raw_dialect", ["teradata", "vantage"])
def test_teradata_vantage_dialect_normalization_in_data_dictionary(raw_dialect: str) -> None:
    """AdbcDataDictionary normalizes both teradata and vantage driver dialects to teradata."""
    dictionary = AdbcDataDictionary()
    driver = Mock()
    driver.dialect = raw_dialect
    assert dictionary._normalize_dialect(driver) == "teradata"
    assert dictionary.resolve_ddl_dialect(driver) == "teradata"


def test_snowflake_version_feature_flags_and_optimal_types() -> None:
    """Static Snowflake JSON support does not require a version query."""
    from sqlspec.data_dictionary import VersionInfo

    dictionary = AdbcDataDictionary()
    driver = Mock()
    driver.dialect = "snowflake"
    driver.select_value_or_none.return_value = "8.43.1"
    for category, expected in {
        "json": "VARIANT",
        "datetime": "TIMESTAMP_TZ",
        "blob": "BINARY",
        "boolean": "BOOLEAN",
        "uuid": "VARCHAR(36)",
    }.items():
        assert dictionary.get_optimal_type(driver, category) == expected
    driver.select_value_or_none.assert_not_called()
    assert dictionary.get_version(driver) == VersionInfo(8, 43, 1)
    assert dictionary.get_feature_flag(driver, "supports_transactions") is True
    assert dictionary.get_feature_flag(driver, "supports_transactional_ddl") is False


@pytest.mark.parametrize(
    ("table", "expected_scope"),
    [
        ("orders", 'IN TABLE "PUBLIC"."ORDERS"'),
        (None, 'IN SCHEMA "PUBLIC"'),
        ('"Mixed.Table"', 'IN TABLE "PUBLIC"."Mixed.Table"'),
    ],
)
def test_snowflake_foreign_key_catalog_fallback(table: str | None, expected_scope: str) -> None:
    """Unsupported GetObjects falls back to SHOW with exact quoted identifiers."""
    from sqlspec.data_dictionary import ForeignKeyMetadata

    dictionary = AdbcDataDictionary()
    driver = Mock()
    driver.dialect = "snowflake"
    driver.connection.adbc_get_objects.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")
    actual_table = "Mixed.Table" if table == '"Mixed.Table"' else "ORDERS"
    driver.select.return_value = [
        {
            "fk_name": "FK_ORD_USR",
            "pk_name": "PK_USR",
            "fk_schema_name": "PUBLIC",
            "fk_table_name": actual_table,
            "fk_column_name": "USER_ID",
            "pk_schema_name": "PUBLIC",
            "pk_table_name": "USERS",
            "pk_column_name": "ID",
        }
    ]
    assert dictionary.get_foreign_keys(driver, table=table, schema="public") == [
        ForeignKeyMetadata(actual_table, "USER_ID", "USERS", "ID", "FK_ORD_USR", "PUBLIC", "PUBLIC")
    ]
    assert driver.select.call_args.args[0].strip() == f"SHOW IMPORTED KEYS {expected_scope};"


@pytest.mark.parametrize("domain", ["schemas", "objects", "views", "sequences", "routines"])
def test_snowflake_catalog_domain_results(domain: str) -> None:
    """Catalog methods expose native capability and normalize result column labels."""
    dictionary = AdbcDataDictionary()
    driver = Mock()
    driver.dialect = "snowflake"
    driver.select_one_or_none.return_value = {"DATABASE_NAME": "APP_DB", "SCHEMA_NAME": "APP"}
    driver.select.return_value = [{"SCHEMA_NAME": "PUBLIC", "OBJECT_NAME": "ORDERS"}]
    result = getattr(dictionary, f"get_{domain}")(driver)
    if domain != "schemas":
        assert driver.select.call_args.kwargs == {"schema_name": "APP"}
    assert result.items == ({"schema_name": "PUBLIC", "object_name": "ORDERS"},)
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.capability.source == MetadataSource.CATALOG
    assert result.capability.fidelity == MetadataFidelity.NATIVE


def test_snowflake_foreign_keys_reject_nul_identifier() -> None:
    """Invalid catalog identifiers fail before SHOW SQL is sent to the driver."""
    from sqlspec.exceptions import SQLSpecError

    driver = Mock()
    driver.dialect = "snowflake"
    driver.connection.adbc_get_objects.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")
    with pytest.raises(SQLSpecError, match="NUL"):
        AdbcDataDictionary().get_foreign_keys(driver, table="orders\x00", schema="public")
    driver.select.assert_not_called()


def test_snowflake_qualified_table_and_database_scope_reflection() -> None:
    """Qualified 2-part and 3-part Snowflake table names resolve schema and database scopes across catalog methods."""
    from sqlspec.exceptions import SQLSpecError

    dictionary = AdbcDataDictionary()
    driver = Mock()
    driver.dialect = "snowflake"
    driver.connection.adbc_get_objects.side_effect = AdbcNotSupportedError("NOT_IMPLEMENTED")
    driver.select.return_value = [
        {
            "SCHEMA_NAME": "SALES",
            "TABLE_NAME": "ORDERS",
            "COLUMN_NAME": "ID",
            "DATA_TYPE": "NUMBER",
            "ORDINAL_POSITION": 1,
            "IS_NULLABLE": "NO",
        }
    ]

    cols = dictionary.get_columns(driver, table="other_db.sales.orders")
    assert cols[0]["column_name"] == "ID"
    first_query = driver.select.call_args.args[0].raw_sql
    assert '"OTHER_DB".information_schema.' in first_query
    assert driver.select.call_args.kwargs == {"schema_name": "SALES", "table_name": "ORDERS"}

    quoted_cols = dictionary.get_columns(driver, table='"sales"."Orders"')
    assert quoted_cols[0]["column_name"] == "ID"
    assert driver.select.call_args.kwargs == {"schema_name": "sales", "table_name": "Orders"}

    driver.select.return_value = []
    dictionary.get_foreign_keys(driver, table="other_db.sales.orders")
    assert driver.select.call_args.args[0].strip() == 'SHOW IMPORTED KEYS IN TABLE "OTHER_DB"."SALES"."ORDERS";'

    dictionary.get_tables(driver, schema="other_db.sales")
    tables_query = driver.select.call_args.args[0].raw_sql
    assert '"OTHER_DB".information_schema.' in tables_query
    assert driver.select.call_args.kwargs == {"schema_name": "SALES"}

    constraints = dictionary.get_constraints(driver, table="other_db.sales.orders")
    assert constraints.capability.support == MetadataSupport.UNSUPPORTED
    assert driver.connection.adbc_get_objects.call_args.kwargs == {
        "depth": "all",
        "catalog_filter": "OTHER_DB",
        "db_schema_filter": "SALES",
        "table_name_filter": "ORDERS",
    }

    assert dictionary.resolve_ddl_object_name(driver, "sales.orders", None) == ("ORDERS", "SALES")
    assert dictionary.resolve_ddl_object_name(driver, "other_db.sales.orders", None) == ("ORDERS", '"OTHER_DB"."SALES"')
    assert dictionary.exact_catalog_identifier(driver, '"OTHER_DB"."SALES"') == '"OTHER_DB"."SALES"'

    sqlite_driver = Mock()
    sqlite_driver.dialect = "sqlite"
    unsupported_views = dictionary.get_views(sqlite_driver)
    assert unsupported_views.capability.support == MetadataSupport.UNSUPPORTED

    for bad_identifier in ("", "sales.", "sales..orders", '""'):
        with pytest.raises(SQLSpecError):
            dictionary.get_columns(driver, table=bad_identifier)


@pytest.mark.parametrize(("dialect", "domain"), [("oracle", "objects"), ("db2", "views"), ("teradata", "objects")])
def test_catalog_domains_needing_extra_binds_report_unsupported(dialect: str, domain: str) -> None:
    """Catalog queries that bind more than ``schema_name`` are reported unsupported instead of failing."""
    driver = Mock()
    driver.dialect = dialect

    result = getattr(AdbcDataDictionary(), f"get_{domain}")(driver)

    assert result.capability.support == MetadataSupport.UNSUPPORTED
    driver.select.assert_not_called()


def test_mysql_schema_listing_binds_declared_schema_parameter() -> None:
    """Catalog queries declaring ``schema_name`` receive it as a named bind."""
    driver = Mock()
    driver.dialect = "mysql"
    driver.select.return_value = [{"SCHEMA_NAME": "app"}]

    result = AdbcDataDictionary().get_schemas(driver)

    assert result.items == ({"schema_name": "app"},)
    assert driver.select.call_args.kwargs == {"schema_name": None}
