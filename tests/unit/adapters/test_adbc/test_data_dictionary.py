"""Unit tests for ADBC native metadata normalization and fallback."""

from typing import Any, cast
from unittest.mock import Mock

import pyarrow as pa
import pytest

pytest.importorskip("adbc_driver_manager")

from adbc_driver_manager import NotSupportedError as AdbcNotSupportedError

from sqlspec.adapters.adbc.data_dictionary import (
    AdbcDataDictionary,
    _arrow_type_to_sql,
    _normalize_native_columns,
    _normalize_native_foreign_keys,
    _normalize_native_statistics,
    _normalize_native_tables,
)
from sqlspec.data_dictionary import MetadataFidelity, MetadataSource, MetadataSupport
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
                    "db_schema_tables": [
                        {"table_name": "t2", "table_type": "table", "table_columns": [], "table_constraints": []}
                    ],
                }
            ],
        }
    ])
    fallback: list[Any] = [Mock()]
    driver.select.return_value = fallback

    result = AdbcDataDictionary().get_foreign_keys(driver, table="t2")

    assert result == fallback
    driver.select.assert_called_once()


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


def test_adbc_ddl_is_unsupported_without_dialect_pack() -> None:
    """ADBC transport metadata should not claim lossless DDL."""
    driver = Mock()
    driver.dialect = "duckdb"

    result = AdbcDataDictionary().get_ddl(
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
