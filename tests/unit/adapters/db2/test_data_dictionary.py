"""Tests for Db2SyncDataDictionary schema reflection and metadata queries."""

from unittest.mock import MagicMock

from sqlspec.adapters.db2.data_dictionary import DB2_CONFIG, Db2SyncDataDictionary, Db2VersionInfo
from sqlspec.data_dictionary import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata


def test_db2_dialect_config_registered() -> None:
    """Verify Db2 dialect configuration is registered with appropriate feature flags."""
    assert DB2_CONFIG.name == "db2"
    assert DB2_CONFIG.get_feature_flag("supports_transactions") is True
    assert DB2_CONFIG.get_feature_flag("supports_on_conflict") is False
    assert DB2_CONFIG.get_optimal_type("uuid") == "VARCHAR(36)"
    assert DB2_CONFIG.get_optimal_type("boolean") == "BOOLEAN"
    assert DB2_CONFIG.get_optimal_type("decimal") == "DECFLOAT"


def test_db2_get_tables() -> None:
    """Verify get_tables generates query and maps SYSCAT.TABLES rows to TableMetadata."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = [
        {"schema_name": "MYSCHEMA", "table_name": "USERS", "table_type": "BASE TABLE"},
        {"schema_name": "MYSCHEMA", "table_name": "ACTIVE_USERS", "table_type": "VIEW"},
    ]

    dd = Db2SyncDataDictionary()
    tables = dd.get_tables(mock_driver, schema="MYSCHEMA")

    assert len(tables) == 2
    assert tables[0] == TableMetadata(schema_name="MYSCHEMA", table_name="USERS", table_type="BASE TABLE")
    assert tables[1] == TableMetadata(schema_name="MYSCHEMA", table_name="ACTIVE_USERS", table_type="VIEW")
    mock_driver.select.assert_called_once()
    args, _ = mock_driver.select.call_args
    assert "FROM SYSCAT.TABLES" in args[0]
    assert args[1] == "MYSCHEMA"


def test_db2_get_columns() -> None:
    """Verify get_columns maps SYSCAT.COLUMNS rows to ColumnMetadata."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = [
        {
            "schema_name": "MYSCHEMA",
            "table_name": "USERS",
            "column_name": "ID",
            "data_type": "BIGINT",
            "is_nullable": 0,
            "column_default": None,
            "ordinal_position": 1,
            "max_length": 8,
            "numeric_scale": 0,
            "is_primary": 1,
            "identity_generation": "A",
            "is_generated": "A",
        },
        {
            "schema_name": "MYSCHEMA",
            "table_name": "USERS",
            "column_name": "NAME",
            "data_type": "VARCHAR",
            "is_nullable": 1,
            "column_default": "'anonymous'",
            "ordinal_position": 2,
            "max_length": 255,
            "numeric_scale": 0,
            "is_primary": 0,
            "identity_generation": None,
            "is_generated": None,
        },
    ]

    dd = Db2SyncDataDictionary()
    columns = dd.get_columns(mock_driver, table="USERS", schema="MYSCHEMA")

    assert len(columns) == 2
    assert columns[0] == ColumnMetadata(
        schema_name="MYSCHEMA",
        table_name="USERS",
        column_name="ID",
        data_type="BIGINT",
        is_nullable=False,
        column_default=None,
        ordinal_position=1,
        max_length=8,
        numeric_scale=0,
        is_primary=True,
        is_unique=True,
        identity_generation="A",
        is_generated=True,
    )
    assert columns[1] == ColumnMetadata(
        schema_name="MYSCHEMA",
        table_name="USERS",
        column_name="NAME",
        data_type="VARCHAR",
        is_nullable=True,
        column_default="'anonymous'",
        ordinal_position=2,
        max_length=255,
        numeric_scale=0,
        is_primary=False,
        is_unique=False,
        identity_generation=None,
        is_generated=False,
    )
    mock_driver.select.assert_called_once()
    args, _ = mock_driver.select.call_args
    assert "FROM SYSCAT.COLUMNS" in args[0]


def test_db2_get_indexes() -> None:
    """Verify get_indexes combines multiple column rows into IndexMetadata."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = [
        {
            "schema_name": "MYSCHEMA",
            "table_name": "ORDERS",
            "index_name": "PK_ORDERS",
            "column_name": "ID",
            "column_position": 1,
            "is_unique": 1,
            "is_primary": 1,
        },
        {
            "schema_name": "MYSCHEMA",
            "table_name": "ORDERS",
            "index_name": "IX_CUSTOMER_DATE",
            "column_name": "CUSTOMER_ID",
            "column_position": 1,
            "is_unique": 0,
            "is_primary": 0,
        },
        {
            "schema_name": "MYSCHEMA",
            "table_name": "ORDERS",
            "index_name": "IX_CUSTOMER_DATE",
            "column_name": "ORDER_DATE",
            "column_position": 2,
            "is_unique": 0,
            "is_primary": 0,
        },
    ]

    dd = Db2SyncDataDictionary()
    indexes = dd.get_indexes(mock_driver, table="ORDERS", schema="MYSCHEMA")

    assert len(indexes) == 2
    assert indexes[0] == IndexMetadata(
        schema_name="MYSCHEMA",
        table_name="ORDERS",
        index_name="PK_ORDERS",
        columns=["ID"],
        is_unique=True,
        is_primary=True,
    )
    assert indexes[1] == IndexMetadata(
        schema_name="MYSCHEMA",
        table_name="ORDERS",
        index_name="IX_CUSTOMER_DATE",
        columns=["CUSTOMER_ID", "ORDER_DATE"],
        is_unique=False,
        is_primary=False,
    )


def test_db2_get_foreign_keys() -> None:
    """Verify get_foreign_keys maps joined SYSCAT.REFERENCES rows to ForeignKeyMetadata."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = [
        {
            "schema_name": "MYSCHEMA",
            "table_name": "ORDERS",
            "constraint_name": "FK_ORDERS_USERS",
            "column_name": "USER_ID",
            "referenced_schema": "MYSCHEMA",
            "referenced_table": "USERS",
            "referenced_column": "ID",
        }
    ]

    dd = Db2SyncDataDictionary()
    fks = dd.get_foreign_keys(mock_driver, table="ORDERS", schema="MYSCHEMA")

    assert len(fks) == 1
    assert fks[0] == ForeignKeyMetadata(
        schema="MYSCHEMA",
        table_name="ORDERS",
        constraint_name="FK_ORDERS_USERS",
        column_name="USER_ID",
        referenced_schema="MYSCHEMA",
        referenced_table="USERS",
        referenced_column="ID",
    )


def test_db2_version_detection_and_caching() -> None:
    """Verify get_version parses service level string and caches result per driver instance."""
    mock_driver = MagicMock()
    mock_driver.select_one_or_none.return_value = {"SERVICE_LEVEL": "DB2 v11.5.8000.123"}

    dd = Db2SyncDataDictionary()
    version = dd.get_version(mock_driver)

    assert isinstance(version, Db2VersionInfo)
    assert version.major == 11
    assert version.minor == 5
    assert version.patch == 8000
    assert version.service_level == "DB2 v11.5.8000.123"

    cached = dd.get_version(mock_driver)
    assert cached is version
    assert mock_driver.select_one_or_none.call_count == 1


def test_db2_feature_flags_and_optimal_types() -> None:
    """Verify get_feature_flag and get_optimal_type delegate to DB2_CONFIG."""
    mock_driver = MagicMock()
    dd = Db2SyncDataDictionary()

    assert dd.get_feature_flag(mock_driver, "supports_transactions") is True
    assert dd.get_feature_flag(mock_driver, "supports_on_conflict") is False
    assert dd.get_feature_flag(mock_driver, "unknown_flag") is False

    assert dd.get_optimal_type(mock_driver, "uuid") == "VARCHAR(36)"
    assert dd.get_optimal_type(mock_driver, "boolean") == "BOOLEAN"
    assert dd.get_optimal_type(mock_driver, "decimal") == "DECFLOAT"


def test_db2_get_tables_empty_and_no_schema() -> None:
    """Verify get_tables handles empty results and query without schema parameter."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    dd = Db2SyncDataDictionary()
    tables = dd.get_tables(mock_driver, schema=None)

    assert tables == []
    mock_driver.select.assert_called_once()
    sql = mock_driver.select.call_args[0][0]
    assert "AND TABSCHEMA = ?" not in sql


def test_db2_get_columns_empty_and_no_schema() -> None:
    """Verify get_columns handles empty results and queries without schema and table filters."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    dd = Db2SyncDataDictionary()
    columns = dd.get_columns(mock_driver, table=None, schema=None)

    assert columns == []
    mock_driver.select.assert_called_once()
    sql = mock_driver.select.call_args[0][0]
    assert "AND c.TABSCHEMA = ?" not in sql
    assert "AND c.TABNAME = ?" not in sql


def test_db2_get_columns_primary_key_variations() -> None:
    """Verify get_columns accurately sets is_primary and is_unique across various keyseq indicators."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = [
        {
            "schema_name": "MYSCHEMA",
            "table_name": "ITEMS",
            "column_name": "PK_COL",
            "data_type": "INTEGER",
            "is_nullable": 0,
            "column_default": None,
            "ordinal_position": 1,
            "max_length": 4,
            "numeric_scale": 0,
            "is_primary": "1",
            "identity_generation": None,
            "is_generated": None,
        },
        {
            "schema_name": "MYSCHEMA",
            "table_name": "ITEMS",
            "column_name": "REG_COL",
            "data_type": "VARCHAR",
            "is_nullable": 1,
            "column_default": None,
            "ordinal_position": 2,
            "max_length": 50,
            "numeric_scale": 0,
            "is_primary": 0,
            "identity_generation": None,
            "is_generated": None,
        },
    ]

    dd = Db2SyncDataDictionary()
    columns = dd.get_columns(mock_driver, table="ITEMS", schema="MYSCHEMA")

    assert len(columns) == 2
    assert columns[0]["is_primary"] is True
    assert columns[0]["is_unique"] is True
    assert columns[1]["is_primary"] is False
    assert columns[1]["is_unique"] is False


def test_db2_get_indexes_empty_and_no_schema() -> None:
    """Verify get_indexes handles empty results and queries without table and schema filters."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    dd = Db2SyncDataDictionary()
    indexes = dd.get_indexes(mock_driver, table=None, schema=None)

    assert indexes == []
    mock_driver.select.assert_called_once()
    sql = mock_driver.select.call_args[0][0]
    assert "AND i.TABSCHEMA = ?" not in sql
    assert "AND i.TABNAME = ?" not in sql


def test_db2_get_foreign_keys_empty_and_no_schema() -> None:
    """Verify get_foreign_keys handles empty results and queries without table and schema filters."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    dd = Db2SyncDataDictionary()
    fks = dd.get_foreign_keys(mock_driver, table=None, schema=None)

    assert fks == []
    mock_driver.select.assert_called_once()
    sql = mock_driver.select.call_args[0][0]
    assert "AND r.TABSCHEMA = ?" not in sql
    assert "AND r.TABNAME = ?" not in sql


def test_db2_version_detection_fallback_on_error() -> None:
    """Verify get_version falls back to default version when driver query fails."""
    mock_driver = MagicMock()
    mock_driver.select_one_or_none.side_effect = RuntimeError("Db2 communication failure")

    dd = Db2SyncDataDictionary()
    version = dd.get_version(mock_driver)

    assert isinstance(version, Db2VersionInfo)
    assert version.major == 11
    assert version.minor == 5
    assert version.patch == 0
    assert version.service_level is None
