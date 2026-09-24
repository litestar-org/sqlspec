"""Tests for Db2SyncDataDictionary schema reflection and metadata queries."""

from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.db2.data_dictionary import DB2_CONFIG, Db2SyncDataDictionary, Db2VersionInfo
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.data_dictionary import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata
from tests.unit.adapters.test_db2._fakes import FakeDb2Connection, FakeDb2Cursor, db2_description


def test_db2_dialect_config_registered() -> None:
    """Verify Db2 dialect configuration is registered with appropriate feature flags."""
    assert DB2_CONFIG.name == "db2"
    assert DB2_CONFIG.get_feature_flag("supports_transactions") is True
    assert DB2_CONFIG.get_feature_flag("supports_on_conflict") is False
    assert DB2_CONFIG.get_optimal_type("uuid") == "VARCHAR(36)"
    assert DB2_CONFIG.get_optimal_type("boolean") == "BOOLEAN"
    assert DB2_CONFIG.get_optimal_type("decimal") == "DECFLOAT"


def _driver_with_results(
    *results: "tuple[tuple[str, ...], list[tuple[Any, ...]]]",
) -> "tuple[Db2SyncDriver, FakeDb2Connection]":
    """Build a real driver whose cursors return rows with Db2-folded column descriptions."""
    cursors = [FakeDb2Cursor(rows=rows, description=db2_description(*names)) for names, rows in results]
    connection = FakeDb2Connection(cursors)
    return Db2SyncDriver(connection), connection


def test_db2_get_tables() -> None:
    """Verify get_tables loads query and maps SYSCAT.TABLES rows to TableMetadata."""
    names = ("schema_name", "table_name", "table_type")
    rows = [("MYSCHEMA", "USERS", "BASE TABLE"), ("MYSCHEMA", "ACTIVE_USERS", "VIEW")]
    driver, connection = _driver_with_results((names, rows), (names, rows))

    dd = Db2SyncDataDictionary()
    tables = dd.get_tables(driver, schema="MYSCHEMA")

    assert len(tables) == 2
    assert tables[0] == TableMetadata(schema_name="MYSCHEMA", table_name="USERS", table_type="BASE TABLE")
    assert tables[1] == TableMetadata(schema_name="MYSCHEMA", table_name="ACTIVE_USERS", table_type="VIEW")
    executed = [cursor.executed[0][0] for cursor in connection.cursors]
    assert len(executed) == 2
    assert "SYSCAT.TABLES" in executed[0]


def test_db2_get_columns() -> None:
    """Verify get_columns maps SYSCAT.COLUMNS rows to ColumnMetadata."""
    names = (
        "schema_name",
        "table_name",
        "column_name",
        "data_type",
        "is_nullable",
        "column_default",
        "ordinal_position",
        "max_length",
        "numeric_scale",
        "is_primary",
        "identity_generation",
        "is_generated",
    )
    rows = [
        ("MYSCHEMA", "USERS", "ID", "BIGINT", 0, None, 1, 8, 0, 1, "A", "A"),
        ("MYSCHEMA", "USERS", "NAME", "VARCHAR", 1, "'anonymous'", 2, 255, 0, 0, None, None),
    ]
    driver, connection = _driver_with_results((names, rows))

    dd = Db2SyncDataDictionary()
    columns = dd.get_columns(driver, table="USERS", schema="MYSCHEMA")

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
    assert len(connection.cursors) == 1
    assert "SYSCAT.COLUMNS" in connection.cursors[0].executed[0][0]


def test_db2_get_indexes() -> None:
    """Verify get_indexes combines multiple column rows into IndexMetadata."""
    names = ("schema_name", "table_name", "index_name", "column_name", "column_position", "is_unique", "is_primary")
    rows = [
        ("MYSCHEMA", "ORDERS", "PK_ORDERS", "ID", 1, 1, 1),
        ("MYSCHEMA", "ORDERS", "IX_CUSTOMER_DATE", "CUSTOMER_ID", 1, 0, 0),
        ("MYSCHEMA", "ORDERS", "IX_CUSTOMER_DATE", "ORDER_DATE", 2, 0, 0),
    ]
    driver, _ = _driver_with_results((names, rows))

    dd = Db2SyncDataDictionary()
    indexes = dd.get_indexes(driver, table="ORDERS", schema="MYSCHEMA")

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
    names = (
        "schema_name",
        "table_name",
        "constraint_name",
        "column_name",
        "referenced_schema",
        "referenced_table",
        "referenced_column",
    )
    rows = [("MYSCHEMA", "ORDERS", "FK_ORDERS_USERS", "USER_ID", "MYSCHEMA", "USERS", "ID")]
    driver, _ = _driver_with_results((names, rows))

    dd = Db2SyncDataDictionary()
    fks = dd.get_foreign_keys(driver, table="ORDERS", schema="MYSCHEMA")

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
    assert mock_driver.select.call_count == 2
    kwargs = mock_driver.select.call_args[1]
    assert kwargs.get("schema_name") is None


def test_db2_get_columns_empty_and_no_schema() -> None:
    """Verify get_columns handles empty results and queries without schema and table filters."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    dd = Db2SyncDataDictionary()
    columns = dd.get_columns(mock_driver, table=None, schema=None)

    assert columns == []
    mock_driver.select.assert_called_once()
    kwargs = mock_driver.select.call_args[1]
    assert kwargs.get("schema_name") is None
    assert kwargs.get("table_name") is None


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
    kwargs = mock_driver.select.call_args[1]
    assert kwargs.get("schema_name") is None
    assert kwargs.get("table_name") is None


def test_db2_get_foreign_keys_empty_and_no_schema() -> None:
    """Verify get_foreign_keys handles empty results and queries without table and schema filters."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    dd = Db2SyncDataDictionary()
    fks = dd.get_foreign_keys(mock_driver, table=None, schema=None)

    assert fks == []
    mock_driver.select.assert_called_once()
    kwargs = mock_driver.select.call_args[1]
    assert kwargs.get("schema_name") is None
    assert kwargs.get("table_name") is None


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


def test_db2_get_constraints_views_schemas() -> None:
    """Verify get_constraints, get_views, and get_schemas execute appropriate queries."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = [{"name": "test"}]

    dd = Db2SyncDataDictionary()
    constraints = dd.get_constraints(mock_driver, table="ITEMS", schema="MYSCHEMA")
    assert len(constraints.items) == 1

    views = dd.get_views(mock_driver, schema="MYSCHEMA")
    assert len(views.items) == 1

    schemas = dd.get_schemas(mock_driver)
    assert len(schemas.items) == 1


def test_db2_metadata_capabilities() -> None:
    """Verify get_metadata_capabilities returns valid profile."""
    mock_driver = MagicMock()
    dd = Db2SyncDataDictionary()
    profile = dd.get_metadata_capabilities(mock_driver)
    assert profile.dialect == "db2"
    assert len(profile.capabilities) > 0
