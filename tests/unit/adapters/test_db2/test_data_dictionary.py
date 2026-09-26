"""Tests for the Db2 sync and async data dictionaries' schema reflection and metadata queries.

Every behavior test runs against both dictionaries: the async dictionary is driven through an
``AsyncDriverDouble`` wrapping the same sync driver or mock the sync case uses.
"""

import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.db2.data_dictionary import (
    DB2_CONFIG,
    Db2AsyncDataDictionary,
    Db2SyncDataDictionary,
    Db2VersionInfo,
)
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataResult,
    MetadataSupport,
    TableMetadata,
)
from sqlspec.exceptions import SQLSpecError
from tests.unit.adapters.test_db2._fakes import (
    AsyncDriverDouble,
    FakeDb2Connection,
    FakeDb2Cursor,
    db2_description,
    db2_error,
)

VERSION_COLUMNS = ("service_level", "bld_level", "fixpack_num", "inst_name")

pytestmark = pytest.mark.anyio


class DictionaryCase:
    """One data dictionary (sync or async) called through the matching driver.

    The async dictionary receives one ``AsyncDriverDouble`` per delegate, so per-driver caches
    see a stable driver across calls.
    """

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.dictionary: Db2SyncDataDictionary | Db2AsyncDataDictionary = (
            Db2SyncDataDictionary() if mode == "sync" else Db2AsyncDataDictionary()
        )
        self._doubles: dict[int, AsyncDriverDouble] = {}

    async def call(self, method: str, driver: Any, *args: Any, **kwargs: Any) -> Any:
        """Call a dictionary method with the sync driver or its async double, awaiting coroutines."""
        target = driver if self.mode == "sync" else self._doubles.setdefault(id(driver), AsyncDriverDouble(driver))
        result = getattr(self.dictionary, method)(target, *args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result


@pytest.fixture(params=["sync", "async"])
def case(request: pytest.FixtureRequest) -> DictionaryCase:
    """Return a sync or async dictionary case."""
    return DictionaryCase(str(request.param))


def test_db2_dialect_config_registered() -> None:
    """Verify Db2 dialect configuration is registered with appropriate feature flags."""
    assert DB2_CONFIG.name == "db2"
    assert DB2_CONFIG.get_feature_flag("supports_transactions") is True
    assert DB2_CONFIG.get_feature_flag("supports_on_conflict") is False
    assert DB2_CONFIG.get_optimal_type("uuid") == "VARCHAR(36)"
    assert DB2_CONFIG.get_optimal_type("boolean") == "BOOLEAN"
    assert DB2_CONFIG.get_optimal_type("decimal") == "DECIMAL(31, 10)"


def _driver_with_results(
    *results: "tuple[tuple[str, ...], list[tuple[Any, ...]]]",
) -> "tuple[Db2SyncDriver, FakeDb2Connection]":
    """Build a real driver whose cursors return rows with Db2-folded column descriptions."""
    cursors = [FakeDb2Cursor(rows=rows, description=db2_description(*names)) for names, rows in results]
    connection = FakeDb2Connection(cursors)
    return Db2SyncDriver(connection), connection


async def test_db2_get_tables(case: DictionaryCase) -> None:
    """Verify get_tables loads query and maps SYSCAT.TABLES rows to TableMetadata."""
    names = ("schema_name", "table_name", "table_type")
    rows = [("MYSCHEMA", "USERS", "BASE TABLE"), ("MYSCHEMA", "ACTIVE_USERS", "VIEW")]
    driver, connection = _driver_with_results((names, rows), (names, rows))

    tables = await case.call("get_tables", driver, schema="MYSCHEMA")

    assert len(tables) == 2
    assert tables[0] == TableMetadata(schema_name="MYSCHEMA", table_name="USERS", table_type="BASE TABLE")
    assert tables[1] == TableMetadata(schema_name="MYSCHEMA", table_name="ACTIVE_USERS", table_type="VIEW")
    executed = [cursor.executed[0][0] for cursor in connection.cursors]
    assert len(executed) == 2
    assert "SYSCAT.TABLES" in executed[0]


async def test_db2_get_columns(case: DictionaryCase) -> None:
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
        "is_unique",
        "identity_generation",
        "is_generated",
    )
    rows = [
        ("MYSCHEMA", "USERS", "ID", "BIGINT", 0, None, 1, 8, 0, 1, 1, "A", "A"),
        ("MYSCHEMA", "USERS", "NAME", "VARCHAR", 1, "'anonymous'", 2, 255, 0, 0, 0, None, None),
    ]
    driver, connection = _driver_with_results((names, rows))

    columns = await case.call("get_columns", driver, table="USERS", schema="MYSCHEMA")

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


async def test_db2_get_indexes(case: DictionaryCase) -> None:
    """Verify get_indexes combines multiple column rows into IndexMetadata."""
    names = ("schema_name", "table_name", "index_name", "column_name", "column_position", "is_unique", "is_primary")
    rows = [
        ("MYSCHEMA", "ORDERS", "PK_ORDERS", "ID", 1, 1, 1),
        ("MYSCHEMA", "ORDERS", "IX_CUSTOMER_DATE", "CUSTOMER_ID", 1, 0, 0),
        ("MYSCHEMA", "ORDERS", "IX_CUSTOMER_DATE", "ORDER_DATE", 2, 0, 0),
    ]
    driver, _ = _driver_with_results((names, rows))

    indexes = await case.call("get_indexes", driver, table="ORDERS", schema="MYSCHEMA")

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


async def test_db2_get_foreign_keys(case: DictionaryCase) -> None:
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

    fks = await case.call("get_foreign_keys", driver, table="ORDERS", schema="MYSCHEMA")

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


async def test_db2_version_detection_and_caching(case: DictionaryCase) -> None:
    """Verify get_version parses service level string and caches result per driver instance."""
    mock_driver = MagicMock()
    mock_driver.select_one_or_none.return_value = {"SERVICE_LEVEL": "DB2 v11.5.8000.123"}

    version = await case.call("get_version", mock_driver)

    assert isinstance(version, Db2VersionInfo)
    assert version.major == 11
    assert version.minor == 5
    assert version.patch == 8000
    assert version.service_level == "DB2 v11.5.8000.123"

    cached = await case.call("get_version", mock_driver)
    assert cached is version
    assert mock_driver.select_one_or_none.call_count == 1


async def test_db2_feature_flags_and_optimal_types(case: DictionaryCase) -> None:
    """Verify get_feature_flag and get_optimal_type delegate to DB2_CONFIG."""
    mock_driver = MagicMock()

    assert await case.call("get_feature_flag", mock_driver, "supports_transactions") is True
    assert await case.call("get_feature_flag", mock_driver, "supports_on_conflict") is False
    assert await case.call("get_feature_flag", mock_driver, "unknown_flag") is False

    assert await case.call("get_optimal_type", mock_driver, "uuid") == "VARCHAR(36)"
    assert await case.call("get_optimal_type", mock_driver, "boolean") == "BOOLEAN"
    assert await case.call("get_optimal_type", mock_driver, "decimal") == "DECIMAL(31, 10)"
    assert await case.call("get_feature_flag", mock_driver, "supports_skip_locked") is True


async def test_db2_list_available_features_resolve_as_flags(case: DictionaryCase) -> None:
    """Every listed feature is a flag the dictionary can answer for a version-less driver."""
    mock_driver = MagicMock()
    mock_driver.select_one_or_none.return_value = None

    features = case.dictionary.list_available_features()

    assert {"supports_transactions", "supports_skip_locked", "supports_on_conflict"} <= set(features)
    flags = {feature: await case.call("get_feature_flag", mock_driver, feature) for feature in features}
    assert flags["supports_transactions"] is True
    assert flags["supports_on_conflict"] is False


async def test_db2_get_tables_empty_and_no_schema(case: DictionaryCase) -> None:
    """Verify get_tables handles empty results and query without schema parameter."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    tables = await case.call("get_tables", mock_driver, schema=None)

    assert tables == []
    assert mock_driver.select.call_count == 2
    kwargs = mock_driver.select.call_args[1]
    assert kwargs.get("schema_name") is None


async def test_db2_get_columns_empty_and_no_schema(case: DictionaryCase) -> None:
    """Verify get_columns handles empty results and queries without schema and table filters."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    columns = await case.call("get_columns", mock_driver, table=None, schema=None)

    assert columns == []
    mock_driver.select.assert_called_once()
    kwargs = mock_driver.select.call_args[1]
    assert kwargs.get("schema_name") is None
    assert kwargs.get("table_name") is None


async def test_db2_get_columns_primary_key_variations(case: DictionaryCase) -> None:
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
            "is_unique": 1,
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
            "is_unique": 0,
            "identity_generation": None,
            "is_generated": None,
        },
    ]

    columns = await case.call("get_columns", mock_driver, table="ITEMS", schema="MYSCHEMA")

    assert len(columns) == 2
    assert columns[0]["is_primary"] is True
    assert columns[0]["is_unique"] is True
    assert columns[1]["is_primary"] is False
    assert columns[1]["is_unique"] is False


async def test_db2_get_indexes_empty_and_no_schema(case: DictionaryCase) -> None:
    """Verify get_indexes handles empty results and queries without table and schema filters."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    indexes = await case.call("get_indexes", mock_driver, table=None, schema=None)

    assert indexes == []
    mock_driver.select.assert_called_once()
    kwargs = mock_driver.select.call_args[1]
    assert kwargs.get("schema_name") is None
    assert kwargs.get("table_name") is None


async def test_db2_get_foreign_keys_empty_and_no_schema(case: DictionaryCase) -> None:
    """Verify get_foreign_keys handles empty results and queries without table and schema filters."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = []

    fks = await case.call("get_foreign_keys", mock_driver, table=None, schema=None)

    assert fks == []
    mock_driver.select.assert_called_once()
    kwargs = mock_driver.select.call_args[1]
    assert kwargs.get("schema_name") is None
    assert kwargs.get("table_name") is None


async def test_db2_get_constraints_views_schemas(case: DictionaryCase) -> None:
    """Verify get_constraints, get_views, and get_schemas execute appropriate queries."""
    mock_driver = MagicMock()
    mock_driver.select.return_value = [{"name": "test"}]

    constraints = await case.call("get_constraints", mock_driver, table="ITEMS", schema="MYSCHEMA")
    assert len(constraints.items) == 1

    views = await case.call("get_views", mock_driver, schema="MYSCHEMA")
    assert len(views.items) == 1

    schemas = await case.call("get_schemas", mock_driver)
    assert len(schemas.items) == 1


async def test_db2_metadata_capabilities_are_exact(case: DictionaryCase) -> None:
    """Every claimed metadata domain is reported as supported."""
    profile = await case.call("get_metadata_capabilities", MagicMock())

    assert profile.dialect == "db2"
    assert tuple(capability.domain for capability in profile.capabilities) == (
        "schemas",
        "objects",
        "tables",
        "columns",
        "constraints",
        "indexes",
        "foreign_keys",
        "views",
        "version",
    )
    assert {capability.support for capability in profile.capabilities} == {MetadataSupport.SUPPORTED}


async def test_unqualified_lookup_binds_null_schema_and_sql_uses_current_schema(case: DictionaryCase) -> None:
    """Unqualified lookups bind a NULL schema that the SQL resolves to CURRENT SCHEMA."""
    names = ("schema_name", "table_name", "table_type")
    driver, connection = _driver_with_results((names, []), (names, []))

    await case.call("get_tables", driver)

    for cursor in connection.cursors:
        sql, parameters = cursor.executed[0]
        assert "COALESCE(CAST(? AS VARCHAR(128)), CURRENT SCHEMA)" in sql
        assert "? IS NULL OR" not in sql
        assert set(parameters) == {None}


@pytest.mark.parametrize(("service_level", "expected"), [("DB2 v11.5.9.0", (11, 5, 9)), ("DB2 v12.1", (12, 1, 0))])
async def test_get_version_parses_service_level(
    service_level: str, expected: "tuple[int, int, int]", case: DictionaryCase
) -> None:
    """The service level reported by the instance is parsed into a version."""
    driver, _ = _driver_with_results((VERSION_COLUMNS, [(service_level, "s1", 0, "db2inst1")]))

    version = await case.call("get_version", driver)

    assert isinstance(version, Db2VersionInfo)
    assert (version.major, version.minor, version.patch) == expected
    assert version.service_level == service_level


@pytest.mark.parametrize("rows", [[("unknown", "s1", 0, "db2inst1")], []])
async def test_get_version_returns_none_for_unparsable_row(rows: "list[tuple[Any, ...]]", case: DictionaryCase) -> None:
    """A missing or unparsable service level yields and caches None."""
    driver, connection = _driver_with_results((VERSION_COLUMNS, rows))

    assert await case.call("get_version", driver) is None
    assert await case.call("get_version", driver) is None
    assert len(connection.cursors) == 1


async def test_get_version_propagates_query_errors(case: DictionaryCase) -> None:
    """A failing version query raises the mapped driver error instead of inventing a version."""
    error = db2_error(-440, "42884", 'No authorized routine named "ENV_GET_INST_INFO" of type "FUNCTION".')
    connection = FakeDb2Connection([FakeDb2Cursor(error=error)])

    with pytest.raises(SQLSpecError):
        await case.call("get_version", Db2SyncDriver(connection))


async def test_get_objects_returns_catalog_objects(case: DictionaryCase) -> None:
    """Catalog objects are returned in an objects-domain result for the current schema."""
    names = ("schema_name", "object_name", "object_type", "created", "remarks")
    rows = [("APP", "ORDERS", "TABLE", None, None), ("APP", "ORDER_SEQ", "SEQUENCE", None, "ids")]
    driver, connection = _driver_with_results((names, rows))

    result = await case.call("get_objects", driver)

    assert isinstance(result, MetadataResult)
    assert result.domain == "objects"
    assert result.items == (
        {"schema_name": "APP", "object_name": "ORDERS", "object_type": "TABLE", "created": None, "remarks": None},
        {
            "schema_name": "APP",
            "object_name": "ORDER_SEQ",
            "object_type": "SEQUENCE",
            "created": None,
            "remarks": "ids",
        },
    )
    sql, parameters = connection.cursors[0].executed[0]
    assert "SYSCAT.SEQUENCES" in sql
    assert "SYSCAT.ROUTINES" in sql
    assert set(parameters) == {None}


async def test_get_objects_binds_folded_schema(case: DictionaryCase) -> None:
    """An explicit schema is folded to its catalog form before binding."""
    names = ("schema_name", "object_name", "object_type", "created", "remarks")
    driver, connection = _driver_with_results((names, []))

    await case.call("get_objects", driver, schema="app")

    assert "APP" in connection.cursors[0].executed[0][1]


async def test_get_columns_reports_unique_separately_from_primary(case: DictionaryCase) -> None:
    """A column covered by a single-column unique index is unique without being a primary key."""
    names = ("schema_name", "table_name", "column_name", "data_type", "is_primary", "is_unique")
    driver, _ = _driver_with_results((names, [("APP", "USERS", "EMAIL", "VARCHAR", 0, 1)]))

    columns = await case.call("get_columns", driver, table="users", schema="app")

    assert columns[0]["is_primary"] is False
    assert columns[0]["is_unique"] is True
