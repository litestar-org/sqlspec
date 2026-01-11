"""Integration tests for AsyncPG PostgreSQL data dictionary."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.typing import VersionInfo

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.driver import AsyncpgDriver

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_version_detection(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test PostgreSQL version detection with real database via asyncpg."""
    data_dict = asyncpg_async_driver.data_dictionary

    version = await data_dict.get_version(asyncpg_async_driver)
    assert version is not None
    assert isinstance(version, VersionInfo)
    assert version.major >= 9
    assert version.minor >= 0
    assert version.patch >= 0


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_feature_flags(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test PostgreSQL feature flags with real database via asyncpg."""
    data_dict = asyncpg_async_driver.data_dictionary

    # Test always supported features in modern PostgreSQL
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_transactions") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_prepared_statements") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_uuid") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_arrays") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_schemas") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_cte") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_window_functions") is True

    # Test version-dependent features (these depend on actual PostgreSQL version)
    version = await data_dict.get_version(asyncpg_async_driver)
    if version and version >= VersionInfo(9, 2, 0):
        assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_json") is True

    if version and version >= VersionInfo(9, 4, 0):
        assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_jsonb") is True

    if version and version >= VersionInfo(8, 2, 0):
        assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_returning") is True

    if version and version >= VersionInfo(9, 5, 0):
        assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_upsert") is True


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_optimal_types(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test PostgreSQL optimal type selection with real database via asyncpg."""
    data_dict = asyncpg_async_driver.data_dictionary

    # Test basic types
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "uuid") == "UUID"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "boolean") == "BOOLEAN"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "timestamp") == "TIMESTAMP WITH TIME ZONE"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "text") == "TEXT"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "blob") == "BYTEA"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "array") == "ARRAY"

    # Test JSON type based on version
    version = await data_dict.get_version(asyncpg_async_driver)
    if version and version >= VersionInfo(9, 4, 0):
        assert await data_dict.get_optimal_type(asyncpg_async_driver, "json") == "JSONB"
    elif version and version >= VersionInfo(9, 2, 0):
        assert await data_dict.get_optimal_type(asyncpg_async_driver, "json") == "JSON"
    else:
        assert await data_dict.get_optimal_type(asyncpg_async_driver, "json") == "TEXT"

    # Test unknown type defaults to TEXT
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "unknown_type") == "TEXT"


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_available_features(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test listing available features for PostgreSQL via asyncpg."""
    data_dict = asyncpg_async_driver.data_dictionary

    features = data_dict.list_available_features()
    assert isinstance(features, list)
    assert len(features) > 0

    expected_features = [
        "supports_json",
        "supports_jsonb",
        "supports_uuid",
        "supports_arrays",
        "supports_returning",
        "supports_upsert",
        "supports_window_functions",
        "supports_cte",
        "supports_transactions",
        "supports_prepared_statements",
        "supports_schemas",
    ]

    for feature in expected_features:
        assert feature in features


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_topology_and_fks(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test topological sort and FK metadata."""
    import uuid

    unique_suffix = uuid.uuid4().hex[:8]
    users_table = f"dd_users_{unique_suffix}"
    orders_table = f"dd_orders_{unique_suffix}"
    items_table = f"dd_items_{unique_suffix}"

    await asyncpg_async_driver.execute_script(f"""
        CREATE TABLE {users_table} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        CREATE TABLE {orders_table} (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES {users_table}(id),
            amount INTEGER
        );
        CREATE TABLE {items_table} (
            id SERIAL PRIMARY KEY,
            order_id INTEGER REFERENCES {orders_table}(id),
            name VARCHAR(50)
        );
    """)

    try:
        # Test 1: Topological Sort
        sorted_tables = await asyncpg_async_driver.data_dictionary.get_tables(asyncpg_async_driver)
        table_names = [table.get("table_name") for table in sorted_tables if table.get("table_name")]
        test_tables = [name for name in table_names if name in (users_table, orders_table, items_table)]
        assert len(test_tables) == 3

        idx_users = test_tables.index(users_table)
        idx_orders = test_tables.index(orders_table)
        idx_items = test_tables.index(items_table)

        assert idx_users < idx_orders
        assert idx_orders < idx_items

        # Test 2: Foreign Keys
        fks = await asyncpg_async_driver.data_dictionary.get_foreign_keys(asyncpg_async_driver, table=orders_table)
        assert len(fks) >= 1
        my_fk = next((fk for fk in fks if fk.referenced_table == users_table), None)
        assert my_fk is not None
        assert my_fk.column_name == "user_id"

        # Test 3: Indexes
        indexes = await asyncpg_async_driver.data_dictionary.get_indexes(asyncpg_async_driver, table=users_table)
        assert len(indexes) >= 1  # PK index

    finally:
        await asyncpg_async_driver.execute_script(f"""
            DROP TABLE IF EXISTS {items_table} CASCADE;
            DROP TABLE IF EXISTS {orders_table} CASCADE;
            DROP TABLE IF EXISTS {users_table} CASCADE;
        """)
