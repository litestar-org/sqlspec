"""Unit tests for mock data dictionary."""

from typing import cast

import pytest

from sqlspec.adapters.mock import MockAsyncConfig, MockSyncConfig
from sqlspec.typing import VersionInfo


def test_mock_data_dictionary_get_version() -> None:
    """Test retrieving SQLite version through data dictionary."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        version = session.data_dictionary.get_version(session)

        assert version is not None
        assert version.major >= 3
        assert version.minor >= 0
        assert version.patch >= 0


def test_mock_data_dictionary_version_caching() -> None:
    """Test that version is cached after first retrieval."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        dd = session.data_dictionary
        driver_id = id(session)

        was_cached, cached_version = cast("tuple[bool, VersionInfo | None]", dd.get_cached_version(driver_id))
        assert was_cached is False

        version1 = dd.get_version(session)
        was_cached, cached_version = cast("tuple[bool, VersionInfo | None]", dd.get_cached_version(driver_id))
        assert was_cached is True
        assert cached_version == version1

        version2 = dd.get_version(session)
        assert version2 == version1


def test_mock_data_dictionary_get_tables() -> None:
    """Test retrieving tables from data dictionary."""
    config = MockSyncConfig(
        initial_sql=[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)",
        ]
    )

    with config.provide_session() as session:
        tables = session.data_dictionary.get_tables(session)

        table_names = [t["table_name"] for t in tables]
        assert "users" in table_names
        assert "orders" in table_names


def test_mock_data_dictionary_get_tables_empty() -> None:
    """Test getting tables when no tables exist."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        tables = session.data_dictionary.get_tables(session)
        assert tables == []


def test_mock_data_dictionary_get_columns() -> None:
    """Test retrieving columns from data dictionary."""
    config = MockSyncConfig(
        initial_sql="CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)"
    )

    with config.provide_session() as session:
        columns = session.data_dictionary.get_columns(session, table="products")

        assert len(columns) >= 3
        column_names = [c["column_name"] for c in columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "price" in column_names


def test_mock_data_dictionary_get_columns_for_schema() -> None:
    """Test retrieving all columns for a schema."""
    config = MockSyncConfig(
        initial_sql=["CREATE TABLE table1 (id INTEGER, name TEXT)", "CREATE TABLE table2 (id INTEGER, value REAL)"]
    )

    with config.provide_session() as session:
        columns = session.data_dictionary.get_columns(session)

        assert len(columns) >= 4


def test_mock_data_dictionary_get_indexes() -> None:
    """Test retrieving indexes from data dictionary."""
    config = MockSyncConfig(
        initial_sql=[
            "CREATE TABLE indexed_table (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
            "CREATE INDEX idx_email ON indexed_table(email)",
        ]
    )

    with config.provide_session() as session:
        indexes = session.data_dictionary.get_indexes(session, table="indexed_table")

        assert len(indexes) > 0


def test_mock_data_dictionary_get_indexes_empty() -> None:
    """Test getting indexes when table has no indexes."""
    config = MockSyncConfig(initial_sql="CREATE TABLE simple (id INTEGER, name TEXT)")

    with config.provide_session() as session:
        indexes = session.data_dictionary.get_indexes(session, table="simple")
        assert len(indexes) == 0 or all(idx.get("index_name") for idx in indexes)


def test_mock_data_dictionary_get_foreign_keys() -> None:
    """Test retrieving foreign keys from data dictionary."""
    config = MockSyncConfig(
        initial_sql=[
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE child (id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))",
        ]
    )

    with config.provide_session() as session:
        fks = session.data_dictionary.get_foreign_keys(session, table="child")

        assert len(fks) > 0


def test_mock_data_dictionary_get_foreign_keys_empty() -> None:
    """Test getting foreign keys when table has none."""
    config = MockSyncConfig(initial_sql="CREATE TABLE standalone (id INTEGER PRIMARY KEY)")

    with config.provide_session() as session:
        fks = session.data_dictionary.get_foreign_keys(session, table="standalone")
        assert fks == []


def test_mock_data_dictionary_get_optimal_type_json() -> None:
    """Test getting optimal type for JSON category."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        json_type = session.data_dictionary.get_optimal_type(session, "json")

        assert json_type in ("JSON", "TEXT")


def test_mock_data_dictionary_get_optimal_type_text() -> None:
    """Test getting optimal type for text category."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        text_type = session.data_dictionary.get_optimal_type(session, "text")
        assert text_type == "TEXT"


def test_mock_data_dictionary_get_feature_flag() -> None:
    """Test checking feature flags."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        supports_cte = session.data_dictionary.get_feature_flag(session, "supports_cte")
        assert isinstance(supports_cte, bool)


def test_mock_data_dictionary_list_available_features() -> None:
    """Test listing available features."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        features = session.data_dictionary.list_available_features()

        assert isinstance(features, list)
        assert len(features) > 0


def test_mock_data_dictionary_dialect() -> None:
    """Test that data dictionary reports correct dialect."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        assert session.data_dictionary.dialect == "sqlite"


@pytest.mark.anyio
async def test_mock_async_data_dictionary_get_version() -> None:
    """Test retrieving SQLite version through async data dictionary."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        version = await session.data_dictionary.get_version(session)

        assert version is not None
        assert version.major >= 3


@pytest.mark.anyio
async def test_mock_async_data_dictionary_version_caching() -> None:
    """Test that version is cached in async data dictionary."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        dd = session.data_dictionary
        driver_id = id(session)

        was_cached, cached_version = cast("tuple[bool, VersionInfo | None]", dd.get_cached_version(driver_id))
        assert was_cached is False

        version1 = await dd.get_version(session)
        was_cached, cached_version = cast("tuple[bool, VersionInfo | None]", dd.get_cached_version(driver_id))
        assert was_cached is True
        assert cached_version == version1


@pytest.mark.anyio
async def test_mock_async_data_dictionary_get_tables() -> None:
    """Test retrieving tables from async data dictionary."""
    config = MockAsyncConfig(initial_sql="CREATE TABLE async_test (id INTEGER)")

    async with config.provide_session() as session:
        tables = await session.data_dictionary.get_tables(session)

        table_names = [t["table_name"] for t in tables]
        assert "async_test" in table_names


@pytest.mark.anyio
async def test_mock_async_data_dictionary_get_columns() -> None:
    """Test retrieving columns from async data dictionary."""
    config = MockAsyncConfig(initial_sql="CREATE TABLE async_cols (id INTEGER, name TEXT)")

    async with config.provide_session() as session:
        columns = await session.data_dictionary.get_columns(session, table="async_cols")

        assert len(columns) >= 2
        column_names = [c["column_name"] for c in columns]
        assert "id" in column_names
        assert "name" in column_names


@pytest.mark.anyio
async def test_mock_async_data_dictionary_get_indexes() -> None:
    """Test retrieving indexes from async data dictionary."""
    config = MockAsyncConfig(
        initial_sql=[
            "CREATE TABLE async_indexed (id INTEGER PRIMARY KEY, value TEXT)",
            "CREATE INDEX idx_value ON async_indexed(value)",
        ]
    )

    async with config.provide_session() as session:
        indexes = await session.data_dictionary.get_indexes(session, table="async_indexed")

        assert len(indexes) > 0


@pytest.mark.anyio
async def test_mock_async_data_dictionary_get_foreign_keys() -> None:
    """Test retrieving foreign keys from async data dictionary."""
    config = MockAsyncConfig(
        initial_sql=[
            "CREATE TABLE async_parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE async_child (id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES async_parent(id))",
        ]
    )

    async with config.provide_session() as session:
        fks = await session.data_dictionary.get_foreign_keys(session, table="async_child")

        assert len(fks) > 0


@pytest.mark.anyio
async def test_mock_async_data_dictionary_get_optimal_type() -> None:
    """Test getting optimal type from async data dictionary."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        text_type = await session.data_dictionary.get_optimal_type(session, "text")
        assert text_type == "TEXT"


@pytest.mark.anyio
async def test_mock_async_data_dictionary_get_feature_flag() -> None:
    """Test checking feature flags in async data dictionary."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        supports_cte = await session.data_dictionary.get_feature_flag(session, "supports_cte")
        assert isinstance(supports_cte, bool)


@pytest.mark.anyio
async def test_mock_async_data_dictionary_list_available_features() -> None:
    """Test listing available features from async data dictionary."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        features = session.data_dictionary.list_available_features()

        assert isinstance(features, list)
        assert len(features) > 0


@pytest.mark.anyio
async def test_mock_async_data_dictionary_dialect() -> None:
    """Test that async data dictionary reports correct dialect."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        assert session.data_dictionary.dialect == "sqlite"
