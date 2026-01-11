"""Unit tests for mock configuration classes."""

import pytest

from sqlspec.adapters.mock import MockAsyncConfig, MockSyncConfig


def test_mock_sync_config_defaults() -> None:
    """Test MockSyncConfig default values."""
    config = MockSyncConfig()

    assert config.target_dialect == "sqlite"
    assert config.initial_sql is None
    assert config.is_async is False
    assert config.supports_transactional_ddl is True


def test_mock_sync_config_with_target_dialect() -> None:
    """Test MockSyncConfig with custom target dialect."""
    config = MockSyncConfig(target_dialect="postgres")

    assert config.target_dialect == "postgres"


def test_mock_sync_config_with_initial_sql_string() -> None:
    """Test MockSyncConfig with initial SQL as string."""
    config = MockSyncConfig(initial_sql="CREATE TABLE test (id INTEGER)")

    assert config.initial_sql == "CREATE TABLE test (id INTEGER)"


def test_mock_sync_config_with_initial_sql_list() -> None:
    """Test MockSyncConfig with initial SQL as list."""
    sql_list = ["CREATE TABLE test1 (id INTEGER)", "CREATE TABLE test2 (id INTEGER)"]
    config = MockSyncConfig(initial_sql=sql_list)

    assert config.initial_sql == sql_list


def test_mock_sync_config_create_connection() -> None:
    """Test that create_connection returns a valid connection."""
    config = MockSyncConfig()
    conn = config.create_connection()

    assert conn is not None
    conn.close()


def test_mock_sync_config_provide_connection_context() -> None:
    """Test provide_connection context manager."""
    config = MockSyncConfig()

    with config.provide_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1


def test_mock_sync_config_provide_session_context() -> None:
    """Test provide_session context manager."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        result = session.select_value("SELECT 42")
        assert result == 42


def test_mock_async_config_defaults() -> None:
    """Test MockAsyncConfig default values."""
    config = MockAsyncConfig()

    assert config.target_dialect == "sqlite"
    assert config.initial_sql is None
    assert config.is_async is True
    assert config.supports_transactional_ddl is True


def test_mock_async_config_with_target_dialect() -> None:
    """Test MockAsyncConfig with custom target dialect."""
    config = MockAsyncConfig(target_dialect="mysql")

    assert config.target_dialect == "mysql"


@pytest.mark.anyio
async def test_mock_async_config_create_connection() -> None:
    """Test that async create_connection returns a valid connection."""
    config = MockAsyncConfig()
    conn = await config.create_connection()

    assert conn is not None
    conn.close()


@pytest.mark.anyio
async def test_mock_async_config_provide_connection_context() -> None:
    """Test async provide_connection context manager."""
    config = MockAsyncConfig()

    async with config.provide_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1


@pytest.mark.anyio
async def test_mock_async_config_provide_session_context() -> None:
    """Test async provide_session context manager."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        result = await session.select_value("SELECT 42")
        assert result == 42


def test_mock_config_with_driver_features() -> None:
    """Test MockSyncConfig with custom driver features."""
    from sqlspec.utils.serializers import from_json, to_json

    config = MockSyncConfig(driver_features={"json_serializer": to_json, "json_deserializer": from_json})

    assert config.driver_features.get("json_serializer") is to_json
    assert config.driver_features.get("json_deserializer") is from_json


def test_mock_config_with_bind_key() -> None:
    """Test MockSyncConfig with bind_key."""
    config = MockSyncConfig(bind_key="test_db")

    assert config.bind_key == "test_db"


def test_mock_config_supports_arrow() -> None:
    """Test that mock config reports Arrow support."""
    config = MockSyncConfig()

    assert config.supports_native_arrow_export is True
    assert config.supports_native_arrow_import is True
    assert config.supports_native_parquet_export is True
    assert config.supports_native_parquet_import is True


def test_mock_async_config_supports_arrow() -> None:
    """Test that async mock config reports Arrow support."""
    config = MockAsyncConfig()

    assert config.supports_native_arrow_export is True
    assert config.supports_native_arrow_import is True
    assert config.supports_native_parquet_export is True
    assert config.supports_native_parquet_import is True
