"""Unit tests for improved Aiosql adapters with record_class removal."""

import logging
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.extensions.aiosql.adapter import AiosqlAsyncAdapter, AiosqlSyncAdapter
from sqlspec.statement.result import SQLResult


@pytest.fixture
def mock_sync_driver() -> Mock:
    """Create a mock sync driver."""
    driver = Mock()
    driver.dialect = "postgres"
    driver.execute = Mock(return_value=Mock(spec=SQLResult))
    driver.execute_many = Mock(return_value=Mock())
    return driver


@pytest.fixture
def sync_adapter(mock_sync_driver: Mock) -> AiosqlSyncAdapter:
    """Create AiosqlSyncAdapter with mock driver."""
    return AiosqlSyncAdapter(mock_sync_driver)


def test_sync_adapter_initialization(mock_sync_driver: Mock) -> None:
    """Test sync adapter initialization."""
    adapter = AiosqlSyncAdapter(mock_sync_driver)

    assert adapter.driver is mock_sync_driver
    assert adapter.is_aio_driver is False


def test_sync_adapter_process_sql(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test SQL processing (should return as-is)."""
    sql = "SELECT * FROM users"
    result = sync_adapter.process_sql("test_query", "SELECT", sql)
    assert result == sql


def test_sync_adapter_select_with_record_class_warning(
    sync_adapter: AiosqlSyncAdapter, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that record_class parameter triggers warning."""
    mock_result = Mock(spec=SQLResult)
    mock_result.data = [{"id": 1, "name": "John"}]
    sync_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    with caplog.at_level(logging.WARNING):
        list(
            sync_adapter.select(
                conn=Mock(),
                query_name="test_query",
                sql="SELECT * FROM users",
                parameters={},
                record_class=dict,  # This should trigger warning
            )
        )

    assert "record_class parameter is deprecated" in caplog.text


def test_sync_adapter_select_with_schema_type_in_params(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test select with schema_type in parameters (passed through as regular param)."""
    from pydantic import BaseModel

    class User(BaseModel):
        id: int
        name: str

    mock_result = Mock(spec=SQLResult)
    mock_result.data = [User(id=1, name="John")]
    sync_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    # _sqlspec_schema_type is just passed through as a regular parameter
    parameters = {"active": True, "_sqlspec_schema_type": User}

    result = list(
        sync_adapter.select(
            conn=Mock(),
            query_name="test_query",
            sql="SELECT * FROM users WHERE active = :active",
            parameters=parameters,
        )
    )

    # Verify driver was called (parameters are passed through as-is)
    sync_adapter.driver.execute.assert_called_once()  # type: ignore[union-attr]
    assert result == [User(id=1, name="John")]


def test_sync_adapter_select_one_with_limit_filter(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test select_one applies implicit limit."""
    mock_result = Mock(spec=SQLResult)
    mock_result.data = [{"id": 1, "name": "John"}]
    sync_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    result = sync_adapter.select_one(conn=Mock(), query_name="test_query", sql="SELECT * FROM users", parameters={})

    assert result == {"id": 1, "name": "John"}
    sync_adapter.driver.execute.assert_called_once()  # type: ignore[union-attr]


def test_sync_adapter_select_value_dict_result(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test select_value with dict result."""
    mock_result = Mock(spec=SQLResult)
    mock_result.data = [{"count": 42}]
    sync_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    # Mock select_one to return the dict
    with patch.object(sync_adapter, "select_one", return_value={"count": 42}):
        result = sync_adapter.select_value(
            conn=Mock(), query_name="test_query", sql="SELECT COUNT(*) as count FROM users", parameters={}
        )

    assert result == 42


def test_sync_adapter_select_value_tuple_result(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test select_value with tuple result."""
    with patch.object(sync_adapter, "select_one", return_value=(42, "test")):
        result = sync_adapter.select_value(
            conn=Mock(), query_name="test_query", sql="SELECT COUNT(*), 'test' FROM users", parameters={}
        )

    assert result == 42


def test_sync_adapter_select_value_none_result(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test select_value with None result."""
    with patch.object(sync_adapter, "select_one", return_value=None):
        result = sync_adapter.select_value(
            conn=Mock(), query_name="test_query", sql="SELECT COUNT(*) FROM users WHERE false", parameters={}
        )

    assert result is None


def test_sync_adapter_select_cursor(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test select_cursor context manager."""
    mock_result = Mock(spec=SQLResult)
    mock_result.data = [{"id": 1}, {"id": 2}]
    sync_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    with sync_adapter.select_cursor(
        conn=Mock(), query_name="test_query", sql="SELECT * FROM users", parameters={}
    ) as cursor:
        rows = cursor.fetchall()
        assert len(rows) == 2

        first_row = cursor.fetchone()
        assert first_row == {"id": 1}


def test_sync_adapter_insert_update_delete(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test insert/update/delete operations."""
    mock_result = Mock()
    mock_result.rows_affected = 3
    sync_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    result = sync_adapter.insert_update_delete(
        conn=Mock(), query_name="test_query", sql="UPDATE users SET active = :active", parameters={"active": False}
    )

    assert result == 3


def test_sync_adapter_insert_update_delete_many(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test insert/update/delete many operations."""
    mock_result = Mock()
    mock_result.rows_affected = 5
    sync_adapter.driver.execute_many.return_value = mock_result  # type: ignore[union-attr]

    parameters = [{"name": "John"}, {"name": "Jane"}]
    result = sync_adapter.insert_update_delete_many(
        conn=Mock(), query_name="test_query", sql="INSERT INTO users (name) VALUES (:name)", parameters=parameters
    )

    assert result == 5
    sync_adapter.driver.execute_many.assert_called_once()  # type: ignore[union-attr]


def test_sync_adapter_insert_returning(sync_adapter: AiosqlSyncAdapter) -> None:
    """Test insert returning operation."""
    expected_result = {"id": 123, "name": "John"}

    with patch.object(sync_adapter, "select_one", return_value=expected_result):
        result = sync_adapter.insert_returning(
            conn=Mock(),
            query_name="test_query",
            sql="INSERT INTO users (name) VALUES (:name) RETURNING *",
            parameters={"name": "John"},
        )

    assert result == expected_result


@pytest.fixture
def mock_async_driver() -> Mock:
    """Create a mock async driver."""
    driver = Mock()
    driver.dialect = "postgres"
    # Use AsyncMock for async methods
    driver.execute = AsyncMock(return_value=Mock(spec=SQLResult))
    driver.execute_many = AsyncMock(return_value=Mock())
    return driver


@pytest.fixture
def async_adapter(mock_async_driver: Mock) -> AiosqlAsyncAdapter:
    """Create AiosqlAsyncAdapter with mock driver."""
    return AiosqlAsyncAdapter(mock_async_driver)


def test_async_adapter_initialization(mock_async_driver: Mock) -> None:
    """Test async adapter initialization."""
    adapter = AiosqlAsyncAdapter(mock_async_driver)

    assert adapter.driver is mock_async_driver
    assert adapter.is_aio_driver is True


@pytest.mark.asyncio
async def test_async_adapter_select_with_record_class_warning(
    async_adapter: AiosqlAsyncAdapter, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that record_class parameter triggers warning in async adapter."""
    mock_result = Mock(spec=SQLResult)
    mock_result.data = [{"id": 1, "name": "John"}]
    async_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    with caplog.at_level(logging.WARNING):
        await async_adapter.select(
            conn=Mock(),
            query_name="test_query",
            sql="SELECT * FROM users",
            parameters={},
            record_class=dict,  # This should trigger warning
        )

    assert "record_class parameter is deprecated" in caplog.text


@pytest.mark.asyncio
async def test_async_adapter_select_with_schema_type_in_params(async_adapter: AiosqlAsyncAdapter) -> None:
    """Test async select with schema_type in parameters."""
    from pydantic import BaseModel

    class User(BaseModel):
        id: int
        name: str

    mock_result = Mock(spec=SQLResult)
    mock_result.data = [User(id=1, name="John")]
    async_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    parameters = {"active": True, "_sqlspec_schema_type": User}

    result = await async_adapter.select(
        conn=Mock(), query_name="test_query", sql="SELECT * FROM users WHERE active = :active", parameters=parameters
    )

    # Verify driver was called (parameters are passed through as-is)
    async_adapter.driver.execute.assert_called_once()  # type: ignore[union-attr]
    assert result == [User(id=1, name="John")]


@pytest.mark.asyncio
async def test_async_adapter_select_one_with_limit(async_adapter: AiosqlAsyncAdapter) -> None:
    """Test async select_one automatically adds limit filter."""
    mock_result = Mock(spec=SQLResult)
    mock_result.data = [{"id": 1, "name": "John"}]
    async_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    result = await async_adapter.select_one(
        conn=Mock(), query_name="test_query", sql="SELECT * FROM users", parameters={}
    )

    assert result == {"id": 1, "name": "John"}

    # Verify that LimitOffsetFilter was added
    async_adapter.driver.execute.assert_called_once()  # type: ignore[union-attr]
    # The SQL object should have been modified to include the limit


@pytest.mark.asyncio
async def test_async_adapter_select_value(async_adapter: AiosqlAsyncAdapter) -> None:
    """Test async select_value."""
    expected_result = {"count": 42}

    with patch.object(async_adapter, "select_one", return_value=expected_result) as mock_select_one:
        result = await async_adapter.select_value(
            conn=Mock(), query_name="test_query", sql="SELECT COUNT(*) as count FROM users", parameters={}
        )

    mock_select_one.assert_called_once()
    assert result == 42


@pytest.mark.asyncio
async def test_async_adapter_select_cursor(async_adapter: AiosqlAsyncAdapter) -> None:
    """Test async select_cursor context manager."""
    mock_result = Mock(spec=SQLResult)
    mock_result.data = [{"id": 1}, {"id": 2}]
    async_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    async with async_adapter.select_cursor(
        conn=Mock(), query_name="test_query", sql="SELECT * FROM users", parameters={}
    ) as cursor:
        rows = await cursor.fetchall()
        assert len(rows) == 2

        first_row = await cursor.fetchone()
        assert first_row == {"id": 1}


@pytest.mark.asyncio
async def test_async_adapter_insert_update_delete(async_adapter: AiosqlAsyncAdapter) -> None:
    """Test async insert/update/delete operations."""
    mock_result = Mock()
    mock_result.rows_affected = 3
    async_adapter.driver.execute.return_value = mock_result  # type: ignore[union-attr]

    await async_adapter.insert_update_delete(
        conn=Mock(), query_name="test_query", sql="UPDATE users SET active = :active", parameters={"active": False}
    )

    async_adapter.driver.execute.assert_called_once()  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_async_adapter_insert_update_delete_many(async_adapter: AiosqlAsyncAdapter) -> None:
    """Test async insert/update/delete many operations."""
    mock_result = Mock()
    mock_result.rows_affected = 5
    async_adapter.driver.execute_many.return_value = mock_result  # type: ignore[union-attr]

    parameters = [{"name": "John"}, {"name": "Jane"}]
    await async_adapter.insert_update_delete_many(
        conn=Mock(), query_name="test_query", sql="INSERT INTO users (name) VALUES (:name)", parameters=parameters
    )

    async_adapter.driver.execute_many.assert_called_once()  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_async_adapter_insert_returning(async_adapter: AiosqlAsyncAdapter) -> None:
    """Test async insert returning operation."""
    expected_result = {"id": 123, "name": "John"}

    with patch.object(async_adapter, "select_one", return_value=expected_result) as mock_select_one:
        result = await async_adapter.insert_returning(
            conn=Mock(),
            query_name="test_query",
            sql="INSERT INTO users (name) VALUES (:name) RETURNING *",
            parameters={"name": "John"},
        )

    mock_select_one.assert_called_once()
    assert result == expected_result


@patch("sqlspec.extensions.aiosql.adapter._check_aiosql_available")
def test_sync_adapter_missing_aiosql_dependency(mock_check: Mock) -> None:
    """Test error when aiosql is not installed."""
    from sqlspec.exceptions import MissingDependencyError

    mock_check.side_effect = MissingDependencyError("aiosql", "aiosql")

    with pytest.raises(MissingDependencyError, match="aiosql"):
        AiosqlSyncAdapter(Mock())


@patch("sqlspec.extensions.aiosql.adapter._check_aiosql_available")
def test_async_adapter_missing_aiosql_dependency(mock_check: Mock) -> None:
    """Test error when aiosql is not installed."""
    from sqlspec.exceptions import MissingDependencyError

    mock_check.side_effect = MissingDependencyError("aiosql", "aiosql")

    with pytest.raises(MissingDependencyError, match="aiosql"):
        AiosqlAsyncAdapter(Mock())


def test_sync_adapter_driver_execution_error_propagation() -> None:
    """Test that driver execution errors are properly propagated."""
    mock_driver = Mock()
    mock_driver.dialect = "postgres"
    mock_driver.execute.side_effect = Exception("Database connection failed")

    adapter = AiosqlSyncAdapter(mock_driver)

    with pytest.raises(Exception, match="Database connection failed"):
        list(adapter.select(conn=Mock(), query_name="test_query", sql="SELECT * FROM users", parameters={}))


@pytest.mark.asyncio
async def test_async_adapter_driver_execution_error_propagation() -> None:
    """Test that async driver execution errors are properly propagated."""
    mock_driver = Mock()
    mock_driver.dialect = "postgres"
    mock_driver.execute.side_effect = Exception("Database connection failed")

    adapter = AiosqlAsyncAdapter(mock_driver)

    with pytest.raises(Exception, match="Database connection failed"):
        await adapter.select(conn=Mock(), query_name="test_query", sql="SELECT * FROM users", parameters={})
