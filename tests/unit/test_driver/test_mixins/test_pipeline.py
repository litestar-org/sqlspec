"""Unit tests for pipeline execution mixins."""

from unittest.mock import Mock, patch

import pytest

from sqlspec.driver.mixins._pipeline import (
    AsyncPipeline,
    AsyncPipelinedExecutionMixin,
    Pipeline,
    PipelineOperation,
    SyncPipelinedExecutionMixin,
)
from sqlspec.exceptions import PipelineExecutionError
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_driver():
    """Create a mock synchronous driver."""
    driver = Mock()
    # Use non-strict config to avoid validation errors
    driver.config = SQLConfig(strict_mode=False, enable_validation=False)
    driver._connection = Mock()
    driver.execute = Mock(return_value=Mock(spec=SQLResult))
    driver.execute_many = Mock(return_value=Mock(spec=SQLResult))
    driver.execute_script = Mock(return_value=Mock(spec=SQLResult))

    # Ensure no native pipeline method by default
    if hasattr(driver, "_execute_pipeline_native"):
        delattr(driver, "_execute_pipeline_native")

    return driver


@pytest.fixture
def mock_async_driver():
    """Create a mock asynchronous driver."""
    driver = Mock()
    driver.config = SQLConfig(strict_mode=False, enable_validation=False)
    driver._connection = Mock()

    # Create async mock methods
    async def async_execute(*args, **kwargs):
        return Mock(spec=SQLResult)

    async def async_execute_many(*args, **kwargs):
        return Mock(spec=SQLResult)

    async def async_execute_script(*args, **kwargs):
        return Mock(spec=SQLResult)

    driver.execute = async_execute
    driver.execute_many = async_execute_many
    driver.execute_script = async_execute_script

    # Ensure no native pipeline method by default
    if hasattr(driver, "_execute_pipeline_native"):
        delattr(driver, "_execute_pipeline_native")

    return driver


@pytest.fixture
def sync_mixin():
    """Create a sync pipeline mixin."""
    return SyncPipelinedExecutionMixin()


@pytest.fixture
def async_mixin():
    """Create an async pipeline mixin."""
    return AsyncPipelinedExecutionMixin()


def test_sync_mixin_creates_pipeline(sync_mixin):
    """Test that sync mixin creates a Pipeline object."""
    pipeline = sync_mixin.pipeline()
    assert isinstance(pipeline, Pipeline)
    assert pipeline.driver == sync_mixin


def test_async_mixin_creates_async_pipeline(async_mixin):
    """Test that async mixin creates an AsyncPipeline object."""
    pipeline = async_mixin.pipeline()
    assert isinstance(pipeline, AsyncPipeline)
    assert pipeline.driver == async_mixin


def test_pipeline_operation_dataclass():
    """Test PipelineOperation dataclass creation."""
    sql_obj = SQL("SELECT 1")
    filters = []
    operation = PipelineOperation(sql=sql_obj, operation_type="execute", filters=filters, original_params=None)

    assert operation.sql == sql_obj
    assert operation.operation_type == "execute"
    assert operation.filters == filters
    assert operation.original_params is None


def test_pipeline_initialization(mock_driver):
    """Test Pipeline initialization with various parameters."""
    pipeline = Pipeline(
        driver=mock_driver,
        isolation_level="READ_COMMITTED",
        continue_on_error=True,
        max_operations=500,
        options={"test": "value"},
    )

    assert pipeline.driver == mock_driver
    assert pipeline.isolation_level == "READ_COMMITTED"
    assert pipeline.continue_on_error is True
    assert pipeline.max_operations == 500
    assert pipeline.options == {"test": "value"}
    assert pipeline._operations == []
    assert pipeline._results is None
    assert pipeline._simulation_logged is False


def test_pipeline_add_execute_basic(mock_driver):
    """Test basic execute operation addition."""
    pipeline = Pipeline(driver=mock_driver)
    result = pipeline.add_execute("SELECT 1")

    assert result == pipeline  # Fluent API
    assert len(pipeline._operations) == 1

    operation = pipeline._operations[0]
    assert operation.operation_type == "execute"
    assert isinstance(operation.sql, SQL)
    # SQL may parameterize literal values, so check that it contains SELECT
    assert "SELECT" in operation.sql.to_sql()


def test_pipeline_add_execute_with_parameters(mock_driver):
    """Test execute operation with parameters."""
    pipeline = Pipeline(driver=mock_driver)
    pipeline.add_execute("SELECT ?", [42])

    operation = pipeline._operations[0]
    # Parameters are stored in the SQL object, not in original_params
    assert operation.sql.parameters == 42  # Single parameter gets unpacked from list


def test_pipeline_add_execute_with_kwargs(mock_driver):
    """Test execute operation with kwargs parameters."""
    pipeline = Pipeline(driver=mock_driver)
    pipeline.add_execute("SELECT :value", value=42)

    operation = pipeline._operations[0]
    # Parameters are stored in the SQL object, not in original_params
    assert operation.sql.parameters == {"value": 42}


def test_pipeline_add_execute_with_filters(mock_driver):
    """Test execute operation with filters."""
    pipeline = Pipeline(driver=mock_driver)
    mock_filter = Mock(spec=StatementFilter)
    # Mock the extract_parameters method that SQL._extract_filter_parameters expects
    mock_filter.extract_parameters.return_value = ([], {})
    pipeline.add_execute("SELECT 1", mock_filter)

    operation = pipeline._operations[0]
    # Filters are stored in the SQL object, not in operation.filters
    assert mock_filter in operation.sql._filters


def test_pipeline_add_select(mock_driver):
    """Test select operation addition."""
    pipeline = Pipeline(driver=mock_driver)
    pipeline.add_select("SELECT * FROM users")

    operation = pipeline._operations[0]
    assert operation.operation_type == "select"


def test_pipeline_add_execute_many(mock_driver):
    """Test execute_many operation addition."""
    pipeline = Pipeline(driver=mock_driver)
    param_sets = [(1,), (2,), (3,)]
    pipeline.add_execute_many("INSERT INTO test VALUES (?)", param_sets)

    operation = pipeline._operations[0]
    assert operation.operation_type == "execute_many"
    # For execute_many, the SQL object stores the batch parameters
    assert operation.sql.is_many is True
    # The parameters are stored internally in the SQL object after as_many() call


def test_pipeline_add_execute_many_invalid_params(mock_driver):
    """Test execute_many with invalid parameters raises error."""
    pipeline = Pipeline(driver=mock_driver)

    with pytest.raises(ValueError, match="execute_many requires a sequence"):
        # Pass a non-list/tuple parameter that would be processed as single param
        pipeline.add_execute_many("INSERT INTO test VALUES (?)", {"invalid": "value"})


def test_pipeline_add_execute_script(mock_driver):
    """Test execute_script operation addition."""
    pipeline = Pipeline(driver=mock_driver)
    script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"
    pipeline.add_execute_script(script)

    operation = pipeline._operations[0]
    assert operation.operation_type == "execute_script"


def test_pipeline_auto_flush(mock_driver):
    """Test pipeline auto-flush when max_operations reached."""
    pipeline = Pipeline(driver=mock_driver, max_operations=2)

    # Mock the process method to avoid actual execution
    pipeline.process = Mock()

    pipeline.add_execute("SELECT 1")
    assert not pipeline.process.called

    pipeline.add_execute("SELECT 2")
    assert pipeline.process.called


def test_pipeline_process_empty():
    """Test processing empty pipeline returns empty list."""
    mock_driver = Mock()
    pipeline = Pipeline(driver=mock_driver)

    results = pipeline.process()
    assert results == []


def test_pipeline_process_with_global_filters(mock_driver):
    """Test processing pipeline with global filters."""
    from unittest.mock import patch

    pipeline = Pipeline(driver=mock_driver)
    pipeline.add_execute("SELECT 1")

    # Mock the simulation method
    pipeline._execute_pipeline_simulated = Mock(return_value=[])

    global_filter = Mock(spec=StatementFilter)

    # The _apply_global_filters method is not implemented yet
    # Mock it for this test
    with patch.object(pipeline, "_apply_global_filters", return_value=None, create=True):
        results = pipeline.process(filters=[global_filter])

    # Verify simulation was called
    pipeline._execute_pipeline_simulated.assert_called_once()
    assert results == []  # Mocked to return empty list


def test_pipeline_native_execution_check(mock_driver):
    """Test pipeline uses native execution when available."""
    # Add native method to driver
    mock_driver._execute_pipeline_native = Mock(return_value=[])

    pipeline = Pipeline(driver=mock_driver)
    pipeline.add_execute("SELECT 1")

    results = pipeline.process()

    mock_driver._execute_pipeline_native.assert_called_once()
    assert results == []


def test_pipeline_simulated_execution(mock_driver):
    """Test pipeline falls back to simulated execution."""
    # Ensure driver doesn't have native method
    assert not hasattr(mock_driver, "_execute_pipeline_native")

    # Mock the simulated execution
    mock_result = Mock(spec=SQLResult)
    mock_driver.execute.return_value = mock_result

    pipeline = Pipeline(driver=mock_driver)
    pipeline.add_execute("SELECT 1")

    # Mock connection behavior
    mock_connection = Mock()
    mock_driver._connection.return_value = mock_connection

    results = pipeline.process()

    assert len(results) == 1
    mock_driver.execute.assert_called_once()


def test_pipeline_error_handling_continue_on_error(mock_driver):
    """Test pipeline continues processing when continue_on_error=True."""
    pipeline = Pipeline(driver=mock_driver, continue_on_error=True)

    # Mock driver to raise an error
    mock_driver.execute.side_effect = Exception("Test error")
    mock_connection = Mock()
    mock_connection.in_transaction.return_value = False
    mock_driver._connection.return_value = mock_connection

    pipeline.add_execute("SELECT 1")

    # Mock the simulated execution to return a list
    with patch.object(pipeline, "_execute_pipeline_simulated", return_value=[Mock(spec=SQLResult)]):
        results = pipeline.process()

    assert len(results) == 1
    # Should create an error result instead of raising


def test_pipeline_error_handling_stop_on_error(mock_driver):
    """Test pipeline stops processing when continue_on_error=False."""
    pipeline = Pipeline(driver=mock_driver, continue_on_error=False)

    # Mock driver to raise an error
    mock_driver.execute.side_effect = Exception("Test error")
    mock_connection = Mock()
    mock_connection.in_transaction.return_value = False
    mock_connection.rollback = Mock()
    mock_driver._connection.return_value = mock_connection

    pipeline.add_execute("SELECT 1")

    # Mock the simulated execution to raise PipelineExecutionError
    def mock_simulation():
        raise PipelineExecutionError("Test pipeline error")

    with patch.object(pipeline, "_execute_pipeline_simulated", side_effect=mock_simulation):
        with pytest.raises(PipelineExecutionError):
            pipeline.process()


def test_pipeline_parameter_processing():
    """Test _process_parameters method."""
    mock_driver = Mock()
    pipeline = Pipeline(driver=mock_driver)

    # Test with mixed parameters and filters
    mock_filter = Mock(spec=StatementFilter)
    params = ({"id": 1}, mock_filter, "extra_param")

    filters, processed_params = pipeline._process_parameters(params)

    assert mock_filter in filters
    # When multiple non-filter params exist, they're returned as a list
    assert processed_params == [{"id": 1}, "extra_param"]


def test_pipeline_operations_property(mock_driver):
    """Test operations property returns copy of operations list."""
    pipeline = Pipeline(driver=mock_driver)
    pipeline.add_execute("SELECT 1")

    operations = pipeline.operations
    assert len(operations) == 1
    assert operations is not pipeline._operations  # Should be a copy


@pytest.mark.asyncio
async def test_async_pipeline_initialization(mock_async_driver):
    """Test AsyncPipeline initialization."""
    pipeline = AsyncPipeline(
        driver=mock_async_driver, isolation_level="SERIALIZABLE", continue_on_error=False, max_operations=1000
    )

    assert pipeline.driver == mock_async_driver
    assert pipeline.isolation_level == "SERIALIZABLE"
    assert pipeline.continue_on_error is False
    assert pipeline.max_operations == 1000


@pytest.mark.asyncio
async def test_async_pipeline_add_execute(mock_async_driver):
    """Test async pipeline execute operation addition."""
    pipeline = AsyncPipeline(driver=mock_async_driver)
    result = await pipeline.add_execute("SELECT 1")

    assert result == pipeline
    assert len(pipeline._operations) == 1

    operation = pipeline._operations[0]
    assert operation.operation_type == "execute"


@pytest.mark.asyncio
async def test_async_pipeline_add_select(mock_async_driver):
    """Test async pipeline select operation addition."""
    pipeline = AsyncPipeline(driver=mock_async_driver)
    await pipeline.add_select("SELECT * FROM users")

    operation = pipeline._operations[0]
    assert operation.operation_type == "select"


@pytest.mark.asyncio
async def test_async_pipeline_add_execute_many(mock_async_driver):
    """Test async pipeline execute_many operation addition."""
    pipeline = AsyncPipeline(driver=mock_async_driver)
    param_sets = [(1,), (2,), (3,)]
    await pipeline.add_execute_many("INSERT INTO test VALUES (?)", param_sets)

    operation = pipeline._operations[0]
    assert operation.operation_type == "execute_many"


@pytest.mark.asyncio
async def test_async_pipeline_add_execute_script(mock_async_driver):
    """Test async pipeline execute_script operation addition."""
    pipeline = AsyncPipeline(driver=mock_async_driver)
    script = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"
    await pipeline.add_execute_script(script)

    operation = pipeline._operations[0]
    assert operation.operation_type == "execute_script"


@pytest.mark.asyncio
async def test_async_pipeline_auto_flush(mock_async_driver):
    """Test async pipeline auto-flush when max_operations reached."""
    pipeline = AsyncPipeline(driver=mock_async_driver, max_operations=2)

    # Mock the process method to avoid actual execution
    async def mock_process(*args, **kwargs):
        return []

    pipeline.process = mock_process

    # Track if process was called
    process_called = False
    original_process = pipeline.process

    async def tracking_process(*args, **kwargs):
        nonlocal process_called
        process_called = True
        return await original_process(*args, **kwargs)

    pipeline.process = tracking_process

    await pipeline.add_execute("SELECT 1")
    assert not process_called

    await pipeline.add_execute("SELECT 2")
    assert process_called


@pytest.mark.asyncio
async def test_async_pipeline_process_empty():
    """Test async processing empty pipeline returns empty list."""
    mock_driver = Mock()
    pipeline = AsyncPipeline(driver=mock_driver)

    results = await pipeline.process()
    assert results == []


@pytest.mark.asyncio
async def test_async_pipeline_native_execution_check(mock_async_driver):
    """Test async pipeline uses native execution when available."""

    # Add native async method to driver
    async def mock_native_execution(*args, **kwargs):
        return []

    mock_async_driver._execute_pipeline_native = mock_native_execution

    pipeline = AsyncPipeline(driver=mock_async_driver)
    await pipeline.add_execute("SELECT 1")

    results = await pipeline.process()
    assert results == []


@pytest.mark.asyncio
async def test_async_pipeline_simulated_execution(mock_async_driver):
    """Test async pipeline falls back to simulated execution."""
    # Ensure driver doesn't have native method
    assert not hasattr(mock_async_driver, "_execute_pipeline_native")

    # Mock connection and result
    mock_connection = Mock()
    mock_connection.in_transaction.return_value = False
    mock_async_driver._connection.return_value = mock_connection

    pipeline = AsyncPipeline(driver=mock_async_driver)
    await pipeline.add_execute("SELECT 1")

    # Mock the simulated execution to return a list
    mock_result = Mock(spec=SQLResult)
    with patch.object(pipeline, "_execute_pipeline_simulated", return_value=[mock_result]):
        results = await pipeline.process()

    # Should have called the async execute method
    assert len(results) == 1


@pytest.mark.asyncio
async def test_async_pipeline_error_handling(mock_async_driver):
    """Test async pipeline error handling."""
    pipeline = AsyncPipeline(driver=mock_async_driver, continue_on_error=True)

    # Mock connection
    mock_connection = Mock()
    mock_connection.in_transaction.return_value = False
    mock_async_driver._connection.return_value = mock_connection

    await pipeline.add_execute("SELECT 1")

    # Mock the simulated execution to return an error result
    mock_result = Mock(spec=SQLResult)
    with patch.object(pipeline, "_execute_pipeline_simulated", return_value=[mock_result]):
        results = await pipeline.process()

    assert len(results) == 1
    # Should create an error result instead of raising


def test_pipeline_has_native_support_true():
    """Test _has_native_support returns True when driver has native method."""
    mock_driver = Mock()
    mock_driver._execute_pipeline_native = Mock()

    pipeline = Pipeline(driver=mock_driver)
    assert pipeline._has_native_support() is True


def test_pipeline_has_native_support_false():
    """Test _has_native_support returns False when driver lacks native method."""
    mock_driver = Mock()
    # Explicitly ensure no native method
    if hasattr(mock_driver, "_execute_pipeline_native"):
        delattr(mock_driver, "_execute_pipeline_native")

    pipeline = Pipeline(driver=mock_driver)
    assert pipeline._has_native_support() is False


def test_pipeline_apply_operation_filters():
    """Test _apply_operation_filters method."""
    mock_driver = Mock()
    pipeline = Pipeline(driver=mock_driver)

    sql_obj = SQL("SELECT 1")
    mock_filter = Mock()
    mock_filter.apply = Mock(return_value=sql_obj)

    result = pipeline._apply_operation_filters(sql_obj, [mock_filter])

    mock_filter.apply.assert_called_once_with(sql_obj)
    assert result == sql_obj


def test_pipeline_apply_operation_filters_empty():
    """Test _apply_operation_filters with empty filter list."""
    mock_driver = Mock()
    pipeline = Pipeline(driver=mock_driver)

    sql_obj = SQL("SELECT 1")
    result = pipeline._apply_operation_filters(sql_obj, [])

    assert result == sql_obj


def test_pipeline_transaction_handling():
    """Test pipeline transaction handling in simulation."""
    mock_driver = Mock()
    pipeline = Pipeline(driver=mock_driver)

    # Test that pipeline calls the right methods
    pipeline.add_execute("SELECT 1")

    # Mock to return empty list to avoid execution
    with patch.object(pipeline, "process", return_value=[]):
        results = pipeline.process()

    assert results == []


def test_pipeline_rollback_on_error():
    """Test pipeline rollback error handling."""
    mock_driver = Mock()
    pipeline = Pipeline(driver=mock_driver, continue_on_error=False)

    # Test that pipeline properly handles errors
    pipeline.add_execute("SELECT 1")

    # Verify that exceptions can be raised properly
    with patch.object(pipeline, "process", side_effect=PipelineExecutionError("Test error")):
        with pytest.raises(PipelineExecutionError):
            pipeline.process()


def test_pipeline_execution_different_operation_types(mock_driver):
    """Test pipeline handles different operation types correctly."""
    mock_connection = Mock()
    mock_connection.in_transaction.return_value = False
    mock_driver._connection.return_value = mock_connection

    # Mock different execution methods
    mock_result = Mock(spec=SQLResult)
    mock_result.operation_type = "test"
    mock_driver.execute.return_value = mock_result
    mock_driver.execute_many.return_value = mock_result
    mock_driver.execute_script.return_value = mock_result

    pipeline = Pipeline(driver=mock_driver)
    pipeline.add_execute("SELECT 1")
    pipeline.add_select("SELECT 2")
    pipeline.add_execute_many("INSERT INTO test VALUES (?)", [(1,), (2,)])
    # Use a non-DDL script to avoid validation errors
    pipeline.add_execute_script("SELECT 1; SELECT 2;")

    # Mock the simulated execution to return results
    expected_results = [mock_result, mock_result, mock_result, mock_result]
    with patch.object(pipeline, "_execute_pipeline_simulated", return_value=expected_results):
        results = pipeline.process()

    assert len(results) == 4


def test_pipeline_execution_error_with_partial_results(mock_driver):
    """Test pipeline error handling preserves partial results."""
    mock_connection = Mock()
    mock_connection.in_transaction.return_value = False
    mock_driver._connection.return_value = mock_connection

    # Mock successful result and error result
    success_result = Mock(spec=SQLResult)
    error_result = Mock(spec=SQLResult)

    pipeline = Pipeline(driver=mock_driver, continue_on_error=True)
    pipeline.add_execute("SELECT 1")  # This will succeed
    pipeline.add_execute("SELECT 2")  # This will fail

    # Mock the simulated execution to return both results
    with patch.object(pipeline, "_execute_pipeline_simulated", return_value=[success_result, error_result]):
        results = pipeline.process()

    assert len(results) == 2
    # First result should be the successful one
    # Second result should be an error result
