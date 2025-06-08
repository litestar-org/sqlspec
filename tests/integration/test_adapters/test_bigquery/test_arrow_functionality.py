"""Test Arrow functionality for BigQuery drivers."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def bigquery_arrow_session() -> "Generator[BigQueryDriver, None, None]":
    """Create a BigQuery session for Arrow testing."""
    # Mock BigQuery client for testing
    mock_client = Mock()
    mock_job = Mock()
    mock_job.result.return_value = [
        Mock(values=lambda: (1, "Product A", 100, 19.99, True)),
        Mock(values=lambda: (2, "Product B", 200, 29.99, True)),
        Mock(values=lambda: (3, "Product C", 300, 39.99, False)),
        Mock(values=lambda: (4, "Product D", 400, 49.99, True)),
        Mock(values=lambda: (5, "Product E", 500, 59.99, False)),
    ]
    mock_client.query.return_value = mock_job

    config = BigQueryConfig(
        connection_config={
            "project": "test-project",
            "location": "US",
        },
        statement_config=SQLConfig(strict_mode=False),
    )

    # Replace the connection creation to use our mock
    config.create_connection = lambda: mock_client

    with config.provide_session() as session:
        yield session


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_fetch_arrow_table(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test fetch_arrow_table method with BigQuery."""
    result = bigquery_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 5
    assert result.data.num_columns >= 5  # id, name, value, price, is_active

    # Check column names
    expected_columns = {"id", "name", "value", "price", "is_active"}
    actual_columns = set(result.column_names())
    assert expected_columns.issubset(actual_columns)

    # Check values
    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_to_parquet(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test to_parquet export with BigQuery."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        bigquery_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE is_active = true",
            str(output_path),
        )

        assert output_path.exists()

        # Read back the parquet file
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products

        # Verify data
        names = table["name"].to_pylist()
        assert "Product A" in names
        assert "Product C" not in names  # Inactive product


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_arrow_with_parameters(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test fetch_arrow_table with parameters on BigQuery."""
    result = bigquery_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= @min_value AND value <= @max_value ORDER BY value",
        {"min_value": 200, "max_value": 400},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_arrow_empty_result(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test fetch_arrow_table with empty result on BigQuery."""
    result = bigquery_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value > @threshold",
        {"threshold": 1000},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 0
    assert result.data.num_columns >= 5  # Schema should still be present


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_arrow_data_types(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test Arrow data type mapping for BigQuery."""
    result = bigquery_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow LIMIT 1")

    assert isinstance(result, ArrowResult)

    # Check schema has expected columns
    schema = result.data.schema
    column_names = [field.name for field in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names
    assert "price" in column_names
    assert "is_active" in column_names

    # Verify BigQuery-specific type mappings
    assert pa.types.is_integer(result.data.schema.field("id").type)
    assert pa.types.is_string(result.data.schema.field("name").type)
    assert pa.types.is_boolean(result.data.schema.field("is_active").type)


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_to_arrow_with_sql_object(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow WHERE is_active = @active", parameters={"active": True})
    result = bigquery_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3
    assert result.data.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_arrow_with_bigquery_functions(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test Arrow functionality with BigQuery-specific functions."""
    result = bigquery_arrow_session.fetch_arrow_table(
        """
        SELECT
            name,
            value,
            price,
            CONCAT('Product: ', name) as formatted_name,
            ROUND(price * 1.1, 2) as price_with_tax,
            CURRENT_TIMESTAMP() as query_time
        FROM test_arrow
        WHERE value BETWEEN @min_val AND @max_val
        ORDER BY value
    """,
        {"min_val": 200, "max_val": 400},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3  # Products B, C, D
    assert "formatted_name" in result.column_names()
    assert "price_with_tax" in result.column_names()
    assert "query_time" in result.column_names()

    # Verify BigQuery function results
    formatted_names = result.data["formatted_name"].to_pylist()
    assert all(name.startswith("Product: ") for name in formatted_names)


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_arrow_with_arrays_and_structs(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test Arrow functionality with BigQuery arrays and structs."""
    result = bigquery_arrow_session.fetch_arrow_table(
        """
        SELECT
            name,
            value,
            [name, CAST(value AS STRING)] as name_value_array,
            STRUCT(name as product_name, value as product_value) as product_struct
        FROM test_arrow
        WHERE is_active = @active
        ORDER BY value
    """,
        {"active": True},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3  # Only active products
    assert "name_value_array" in result.column_names()
    assert "product_struct" in result.column_names()

    # Verify array and struct columns exist (exact validation depends on Arrow schema mapping)
    schema = result.data.schema
    assert any(field.name == "name_value_array" for field in schema)
    assert any(field.name == "product_struct" for field in schema)


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_arrow_with_window_functions(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test Arrow functionality with BigQuery window functions."""
    result = bigquery_arrow_session.fetch_arrow_table("""
        SELECT
            name,
            value,
            price,
            ROW_NUMBER() OVER (ORDER BY value DESC) as rank_by_value,
            LAG(value) OVER (ORDER BY id) as prev_value,
            SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
        FROM test_arrow
        ORDER BY id
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 5
    assert "rank_by_value" in result.column_names()
    assert "prev_value" in result.column_names()
    assert "running_total" in result.column_names()

    # Verify window function results
    ranks = result.data["rank_by_value"].to_pylist()
    assert len(set(ranks)) == 5  # All ranks should be unique

    running_totals = result.data["running_total"].to_pylist()
    # Running total should be monotonically increasing
    assert all(running_totals[i] <= running_totals[i + 1] for i in range(len(running_totals) - 1))


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_arrow_with_ml_functions(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test Arrow functionality with BigQuery ML functions."""
    # This test assumes a trained model exists
    result = bigquery_arrow_session.fetch_arrow_table("""
        SELECT
            name,
            value,
            price,
            value * price as feature_interaction,
            CASE
                WHEN value > 300 THEN 'high_value'
                ELSE 'low_value'
            END as value_category
        FROM test_arrow
        ORDER BY value
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 5
    assert "feature_interaction" in result.column_names()
    assert "value_category" in result.column_names()

    # Verify feature engineering
    categories = result.data["value_category"].to_pylist()
    assert "high_value" in categories
    assert "low_value" in categories


@pytest.mark.skip(reason="BigQuery tests require actual credentials or more complex mocking")
def test_bigquery_parquet_export_with_partitioning(bigquery_arrow_session: BigQueryDriver) -> None:
    """Test Parquet export with BigQuery partitioning patterns."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "partitioned_output.parquet"

        # Export with partitioning-style query
        bigquery_arrow_session.export_to_storage(
            """
            SELECT
                name,
                value,
                is_active,
                DATE(created_at) as partition_date
            FROM test_arrow
            WHERE is_active = @active
            """,
            str(output_path),
            {"active": True},
            compression="snappy",
        )

        assert output_path.exists()

        # Verify the partitioned data
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products
        assert "partition_date" in table.column_names

        # Check that partition_date column exists and has valid dates
        partition_dates = table["partition_date"].to_pylist()
        assert all(date is not None for date in partition_dates)
