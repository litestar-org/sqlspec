"""Test Arrow functionality for BigQuery drivers."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def bigquery_arrow_session(bigquery_service: BigQueryService) -> "Generator[BigQueryDriver, None, None]":
    """Create a BigQuery session for Arrow testing using real BigQuery service."""
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials

    config = BigQueryConfig(
        project=bigquery_service.project,
        dataset_id=bigquery_service.dataset,
        client_options=ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
        credentials=AnonymousCredentials(),  # type: ignore[no-untyped-call]
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test dataset and table

        # First drop the table if it exists
        try:
            session.execute_script(
                f"DROP TABLE IF EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
            )
        except Exception:
            pass  # Ignore errors if table doesn't exist

        session.execute_script(f"""
            CREATE TABLE `{bigquery_service.project}.{bigquery_service.dataset}.test_arrow` (
                id INT64,
                name STRING,
                value INT64,
                price FLOAT64,
                is_active BOOL
            )
        """)

        session.execute_script(f"""
            INSERT INTO `{bigquery_service.project}.{bigquery_service.dataset}.test_arrow` (id, name, value, price, is_active) VALUES
                (1, 'Product A', 100, 19.99, true),
                (2, 'Product B', 200, 29.99, true),
                (3, 'Product C', 300, 39.99, false),
                (4, 'Product D', 400, 49.99, true),
                (5, 'Product E', 500, 59.99, false)
        """)

        yield session


@pytest.mark.xdist_group("bigquery")
def test_bigquery_fetch_arrow_table(bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test fetch_arrow_table method with BigQuery."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"

    result = bigquery_arrow_session.fetch_arrow_table(f"SELECT * FROM {table_name} ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert result.data.num_columns >= 5  # id, name, value, price, is_active

    # Check column names
    expected_columns = {"id", "name", "value", "price", "is_active"}
    actual_columns = set(result.column_names)
    assert expected_columns.issubset(actual_columns)

    # Check values
    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


@pytest.mark.xdist_group("bigquery")
def test_bigquery_to_parquet(bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test to_parquet export with BigQuery."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
        bigquery_arrow_session.export_to_storage(
            f"SELECT * FROM {table_name} WHERE is_active = true", destination_uri=str(output_path)
        )

        assert output_path.exists()

        # Read back the parquet file
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products

        # Verify data
        names = table["name"].to_pylist()
        assert "Product A" in names
        assert "Product C" not in names  # Inactive product


@pytest.mark.xdist_group("bigquery")
def test_bigquery_arrow_with_parameters(
    bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test fetch_arrow_table with parameters on BigQuery."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
    result = bigquery_arrow_session.fetch_arrow_table(
        f"SELECT * FROM {table_name} WHERE value >= @min_value AND value <= @max_value ORDER BY value",
        {"min_value": 200, "max_value": 400},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.xdist_group("bigquery")
def test_bigquery_arrow_empty_result(bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test fetch_arrow_table with empty result on BigQuery."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
    result = bigquery_arrow_session.fetch_arrow_table(
        f"SELECT * FROM {table_name} WHERE value > @threshold", {"threshold": 1000}
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 0
    assert result.data.num_columns >= 5  # Schema should still be present


@pytest.mark.xdist_group("bigquery")
def test_bigquery_arrow_data_types(bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test Arrow data type mapping for BigQuery."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
    result = bigquery_arrow_session.fetch_arrow_table(f"SELECT * FROM {table_name} LIMIT 1")

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


@pytest.mark.xdist_group("bigquery")
def test_bigquery_to_arrow_with_sql_object(
    bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL, SQLConfig

    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
    sql_obj = SQL(
        f"SELECT name, value FROM {table_name} WHERE is_active = @active",
        parameters={"active": True},
        _dialect="bigquery",
        _config=SQLConfig(strict_mode=False),
    )
    result = bigquery_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert result.data.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


@pytest.mark.xdist_group("bigquery")
def test_bigquery_arrow_with_bigquery_functions(
    bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test Arrow functionality with BigQuery-specific functions."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
    result = bigquery_arrow_session.fetch_arrow_table(
        f"""
        SELECT
            name,
            value,
            price,
            CONCAT('Product: ', name) as formatted_name,
            ROUND(price * 1.1, 2) as price_with_tax,
            'processed' as status
        FROM {table_name}
        WHERE value BETWEEN @min_val AND @max_val
        ORDER BY value
    """,
        {"min_val": 200, "max_val": 400},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3  # Products B, C, D
    assert "formatted_name" in result.column_names
    assert "price_with_tax" in result.column_names
    assert "status" in result.column_names

    # Verify BigQuery function results
    formatted_names = result.data["formatted_name"].to_pylist()
    assert all(name.startswith("Product: ") for name in formatted_names if name is not None)


@pytest.mark.xdist_group("bigquery")
def test_bigquery_arrow_with_arrays_and_structs(
    bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test Arrow functionality with BigQuery arrays and structs."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
    result = bigquery_arrow_session.fetch_arrow_table(
        f"""
        SELECT
            name,
            value,
            [name, CAST(value AS STRING)] as name_value_array,
            STRUCT(name as product_name, value as product_value) as product_struct
        FROM {table_name}
        WHERE is_active = @active
        ORDER BY value
    """,
        {"active": True},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3  # Only active products
    assert "name_value_array" in result.column_names
    assert "product_struct" in result.column_names

    # Verify array and struct columns exist (exact validation depends on Arrow schema mapping)
    schema = result.data.schema
    assert any(field.name == "name_value_array" for field in schema)
    assert any(field.name == "product_struct" for field in schema)


@pytest.mark.xdist_group("bigquery")
def test_bigquery_arrow_with_window_functions(
    bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test Arrow functionality with BigQuery window functions."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
    result = bigquery_arrow_session.fetch_arrow_table(f"""
        SELECT
            name,
            value,
            price,
            ROW_NUMBER() OVER (ORDER BY value DESC) as rank_by_value,
            LAG(value) OVER (ORDER BY id) as prev_value,
            SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
        FROM {table_name}
        ORDER BY id
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert "rank_by_value" in result.column_names
    assert "prev_value" in result.column_names
    assert "running_total" in result.column_names

    # Verify window function results
    ranks = result.data["rank_by_value"].to_pylist()
    assert len(set(ranks)) == 5  # All ranks should be unique

    running_totals = result.data["running_total"].to_pylist()
    # Running total should be monotonically increasing
    assert running_totals is not None
    assert all(running_totals[i] <= running_totals[i + 1] for i in range(len(running_totals) - 1))  # type: ignore[operator]


@pytest.mark.xdist_group("bigquery")
@pytest.mark.skip("BigQuery emulator has issues with parameter binding for computed columns")
def test_bigquery_arrow_with_ml_functions(
    bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test Arrow functionality with BigQuery feature engineering."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
    result = bigquery_arrow_session.fetch_arrow_table(f"""
        SELECT
            name,
            value,
            price,
            value * price as feature_interaction,
            'computed' as process_status
        FROM {table_name}
        ORDER BY value
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert "feature_interaction" in result.column_names
    assert "process_status" in result.column_names

    # Verify feature engineering
    interactions = result.data["feature_interaction"].to_pylist()
    assert all(
        interaction is not None and interaction > 0 for interaction in interactions
    )  # All should be positive numbers


@pytest.mark.xdist_group("bigquery")
def test_bigquery_parquet_export_with_partitioning(
    bigquery_arrow_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test Parquet export with BigQuery partitioning patterns."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "partitioned_output.parquet"

        table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_arrow`"
        # Export with partitioning-style query
        from sqlspec.statement.sql import SQL, SQLConfig

        query = SQL(
            f"""
            SELECT
                name,
                value,
                is_active,
                DATE('2024-01-01') as partition_date
            FROM {table_name}
            WHERE is_active = @active
            """,
            parameters={"active": True},
            _dialect="bigquery",
            _config=SQLConfig(strict_mode=False),
        )

        bigquery_arrow_session.export_to_storage(
            query, destination_uri=str(output_path), format="parquet", compression="snappy"
        )

        assert output_path.exists()

        # Verify the partitioned data
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products
        assert "partition_date" in table.column_names

        # Check that partition_date column exists and has valid dates
        partition_dates = table["partition_date"].to_pylist()
        assert all(date is not None for date in partition_dates)
