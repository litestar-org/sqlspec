"""Integration tests for BigQuery driver implementation with CORE_ROUND_3 architecture."""

import operator
from collections.abc import Generator
from typing import Literal

import pytest
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver
from sqlspec.core.result import SQLResult

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
def bigquery_session(bigquery_service: BigQueryService) -> Generator[BigQueryDriver, None, None]:
    """Create a BigQuery session with test table."""
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials

    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )

    with config.provide_session() as session:
        # Create test table (BigQuery emulator doesn't support DEFAULT values)
        session.execute_script(f"""
            CREATE OR REPLACE TABLE `{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table` (
                id INT64,
                name STRING NOT NULL,
                value INT64,
                created_at TIMESTAMP
            )
        """)
        yield session
        # Cleanup
        session.execute_script(
            f"DROP TABLE IF EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"
        )


@pytest.mark.xdist_group("bigquery")
def test_bigquery_basic_crud(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test basic CRUD operations."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    # INSERT
    insert_result = bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", (1, "test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (1, 0)  # The emulator may not correctly report this.

    # SELECT
    select_result = bigquery_session.execute(f"SELECT name, value FROM {table_name} WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = bigquery_session.execute(f"UPDATE {table_name} SET value = ? WHERE name = ?", (100, "test_name"))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected in (1, 0)  # The emulator may not correctly report this.

    # Verify UPDATE
    verify_result = bigquery_session.execute(f"SELECT value FROM {table_name} WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = bigquery_session.execute(f"DELETE FROM {table_name} WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected in (1, 0)  # The emulator may not correctly report this.

    # Verify DELETE
    empty_result = bigquery_session.execute(f"SELECT COUNT(*) as count FROM {table_name}")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.xdist_group("bigquery")
def test_bigquery_parameter_styles(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test BigQuery named parameter binding (only supported style)."""
    # Use fully qualified table name like main branch
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    # Insert test data
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name) VALUES (@id, @name)", {"id": 1, "name": "test_value"}
    )

    # Test named parameter style (BigQuery's native parameter style)
    sql = f"SELECT name FROM {table_name} WHERE name = @name"
    parameters = {"name": "test_value"}

    result = bigquery_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("bigquery")
def test_bigquery_execute_many(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test execute_many functionality."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"
    parameters_list = [(1, "name1", 1), (2, "name2", 2), (3, "name3", 3)]

    result = bigquery_session.execute_many(
        f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", parameters_list
    )
    assert isinstance(result, SQLResult)
    # BigQuery emulator doesn't report correct affected row counts for multi-statement scripts
    # Accept 0 as valid since the data insertion is verified below
    assert result.rows_affected >= 0

    # Verify all records were inserted
    select_result = bigquery_session.execute(f"SELECT COUNT(*) as count FROM {table_name}")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)

    # Verify data integrity
    ordered_result = bigquery_session.execute(f"SELECT name, value FROM {table_name} ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("bigquery")
def test_bigquery_execute_script(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test execute_script functionality."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"
    script = f"""
        INSERT INTO {table_name} (id, name, value) VALUES (1, 'script_test1', 999);
        INSERT INTO {table_name} (id, name, value) VALUES (2, 'script_test2', 888);
        UPDATE {table_name} SET value = 1000 WHERE name = 'script_test1';
    """

    result = bigquery_session.execute_script(script)
    # Script execution returns SQLResult object
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    # Verify script effects
    select_result = bigquery_session.execute(
        f"SELECT name, value FROM {table_name} WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


@pytest.mark.xdist_group("bigquery")
def test_bigquery_result_methods(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test SQLResult methods."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    # Insert test data
    bigquery_session.execute_many(
        f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)",
        [(1, "result1", 10), (2, "result2", 20), (3, "result3", 30)],
    )

    # Test SQLResult methods
    result = bigquery_session.execute(f"SELECT * FROM {table_name} ORDER BY name")
    assert isinstance(result, SQLResult)

    # Test get_first()
    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    # Test get_count()
    assert result.get_count() == 3

    # Test is_empty()
    assert not result.is_empty()

    # Test empty result
    empty_result = bigquery_session.execute(f"SELECT * FROM {table_name} WHERE name = ?", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("bigquery")
def test_bigquery_error_handling(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test error handling and exception propagation."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    # Test invalid SQL
    with pytest.raises(Exception):  # google.cloud.exceptions.BadRequest
        bigquery_session.execute("INVALID SQL STATEMENT")

    # Test constraint violation
    bigquery_session.execute(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", (1, "unique_test", 1))

    # Try to insert with invalid column reference
    with pytest.raises(Exception):  # google.cloud.exceptions.BadRequest
        bigquery_session.execute(f"SELECT nonexistent_column FROM {table_name}")


@pytest.mark.xdist_group("bigquery")
@pytest.mark.xfail(
    reason="BigQuery emulator has issues with complex data types and parameter marshaling (JSON unmarshaling errors)"
)
def test_bigquery_data_types(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test BigQuery data type handling."""
    # Create table with various BigQuery data types
    bigquery_session.execute_script(f"""
        CREATE TABLE IF NOT EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.data_types_test` (
            id INT64,
            string_col STRING,
            int_col INT64,
            float_col FLOAT64,
            bool_col BOOL,
            date_col DATE,
            datetime_col DATETIME,
            timestamp_col TIMESTAMP,
            array_col ARRAY<INT64>,
            json_col JSON
        )
    """)

    # Insert data with various types
    bigquery_session.execute(
        f"""
        INSERT INTO `{bigquery_service.project}.{bigquery_service.dataset}.data_types_test` (
            id, string_col, int_col, float_col, bool_col,
            date_col, datetime_col, timestamp_col, array_col, json_col
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """,
        (
            1,
            "string_value",
            42,
            123.45,
            True,
            "2024-01-15",
            "2024-01-15 10:30:00",
            "2024-01-15 10:30:00 UTC",
            [1, 2, 3],
            {"name": "test", "value": 42},
        ),
    )

    # Retrieve and verify data
    select_result = bigquery_session.execute(f"""
        SELECT string_col, int_col, float_col, bool_col
        FROM `{bigquery_service.project}.{bigquery_service.dataset}.data_types_test`
    """)
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["string_col"] == "string_value"
    assert row["int_col"] == 42
    assert row["float_col"] == 123.45
    assert row["bool_col"] is True

    # Clean up
    bigquery_session.execute_script(
        f"DROP TABLE `{bigquery_service.project}.{bigquery_service.dataset}.data_types_test`"
    )


@pytest.mark.xdist_group("bigquery")
def test_bigquery_complex_queries(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test complex SQL queries."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    # Insert test data
    test_data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35), (4, "Diana", 28)]

    bigquery_session.execute_many(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", test_data)

    # Test JOIN (self-join)
    join_result = bigquery_session.execute(f"""
        SELECT t1.name as name1, t2.name as name2, t1.value as value1, t2.value as value2
        FROM {table_name} t1
        CROSS JOIN {table_name} t2
        WHERE t1.value < t2.value
        ORDER BY t1.name, t2.name
        LIMIT 3
    """)
    assert isinstance(join_result, SQLResult)
    assert join_result.data is not None
    assert len(join_result.data) == 3

    # Test aggregation
    agg_result = bigquery_session.execute(f"""
        SELECT
            COUNT(*) as total_count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM {table_name}
    """)
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["total_count"] == 4
    assert agg_result.data[0]["avg_value"] == 29.5
    assert agg_result.data[0]["min_value"] == 25
    assert agg_result.data[0]["max_value"] == 35

    # Test subquery
    subquery_result = bigquery_session.execute(f"""
        SELECT name, value
        FROM {table_name}
        WHERE value > (SELECT AVG(value) FROM {table_name})
        ORDER BY value
    """)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2  # Bob and Charlie
    assert subquery_result.data[0]["name"] == "Bob"
    assert subquery_result.data[1]["name"] == "Charlie"


@pytest.mark.xdist_group("bigquery")
def test_bigquery_schema_operations(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test schema operations (DDL)."""
    # Create a new table
    bigquery_session.execute_script(f"""
        CREATE TABLE IF NOT EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.schema_test` (
            id INT64,
            description STRING NOT NULL,
            created_at TIMESTAMP
        )
    """)

    # Insert data into new table
    insert_result = bigquery_session.execute(
        f"INSERT INTO `{bigquery_service.project}.{bigquery_service.dataset}.schema_test` (id, description, created_at) VALUES (?, ?, ?)",
        (1, "test description", "2024-01-15 10:30:00 UTC"),
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (1, 0)

    # Skip INFORMATION_SCHEMA verification - not supported by BigQuery emulator
    # In production BigQuery, you would use INFORMATION_SCHEMA.COLUMNS to verify table structure

    # Drop table
    bigquery_session.execute_script(f"DROP TABLE `{bigquery_service.project}.{bigquery_service.dataset}.schema_test`")


@pytest.mark.xdist_group("bigquery")
def test_bigquery_column_names_and_metadata(
    bigquery_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test column names and result metadata."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    # Insert test data
    bigquery_session.execute(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", (1, "metadata_test", 123))

    # Test column names
    result = bigquery_session.execute(
        f"SELECT id, name, value, created_at FROM {table_name} WHERE name = ?", ("metadata_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result.data) == 1

    # Test that we can access data by column name
    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    # created_at will be NULL since we didn't provide a value and BigQuery emulator doesn't support DEFAULT
    assert "created_at" in row


@pytest.mark.xdist_group("bigquery")
def test_bigquery_performance_bulk_operations(
    bigquery_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test performance with bulk operations."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    # Generate bulk data
    bulk_data = [(i, f"bulk_user_{i}", i * 10) for i in range(1, 101)]

    # Bulk insert
    result = bigquery_session.execute_many(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected in (100, 0)  # The emulator may not correctly report this.

    # Bulk select
    select_result = bigquery_session.execute(
        f"SELECT COUNT(*) as count FROM {table_name} WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    # Test pagination-like query
    page_result = bigquery_session.execute(f"""
        SELECT name, value FROM {table_name}
        WHERE name LIKE 'bulk_user_%'
        ORDER BY value
        LIMIT 10 OFFSET 20
    """)
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_21"


@pytest.mark.xdist_group("bigquery")
def test_bigquery_specific_features(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test BigQuery-specific features."""
    # Test BigQuery built-in functions (skip CURRENT_TIMESTAMP due to emulator issue)
    functions_result = bigquery_session.execute("""
        SELECT
            GENERATE_UUID() as uuid_val,
            FARM_FINGERPRINT('test') as fingerprint
    """)
    assert isinstance(functions_result, SQLResult)
    assert functions_result.data is not None
    assert functions_result.data[0]["uuid_val"] is not None
    assert functions_result.data[0]["fingerprint"] is not None

    # Test array operations
    array_result = bigquery_session.execute("""
        SELECT
            ARRAY[1, 2, 3, 4, 5] as numbers,
            ARRAY_LENGTH(ARRAY[1, 2, 3, 4, 5]) as array_len
    """)
    assert isinstance(array_result, SQLResult)
    assert array_result.data is not None
    assert array_result.data[0]["numbers"] == [1, 2, 3, 4, 5]
    assert array_result.data[0]["array_len"] == 5

    # Test STRUCT operations
    struct_result = bigquery_session.execute("""
        SELECT
            STRUCT('Alice' as name, 25 as age) as person,
            STRUCT('Alice' as name, 25 as age).name as person_name
    """)
    assert isinstance(struct_result, SQLResult)
    assert struct_result.data is not None
    assert struct_result.data[0]["person"]["name"] == "Alice"
    assert struct_result.data[0]["person"]["age"] == 25
    assert struct_result.data[0]["person_name"] == "Alice"


@pytest.mark.xdist_group("bigquery")
def test_bigquery_analytical_functions(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test BigQuery analytical and window functions."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    # Insert test data for analytics
    analytics_data = [
        (1, "Product A", 1000),
        (2, "Product B", 1500),
        (3, "Product A", 1200),
        (4, "Product C", 800),
        (5, "Product B", 1800),
    ]

    bigquery_session.execute_many(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", analytics_data)

    # Test window functions
    window_result = bigquery_session.execute(f"""
        SELECT
            name,
            value,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY value DESC) as row_num,
            RANK() OVER (PARTITION BY name ORDER BY value DESC) as rank_val,
            SUM(value) OVER (PARTITION BY name) as total_by_product,
            LAG(value) OVER (ORDER BY id) as previous_value
        FROM {table_name}
        ORDER BY id
    """)
    assert isinstance(window_result, SQLResult)
    assert window_result.data is not None
    assert len(window_result.data) == 5

    # Verify window function results
    product_a_rows = [row for row in window_result.data if row["name"] == "Product A"]
    assert len(product_a_rows) == 2
    # Highest value should have row_num = 1
    highest_a = max(product_a_rows, key=operator.itemgetter("value"))
    assert highest_a["row_num"] == 1
