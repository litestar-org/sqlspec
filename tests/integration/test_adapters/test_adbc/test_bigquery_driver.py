"""Integration tests for ADBC BigQuery driver implementation."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import SQLResult

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_bigquery_session(bigquery_service: BigQueryService) -> Generator[AdbcDriver, None]:
    """Create an ADBC BigQuery session using emulator."""
    # ADBC BigQuery driver doesn't support emulator configuration
    # Skip this fixture as ADBC BigQuery requires real GCP credentials and service
    pytest.skip("ADBC BigQuery driver requires real GCP service, not compatible with emulator")


@pytest.mark.xdist_group("bigquery")
@xfail_if_driver_missing
def test_bigquery_connection(adbc_bigquery_session: AdbcDriver) -> None:
    """Test basic ADBC BigQuery connection using emulator."""
    assert adbc_bigquery_session is not None
    assert isinstance(adbc_bigquery_session, AdbcDriver)

    # Test basic query
    result = adbc_bigquery_session.execute("SELECT 1 as test_value")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["test_value"] == 1


@pytest.mark.xdist_group("bigquery")
@xfail_if_driver_missing
def test_bigquery_create_table(adbc_bigquery_session: AdbcDriver, bigquery_service: BigQueryService) -> None:
    """Test creating a table with BigQuery ADBC."""
    # Create a test table using full table path
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_table`"

    adbc_bigquery_session.execute_script(f"""
        CREATE OR REPLACE TABLE {table_name} (
            id INT64,
            name STRING,
            value FLOAT64
        )
    """)

    # Insert test data
    adbc_bigquery_session.execute(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", (1, "test", 123.45))

    # Query the data back
    result = adbc_bigquery_session.execute(f"SELECT * FROM {table_name}")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["id"] == 1
    assert result.data[0]["name"] == "test"
    assert result.data[0]["value"] == 123.45


@pytest.mark.skipif(
    "not config.getoption('--run-bigquery-tests', default=False)",
    reason="BigQuery ADBC tests require --run-bigquery-tests flag and valid GCP credentials",
)
@pytest.mark.xdist_group("adbc_bigquery")
@xfail_if_driver_missing
def test_basic_operations() -> None:
    """Test basic BigQuery ADBC operations (requires valid GCP setup)."""
    # Note: This test would require actual BigQuery project setup
    # For now, we'll create a placeholder that demonstrates the expected structure

    # This would typically require:
    # 1. Valid GCP project with BigQuery enabled
    # 2. Service account credentials
    # 3. Configured dataset

    config = AdbcConfig(
        driver_name="adbc_driver_bigquery",
        project_id="test-project",  # Would need to be real
        dataset_id="test_dataset",  # Would need to be real
    )

    # Since we don't have real credentials, this will fail and be xfailed
    with config.provide_session() as session:
        # Test basic query that would work in BigQuery
        result = session.execute("SELECT 1 as test_value")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["test_value"] == 1


@pytest.mark.skipif(
    "not config.getoption('--run-bigquery-tests', default=False)",
    reason="BigQuery ADBC tests require --run-bigquery-tests flag and valid GCP credentials",
)
@pytest.mark.xdist_group("adbc_bigquery")
@xfail_if_driver_missing
def test_data_types() -> None:
    """Test BigQuery-specific data types with ADBC (requires valid GCP setup)."""
    config = AdbcConfig(
        driver_name="adbc_driver_bigquery",
        project_id="test-project",  # Would need to be real
        dataset_id="test_dataset",  # Would need to be real
    )

    with config.provide_session() as session:
        # Test BigQuery built-in functions
        functions_result = session.execute("""
            SELECT
                CURRENT_TIMESTAMP() as current_ts,
                GENERATE_UUID() as uuid_val,
                FARM_FINGERPRINT('test') as fingerprint
        """)
        assert isinstance(functions_result, SQLResult)
        assert functions_result.data is not None
        assert functions_result.data[0]["current_ts"] is not None
        assert functions_result.data[0]["uuid_val"] is not None
        assert functions_result.data[0]["fingerprint"] is not None

        # Test array operations
        array_result = session.execute("""
            SELECT
                ARRAY[1, 2, 3, 4, 5] as numbers,
                ARRAY_LENGTH(ARRAY[1, 2, 3, 4, 5]) as array_len
        """)
        assert isinstance(array_result, SQLResult)
        assert array_result.data is not None
        assert array_result.data[0]["numbers"] == [1, 2, 3, 4, 5]
        assert array_result.data[0]["array_len"] == 5


@pytest.mark.skipif(
    "not config.getoption('--run-bigquery-tests', default=False)",
    reason="BigQuery ADBC tests require --run-bigquery-tests flag and valid GCP credentials",
)
@pytest.mark.xdist_group("adbc_bigquery")
@xfail_if_driver_missing
def test_bigquery_specific_features() -> None:
    """Test BigQuery-specific SQL features (requires valid GCP setup)."""
    config = AdbcConfig(
        driver_name="adbc_driver_bigquery",
        project_id="test-project",  # Would need to be real
        dataset_id="test_dataset",  # Would need to be real
    )

    with config.provide_session() as session:
        # Test STRUCT type
        struct_result = session.execute("""
            SELECT
                STRUCT(1 as x, 'hello' as y) as my_struct,
                STRUCT<name STRING, age INT64>('Alice', 30) as person
        """)
        assert isinstance(struct_result, SQLResult)
        assert struct_result.data is not None
        assert struct_result.data[0]["my_struct"] is not None
        assert struct_result.data[0]["person"] is not None

        # Test BigQuery UNNEST
        unnest_result = session.execute("""
            SELECT x
            FROM UNNEST([1, 2, 3, 4, 5]) AS x
            WHERE x > 2
        """)
        assert isinstance(unnest_result, SQLResult)
        assert unnest_result.data is not None
        assert len(unnest_result.data) == 3

        # Test BigQuery date functions
        date_result = session.execute("""
            SELECT
                DATE('2024-01-15') as date_val,
                DATE_ADD(DATE('2024-01-15'), INTERVAL 7 DAY) as week_later,
                FORMAT_DATE('%A, %B %d, %Y', DATE('2024-01-15')) as formatted
        """)
        assert isinstance(date_result, SQLResult)
        assert date_result.data is not None
        assert date_result.data[0]["date_val"] is not None
        assert date_result.data[0]["week_later"] is not None
        assert date_result.data[0]["formatted"] is not None


@pytest.mark.skipif(
    "not config.getoption('--run-bigquery-tests', default=False)",
    reason="BigQuery ADBC tests require --run-bigquery-tests flag and valid GCP credentials",
)
@pytest.mark.xdist_group("adbc_bigquery")
@xfail_if_driver_missing
def test_parameterized_queries() -> None:
    """Test parameterized queries with BigQuery ADBC (requires valid GCP setup)."""
    config = AdbcConfig(
        driver_name="adbc_driver_bigquery",
        project_id="test-project",  # Would need to be real
        dataset_id="test_dataset",  # Would need to be real
    )

    with config.provide_session() as session:
        # BigQuery uses @parameter_name style parameters
        # Test with basic parameter
        result = session.execute("SELECT @value as test_value", {"value": 42})
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["test_value"] == 42

        # Test with multiple parameters
        multi_result = session.execute(
            """
            SELECT
                @name as name,
                @age as age,
                @active as is_active
            """,
            {"name": "Alice", "age": 30, "active": True},
        )
        assert isinstance(multi_result, SQLResult)
        assert multi_result.data is not None
        assert multi_result.data[0]["name"] == "Alice"
        assert multi_result.data[0]["age"] == 30
        assert multi_result.data[0]["is_active"] is True


@pytest.mark.skipif(
    "not config.getoption('--run-bigquery-tests', default=False)",
    reason="BigQuery ADBC tests require --run-bigquery-tests flag and valid GCP credentials",
)
@pytest.mark.xdist_group("adbc_bigquery")
@xfail_if_driver_missing
def test_bigquery_analytics_functions() -> None:
    """Test BigQuery analytics and window functions (requires valid GCP setup)."""
    config = AdbcConfig(
        driver_name="adbc_driver_bigquery",
        project_id="test-project",  # Would need to be real
        dataset_id="test_dataset",  # Would need to be real
    )

    with config.provide_session() as session:
        # Test window functions with inline data
        window_result = session.execute("""
            WITH sales_data AS (
                SELECT 'North' as region, 'Q1' as quarter, 100 as amount
                UNION ALL SELECT 'North', 'Q2', 150
                UNION ALL SELECT 'North', 'Q3', 200
                UNION ALL SELECT 'South', 'Q1', 80
                UNION ALL SELECT 'South', 'Q2', 120
                UNION ALL SELECT 'South', 'Q3', 160
            )
            SELECT
                region,
                quarter,
                amount,
                SUM(amount) OVER (PARTITION BY region ORDER BY quarter) as running_total,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rank_in_region
            FROM sales_data
            ORDER BY region, quarter
        """)
        assert isinstance(window_result, SQLResult)
        assert window_result.data is not None
        assert len(window_result.data) == 6

        # Test APPROX functions (BigQuery specific)
        approx_result = session.execute("""
            WITH numbers AS (
                SELECT x
                FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS x
            )
            SELECT
                APPROX_COUNT_DISTINCT(x) as approx_distinct,
                APPROX_QUANTILES(x, 4) as quartiles,
                APPROX_TOP_COUNT(MOD(x, 10), 3) as top_3_mods
            FROM numbers
        """)
        assert isinstance(approx_result, SQLResult)
        assert approx_result.data is not None
        assert approx_result.data[0]["approx_distinct"] is not None


# Note: Additional BigQuery-specific tests could include:
# - Testing BigQuery ML functions (CREATE MODEL, ML.PREDICT, etc.)
# - Testing partitioned and clustered tables
# - Testing external tables and federated queries
# - Testing scripting and procedural language features
# - Testing geographic/spatial functions
# - Testing streaming inserts
# However, these would all require actual BigQuery infrastructure and credentials
