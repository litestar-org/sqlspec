"""Integration tests for BigQuery native Arrow support with Storage API.

These tests require:
1. Google Cloud credentials (GOOGLE_APPLICATION_CREDENTIALS or ADC)
2. A BigQuery project with Storage API enabled
3. google-cloud-bigquery-storage package installed

Tests are skipped if credentials or packages are not available.
"""

import os

import pytest

from sqlspec._typing import PYARROW_INSTALLED

# Check if BigQuery dependencies are available
pytest.importorskip("google.cloud.bigquery", reason="google-cloud-bigquery not installed")

from sqlspec.adapters.bigquery import BigQueryConfig

pytestmark = [
    pytest.mark.xdist_group("bigquery"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
    pytest.mark.skipif(
        not os.getenv("BIGQUERY_PROJECT_ID") and not os.getenv("GOOGLE_CLOUD_PROJECT"),
        reason="BigQuery credentials not configured"
    ),
]


@pytest.fixture
def bigquery_config() -> BigQueryConfig:
    """Create BigQuery configuration."""
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")

    return BigQueryConfig(
        connection_config={
            "project": project_id,
            "location": "US",
        }
    )


@pytest.fixture
def test_table_name() -> str:
    """Generate a unique test table name."""
    import uuid
    return f"test_arrow_{uuid.uuid4().hex[:8]}"


def test_select_to_arrow_basic(bigquery_config: BigQueryConfig, test_table_name: str) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")
    full_table = f"{dataset_id}.{test_table_name}"

    try:
        with bigquery_config.provide_session() as session:
            # Create test table
            session.execute(f"""
                CREATE TABLE `{full_table}` (
                    id INT64,
                    name STRING,
                    age INT64
                )
            """)
            session.execute(f"""
                INSERT INTO `{full_table}` (id, name, age)
                VALUES (1, 'Alice', 30), (2, 'Bob', 25)
            """)

            # Test Arrow query
            result = session.select_to_arrow(f"SELECT * FROM `{full_table}` ORDER BY id")

            assert result is not None
            assert isinstance(result.data, (pa.Table, pa.RecordBatch))
            assert result.rows_affected == 2

            # Convert to pandas and verify
            df = result.to_pandas()
            assert len(df) == 2
            assert list(df["name"]) == ["Alice", "Bob"]
            assert list(df["age"]) == [30, 25]
    finally:
        # Cleanup
        with bigquery_config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS `{full_table}`")
        bigquery_config.close_pool()


def test_select_to_arrow_table_format(bigquery_config: BigQueryConfig, test_table_name: str) -> None:
    """Test select_to_arrow with table return format."""
    import pyarrow as pa

    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")
    full_table = f"{dataset_id}.{test_table_name}"

    try:
        with bigquery_config.provide_session() as session:
            session.execute(f"CREATE TABLE `{full_table}` (id INT64, value STRING)")
            session.execute(f"INSERT INTO `{full_table}` VALUES (1, 'a'), (2, 'b'), (3, 'c')")

            result = session.select_to_arrow(f"SELECT * FROM `{full_table}`", return_format="table")

            assert isinstance(result.data, pa.Table)
            assert result.rows_affected == 3
    finally:
        with bigquery_config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS `{full_table}`")
        bigquery_config.close_pool()


def test_select_to_arrow_batch_format(bigquery_config: BigQueryConfig, test_table_name: str) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")
    full_table = f"{dataset_id}.{test_table_name}"

    try:
        with bigquery_config.provide_session() as session:
            session.execute(f"CREATE TABLE `{full_table}` (id INT64, value STRING)")
            session.execute(f"INSERT INTO `{full_table}` VALUES (1, 'a'), (2, 'b')")

            result = session.select_to_arrow(f"SELECT * FROM `{full_table}`", return_format="batch")

            assert isinstance(result.data, pa.RecordBatch)
            assert result.rows_affected == 2
    finally:
        with bigquery_config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS `{full_table}`")
        bigquery_config.close_pool()


def test_select_to_arrow_with_parameters(bigquery_config: BigQueryConfig, test_table_name: str) -> None:
    """Test select_to_arrow with query parameters."""
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")
    full_table = f"{dataset_id}.{test_table_name}"

    try:
        with bigquery_config.provide_session() as session:
            session.execute(f"""
                CREATE TABLE `{full_table}` (id INT64, name STRING, age INT64)
            """)
            session.execute(f"""
                INSERT INTO `{full_table}` VALUES
                (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)
            """)

            # Query with named parameter (BigQuery style)
            result = session.select_to_arrow(
                f"SELECT * FROM `{full_table}` WHERE age > @min_age",
                {"min_age": 25}
            )

            df = result.to_pandas()
            assert len(df) == 2
            assert set(df["name"]) == {"Alice", "Charlie"}
    finally:
        with bigquery_config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS `{full_table}`")
        bigquery_config.close_pool()


def test_select_to_arrow_empty_result(bigquery_config: BigQueryConfig, test_table_name: str) -> None:
    """Test select_to_arrow with no matching rows."""
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")
    full_table = f"{dataset_id}.{test_table_name}"

    try:
        with bigquery_config.provide_session() as session:
            session.execute(f"CREATE TABLE `{full_table}` (id INT64, value STRING)")

            result = session.select_to_arrow(f"SELECT * FROM `{full_table}` WHERE id > 100")

            assert result.rows_affected == 0
            df = result.to_pandas()
            assert len(df) == 0
    finally:
        with bigquery_config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS `{full_table}`")
        bigquery_config.close_pool()


def test_select_to_arrow_null_handling(bigquery_config: BigQueryConfig, test_table_name: str) -> None:
    """Test select_to_arrow with NULL values."""
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")
    full_table = f"{dataset_id}.{test_table_name}"

    try:
        with bigquery_config.provide_session() as session:
            session.execute(f"CREATE TABLE `{full_table}` (id INT64, value STRING)")
            session.execute(f"INSERT INTO `{full_table}` VALUES (1, 'a'), (2, NULL), (3, 'c')")

            result = session.select_to_arrow(f"SELECT * FROM `{full_table}` ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 3
            assert df["value"].isna()[1]  # Second row should be NULL
    finally:
        with bigquery_config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS `{full_table}`")
        bigquery_config.close_pool()


@pytest.mark.skipif(
    "google.cloud.bigquery_storage_v1" not in __import__("sys").modules,
    reason="BigQuery Storage API not available"
)
def test_storage_api_detection(bigquery_config: BigQueryConfig) -> None:
    """Test that Storage API availability is correctly detected."""
    try:
        with bigquery_config.provide_session() as session:
            # Check if Storage API is available
            has_storage_api = session._storage_api_available()  # type: ignore[reportPrivateUsage]

            # If available, test native path
            if has_storage_api:
                result = session.select_to_arrow("SELECT 1 AS id, 'test' AS value")
                assert result.rows_affected == 1
    finally:
        bigquery_config.close_pool()


def test_fallback_to_conversion_path(bigquery_config: BigQueryConfig, test_table_name: str) -> None:
    """Test fallback to dict conversion when native_only=False (default)."""
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")
    full_table = f"{dataset_id}.{test_table_name}"

    try:
        with bigquery_config.provide_session() as session:
            session.execute(f"CREATE TABLE `{full_table}` (id INT64, value STRING)")
            session.execute(f"INSERT INTO `{full_table}` VALUES (1, 'test')")

            # This should work even if Storage API is not available
            # (will use conversion path)
            result = session.select_to_arrow(
                f"SELECT * FROM `{full_table}`",
                native_only=False  # Explicit
            )

            assert result.rows_affected == 1
            df = result.to_pandas()
            assert len(df) == 1
    finally:
        with bigquery_config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS `{full_table}`")
        bigquery_config.close_pool()


def test_select_to_arrow_to_polars(bigquery_config: BigQueryConfig, test_table_name: str) -> None:
    """Test select_to_arrow with polars conversion."""
    pytest.importorskip("polars", reason="polars not installed")

    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "sqlspec_test")
    full_table = f"{dataset_id}.{test_table_name}"

    try:
        with bigquery_config.provide_session() as session:
            session.execute(f"CREATE TABLE `{full_table}` (id INT64, value STRING)")
            session.execute(f"INSERT INTO `{full_table}` VALUES (1, 'a'), (2, 'b')")

            result = session.select_to_arrow(f"SELECT * FROM `{full_table}` ORDER BY id")

            pl_df = result.to_polars()
            assert len(pl_df) == 2
            assert list(pl_df["value"]) == ["a", "b"]
    finally:
        with bigquery_config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS `{full_table}`")
        bigquery_config.close_pool()
