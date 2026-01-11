"""Integration tests for BigQuery native Arrow support with Storage API."""

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from sqlspec.adapters.bigquery.core import storage_api_available
from sqlspec.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.driver import BigQueryDriver

pytest.importorskip("google.cloud.bigquery", reason="google-cloud-bigquery not installed")

pytestmark = [
    pytest.mark.xdist_group("bigquery"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


def _qualified_table(table_schema_prefix: str, table_name: str) -> str:
    return f"{table_schema_prefix}.`{table_name}`"


def _drop_table(session: "BigQueryDriver", fq_table: str) -> None:
    session.execute(f"DROP TABLE IF EXISTS {fq_table}")


def test_select_to_arrow_basic(bigquery_session: "BigQueryDriver", table_schema_prefix: str) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    table_name = f"test_arrow_{uuid4().hex[:8]}"
    fq_table = _qualified_table(table_schema_prefix, table_name)

    try:
        bigquery_session.execute(
            f"""
                CREATE TABLE {fq_table} (
                    id INT64,
                    name STRING,
                    age INT64
                )
            """
        )
        bigquery_session.execute(
            f"""
                INSERT INTO {fq_table} (id, name, age)
                VALUES (1, 'Alice', 30), (2, 'Bob', 25)
            """
        )

        result = bigquery_session.select_to_arrow(f"SELECT * FROM {fq_table} ORDER BY id")

        assert result is not None
        assert isinstance(result.data, (pa.Table, pa.RecordBatch))
        assert result.rows_affected == 2

        df = result.to_pandas()
        assert len(df) == 2
        assert list(df["name"]) == ["Alice", "Bob"]
        assert list(df["age"]) == [30, 25]
    finally:
        _drop_table(bigquery_session, fq_table)


def test_select_to_arrow_table_format(bigquery_session: "BigQueryDriver", table_schema_prefix: str) -> None:
    """Test select_to_arrow with table return format."""
    import pyarrow as pa

    table_name = f"test_arrow_{uuid4().hex[:8]}"
    fq_table = _qualified_table(table_schema_prefix, table_name)

    try:
        bigquery_session.execute(f"CREATE TABLE {fq_table} (id INT64, value STRING)")
        bigquery_session.execute(f"INSERT INTO {fq_table} (id, value) VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        result = bigquery_session.select_to_arrow(f"SELECT * FROM {fq_table}", return_format="table")

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3
    finally:
        _drop_table(bigquery_session, fq_table)


def test_select_to_arrow_batch_format(bigquery_session: "BigQueryDriver", table_schema_prefix: str) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    table_name = f"test_arrow_{uuid4().hex[:8]}"
    fq_table = _qualified_table(table_schema_prefix, table_name)

    try:
        bigquery_session.execute(f"CREATE TABLE {fq_table} (id INT64, value STRING)")
        bigquery_session.execute(f"INSERT INTO {fq_table} (id, value) VALUES (1, 'a'), (2, 'b')")

        result = bigquery_session.select_to_arrow(f"SELECT * FROM {fq_table}", return_format="batch")

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2
    finally:
        _drop_table(bigquery_session, fq_table)


def test_select_to_arrow_with_parameters(bigquery_session: "BigQueryDriver", table_schema_prefix: str) -> None:
    """Test select_to_arrow with query parameters."""
    table_name = f"test_arrow_{uuid4().hex[:8]}"
    fq_table = _qualified_table(table_schema_prefix, table_name)

    try:
        bigquery_session.execute(
            f"""
                CREATE TABLE {fq_table} (id INT64, name STRING, age INT64)
            """
        )
        bigquery_session.execute(
            f"""
                INSERT INTO {fq_table} (id, name, age) VALUES
                (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)
            """
        )

        result = bigquery_session.select_to_arrow(f"SELECT * FROM {fq_table} WHERE age > @min_age", {"min_age": 25})

        df = result.to_pandas()
        assert len(df) == 2
        assert set(df["name"]) == {"Alice", "Charlie"}
    finally:
        _drop_table(bigquery_session, fq_table)


def test_select_to_arrow_empty_result(bigquery_session: "BigQueryDriver", table_schema_prefix: str) -> None:
    """Test select_to_arrow with no matching rows."""

    table_name = f"test_arrow_empty_{uuid4().hex[:8]}"
    fq_table = _qualified_table(table_schema_prefix, table_name)

    try:
        bigquery_session.execute(f"CREATE TABLE {fq_table} (id INT64, value STRING)")

        result = bigquery_session.select_to_arrow(f"SELECT * FROM {fq_table} WHERE id > 100")

        assert result.rows_affected == 0
        assert len(result.to_pandas()) == 0
    finally:
        _drop_table(bigquery_session, fq_table)


def test_select_to_arrow_null_handling(bigquery_session: "BigQueryDriver", table_schema_prefix: str) -> None:
    """Test select_to_arrow with NULL values."""

    table_name = f"test_arrow_null_{uuid4().hex[:8]}"
    fq_table = _qualified_table(table_schema_prefix, table_name)

    try:
        bigquery_session.execute(f"CREATE TABLE {fq_table} (id INT64, value STRING)")
        bigquery_session.execute(f"INSERT INTO {fq_table} (id, value) VALUES (1, 'a'), (2, NULL), (3, 'c')")

        result = bigquery_session.select_to_arrow(f"SELECT * FROM {fq_table} ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 3
        assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        _drop_table(bigquery_session, fq_table)


@pytest.mark.skipif(
    "google.cloud.bigquery_storage_v1" not in __import__("sys").modules, reason="BigQuery Storage API not available"
)
def test_storage_api_detection(bigquery_config: "BigQueryConfig") -> None:
    """Test that Storage API availability is correctly detected."""

    try:
        with bigquery_config.provide_session() as session:
            has_storage_api = storage_api_available()

            if has_storage_api:
                result = session.select_to_arrow("SELECT 1 AS id, 'test' AS value")
                assert result.rows_affected == 1
    finally:
        bigquery_config.close_pool()


def test_fallback_to_conversion_path(bigquery_session: "BigQueryDriver", table_schema_prefix: str) -> None:
    """Test fallback to dict conversion when native_only=False (default)."""

    table_name = f"test_arrow_fallback_{uuid4().hex[:8]}"
    fq_table = _qualified_table(table_schema_prefix, table_name)

    try:
        bigquery_session.execute(f"CREATE TABLE {fq_table} (id INT64, value STRING)")
        bigquery_session.execute(f"INSERT INTO {fq_table} (id, value) VALUES (1, 'test')")

        result = bigquery_session.select_to_arrow(f"SELECT * FROM {fq_table}", native_only=False)

        assert result.rows_affected == 1
        df = result.to_pandas()
        assert len(df) == 1
    finally:
        _drop_table(bigquery_session, fq_table)


def test_select_to_arrow_to_polars(bigquery_session: "BigQueryDriver", table_schema_prefix: str) -> None:
    """Test select_to_arrow with polars conversion."""
    pytest.importorskip("polars", reason="polars not installed")

    table_name = f"test_arrow_polars_{uuid4().hex[:8]}"
    fq_table = _qualified_table(table_schema_prefix, table_name)

    try:
        bigquery_session.execute(f"CREATE TABLE {fq_table} (id INT64, value STRING)")
        bigquery_session.execute(f"INSERT INTO {fq_table} (id, value) VALUES (1, 'a'), (2, 'b')")

        result = bigquery_session.select_to_arrow(f"SELECT * FROM {fq_table} ORDER BY id")
        df = result.to_polars()

        assert len(df) == 2
        assert df["value"].to_list() == ["a", "b"]
    finally:
        _drop_table(bigquery_session, fq_table)
