"""Integration tests for Spanner Arrow support.

All operations use SQLSpec interface, not raw SDK calls.
"""

import pytest

from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.typing import PYARROW_INSTALLED

pytestmark = [pytest.mark.spanner, pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")]


def test_select_to_arrow_basic(spanner_config: SpannerSyncConfig, test_arrow_table: str) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    with spanner_config.provide_write_session() as session:
        for i, name in enumerate(["Alice", "Bob", "Charlie"], start=1):
            session.execute(
                f"INSERT INTO {test_arrow_table} (id, name, value) VALUES (@id, @name, @value)",
                {"id": i, "name": name, "value": i * 10},
            )

    with spanner_config.provide_session() as session:
        result = session.select_to_arrow(f"SELECT * FROM {test_arrow_table} ORDER BY id")

        assert result is not None
        assert isinstance(result.data, (pa.Table, pa.RecordBatch))
        assert result.rows_affected == 3

        df = result.to_pandas()
        assert len(df) == 3
        assert list(df["name"]) == ["Alice", "Bob", "Charlie"]
        assert list(df["value"]) == [10, 20, 30]

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_arrow_table} WHERE TRUE")


def test_select_to_arrow_with_parameters(spanner_config: SpannerSyncConfig, test_arrow_table: str) -> None:
    """Test select_to_arrow with parameterized query."""
    with spanner_config.provide_write_session() as session:
        for i in range(1, 6):
            session.execute(
                f"INSERT INTO {test_arrow_table} (id, name, value) VALUES (@id, @name, @value)",
                {"id": i, "name": f"Item {i}", "value": i * 100},
            )

    with spanner_config.provide_session() as session:
        result = session.select_to_arrow(
            f"SELECT * FROM {test_arrow_table} WHERE value > @min_value ORDER BY id", {"min_value": 200}
        )

        assert result.rows_affected == 3
        df = result.to_pandas()
        assert list(df["value"]) == [300, 400, 500]

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_arrow_table} WHERE TRUE")


def test_select_to_arrow_empty_result(spanner_config: SpannerSyncConfig, test_arrow_table: str) -> None:
    """Test select_to_arrow with empty result set."""
    with spanner_config.provide_session() as session:
        result = session.select_to_arrow(f"SELECT * FROM {test_arrow_table}")

        assert result.rows_affected == 0
        assert len(result.to_pandas()) == 0


def test_select_to_arrow_table_format(spanner_config: SpannerSyncConfig, test_arrow_table: str) -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    with spanner_config.provide_write_session() as session:
        for i in range(1, 4):
            session.execute(
                f"INSERT INTO {test_arrow_table} (id, name, value) VALUES (@id, @name, @value)",
                {"id": i, "name": f"Row {i}", "value": i},
            )

    with spanner_config.provide_session() as session:
        result = session.select_to_arrow(f"SELECT * FROM {test_arrow_table} ORDER BY id", return_format="table")

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_arrow_table} WHERE TRUE")


def test_select_to_arrow_batch_format(spanner_config: SpannerSyncConfig, test_arrow_table: str) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    with spanner_config.provide_write_session() as session:
        for i in range(1, 3):
            session.execute(
                f"INSERT INTO {test_arrow_table} (id, name, value) VALUES (@id, @name, @value)",
                {"id": i, "name": f"Batch {i}", "value": i * 5},
            )

    with spanner_config.provide_session() as session:
        result = session.select_to_arrow(f"SELECT * FROM {test_arrow_table} ORDER BY id", return_format="batch")

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_arrow_table} WHERE TRUE")


def test_select_to_arrow_to_polars(spanner_config: SpannerSyncConfig, test_arrow_table: str) -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""
    pytest.importorskip("polars")

    with spanner_config.provide_write_session() as session:
        for i in range(1, 3):
            session.execute(
                f"INSERT INTO {test_arrow_table} (id, name, value) VALUES (@id, @name, @value)",
                {"id": i, "name": f"Polars {i}", "value": i * 7},
            )

    with spanner_config.provide_session() as session:
        result = session.select_to_arrow(f"SELECT * FROM {test_arrow_table} ORDER BY id")
        df = result.to_polars()

        assert len(df) == 2
        assert df["name"].to_list() == ["Polars 1", "Polars 2"]

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_arrow_table} WHERE TRUE")
