"""Integration tests for BigQuery driver implementation."""

import operator
import os

import pytest
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.bigquery import BigQueryDriver

BIGQUERY_ENABLED = os.environ.get("CI") == "true" or os.environ.get("SQLSPEC_ENABLE_BIGQUERY_TESTS") == "1"

pytestmark = [
    pytest.mark.xdist_group("bigquery"),
    pytest.mark.skipif(
        not BIGQUERY_ENABLED,
        reason="BigQuery emulator is optional locally; set SQLSPEC_ENABLE_BIGQUERY_TESTS=1 to enable",
    ),
]


@pytest.fixture(scope="session")
def _driver_test_table_resource(bigquery_session: "BigQueryDriver", bigquery_service: "BigQueryService") -> str:
    """Create the driver-specific test table once per session."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    bigquery_session.execute(f"""
        CREATE OR REPLACE TABLE {table_name} (
            id INT64,
            name STRING NOT NULL,
            value INT64,
            created_at TIMESTAMP
        )
    """)
    return table_name


@pytest.fixture
def driver_test_table(bigquery_session: "BigQueryDriver", _driver_test_table_resource: str) -> str:
    """Empty the shared driver_test_table before each test."""
    bigquery_session.execute(f"DELETE FROM {_driver_test_table_resource} WHERE TRUE")
    return _driver_test_table_resource


@pytest.fixture
def native_driver_test_table(native_bigquery_service: "BigQueryService", driver_test_table: str) -> str:
    """Empty the table only when native BigQuery features are available."""
    del native_bigquery_service
    return driver_test_table


def test_bigquery_complex_queries(bigquery_session: "BigQueryDriver", native_driver_test_table: str) -> None:
    """Test complex SQL queries."""
    test_data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35), (4, "Diana", 28)]

    bigquery_session.execute_many(
        f"INSERT INTO {native_driver_test_table} (id, name, value) VALUES (?, ?, ?)", test_data
    )

    join_result = bigquery_session.execute(f"""
        SELECT t1.name as name1, t2.name as name2, t1.value as value1, t2.value as value2
        FROM {native_driver_test_table} t1
        CROSS JOIN {native_driver_test_table} t2
        WHERE t1.value < t2.value
        ORDER BY t1.name, t2.name
        LIMIT 3
    """)
    assert isinstance(join_result, SQLResult)
    assert join_result.data is not None
    assert len(join_result.data) == 3

    agg_result = bigquery_session.execute(f"""
        SELECT
            COUNT(*) as total_count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM {native_driver_test_table}
    """)
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.get_data()[0]["total_count"] == 4
    assert agg_result.get_data()[0]["avg_value"] == 29.5
    assert agg_result.get_data()[0]["min_value"] == 25
    assert agg_result.get_data()[0]["max_value"] == 35

    subquery_result = bigquery_session.execute(f"""
        SELECT name, value
        FROM {native_driver_test_table}
        WHERE value > (SELECT AVG(value) FROM {native_driver_test_table})
        ORDER BY value
    """)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2
    assert subquery_result.get_data()[0]["name"] == "Bob"
    assert subquery_result.get_data()[1]["name"] == "Charlie"


def test_bigquery_statement_stack_continue_on_error(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Continue-on-error should surface BigQuery failures but keep executing."""

    bigquery_session.execute(f"DELETE FROM {driver_test_table} WHERE id IS NOT NULL")

    stack = (
        StatementStack()
        .push_execute(f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", (1, "stack-initial", 50))
        .push_execute(  # invalid column triggers deterministic error
            f"INSERT INTO {driver_test_table} (nonexistent_column) VALUES (1)"
        )
        .push_execute(f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", (2, "stack-final", 75))
    )

    results = bigquery_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[1].error is not None

    verify = bigquery_session.execute(f"SELECT COUNT(*) AS total FROM {driver_test_table}")
    assert verify.data is not None
    assert verify.get_data()[0]["total"] == 2


def test_bigquery_schema_operations(bigquery_session: "BigQueryDriver", bigquery_service: "BigQueryService") -> None:
    """Test schema operations (DDL)."""

    bigquery_session.execute_script(f"""
        CREATE TABLE IF NOT EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.schema_test` (
            id INT64,
            description STRING NOT NULL,
            created_at TIMESTAMP
        )
    """)

    insert_result = bigquery_session.execute(
        f"INSERT INTO `{bigquery_service.project}.{bigquery_service.dataset}.schema_test` (id, description, created_at) VALUES (?, ?, ?)",
        (1, "test description", "2024-01-15 10:30:00 UTC"),
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (1, 0)

    bigquery_session.execute_script(f"DROP TABLE `{bigquery_service.project}.{bigquery_service.dataset}.schema_test`")


def test_bigquery_column_names_and_metadata(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test column names and result metadata."""

    bigquery_session.execute(
        f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", (1, "metadata_test", 123)
    )

    result = bigquery_session.execute(
        f"SELECT id, name, value, created_at FROM {driver_test_table} WHERE name = ?", ("metadata_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result.data) == 1

    row = result.get_data()[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None

    assert "created_at" in row


def test_bigquery_performance_bulk_operations(
    bigquery_session: "BigQueryDriver", native_driver_test_table: str
) -> None:
    """Test performance with bulk operations."""
    bulk_data = [(i, f"bulk_user_{i}", i * 10) for i in range(1, 101)]

    result = bigquery_session.execute_many(
        f"INSERT INTO {native_driver_test_table} (id, name, value) VALUES (?, ?, ?)", bulk_data
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected in (100, 0)

    select_result = bigquery_session.execute(
        f"SELECT COUNT(*) as count FROM {native_driver_test_table} WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.get_data()[0]["count"] == 100

    page_result = bigquery_session.execute(f"""
        SELECT name, value FROM {native_driver_test_table}
        WHERE name LIKE 'bulk_user_%'
        ORDER BY value
        LIMIT 10 OFFSET 20
    """)
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.get_data()[0]["name"] == "bulk_user_21"


def test_bigquery_analytical_functions(bigquery_session: "BigQueryDriver", native_driver_test_table: str) -> None:
    """Test BigQuery analytical and window functions."""
    analytics_data = [
        (1, "Product A", 1000),
        (2, "Product B", 1500),
        (3, "Product A", 1200),
        (4, "Product C", 800),
        (5, "Product B", 1800),
    ]

    bigquery_session.execute_many(
        f"INSERT INTO {native_driver_test_table} (id, name, value) VALUES (?, ?, ?)", analytics_data
    )

    window_result = bigquery_session.execute(f"""
        SELECT
            name,
            value,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY value DESC) as row_num,
            RANK() OVER (PARTITION BY name ORDER BY value DESC) as rank_val,
            SUM(value) OVER (PARTITION BY name) as total_by_product,
            LAG(value) OVER (ORDER BY id) as previous_value
        FROM {native_driver_test_table}
        ORDER BY id
    """)
    assert isinstance(window_result, SQLResult)
    assert window_result.data is not None
    assert len(window_result.data) == 5

    product_a_rows = [row for row in window_result.get_data() if row["name"] == "Product A"]
    assert len(product_a_rows) == 2

    highest_a = max(product_a_rows, key=operator.itemgetter("value"))
    assert highest_a["row_num"] == 1


def test_bigquery_for_update_generates_sql_but_unsupported(
    bigquery_session: "BigQueryDriver", bigquery_service: "BigQueryService"
) -> None:
    """Test that FOR UPDATE is stripped by sqlglot for BigQuery since it's not supported."""

    # BigQuery doesn't support FOR UPDATE - sqlglot automatically strips it out
    query = sql.select("*").from_("test_table").for_update()
    stmt = query.build()

    # sqlglot now strips out unsupported FOR UPDATE for BigQuery
    assert "FOR UPDATE" not in stmt.sql
    assert "SELECT" in stmt.sql  # But the rest of the query works

    # Note: BigQuery is a columnar, analytics-focused database that doesn't support row-level locking


def test_bigquery_for_share_generates_sql_but_unsupported(
    bigquery_session: "BigQueryDriver", bigquery_service: "BigQueryService"
) -> None:
    """Test that FOR SHARE is stripped by sqlglot for BigQuery since it's not supported."""

    # BigQuery doesn't support FOR SHARE - sqlglot automatically strips it out
    query = sql.select("*").from_("test_table").for_share()
    stmt = query.build()

    # sqlglot now strips out unsupported FOR SHARE for BigQuery
    assert "FOR SHARE" not in stmt.sql
    assert "SELECT" in stmt.sql  # But the rest of the query works

    # BigQuery is designed for analytical workloads and doesn't support transactional locking


def test_bigquery_for_update_skip_locked_generates_sql_but_unsupported(
    bigquery_session: "BigQueryDriver", bigquery_service: "BigQueryService"
) -> None:
    """Test that FOR UPDATE SKIP LOCKED is stripped by sqlglot for BigQuery since it's not supported."""

    # BigQuery doesn't support FOR UPDATE SKIP LOCKED - sqlglot automatically strips it out
    query = sql.select("*").from_("test_table").for_update(skip_locked=True)
    stmt = query.build()

    # sqlglot now strips out unsupported FOR UPDATE for BigQuery
    assert "FOR UPDATE" not in stmt.sql
    assert "SKIP LOCKED" not in stmt.sql
    assert "SELECT" in stmt.sql  # But the rest of the query works

    # BigQuery doesn't support row-level locking or transaction isolation at the row level


def test_bigquery_execute_many_qmark_with_dict_params(
    bigquery_session: "BigQueryDriver", native_driver_test_table: str
) -> None:
    """Test execute_many with QMARK placeholders and dict parameters.

    This is a regression test for parameter style mismatch when using
    QMARK (?) placeholders with dict parameters. The parameter converter
    should properly align the dict keys with the converted @param_N style.
    """
    sql = f"INSERT INTO {native_driver_test_table} (id, name, value) VALUES (?, ?, ?)"
    params = [{"id": 1, "name": "qmark_dict_a", "value": 100}, {"id": 2, "name": "qmark_dict_b", "value": 200}]

    result = bigquery_session.execute_many(sql, params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0

    verify = bigquery_session.execute(
        f"SELECT name, value FROM {native_driver_test_table} WHERE name LIKE 'qmark_dict%' ORDER BY name"
    )
    assert verify.data is not None
    assert len(verify.data) == 2
    assert verify.get_data()[0]["name"] == "qmark_dict_a"
    assert verify.get_data()[0]["value"] == 100
    assert verify.get_data()[1]["name"] == "qmark_dict_b"
    assert verify.get_data()[1]["value"] == 200


def test_bigquery_execute_many_named_params(bigquery_session: "BigQueryDriver", native_driver_test_table: str) -> None:
    """Test execute_many with named parameters (native @name style)."""
    sql = f"INSERT INTO {native_driver_test_table} (id, name, value) VALUES (@id, @name, @value)"
    params = [{"id": 1, "name": "named_a", "value": 10}, {"id": 2, "name": "named_b", "value": 20}]

    result = bigquery_session.execute_many(sql, params)
    assert isinstance(result, SQLResult)

    verify = bigquery_session.execute(
        f"SELECT name, value FROM {native_driver_test_table} WHERE name LIKE 'named_%' ORDER BY name"
    )
    assert verify.data is not None
    assert len(verify.data) == 2
    assert verify.get_data()[0]["name"] == "named_a"
    assert verify.get_data()[1]["name"] == "named_b"


def test_bigquery_execute_many_update_with_inlining(
    bigquery_session: "BigQueryDriver", native_driver_test_table: str
) -> None:
    """Test that UPDATE statements use literal inlining fallback."""
    bigquery_session.execute_many(
        f"INSERT INTO {native_driver_test_table} (id, name, value) VALUES (?, ?, ?)",
        [(1, "update_test_a", 10), (2, "update_test_b", 20)],
    )

    sql = f"UPDATE {native_driver_test_table} SET value = @new_val WHERE name = @key"
    params = [{"key": "update_test_a", "new_val": 100}, {"key": "update_test_b", "new_val": 200}]

    result = bigquery_session.execute_many(sql, params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0

    verify = bigquery_session.execute(
        f"SELECT name, value FROM {native_driver_test_table} WHERE name LIKE 'update_test%' ORDER BY name"
    )
    assert verify.data is not None
    assert len(verify.data) == 2
    assert verify.get_data()[0]["value"] == 100
    assert verify.get_data()[1]["value"] == 200
