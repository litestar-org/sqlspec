"""Integration tests for BigQuery driver implementation."""

import operator
import os
from collections.abc import Generator
from typing import Literal

import pytest
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver
from tests.conftest import requires_interpreted

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]

BIGQUERY_ENABLED = os.environ.get("SQLSPEC_ENABLE_BIGQUERY_TESTS") == "1"

pytestmark = [
    pytest.mark.xdist_group("bigquery"),
    pytest.mark.skipif(
        not BIGQUERY_ENABLED, reason="BigQuery emulator is optional; set SQLSPEC_ENABLE_BIGQUERY_TESTS=1 to enable"
    ),
]


def test_connection(bigquery_config: "BigQueryConfig") -> None:
    """Test database connection."""
    with bigquery_config.provide_session() as driver:
        result = driver.execute("SELECT 1 as one")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data == [{"one": 1}]


@pytest.fixture
def driver_test_table(
    bigquery_session: "BigQueryDriver", bigquery_service: "BigQueryService"
) -> "Generator[str, None, None]":
    """Create and cleanup driver-specific test table."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.driver_test_table`"

    bigquery_session.execute(f"""
        CREATE OR REPLACE TABLE {table_name} (
            id INT64,
            name STRING NOT NULL,
            value INT64,
            created_at TIMESTAMP
        )
    """)

    yield table_name

    # Cleanup
    try:
        bigquery_session.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass


def test_bigquery_basic_crud(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test basic CRUD operations."""

    insert_result = bigquery_session.execute(
        f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", (1, "test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (1, 0)

    select_result = bigquery_session.execute(
        f"SELECT name, value FROM {driver_test_table} WHERE name = ?", ("test_name",)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    update_result = bigquery_session.execute(
        f"UPDATE {driver_test_table} SET value = ? WHERE name = ?", (100, "test_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected in (1, 0)

    verify_result = bigquery_session.execute(f"SELECT value FROM {driver_test_table} WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    delete_result = bigquery_session.execute(f"DELETE FROM {driver_test_table} WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected in (1, 0)

    empty_result = bigquery_session.execute(f"SELECT COUNT(*) as count FROM {driver_test_table}")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


def test_bigquery_parameter_styles(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test BigQuery named parameter binding (only supported style)."""

    bigquery_session.execute(
        f"INSERT INTO {driver_test_table} (id, name) VALUES (@id, @name)", {"id": 1, "name": "test_value"}
    )

    sql = f"SELECT name FROM {driver_test_table} WHERE name = @name"
    parameters = {"name": "test_value"}

    result = bigquery_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_value"


def test_bigquery_execute_many(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test execute_many functionality."""
    parameters_list = [(1, "name1", 1), (2, "name2", 2), (3, "name3", 3)]

    result = bigquery_session.execute_many(
        f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", parameters_list
    )
    assert isinstance(result, SQLResult)

    assert result.rows_affected >= 0

    select_result = bigquery_session.execute(f"SELECT COUNT(*) as count FROM {driver_test_table}")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)

    ordered_result = bigquery_session.execute(f"SELECT name, value FROM {driver_test_table} ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


def test_bigquery_execute_script(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test execute_script functionality."""
    script = f"""
        INSERT INTO {driver_test_table} (id, name, value) VALUES (1, 'script_test1', 999);
        INSERT INTO {driver_test_table} (id, name, value) VALUES (2, 'script_test2', 888);
        UPDATE {driver_test_table} SET value = 1000 WHERE name = 'script_test1';
    """

    result = bigquery_session.execute_script(script)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    select_result = bigquery_session.execute(
        f"SELECT name, value FROM {driver_test_table} WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


def test_bigquery_result_methods(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test SQLResult methods."""

    bigquery_session.execute_many(
        f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)",
        [(1, "result1", 10), (2, "result2", 20), (3, "result3", 30)],
    )

    result = bigquery_session.execute(f"SELECT * FROM {driver_test_table} ORDER BY name")
    assert isinstance(result, SQLResult)

    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    assert result.get_count() == 3

    assert not result.is_empty()

    empty_result = bigquery_session.execute(f"SELECT * FROM {driver_test_table} WHERE name = ?", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


def test_bigquery_complex_queries(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test complex SQL queries."""

    test_data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35), (4, "Diana", 28)]

    bigquery_session.execute_many(f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", test_data)

    join_result = bigquery_session.execute(f"""
        SELECT t1.name as name1, t2.name as name2, t1.value as value1, t2.value as value2
        FROM {driver_test_table} t1
        CROSS JOIN {driver_test_table} t2
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
        FROM {driver_test_table}
    """)
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["total_count"] == 4
    assert agg_result.data[0]["avg_value"] == 29.5
    assert agg_result.data[0]["min_value"] == 25
    assert agg_result.data[0]["max_value"] == 35

    subquery_result = bigquery_session.execute(f"""
        SELECT name, value
        FROM {driver_test_table}
        WHERE value > (SELECT AVG(value) FROM {driver_test_table})
        ORDER BY value
    """)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2
    assert subquery_result.data[0]["name"] == "Bob"
    assert subquery_result.data[1]["name"] == "Charlie"


def test_bigquery_statement_stack_sequential(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """StatementStack executions should remain sequential on BigQuery."""

    bigquery_session.execute(f"DELETE FROM {driver_test_table} WHERE id IS NOT NULL")

    stack = (
        StatementStack()
        .push_execute(f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", (1, "stack-one", 10))
        .push_execute(f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", (2, "stack-two", 20))
        .push_execute(f"SELECT COUNT(*) AS total FROM {driver_test_table} WHERE name LIKE ?", ("stack-%",))
    )

    results = bigquery_session.execute_stack(stack)

    assert len(results) == 3
    assert results[2].result is not None
    assert results[2].result.data is not None
    assert results[2].result.data[0]["total"] == 2


@requires_interpreted
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
    assert verify.data[0]["total"] == 2


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

    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None

    assert "created_at" in row


def test_bigquery_performance_bulk_operations(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test performance with bulk operations."""

    bulk_data = [(i, f"bulk_user_{i}", i * 10) for i in range(1, 101)]

    result = bigquery_session.execute_many(
        f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", bulk_data
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected in (100, 0)

    select_result = bigquery_session.execute(
        f"SELECT COUNT(*) as count FROM {driver_test_table} WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    page_result = bigquery_session.execute(f"""
        SELECT name, value FROM {driver_test_table}
        WHERE name LIKE 'bulk_user_%'
        ORDER BY value
        LIMIT 10 OFFSET 20
    """)
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_21"


def test_bigquery_specific_features(bigquery_session: "BigQueryDriver", bigquery_service: "BigQueryService") -> None:
    """Test BigQuery-specific features."""

    functions_result = bigquery_session.execute("""
        SELECT
            GENERATE_UUID() as uuid_val,
            FARM_FINGERPRINT('test') as fingerprint
    """)
    assert isinstance(functions_result, SQLResult)
    assert functions_result.data is not None
    assert functions_result.data[0]["uuid_val"] is not None
    assert functions_result.data[0]["fingerprint"] is not None

    array_result = bigquery_session.execute("""
        SELECT
            ARRAY[1, 2, 3, 4, 5] as numbers,
            ARRAY_LENGTH(ARRAY[1, 2, 3, 4, 5]) as array_len
    """)
    assert isinstance(array_result, SQLResult)
    assert array_result.data is not None
    assert array_result.data[0]["numbers"] == [1, 2, 3, 4, 5]
    assert array_result.data[0]["array_len"] == 5

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


def test_bigquery_analytical_functions(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test BigQuery analytical and window functions."""

    analytics_data = [
        (1, "Product A", 1000),
        (2, "Product B", 1500),
        (3, "Product A", 1200),
        (4, "Product C", 800),
        (5, "Product B", 1800),
    ]

    bigquery_session.execute_many(f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)", analytics_data)

    window_result = bigquery_session.execute(f"""
        SELECT
            name,
            value,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY value DESC) as row_num,
            RANK() OVER (PARTITION BY name ORDER BY value DESC) as rank_val,
            SUM(value) OVER (PARTITION BY name) as total_by_product,
            LAG(value) OVER (ORDER BY id) as previous_value
        FROM {driver_test_table}
        ORDER BY id
    """)
    assert isinstance(window_result, SQLResult)
    assert window_result.data is not None
    assert len(window_result.data) == 5

    product_a_rows = [row for row in window_result.data if row["name"] == "Product A"]
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
    bigquery_session: "BigQueryDriver", driver_test_table: str
) -> None:
    """Test execute_many with QMARK placeholders and dict parameters.

    This is a regression test for parameter style mismatch when using
    QMARK (?) placeholders with dict parameters. The parameter converter
    should properly align the dict keys with the converted @param_N style.
    """
    sql = f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)"
    params = [{"id": 1, "name": "qmark_dict_a", "value": 100}, {"id": 2, "name": "qmark_dict_b", "value": 200}]

    result = bigquery_session.execute_many(sql, params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0

    verify = bigquery_session.execute(
        f"SELECT name, value FROM {driver_test_table} WHERE name LIKE 'qmark_dict%' ORDER BY name"
    )
    assert verify.data is not None
    assert len(verify.data) == 2
    assert verify.data[0]["name"] == "qmark_dict_a"
    assert verify.data[0]["value"] == 100
    assert verify.data[1]["name"] == "qmark_dict_b"
    assert verify.data[1]["value"] == 200


def test_bigquery_execute_many_named_params(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test execute_many with named parameters (native @name style)."""
    sql = f"INSERT INTO {driver_test_table} (id, name, value) VALUES (@id, @name, @value)"
    params = [{"id": 1, "name": "named_a", "value": 10}, {"id": 2, "name": "named_b", "value": 20}]

    result = bigquery_session.execute_many(sql, params)
    assert isinstance(result, SQLResult)

    verify = bigquery_session.execute(
        f"SELECT name, value FROM {driver_test_table} WHERE name LIKE 'named_%' ORDER BY name"
    )
    assert verify.data is not None
    assert len(verify.data) == 2
    assert verify.data[0]["name"] == "named_a"
    assert verify.data[1]["name"] == "named_b"


def test_bigquery_execute_many_update_with_inlining(bigquery_session: "BigQueryDriver", driver_test_table: str) -> None:
    """Test that UPDATE statements use literal inlining fallback."""
    bigquery_session.execute_many(
        f"INSERT INTO {driver_test_table} (id, name, value) VALUES (?, ?, ?)",
        [(1, "update_test_a", 10), (2, "update_test_b", 20)],
    )

    sql = f"UPDATE {driver_test_table} SET value = @new_val WHERE name = @key"
    params = [{"key": "update_test_a", "new_val": 100}, {"key": "update_test_b", "new_val": 200}]

    result = bigquery_session.execute_many(sql, params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0

    verify = bigquery_session.execute(
        f"SELECT name, value FROM {driver_test_table} WHERE name LIKE 'update_test%' ORDER BY name"
    )
    assert verify.data is not None
    assert len(verify.data) == 2
    assert verify.data[0]["value"] == 100
    assert verify.data[1]["value"] == 200
