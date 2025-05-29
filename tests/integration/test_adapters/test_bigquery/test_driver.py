from __future__ import annotations

import pytest
from google.cloud.bigquery import ScalarQueryParameter

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.sql.result import ExecuteResult, SelectResult


@pytest.mark.xdist_group("bigquery")
def test_execute_script_multiple_statements(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test execute_script with multiple statements."""
    table_name = f"{table_schema_prefix}.test_table_exec_script"  # Unique name
    with bigquery_session.provide_session() as driver:
        script = f"""
        CREATE TABLE {table_name} (id INT64, name STRING);
        INSERT INTO {table_name} (id, name) VALUES (1, 'script_test');
        INSERT INTO {table_name} (id, name) VALUES (2, 'script_test_2');
        """
        driver.execute_script(script)

        # Verify execution
        result = driver.execute(f"SELECT COUNT(*) AS count FROM {table_name} WHERE name LIKE 'script_test%'")
        assert isinstance(result, SelectResult)
        assert result.rows is not None
        assert result.rows[0]["count"] == 2

        value_result = driver.execute(
            f"SELECT name FROM {table_name} WHERE id = @id",
            [ScalarQueryParameter("id", "INT64", 1)],
        )
        assert isinstance(value_result, SelectResult)
        assert value_result.rows is not None
        assert value_result.rows[0]["name"] == "script_test"

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_insert(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test insert functionality using named parameters."""
    table_name = f"{table_schema_prefix}.test_table_insert"  # Unique name
    with bigquery_session.provide_session() as driver:
        # Create test table
        sql = f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record using named parameters (@)
        insert_sql = f"INSERT INTO {table_name} (id, name) VALUES (@id, @name)"
        params = [
            ScalarQueryParameter("id", "INT64", 1),
            ScalarQueryParameter("name", "STRING", "test_insert"),
        ]
        insert_result = driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)

        # Verify insertion
        select_result = driver.execute(
            f"SELECT name FROM {table_name} WHERE id = @id",
            [ScalarQueryParameter("id", "INT64", 1)],
        )
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1
        assert select_result.rows[0]["name"] == "test_insert"

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_select(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test select functionality using named parameters."""
    table_name = f"{table_schema_prefix}.test_table_select"  # Unique name
    with bigquery_session.provide_session() as driver:
        # Create test table
        sql = f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = f"INSERT INTO {table_name} (id, name) VALUES (@id, @name)"
        insert_result = driver.execute(
            insert_sql,
            [
                ScalarQueryParameter("id", "INT64", 10),
                ScalarQueryParameter("name", "STRING", "test_select"),
            ],
        )
        assert isinstance(insert_result, ExecuteResult)

        # Select and verify using named parameters (@)
        select_sql = f"SELECT name, id FROM {table_name} WHERE id = @id"
        select_result = driver.execute(select_sql, [ScalarQueryParameter("id", "INT64", 10)])
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1
        assert select_result.rows[0]["name"] == "test_select"
        assert select_result.rows[0]["id"] == 10

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_select_value(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test select value functionality using named parameters."""
    table_name = f"{table_schema_prefix}.test_table_select_value"  # Unique name
    with bigquery_session.provide_session() as driver:
        # Create test table
        sql = f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = f"INSERT INTO {table_name} (id, name) VALUES (@id, @name)"
        insert_result = driver.execute(
            insert_sql,
            [
                ScalarQueryParameter("id", "INT64", 20),
                ScalarQueryParameter("name", "STRING", "test_select_value"),
            ],
        )
        assert isinstance(insert_result, ExecuteResult)

        # Select and verify using named parameters (@)
        select_sql = f"SELECT name FROM {table_name} WHERE id = @id"
        value_result = driver.execute(select_sql, [ScalarQueryParameter("id", "INT64", 20)])
        assert isinstance(value_result, SelectResult)
        assert value_result.rows is not None
        assert len(value_result.rows) == 1
        assert value_result.column_names is not None

        # Extract single value using column name
        value = value_result.rows[0][value_result.column_names[0]]
        assert value == "test_select_value"

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_select_one(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test select one functionality using named parameters."""
    table_name = f"{table_schema_prefix}.test_table_select_one"  # Unique name
    with bigquery_session.provide_session() as driver:
        # Create test table
        sql = f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = f"INSERT INTO {table_name} (id, name) VALUES (@id, @name)"
        insert_result = driver.execute(
            insert_sql,
            [
                ScalarQueryParameter("id", "INT64", 30),
                ScalarQueryParameter("name", "STRING", "test_select_one"),
            ],
        )
        assert isinstance(insert_result, ExecuteResult)

        # Select and verify using named parameters (@)
        select_sql = f"SELECT name, id FROM {table_name} WHERE id = @id"
        select_result = driver.execute(select_sql, [ScalarQueryParameter("id", "INT64", 30)])
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1

        row = select_result.get_first()
        assert row is not None
        assert row["name"] == "test_select_one"
        assert row["id"] == 30

        # Test not found
        not_found_result = driver.execute(select_sql, [ScalarQueryParameter("id", "INT64", 999)])
        assert isinstance(not_found_result, SelectResult)
        assert not_found_result.rows is not None
        assert len(not_found_result.rows) == 0

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_select_one_or_none(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test select one or none functionality using named parameters."""
    table_name = f"{table_schema_prefix}.test_table_select_one_none"  # Unique name
    with bigquery_session.provide_session() as driver:
        # Create test table
        sql = f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = f"INSERT INTO {table_name} (id, name) VALUES (@id, @name)"
        insert_result = driver.execute(
            insert_sql,
            [
                ScalarQueryParameter("id", "INT64", 40),
                ScalarQueryParameter("name", "STRING", "test_select_one_or_none"),
            ],
        )
        assert isinstance(insert_result, ExecuteResult)

        # Select and verify found
        select_sql = f"SELECT name, id FROM {table_name} WHERE id = @id"
        select_result = driver.execute(select_sql, [ScalarQueryParameter("id", "INT64", 40)])
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1

        row = select_result.get_first()
        assert row is not None
        assert row["name"] == "test_select_one_or_none"
        assert row["id"] == 40

        # Select and verify not found
        not_found_result = driver.execute(select_sql, [ScalarQueryParameter("id", "INT64", 999)])
        assert isinstance(not_found_result, SelectResult)
        assert not_found_result.rows is not None
        assert len(not_found_result.rows) == 0

        row_none = not_found_result.get_first()
        assert row_none is None

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_params_positional_list(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test parameter binding using positional placeholders (?) and a list of primitives."""
    with bigquery_session.provide_session() as driver:
        # Create test table
        create_sql = f"""
        CREATE TABLE {table_schema_prefix}.test_params_pos (
            id INT64,
            value STRING
        );
        """
        driver.execute_script(create_sql)

        insert_sql = f"INSERT INTO {table_schema_prefix}.test_params_pos (id, value) VALUES (?, ?)"
        params_list = [50, "positional_test"]
        insert_result = driver.execute(insert_sql, params_list)
        assert isinstance(insert_result, ExecuteResult)

        # Select and verify using positional parameters (?) and list
        select_sql = f"SELECT value, id FROM {table_schema_prefix}.test_params_pos WHERE id = ?"
        select_result = driver.execute(select_sql, [50])  # Note: single param needs to be in a list
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1

        row = select_result.rows[0]
        assert row["value"] == "positional_test"
        assert row["id"] == 50

        driver.execute_script(f"DROP TABLE IF EXISTS {table_schema_prefix}.test_params_pos")


@pytest.mark.xdist_group("bigquery")
def test_driver_params_named_dict(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test parameter binding using named placeholders (@) and a dictionary of primitives."""
    with bigquery_session.provide_session() as driver:
        # Create test table
        create_sql = f"""
        CREATE TABLE {table_schema_prefix}.test_params_dict (
            id INT64,
            name STRING,
            amount NUMERIC
        );
        """
        driver.execute_script(create_sql)

        # Insert using named parameters (@) and dict
        from decimal import Decimal

        insert_sql = f"INSERT INTO {table_schema_prefix}.test_params_dict (id, name, amount) VALUES (@id_val, @name_val, @amount_val)"
        params_dict = {"id_val": 60, "name_val": "dict_test", "amount_val": Decimal("123.45")}
        insert_result = driver.execute(insert_sql, params_dict)
        assert isinstance(insert_result, ExecuteResult)

        # Select and verify using named parameters (@) and dict
        select_sql = f"SELECT name, id, amount FROM {table_schema_prefix}.test_params_dict WHERE id = @search_id"
        select_result = driver.execute(select_sql, {"search_id": 60})
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1

        row = select_result.rows[0]
        assert row["name"] == "dict_test"
        assert row["id"] == 60
        assert row["amount"] == Decimal("123.45")

        driver.execute_script(f"DROP TABLE IF EXISTS {table_schema_prefix}.test_params_dict")


@pytest.mark.xdist_group("bigquery")
def test_driver_params_named_kwargs(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test parameter binding using named placeholders (@) and keyword arguments."""
    with bigquery_session.provide_session() as driver:
        # Create test table
        create_sql = f"""
        CREATE TABLE {table_schema_prefix}.test_params_kwargs (
            id INT64,
            label STRING,
            active BOOL
        );
        """
        driver.execute_script(create_sql)

        # Insert using named parameters (@) and kwargs
        insert_sql = f"INSERT INTO {table_schema_prefix}.test_params_kwargs (id, label, active) VALUES (@id_val, @label_val, @active_val)"
        insert_result = driver.execute(insert_sql, id_val=70, label_val="kwargs_test", active_val=True)
        assert isinstance(insert_result, ExecuteResult)

        # Select and verify using named parameters (@) and kwargs
        select_sql = f"SELECT label, id, active FROM {table_schema_prefix}.test_params_kwargs WHERE id = @search_id"
        select_result = driver.execute(select_sql, search_id=70)
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1

        row = select_result.rows[0]
        assert row["label"] == "kwargs_test"
        assert row["id"] == 70
        assert row["active"] is True

        driver.execute_script(f"DROP TABLE IF EXISTS {table_schema_prefix}.test_params_kwargs")


@pytest.mark.xdist_group("bigquery")
def test_execute_many_insert(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test execute_many functionality for batch inserts."""
    table_name = f"{table_schema_prefix}.test_many_table"
    with bigquery_session.provide_session() as driver:
        # Create test table
        create_sql = f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(create_sql)

        insert_sql = f"INSERT INTO {table_name} (id, name) VALUES (?, ?)"
        params_list = [[1, "name1"], [2, "name2"], [3, "name3"]]

        result = driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        # Note: BigQuery may not return exact rows_affected for batch operations

        # Verify all records were inserted
        select_result = driver.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert select_result.rows[0]["count"] == len(params_list)

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_update_operation(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test UPDATE operations."""
    table_name = f"{table_schema_prefix}.test_update_table"
    with bigquery_session.provide_session() as driver:
        # Create test table
        create_sql = f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(create_sql)

        # Insert a record first
        insert_result = driver.execute(f"INSERT INTO {table_name} (id, name) VALUES (?, ?)", [42, "original_name"])
        assert isinstance(insert_result, ExecuteResult)

        # Update the record
        update_result = driver.execute(f"UPDATE {table_name} SET name = ? WHERE id = ?", ["updated_name", 42])
        assert isinstance(update_result, ExecuteResult)

        # Verify the update
        select_result = driver.execute(f"SELECT name FROM {table_name} WHERE id = ?", [42])
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert select_result.rows[0]["name"] == "updated_name"

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_delete_operation(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test DELETE operations."""
    table_name = f"{table_schema_prefix}.test_delete_table"
    with bigquery_session.provide_session() as driver:
        # Create test table
        create_sql = f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(create_sql)

        # Insert a record first
        insert_result = driver.execute(f"INSERT INTO {table_name} (id, name) VALUES (?, ?)", [99, "to_delete"])
        assert isinstance(insert_result, ExecuteResult)

        # Delete the record
        delete_result = driver.execute(f"DELETE FROM {table_name} WHERE id = ?", [99])
        assert isinstance(delete_result, ExecuteResult)

        # Verify the deletion
        select_result = driver.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert select_result.rows[0]["count"] == 0

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")
