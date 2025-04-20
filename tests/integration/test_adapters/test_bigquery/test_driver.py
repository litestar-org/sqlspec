from __future__ import annotations

import pytest
from google.cloud.bigquery import ScalarQueryParameter

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.exceptions import NotFoundError


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
        results = driver.select(f"SELECT COUNT(*) AS count FROM {table_name} WHERE name LIKE 'script_test%'")
        assert results[0]["count"] == 2

        value = driver.select_value(
            f"SELECT name FROM {table_name} WHERE id = @id",
            [ScalarQueryParameter("id", "INT64", 1)],
        )
        assert value == "script_test"
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
        driver.insert_update_delete(insert_sql, params)

        # Verify insertion
        results = driver.select(
            f"SELECT name FROM {table_name} WHERE id = @id",
            [ScalarQueryParameter("id", "INT64", 1)],
        )
        assert len(results) == 1
        assert results[0]["name"] == "test_insert"
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
        driver.insert_update_delete(
            insert_sql,
            [
                ScalarQueryParameter("id", "INT64", 10),
                ScalarQueryParameter("name", "STRING", "test_select"),
            ],
        )

        # Select and verify using named parameters (@)
        select_sql = f"SELECT name, id FROM {table_name} WHERE id = @id"
        results = driver.select(select_sql, [ScalarQueryParameter("id", "INT64", 10)])
        assert len(results) == 1
        assert results[0]["name"] == "test_select"
        assert results[0]["id"] == 10
        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_select_value(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test select_value functionality using named parameters."""
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
        driver.insert_update_delete(
            insert_sql,
            [
                ScalarQueryParameter("id", "INT64", 20),
                ScalarQueryParameter("name", "STRING", "test_select_value"),
            ],
        )

        # Select and verify using named parameters (@)
        select_sql = f"SELECT name FROM {table_name} WHERE id = @id"
        value = driver.select_value(select_sql, [ScalarQueryParameter("id", "INT64", 20)])
        assert value == "test_select_value"
        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_select_one(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test select_one functionality using named parameters."""
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
        driver.insert_update_delete(
            insert_sql,
            [
                ScalarQueryParameter("id", "INT64", 30),
                ScalarQueryParameter("name", "STRING", "test_select_one"),
            ],
        )

        # Select and verify using named parameters (@)
        select_sql = f"SELECT name, id FROM {table_name} WHERE id = @id"
        row = driver.select_one(select_sql, [ScalarQueryParameter("id", "INT64", 30)])
        assert row["name"] == "test_select_one"
        assert row["id"] == 30

        # Test not found
        with pytest.raises(NotFoundError):
            driver.select_one(select_sql, [ScalarQueryParameter("id", "INT64", 999)])

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("bigquery")
def test_driver_select_one_or_none(bigquery_session: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test select_one_or_none functionality using named parameters."""
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
        driver.insert_update_delete(
            insert_sql,
            [
                ScalarQueryParameter("id", "INT64", 40),
                ScalarQueryParameter("name", "STRING", "test_select_one_or_none"),
            ],
        )

        # Select and verify found
        select_sql = f"SELECT name, id FROM {table_name} WHERE id = @id"
        row = driver.select_one_or_none(select_sql, [ScalarQueryParameter("id", "INT64", 40)])
        assert row is not None
        assert row["name"] == "test_select_one_or_none"
        assert row["id"] == 40

        # Select and verify not found
        row_none = driver.select_one_or_none(select_sql, [ScalarQueryParameter("id", "INT64", 999)])
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
        affected = driver.insert_update_delete(insert_sql, params_list)
        assert affected >= 0  # BigQuery DML might not return exact rows

        # Select and verify using positional parameters (?) and list
        select_sql = f"SELECT value, id FROM {table_schema_prefix}.test_params_pos WHERE id = ?"
        row = driver.select_one(select_sql, [50])  # Note: single param needs to be in a list
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
        driver.insert_update_delete(insert_sql, params_dict)

        # Select and verify using named parameters (@) and dict
        select_sql = f"SELECT name, id, amount FROM {table_schema_prefix}.test_params_dict WHERE id = @search_id"
        row = driver.select_one(select_sql, {"search_id": 60})
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
        driver.insert_update_delete(insert_sql, id_val=70, label_val="kwargs_test", active_val=True)

        # Select and verify using named parameters (@) and kwargs
        select_sql = f"SELECT label, id, active FROM {table_schema_prefix}.test_params_kwargs WHERE id = @search_id"
        row = driver.select_one(select_sql, search_id=70)
        assert row["label"] == "kwargs_test"
        assert row["id"] == 70
        assert row["active"] is True

        driver.execute_script(f"DROP TABLE IF EXISTS {table_schema_prefix}.test_params_kwargs")
