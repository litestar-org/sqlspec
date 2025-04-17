import sqlite3

import pytest

from sqlspec.adapters.sqlite import Sqlite


def test_driver() -> None:
    """Test driver components."""
    adapter = Sqlite()

    # Check SQLite version for RETURNING support (3.35.0+)
    sqlite_version = sqlite3.sqlite_version_info
    returning_supported = sqlite_version >= (3, 35, 0)

    # Test provide_session
    with adapter.provide_session() as session:
        assert session is not None

        # Test execute_script for schema changes (no parameters)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
        """
        # Use execute_script without parameters for DDL
        session.execute_script(create_table_sql, None)

        inserted_id = None
        try:
            if returning_supported:
                # Test insert_update_delete_returning with RETURNING
                insert_sql = """
                INSERT INTO test_table (name)
                VALUES (:name)
                RETURNING id, name
                """
                params = {"name": "test_name"}
                result = session.insert_update_delete_returning(insert_sql, params)

                assert result is not None, "insert_update_delete_returning should return a result"
                assert isinstance(result, dict), "Result should be a dictionary"
                assert result.get("name") == "test_name", "Inserted name does not match"
                assert result.get("id") is not None, "Returned ID should not be None"
                inserted_id = result["id"]  # Store the returned ID
            else:
                # Alternative for older SQLite: Insert and then get last row id
                insert_sql_no_returning = "INSERT INTO test_table (name) VALUES (:name)"
                params = {"name": "test_name"}
                # Use insert_update_delete for single statement with params
                session.insert_update_delete(insert_sql_no_returning, params)
                # Get the last inserted ID using select_value
                select_last_id_sql = "SELECT last_insert_rowid()"
                # select_value typically doesn't take parameters if the SQL doesn't need them
                inserted_id = session.select_value(select_last_id_sql)
                assert inserted_id is not None, "Could not retrieve last inserted ID using last_insert_rowid()"

            # Ensure we have an ID before proceeding
            assert inserted_id is not None, "inserted_id was not set"

            # Test select using the inserted ID
            select_sql = "SELECT id, name FROM test_table WHERE id = :id"
            params_select = {"id": inserted_id}
            results = session.select(select_sql, params_select)
            assert len(results) == 1, "Select should return one row for the inserted ID"
            assert results[0].get("name") == "test_name", "Selected name does not match"
            assert results[0].get("id") == inserted_id, "Selected ID does not match"

            # Test select_one using the inserted ID
            select_one_sql = "SELECT id, name FROM test_table WHERE id = :id"
            params_select_one = {"id": inserted_id}
            result_one = session.select_one(select_one_sql, params_select_one)
            assert result_one is not None, "select_one should return a result for the inserted ID"
            assert isinstance(result_one, dict), "select_one result should be a dictionary"
            assert result_one.get("name") == "test_name", "select_one name does not match"
            assert result_one.get("id") == inserted_id, "select_one ID does not match"

            # Test select_value using the actual inserted ID
            value_sql = "SELECT name FROM test_table WHERE id = :id"
            params_value = {"id": inserted_id}
            value = session.select_value(value_sql, params_value)
            assert value == "test_name", "select_value returned incorrect value"

        except Exception as e:
            # Fail the test if any database operation raises an exception
            pytest.fail(f"Database operation failed: {e}")

        finally:
            # Clean up: Drop the test table
            # Use execute_script without parameters for DDL
            session.execute_script("DROP TABLE IF EXISTS test_table", None)
