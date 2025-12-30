"""Integration tests for Spanner exception mapping.

These tests verify that Spanner SDK exceptions are properly mapped
to SQLSpec exception types.
"""

from uuid import uuid4

import pytest

from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.exceptions import NotFoundError, SQLConversionError, SQLParsingError, UniqueViolationError
from sqlspec.exceptions import NotFoundError as SQLSpecNotFoundError

pytestmark = pytest.mark.spanner


def test_dml_in_read_only_session(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that DML in read-only session raises SQLConversionError."""
    with spanner_config.provide_session() as session:
        with pytest.raises(SQLConversionError, match="Cannot execute DML"):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": str(uuid4()), "name": "Test", "email": "test@example.com", "age": 30},
            )


def test_sql_parsing_error_invalid_syntax(spanner_config: SpannerSyncConfig) -> None:
    """Test that invalid SQL syntax raises an error.

    Note: SQLSpec may raise SQLParsingError for SQL parsing failures or
    SQLConversionError if the invalid SQL can't be classified as a SELECT
    and is attempted as DML in a read-only context.
    """
    with spanner_config.provide_session() as session:
        with pytest.raises((SQLParsingError, SQLConversionError)):
            session.execute("selectall * FORM users")


def test_sql_parsing_error_invalid_table(spanner_config: SpannerSyncConfig) -> None:
    """Test that querying non-existent table raises appropriate error."""
    with spanner_config.provide_session() as session:
        with pytest.raises((NotFoundError, SQLParsingError)):
            session.select(f"SELECT * FROM nonexistent_table_{uuid4().hex[:8]}")


def test_unique_violation_duplicate_key(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that duplicate primary key raises UniqueViolationError."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "First", "email": "first@example.com", "age": 25},
        )

    with spanner_config.provide_write_session() as session:
        with pytest.raises(UniqueViolationError):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": user_id, "name": "Duplicate", "email": "dup@example.com", "age": 30},
            )

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_invalid_parameter_type(spanner_config: SpannerSyncConfig) -> None:
    """Test that invalid parameter type raises SQLParsingError."""
    with spanner_config.provide_session() as session:
        with pytest.raises(SQLParsingError):
            session.select_value("SELECT @num + 1", {"num": "not_a_number"})


def test_execute_many_in_read_only_session(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that execute_many in read-only session raises SQLConversionError."""
    parameters = [
        {"id": str(uuid4()), "name": "User 1", "email": "u1@example.com", "age": 20},
        {"id": str(uuid4()), "name": "User 2", "email": "u2@example.com", "age": 25},
    ]

    with spanner_config.provide_session() as session:
        with pytest.raises(SQLConversionError, match="execute_many requires"):
            session.execute_many(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", parameters
            )


def test_select_one_no_results(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that select_one with no results raises appropriate error."""

    with spanner_config.provide_session() as session:
        with pytest.raises(SQLSpecNotFoundError):
            session.select_one(f"SELECT * FROM {test_users_table} WHERE id = @id", {"id": "definitely-does-not-exist"})


def test_invalid_column_name(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that invalid column name raises error."""
    with spanner_config.provide_session() as session:
        with pytest.raises((SQLParsingError, NotFoundError)):
            session.select(f"SELECT nonexistent_column FROM {test_users_table}")


def test_update_nonexistent_row_no_error(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that UPDATE on non-existent row succeeds with 0 rows affected."""
    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"UPDATE {test_users_table} SET name = @name WHERE id = @id",
            {"id": "nonexistent-id", "name": "Should Not Exist"},
        )
        assert result.rows_affected == 0


def test_delete_nonexistent_row_no_error(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that DELETE on non-existent row succeeds with 0 rows affected."""
    with spanner_config.provide_write_session() as session:
        result = session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": "nonexistent-id"})
        assert result.rows_affected == 0
