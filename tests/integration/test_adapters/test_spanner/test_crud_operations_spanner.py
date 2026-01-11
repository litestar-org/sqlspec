"""Integration tests for Spanner CRUD operations using SQLSpec interface.

These tests verify that DML operations (INSERT, UPDATE, DELETE) work correctly
through the SQLSpec session interface, not raw SDK calls.
"""

from uuid import uuid4

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver

pytestmark = pytest.mark.spanner


def test_select_one(spanner_session: SpannerSyncDriver) -> None:
    """Test basic SELECT 1 query."""
    assert spanner_session.select_value("SELECT 1") == 1


def test_select_string(spanner_session: SpannerSyncDriver) -> None:
    """Test SELECT with string value."""
    result = spanner_session.select_value("SELECT 'hello'")
    assert result == "hello"


def test_select_with_parameters(spanner_session: SpannerSyncDriver) -> None:
    """Test SELECT with parameterized query."""
    result = spanner_session.select_value("SELECT @value", {"value": 42})
    assert result == 42


def test_insert_through_session(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test INSERT operation through session.execute() - the key SQLSpec interface."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Test User", "email": "test@example.com", "age": 30},
        )
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT id, name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row is not None
        assert row["name"] == "Test User"
        assert row["email"] == "test@example.com"
        assert row["age"] == 30

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_update_through_session(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test UPDATE operation through session.execute()."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Original Name", "email": "original@example.com", "age": 25},
        )

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"UPDATE {test_users_table} SET name = @name, age = @age WHERE id = @id",
            {"id": user_id, "name": "Updated Name", "age": 35},
        )
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT name, age FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row is not None
        assert row["name"] == "Updated Name"
        assert row["age"] == 35

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_delete_through_session(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test DELETE operation through session.execute()."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "To Delete", "email": "delete@example.com", "age": 40},
        )

    with spanner_config.provide_session() as session:
        row = session.select_one_or_none(f"SELECT id FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row is not None

    with spanner_config.provide_write_session() as session:
        result = session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one_or_none(f"SELECT id FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row is None


def test_full_crud_cycle(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test full CRUD cycle: INSERT -> SELECT -> UPDATE -> SELECT -> DELETE -> SELECT."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        insert_result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "CRUD Test", "email": "crud@example.com", "age": 30},
        )
        assert insert_result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT id, name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row["name"] == "CRUD Test"
        assert row["email"] == "crud@example.com"
        assert row["age"] == 30

    with spanner_config.provide_write_session() as session:
        update_result = session.execute(
            f"UPDATE {test_users_table} SET name = @name WHERE id = @id", {"id": user_id, "name": "Updated CRUD"}
        )
        assert update_result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT name FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row["name"] == "Updated CRUD"

    with spanner_config.provide_write_session() as session:
        delete_result = session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert delete_result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one_or_none(f"SELECT id FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row is None


def test_select_multiple_rows(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test selecting multiple rows with INSERT through session."""
    user_ids = [str(uuid4()) for _ in range(3)]

    with spanner_config.provide_write_session() as session:
        for i, uid in enumerate(user_ids):
            result = session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": uid, "name": f"User {i}", "email": f"user{i}@example.com", "age": 20 + i},
            )
            assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        results = session.select(
            f"SELECT id, name FROM {test_users_table} WHERE age >= @min_age ORDER BY age", {"min_age": 20}
        )
        assert len(results) >= 3
        names = [r["name"] for r in results if r["name"].startswith("User ")]
        assert "User 0" in names
        assert "User 1" in names
        assert "User 2" in names

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_result_metadata(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that SQLResult contains proper metadata."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Metadata Test", "email": "meta@example.com", "age": 28},
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1
        assert result.statement is not None

    with spanner_config.provide_session() as session:
        select_result = session.execute(
            f"SELECT id, name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id}
        )

        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.column_names is not None
        assert "id" in select_result.column_names
        assert "name" in select_result.column_names

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})
