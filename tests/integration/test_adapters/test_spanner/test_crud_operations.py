"""Integration tests for Spanner CRUD operations."""

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database

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


def test_insert_select_update_delete(
    spanner_config: SpannerSyncConfig, spanner_database: "Database", test_users_table: str
) -> None:
    """Test full CRUD operations with transaction context."""
    user_id = str(uuid4())
    database = spanner_database

    def insert_user(transaction: "Any") -> None:
        transaction.execute_update(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            params={"id": user_id, "name": "Test User", "email": "test@example.com", "age": 30},
            param_types={
                "id": {"code": "STRING"},
                "name": {"code": "STRING"},
                "email": {"code": "STRING"},
                "age": {"code": "INT64"},
            },
        )

    database.run_in_transaction(insert_user)  # type: ignore[no-untyped-call]

    with spanner_config.provide_session() as session:
        result = session.select_one(
            f"SELECT id, name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id}
        )
        assert result is not None
        assert result["name"] == "Test User"
        assert result["email"] == "test@example.com"
        assert result["age"] == 30

    def update_user(transaction: "Any") -> None:
        transaction.execute_update(
            f"UPDATE {test_users_table} SET name = @name WHERE id = @id",
            params={"id": user_id, "name": "Updated User"},
            param_types={"id": {"code": "STRING"}, "name": {"code": "STRING"}},
        )

    database.run_in_transaction(update_user)  # type: ignore[no-untyped-call]

    with spanner_config.provide_session() as session:
        result = session.select_one(f"SELECT name FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert result is not None
        assert result["name"] == "Updated User"

    def delete_user(transaction: "Any") -> None:
        transaction.execute_update(
            f"DELETE FROM {test_users_table} WHERE id = @id",
            params={"id": user_id},
            param_types={"id": {"code": "STRING"}},
        )

    database.run_in_transaction(delete_user)  # type: ignore[no-untyped-call]

    with spanner_config.provide_session() as session:
        result = session.select_one_or_none(f"SELECT id FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert result is None


def test_select_multiple_rows(
    spanner_config: SpannerSyncConfig, spanner_database: "Database", test_users_table: str
) -> None:
    """Test selecting multiple rows."""
    database = spanner_database
    user_ids = [str(uuid4()) for _ in range(3)]

    def insert_users(transaction: "Any") -> None:
        for i, uid in enumerate(user_ids):
            transaction.execute_update(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                params={"id": uid, "name": f"User {i}", "email": f"user{i}@example.com", "age": 20 + i},
                param_types={
                    "id": {"code": "STRING"},
                    "name": {"code": "STRING"},
                    "email": {"code": "STRING"},
                    "age": {"code": "INT64"},
                },
            )

    database.run_in_transaction(insert_users)  # type: ignore[no-untyped-call]

    with spanner_config.provide_session() as session:
        results = session.select(
            f"SELECT id, name FROM {test_users_table} WHERE age >= @min_age ORDER BY age", {"min_age": 20}
        )
        assert len(results) == 3
        assert results[0]["name"] == "User 0"
        assert results[2]["name"] == "User 2"

    def cleanup(transaction: "Any") -> None:
        for uid in user_ids:
            transaction.execute_update(
                f"DELETE FROM {test_users_table} WHERE id = @id",
                params={"id": uid},
                param_types={"id": {"code": "STRING"}},
            )

    database.run_in_transaction(cleanup)  # type: ignore[no-untyped-call]
