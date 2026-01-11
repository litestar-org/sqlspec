"""Comprehensive driver tests for Spanner SQLSpec interface.

These tests verify all driver methods work correctly through SQLSpec,
not raw SDK calls.
"""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver

pytestmark = pytest.mark.spanner


def test_connection_pooling(spanner_session: "SpannerSyncDriver") -> None:
    """Test that we can acquire a session and execute a simple query."""
    result = spanner_session.select_value("SELECT 1")
    assert result == 1


def test_session_management(spanner_config: "SpannerSyncConfig") -> None:
    """Test session lifecycle."""
    with spanner_config.provide_session() as session:
        assert session.select_value("SELECT 1") == 1


def test_driver_select_value(spanner_session: "SpannerSyncDriver") -> None:
    """Test select_value() returns scalar value."""
    result = spanner_session.select_value("SELECT 42")
    assert result == 42


def test_driver_select_value_with_params(spanner_session: "SpannerSyncDriver") -> None:
    """Test select_value() with parameters."""
    result = spanner_session.select_value("SELECT @val", {"val": 100})
    assert result == 100


def test_driver_select_value_string(spanner_session: "SpannerSyncDriver") -> None:
    """Test select_value() returns string."""
    result = spanner_session.select_value("SELECT 'test_string'")
    assert result == "test_string"


def test_driver_select_one(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test select_one() returns single row as dict."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Select One", "email": "selectone@example.com", "age": 25},
        )

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT id, name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row is not None
        assert str(row["id"]) == user_id
        assert row["name"] == "Select One"
        assert row["email"] == "selectone@example.com"
        assert row["age"] == 25

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_driver_select_one_or_none_found(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test select_one_or_none() returns row when found."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Found User", "email": "found@example.com", "age": 30},
        )

    with spanner_config.provide_session() as session:
        row = session.select_one_or_none(f"SELECT id, name FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row is not None
        assert row["name"] == "Found User"

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_driver_select_one_or_none_not_found(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test select_one_or_none() returns None when not found."""
    with spanner_config.provide_session() as session:
        row = session.select_one_or_none(f"SELECT id FROM {test_users_table} WHERE id = @id", {"id": "nonexistent-id"})
        assert row is None


def test_driver_select_returns_list(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test select() returns list of dicts."""
    user_ids = [str(uuid4()) for _ in range(3)]

    with spanner_config.provide_write_session() as session:
        for i, uid in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": uid, "name": f"User {i}", "email": f"user{i}@example.com", "age": 20 + i},
            )

    with spanner_config.provide_session() as session:
        rows = session.select(
            f"SELECT id, name, age FROM {test_users_table} WHERE age >= @min_age ORDER BY age", {"min_age": 20}
        )
        assert len(rows) >= 3
        names = [r["name"] for r in rows if r["name"].startswith("User ")]
        assert "User 0" in names
        assert "User 1" in names
        assert "User 2" in names

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_driver_execute_returns_sql_result(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test execute() returns SQLResult with metadata."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Result Test", "email": "result@example.com", "age": 35},
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1
        assert result.statement is not None

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_driver_execute_select_returns_data(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test execute() with SELECT returns data in SQLResult."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Data Test", "email": "data@example.com", "age": 40},
        )

    with spanner_config.provide_session() as session:
        result = session.execute(f"SELECT id, name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id})

        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.column_names is not None
        assert "id" in result.column_names
        assert "name" in result.column_names

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_driver_parameter_style_named_at(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test @name parameter style works correctly."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) "
            f"VALUES (@user_id, @user_name, @user_email, @user_age)",
            {"user_id": user_id, "user_name": "Param Test", "user_email": "param@example.com", "user_age": 45},
        )
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(
            f"SELECT name, age FROM {test_users_table} WHERE id = @id AND age = @age", {"id": user_id, "age": 45}
        )
        assert row["name"] == "Param Test"
        assert row["age"] == 45

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_driver_data_type_int64(spanner_session: "SpannerSyncDriver") -> None:
    """Test INT64 data type handling."""
    result = spanner_session.select_value("SELECT @val", {"val": 9223372036854775807})
    assert result == 9223372036854775807


def test_driver_data_type_float64(spanner_session: "SpannerSyncDriver") -> None:
    """Test FLOAT64 data type handling."""
    result = spanner_session.select_value("SELECT @val", {"val": 3.14159265359})
    assert abs(result - 3.14159265359) < 1e-10


def test_driver_data_type_string(spanner_session: "SpannerSyncDriver") -> None:
    """Test STRING data type handling."""
    test_str = "Hello, Spanner! 日本語"
    result = spanner_session.select_value("SELECT @val", {"val": test_str})
    assert result == test_str


def test_driver_data_type_bool(spanner_session: "SpannerSyncDriver") -> None:
    """Test BOOL data type handling."""
    assert spanner_session.select_value("SELECT @val", {"val": True}) is True
    assert spanner_session.select_value("SELECT @val", {"val": False}) is False


def test_driver_data_type_timestamp(spanner_session: "SpannerSyncDriver") -> None:
    """Test TIMESTAMP data type handling."""
    ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
    result = spanner_session.select_value("SELECT @val", {"val": ts})
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15


def test_driver_data_type_null(spanner_session: "SpannerSyncDriver") -> None:
    """Test NULL handling."""
    result = spanner_session.select_value("SELECT @val", {"val": None})
    assert result is None


def test_driver_multiple_operations_same_session(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test multiple operations in a single session."""
    user_ids = [str(uuid4()) for _ in range(2)]

    with spanner_config.provide_write_session() as session:
        result1 = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_ids[0], "name": "User A", "email": "a@example.com", "age": 25},
        )
        result2 = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_ids[1], "name": "User B", "email": "b@example.com", "age": 30},
        )

        assert result1.rows_affected == 1
        assert result2.rows_affected == 1

    with spanner_config.provide_session() as session:
        rows = session.select(f"SELECT id, name FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids})
        assert len(rows) == 2

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_driver_array_parameter(spanner_session: "SpannerSyncDriver") -> None:
    """Test array parameter expansion."""
    result = spanner_session.select_value("SELECT ARRAY_LENGTH(@arr)", {"arr": [1, 2, 3, 4, 5]})
    assert result == 5


def test_driver_empty_result_set(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test handling empty result sets."""
    with spanner_config.provide_session() as session:
        rows = session.select(f"SELECT id FROM {test_users_table} WHERE id = @id", {"id": "definitely-does-not-exist"})
        assert rows == []


def test_driver_large_string(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test handling large strings."""
    user_id = str(uuid4())
    large_name = "A" * 100

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": large_name, "email": "large@example.com", "age": 50},
        )
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT name FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row["name"] == large_name

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_driver_update_multiple_columns(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test UPDATE affecting multiple columns."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Original", "email": "original@example.com", "age": 20},
        )

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"UPDATE {test_users_table} SET name = @name, email = @email, age = @age WHERE id = @id",
            {"id": user_id, "name": "Updated", "email": "updated@example.com", "age": 21},
        )
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row["name"] == "Updated"
        assert row["email"] == "updated@example.com"
        assert row["age"] == 21

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_driver_delete_with_condition(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test DELETE with WHERE condition."""
    user_ids = [str(uuid4()) for _ in range(3)]

    with spanner_config.provide_write_session() as session:
        for i, uid in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": uid, "name": f"Delete Test {i}", "email": f"delete{i}@example.com", "age": 10 + i},
            )

    with spanner_config.provide_write_session() as session:
        result = session.execute(f"DELETE FROM {test_users_table} WHERE age < @max_age", {"max_age": 12})
        assert result.rows_affected == 2

    with spanner_config.provide_session() as session:
        rows = session.select(f"SELECT id FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids})
        assert len(rows) == 1

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_driver_column_order_preserved(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """Test that column order is preserved in results."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Column Order", "email": "order@example.com", "age": 55},
        )

    with spanner_config.provide_session() as session:
        result = session.execute(f"SELECT age, name, id FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert result.column_names == ["age", "name", "id"]

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})
