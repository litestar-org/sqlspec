"""Integration tests for Spanner parameter binding.

These tests verify that parameter binding works correctly with Spanner's
@name parameter style through SQLSpec.
"""

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver

pytestmark = pytest.mark.spanner


def test_named_at_parameter_basic(spanner_session: SpannerSyncDriver) -> None:
    """Test basic @name parameter binding."""
    result = spanner_session.select_value("SELECT @value", {"value": 42})
    assert result == 42


def test_named_at_parameter_string(spanner_session: SpannerSyncDriver) -> None:
    """Test @name parameter binding with string."""
    result = spanner_session.select_value("SELECT @text", {"text": "Hello World"})
    assert result == "Hello World"


def test_named_at_parameter_multiple(spanner_session: SpannerSyncDriver) -> None:
    """Test multiple @name parameters in single query."""
    result = spanner_session.select_value("SELECT @a + @b + @c", {"a": 10, "b": 20, "c": 30})
    assert result == 60


def test_named_at_parameter_reuse(spanner_session: SpannerSyncDriver) -> None:
    """Test reusing same parameter multiple times."""
    result = spanner_session.select_value("SELECT @val + @val", {"val": 25})
    assert result == 50


def test_dict_parameter_binding(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test dictionary parameter binding for INSERT."""
    user_id = str(uuid4())
    params = {"id": user_id, "name": "Dict Params", "email": "dict@example.com", "age": 35}

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", params
        )
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT name, age FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row["name"] == "Dict Params"
        assert row["age"] == 35

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_parameter_type_inference_int64(spanner_session: SpannerSyncDriver) -> None:
    """Test automatic INT64 type inference."""
    result = spanner_session.select_value("SELECT @num", {"num": 9223372036854775807})
    assert result == 9223372036854775807


def test_parameter_type_inference_float64(spanner_session: SpannerSyncDriver) -> None:
    """Test automatic FLOAT64 type inference."""
    result = spanner_session.select_value("SELECT @num", {"num": 3.14159})
    assert abs(result - 3.14159) < 1e-5


def test_parameter_type_inference_bool(spanner_session: SpannerSyncDriver) -> None:
    """Test automatic BOOL type inference."""
    assert spanner_session.select_value("SELECT @flag", {"flag": True}) is True
    assert spanner_session.select_value("SELECT @flag", {"flag": False}) is False


def test_parameter_type_inference_string(spanner_session: SpannerSyncDriver) -> None:
    """Test automatic STRING type inference."""
    result = spanner_session.select_value("SELECT @text", {"text": "Test String"})
    assert result == "Test String"


def test_null_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test NULL parameter handling."""
    result = spanner_session.select_value("SELECT @val", {"val": None})
    assert result is None


def test_null_parameter_in_insert(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test NULL parameter in INSERT operation."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Null Test", "email": None, "age": None},
        )
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row["name"] == "Null Test"
        assert row["email"] is None
        assert row["age"] is None

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_array_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test array parameter binding."""
    result = spanner_session.select_value("SELECT ARRAY_LENGTH(@arr)", {"arr": [1, 2, 3, 4, 5]})
    assert result == 5


def test_array_parameter_in_unnest(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test array parameter with UNNEST for IN clause."""
    user_ids = [str(uuid4()) for _ in range(3)]

    with spanner_config.provide_write_session() as session:
        for i, uid in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": uid, "name": f"Array Test {i}", "email": f"arr{i}@example.com", "age": 20 + i},
            )

    with spanner_config.provide_session() as session:
        rows = session.select(f"SELECT id, name FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids})
        assert len(rows) == 3

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_string_array_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test string array parameter."""
    result = spanner_session.select_value("SELECT ARRAY_LENGTH(@names)", {"names": ["Alice", "Bob", "Charlie"]})
    assert result == 3


def test_timestamp_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test TIMESTAMP parameter binding."""
    ts = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    result = spanner_session.select_value("SELECT @ts", {"ts": ts})
    assert result.year == 2024
    assert result.month == 6
    assert result.day == 15


def test_date_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test DATE parameter binding."""
    d = date(2024, 12, 25)
    result = spanner_session.select_value("SELECT @d", {"d": d})
    if isinstance(result, datetime):
        assert result.year == 2024
        assert result.month == 12
        assert result.day == 25
    else:
        assert result.year == 2024
        assert result.month == 12
        assert result.day == 25


def test_empty_string_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test empty string parameter."""
    result = spanner_session.select_value("SELECT @text", {"text": ""})
    assert result == ""


def test_unicode_string_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test Unicode string parameter."""
    unicode_text = "Hello 日本語 🌍 Привет"
    result = spanner_session.select_value("SELECT @text", {"text": unicode_text})
    assert result == unicode_text


def test_special_characters_in_string(spanner_session: SpannerSyncDriver) -> None:
    """Test special characters in string parameter."""
    special = "Line1\nLine2\tTabbed\"Quoted'Single"
    result = spanner_session.select_value("SELECT @text", {"text": special})
    assert result == special


def test_parameter_name_with_underscore(spanner_session: SpannerSyncDriver) -> None:
    """Test parameter names with underscores."""
    result = spanner_session.select_value("SELECT @user_name", {"user_name": "TestUser"})
    assert result == "TestUser"


def test_parameter_name_with_numbers(spanner_session: SpannerSyncDriver) -> None:
    """Test parameter names with numbers."""
    result = spanner_session.select_value("SELECT @param1 + @param2", {"param1": 100, "param2": 200})
    assert result == 300


def test_large_integer_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test large integer parameter values."""
    large_int = 2**62
    result = spanner_session.select_value("SELECT @big", {"big": large_int})
    assert result == large_int


def test_negative_integer_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test negative integer parameter."""
    result = spanner_session.select_value("SELECT @num", {"num": -12345})
    assert result == -12345


def test_zero_parameter(spanner_session: SpannerSyncDriver) -> None:
    """Test zero value parameter."""
    assert spanner_session.select_value("SELECT @num", {"num": 0}) == 0
    assert spanner_session.select_value("SELECT @num", {"num": 0.0}) == 0.0


def test_parameter_in_where_clause(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test parameter binding in WHERE clause."""
    user_ids = [str(uuid4()) for _ in range(3)]

    with spanner_config.provide_write_session() as session:
        for i, uid in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": uid, "name": f"Where Test {i}", "email": f"where{i}@example.com", "age": 25 + i * 5},
            )

    with spanner_config.provide_session() as session:
        rows = session.select(
            f"SELECT id, name FROM {test_users_table} WHERE age >= @min_age AND age <= @max_age",
            {"min_age": 25, "max_age": 30},
        )
        names = [r["name"] for r in rows if r["name"].startswith("Where Test")]
        assert len(names) == 2

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_parameter_in_order_by_limit(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test parameter in LIMIT clause (Spanner supports parameterized LIMIT)."""
    user_ids = [str(uuid4()) for _ in range(5)]

    with spanner_config.provide_write_session() as session:
        for i, uid in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": uid, "name": f"Limit Test {i}", "email": f"limit{i}@example.com", "age": i + 1},
            )

    with spanner_config.provide_session() as session:
        rows = session.select(
            f"SELECT name FROM {test_users_table} WHERE name LIKE 'Limit Test%' ORDER BY age LIMIT @lim", {"lim": 3}
        )
        assert len(rows) == 3

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})
