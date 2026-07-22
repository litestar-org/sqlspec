"""Spanner-specific parameter variant coverage.

These tests intentionally stay adapter-local because Spanner integration tests
are optional-gated and its read/write session behavior is not part of the
shared C5 contract for active default adapters.
"""

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver

pytestmark = [pytest.mark.spanner, pytest.mark.xdist_group("spanner")]


def test_spanner_native_named_at_parameters_allow_reuse_and_identifier_variants(
    spanner_session: SpannerSyncDriver,
) -> None:
    """Spanner binds native @name parameters with reuse, underscores, and numeric suffixes."""
    row = spanner_session.select_one(
        "SELECT @user_name AS user_name, @param1 + @param2 AS total, @user_name AS repeated",
        {"user_name": "TestUser", "param1": 100, "param2": 200},
    )

    assert row["user_name"] == "TestUser"
    assert row["total"] == 300
    assert row["repeated"] == "TestUser"


def test_spanner_native_types_round_trip_through_read_session(spanner_session: SpannerSyncDriver) -> None:
    """Spanner infers native scalar parameter types for read-only SELECT statements."""
    timestamp = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    row = spanner_session.select_one(
        "SELECT @num AS num, @ratio AS ratio, @flag AS flag, @text AS text, @ts AS ts",
        {"num": 9223372036854775807, "ratio": 3.14159, "flag": True, "text": "", "ts": timestamp},
    )

    assert row["num"] == 9223372036854775807
    assert abs(row["ratio"] - 3.14159) < 1e-5
    assert row["flag"] is True
    assert row["text"] == ""
    assert row["ts"].year == 2024
    assert row["ts"].month == 6
    assert row["ts"].day == 15


def test_spanner_date_parameter_allows_emulator_datetime_return(spanner_session: SpannerSyncDriver) -> None:
    """Spanner emulator may return DATE parameters as datetime-like objects."""
    result = spanner_session.select_value("SELECT @value", {"value": date(2024, 12, 25)})

    assert result.year == 2024
    assert result.month == 12
    assert result.day == 25


def test_spanner_null_parameters_round_trip_through_write_and_read_sessions(
    spanner_config: SpannerSyncConfig, test_users_table: str
) -> None:
    """Spanner preserves NULL values across explicit write and read sessions."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        result = session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "Null Test", "email": None, "age": None},
        )
        assert result.rows_affected == 1

    with spanner_config.provide_read_session() as session:
        row = session.select_one(f"SELECT name, email, age FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row["name"] == "Null Test"
        assert row["email"] is None
        assert row["age"] is None

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_spanner_array_parameter_with_unnest_uses_read_and_write_sessions(
    spanner_config: SpannerSyncConfig, test_users_table: str
) -> None:
    """Spanner binds ARRAY parameters for UNNEST predicates across session modes."""
    user_ids = [str(uuid4()) for _ in range(3)]

    with spanner_config.provide_write_session() as session:
        for index, user_id in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": user_id, "name": f"Array Test {index}", "email": f"arr{index}@example.com", "age": 20},
            )

    with spanner_config.provide_read_session() as session:
        rows = session.select(
            f"SELECT id, name FROM {test_users_table} WHERE id IN UNNEST(@ids) ORDER BY name", {"ids": user_ids}
        )

    assert [row["name"] for row in rows] == ["Array Test 0", "Array Test 1", "Array Test 2"]

    with spanner_config.provide_write_session() as session:
        for user_id in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_spanner_parameterized_limit_uses_native_parameter_syntax(
    spanner_config: SpannerSyncConfig, test_users_table: str
) -> None:
    """Spanner accepts native @name parameters in LIMIT clauses."""
    user_ids = [str(uuid4()) for _ in range(5)]

    with spanner_config.provide_write_session() as session:
        for index, user_id in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": user_id, "name": f"Limit Test {index}", "email": f"limit{index}@example.com", "age": index},
            )

    with spanner_config.provide_read_session() as session:
        rows = session.select(
            f"SELECT name FROM {test_users_table} WHERE name LIKE 'Limit Test%' ORDER BY age LIMIT @row_limit",
            {"row_limit": 3},
        )

    assert [row["name"] for row in rows] == ["Limit Test 0", "Limit Test 1", "Limit Test 2"]

    with spanner_config.provide_write_session() as session:
        for user_id in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})
