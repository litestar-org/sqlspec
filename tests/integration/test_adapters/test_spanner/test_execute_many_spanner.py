"""Integration tests for Spanner execute_many (batch) operations.

These tests verify that batch DML operations work correctly through
SQLSpec's execute_many() interface.
"""

from uuid import uuid4

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.exceptions import SQLConversionError

pytestmark = pytest.mark.spanner


def test_execute_many_basic_insert(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test basic batch INSERT with execute_many()."""
    user_ids = [str(uuid4()) for _ in range(5)]
    parameters = [
        {"id": uid, "name": f"Batch User {i}", "email": f"batch{i}@example.com", "age": 20 + i}
        for i, uid in enumerate(user_ids)
    ]

    with spanner_config.provide_write_session() as session:
        result = session.execute_many(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", parameters
        )
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 5

    with spanner_config.provide_session() as session:
        rows = session.select(
            f"SELECT id, name FROM {test_users_table} WHERE id IN UNNEST(@ids) ORDER BY name", {"ids": user_ids}
        )
        assert len(rows) == 5
        names = [r["name"] for r in rows]
        assert "Batch User 0" in names
        assert "Batch User 4" in names

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_execute_many_update(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test batch UPDATE with execute_many()."""
    user_ids = [str(uuid4()) for _ in range(3)]

    with spanner_config.provide_write_session() as session:
        for i, uid in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": uid, "name": f"Original {i}", "email": f"orig{i}@example.com", "age": 30 + i},
            )

    update_params = [{"id": uid, "name": f"Updated {i}", "age": 40 + i} for i, uid in enumerate(user_ids)]

    with spanner_config.provide_write_session() as session:
        result = session.execute_many(
            f"UPDATE {test_users_table} SET name = @name, age = @age WHERE id = @id", update_params
        )
        assert result.rows_affected == 3

    with spanner_config.provide_session() as session:
        rows = session.select(
            f"SELECT id, name, age FROM {test_users_table} WHERE id IN UNNEST(@ids) ORDER BY age", {"ids": user_ids}
        )
        assert len(rows) == 3
        assert rows[0]["name"] == "Updated 0"
        assert rows[0]["age"] == 40
        assert rows[2]["name"] == "Updated 2"
        assert rows[2]["age"] == 42

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_execute_many_delete(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test batch DELETE with execute_many()."""
    user_ids = [str(uuid4()) for _ in range(4)]

    with spanner_config.provide_write_session() as session:
        for i, uid in enumerate(user_ids):
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": uid, "name": f"ToDelete {i}", "email": f"del{i}@example.com", "age": 25 + i},
            )

    delete_params = [{"id": uid} for uid in user_ids[:2]]

    with spanner_config.provide_write_session() as session:
        result = session.execute_many(f"DELETE FROM {test_users_table} WHERE id = @id", delete_params)
        assert result.rows_affected == 2

    with spanner_config.provide_session() as session:
        rows = session.select(f"SELECT id FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids})
        assert len(rows) == 2

    with spanner_config.provide_write_session() as session:
        for uid in user_ids[2:]:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_execute_many_large_batch(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test large batch INSERT (100+ rows)."""
    batch_size = 100
    user_ids = [str(uuid4()) for _ in range(batch_size)]
    parameters = [
        {"id": uid, "name": f"Large Batch {i}", "email": f"large{i}@example.com", "age": i % 100}
        for i, uid in enumerate(user_ids)
    ]

    with spanner_config.provide_write_session() as session:
        result = session.execute_many(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", parameters
        )
        assert result.rows_affected == batch_size

    with spanner_config.provide_session() as session:
        count_result = session.select_value(
            f"SELECT COUNT(*) FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids}
        )
        assert count_result == batch_size

    with spanner_config.provide_write_session() as session:
        delete_params = [{"id": uid} for uid in user_ids]
        session.execute_many(f"DELETE FROM {test_users_table} WHERE id = @id", delete_params)


def test_execute_many_single_item(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test execute_many with single parameter set."""
    user_id = str(uuid4())

    with spanner_config.provide_write_session() as session:
        result = session.execute_many(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            [{"id": user_id, "name": "Single", "email": "single@example.com", "age": 35}],
        )
        assert result.rows_affected == 1

    with spanner_config.provide_session() as session:
        row = session.select_one(f"SELECT name FROM {test_users_table} WHERE id = @id", {"id": user_id})
        assert row["name"] == "Single"

    with spanner_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": user_id})


def test_execute_many_requires_write_session(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test that execute_many fails on read-only Snapshot."""
    parameters = [{"id": str(uuid4()), "name": "Fail", "email": "fail@example.com", "age": 30}]

    with spanner_config.provide_session() as session:
        with pytest.raises(SQLConversionError, match="execute_many requires"):
            session.execute_many(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", parameters
            )


def test_execute_many_mixed_values(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test execute_many with varying parameter values."""
    user_ids = [str(uuid4()) for _ in range(3)]
    parameters = [
        {"id": user_ids[0], "name": "Short", "email": "s@e.com", "age": 18},
        {"id": user_ids[1], "name": "A" * 50, "email": "long" * 10 + "@example.com", "age": 99},
        {"id": user_ids[2], "name": "Normal Name", "email": "normal@example.com", "age": 45},
    ]

    with spanner_config.provide_write_session() as session:
        result = session.execute_many(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", parameters
        )
        assert result.rows_affected == 3

    with spanner_config.provide_session() as session:
        rows = session.select(
            f"SELECT id, name, age FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids}
        )
        assert len(rows) == 3
        ages = sorted([r["age"] for r in rows])
        assert ages == [18, 45, 99]

    with spanner_config.provide_write_session() as session:
        for uid in user_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})


def test_execute_many_consecutive_batches(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    """Test multiple consecutive execute_many calls in same session."""
    batch1_ids = [str(uuid4()) for _ in range(3)]
    batch2_ids = [str(uuid4()) for _ in range(3)]

    batch1_params = [
        {"id": uid, "name": f"Batch1 {i}", "email": f"b1_{i}@example.com", "age": 20 + i}
        for i, uid in enumerate(batch1_ids)
    ]
    batch2_params = [
        {"id": uid, "name": f"Batch2 {i}", "email": f"b2_{i}@example.com", "age": 30 + i}
        for i, uid in enumerate(batch2_ids)
    ]

    with spanner_config.provide_write_session() as session:
        result1 = session.execute_many(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", batch1_params
        )
        result2 = session.execute_many(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", batch2_params
        )

        assert result1.rows_affected == 3
        assert result2.rows_affected == 3

    with spanner_config.provide_session() as session:
        all_ids = batch1_ids + batch2_ids
        rows = session.select(f"SELECT id FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": all_ids})
        assert len(rows) == 6

    with spanner_config.provide_write_session() as session:
        all_ids = batch1_ids + batch2_ids
        for uid in all_ids:
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", {"id": uid})
