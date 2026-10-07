"""Spanner ``THEN RETURN`` DML tests for the sync and async adapters."""

from uuid import uuid4

import pytest

from tests.integration.adapters.spanner.spanner._modes import SpannerModeConfig, invoke, mode_session

pytestmark = [pytest.mark.spanner, pytest.mark.anyio]


async def test_then_return_returns_rows_for_each_dml_kind(
    spanner_mode_config: SpannerModeConfig, test_users_table: str
) -> None:
    """INSERT, UPDATE, and DELETE with ``THEN RETURN`` return the affected rows."""
    user_id = str(uuid4())
    async with mode_session(spanner_mode_config, "write") as session:
        inserted = await invoke(
            session.execute(
                f"INSERT INTO {test_users_table} (id, name, age) VALUES (@id, @name, @age) THEN RETURN id, name",
                id=user_id,
                name="returning",
                age=30,
            )
        )
        updated = await invoke(
            session.execute(
                f"UPDATE {test_users_table} SET age = age + 1 WHERE id = @id THEN RETURN WITH ACTION AS act age",
                id=user_id,
            )
        )
        deleted = await invoke(
            session.execute(f"DELETE FROM {test_users_table} WHERE id = @id THEN RETURN name", id=user_id)
        )

    assert inserted.get_data() == [{"id": user_id, "name": "returning"}]
    assert updated.get_data() == [{"act": "UPDATE", "age": 31}]
    assert deleted.get_data() == [{"name": "returning"}]


async def test_then_return_rows_survive_repeated_execution(
    spanner_mode_config: SpannerModeConfig, test_users_table: str
) -> None:
    """Repeating a cached ``THEN RETURN`` statement still returns its rows."""
    sql = f"INSERT INTO {test_users_table} (id, name) VALUES (@id, @name) THEN RETURN name"
    async with mode_session(spanner_mode_config, "write") as session:
        first = await invoke(session.execute(sql, id=str(uuid4()), name="first"))
        second = await invoke(session.execute(sql, id=str(uuid4()), name="second"))

    assert first.get_data() == [{"name": "first"}]
    assert second.get_data() == [{"name": "second"}]
