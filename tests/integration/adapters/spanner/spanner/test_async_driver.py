"""Async Spanner driver integration tests against the Spanner emulator."""

from uuid import uuid4

import pyarrow as pa
import pytest

from sqlspec import SQLResult
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerAsyncDriver

pytestmark = [pytest.mark.spanner, pytest.mark.anyio]


@pytest.mark.parametrize("commit", [False, True])
async def test_async_explicit_transaction_completion(
    spanner_async_config: SpannerAsyncConfig, test_users_table: str, commit: bool
) -> None:
    user_id = str(uuid4())
    async with spanner_async_config.provide_write_session() as session:
        await session.execute(
            f"INSERT INTO {test_users_table} (id, name) VALUES (@id, @name)", id=user_id, name="transaction completion"
        )
        if commit:
            await session.commit()
            await session.commit()
        else:
            await session.rollback()
            await session.rollback()
    async with spanner_async_config.provide_read_session() as session:
        assert await session.select_value(f"SELECT COUNT(*) FROM {test_users_table} WHERE id = @id", id=user_id) == int(
            commit
        )
    if commit:
        async with spanner_async_config.provide_write_session() as session:
            await session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", id=user_id)


async def test_async_connection_pooling(spanner_async_session: "SpannerAsyncDriver") -> None:
    """Test acquiring an async session and executing a scalar query."""
    result = await spanner_async_session.select_value("SELECT 1")
    assert result == 1


async def test_async_rollback_discards_buffered_arrow_mutations(
    spanner_async_config: SpannerAsyncConfig, test_arrow_table: str
) -> None:
    async with spanner_async_config.provide_write_session() as session:
        await session.load_from_arrow(test_arrow_table, pa.table({"id": [103], "name": ["rolled back"], "value": [1]}))
        await session.rollback()
    async with spanner_async_config.provide_read_session() as session:
        assert await session.select_value(f"SELECT COUNT(*) FROM {test_arrow_table} WHERE id = @id", id=103) == 0


async def test_async_session_management(spanner_async_config: "SpannerAsyncConfig") -> None:
    """Test async session lifecycle."""
    async with spanner_async_config.provide_session() as session:
        assert await session.select_value("SELECT 1") == 1


async def test_async_driver_select_value_with_params(spanner_async_session: "SpannerAsyncDriver") -> None:
    """Test async select_value() with parameters."""
    result = await spanner_async_session.select_value("SELECT @val", val=100)
    assert result == 100


async def test_async_driver_select_one_and_dml(
    spanner_async_config: "SpannerAsyncConfig", test_users_table: str
) -> None:
    """Test async INSERT, SELECT, UPDATE, and DELETE."""
    user_id = str(uuid4())

    async with spanner_async_config.provide_write_session() as session:
        insert_result = await session.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            id=user_id,
            name="Async User",
            email="async@example.com",
            age=28,
        )
        assert isinstance(insert_result, SQLResult)
        assert insert_result.rows_affected == 1

    async with spanner_async_config.provide_session() as session:
        row = await session.select_one(
            f"SELECT id, name, email, age FROM {test_users_table} WHERE id = @id", id=user_id
        )
        assert row is not None
        assert str(row["id"]) == user_id
        assert row["name"] == "Async User"
        assert row["email"] == "async@example.com"
        assert row["age"] == 28

    async with spanner_async_config.provide_write_session() as session:
        update_result = await session.execute(
            f"UPDATE {test_users_table} SET age = @age WHERE id = @id", id=user_id, age=29
        )
        assert update_result.rows_affected == 1

    async with spanner_async_config.provide_write_session() as session:
        delete_result = await session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", id=user_id)
        assert delete_result.rows_affected == 1


async def test_async_driver_execute_many(spanner_async_config: "SpannerAsyncConfig", test_users_table: str) -> None:
    """Test async execute_many() using native batch_update."""
    user_ids = [str(uuid4()) for _ in range(3)]
    params = [
        {"id": uid, "name": f"Batch {idx}", "email": f"batch{idx}@example.com", "age": 20 + idx}
        for idx, uid in enumerate(user_ids)
    ]

    async with spanner_async_config.provide_write_session() as session:
        result = await session.execute_many(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)", params
        )
        assert result.rows_affected == 3

    async with spanner_async_config.provide_session() as session:
        rows = await session.select(f"SELECT id, name FROM {test_users_table} WHERE id IN UNNEST(@ids)", ids=user_ids)
        assert len(rows) == 3

    async with spanner_async_config.provide_write_session() as session:
        for uid in user_ids:
            await session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", id=uid)


async def test_async_driver_execute_script(spanner_async_config: "SpannerAsyncConfig", test_users_table: str) -> None:
    """Test async execute_script() across multiple DML statements."""
    uid1 = str(uuid4())
    uid2 = str(uuid4())

    script = f"""
    INSERT INTO {test_users_table} (id, name, email, age) VALUES ('{uid1}', 'Script 1', 's1@example.com', 31);
    INSERT INTO {test_users_table} (id, name, email, age) VALUES ('{uid2}', 'Script 2', 's2@example.com', 32);
    """

    async with spanner_async_config.provide_write_session() as session:
        result = await session.execute_script(script)
        assert result.total_statements == 2
        assert result.successful_statements == 2

    async with spanner_async_config.provide_write_session() as session:
        await session.execute(f"DELETE FROM {test_users_table} WHERE id IN UNNEST(@ids)", ids=[uid1, uid2])


async def test_async_driver_select_stream(spanner_async_config: "SpannerAsyncConfig", test_users_table: str) -> None:
    """Test async select_stream() yielding rows."""
    user_ids = [str(uuid4()) for _ in range(2)]

    async with spanner_async_config.provide_write_session() as session:
        for idx, uid in enumerate(user_ids):
            await session.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                id=uid,
                name=f"Stream {idx}",
                email=f"stream{idx}@example.com",
                age=40 + idx,
            )

    async with spanner_async_config.provide_session() as session:
        async with session.select_stream(
            f"SELECT id, name FROM {test_users_table} WHERE id IN UNNEST(@ids) ORDER BY name", {"ids": user_ids}
        ) as stream:
            collected = [row async for row in stream]

    assert len(collected) == 2

    async with spanner_async_config.provide_write_session() as session:
        await session.execute(f"DELETE FROM {test_users_table} WHERE id IN UNNEST(@ids)", ids=user_ids)


async def test_async_driver_arrow_roundtrip(spanner_async_config: "SpannerAsyncConfig", test_arrow_table: str) -> None:
    """Test async load_from_arrow() and select_to_arrow()."""
    table = pa.table({"id": [101, 102], "name": ["Arrow 1", "Arrow 2"], "value": [10, 20]})

    async with spanner_async_config.provide_write_session() as session:
        job = await session.load_from_arrow(test_arrow_table, table)
        assert job.telemetry["rows_processed"] == 2

    async with spanner_async_config.provide_session() as session:
        arrow_result = await session.select_to_arrow(
            f"SELECT id, name, value FROM {test_arrow_table} WHERE id IN (101, 102) ORDER BY id"
        )
        assert arrow_result.rows_affected == 2

    async with spanner_async_config.provide_write_session() as session:
        await session.execute(f"DELETE FROM {test_arrow_table} WHERE id IN (101, 102)")


async def test_async_config_run_in_transaction(
    spanner_async_config: "SpannerAsyncConfig", test_users_table: str
) -> None:
    """Test SpannerAsyncConfig.run_in_transaction() executing read-write work."""
    user_id = str(uuid4())

    async def _work(driver: "SpannerAsyncDriver") -> str:
        await driver.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            id=user_id,
            name="Tx Runner",
            email="tx@example.com",
            age=50,
        )
        return user_id

    returned_id = await spanner_async_config.run_in_transaction(_work)
    assert returned_id == user_id

    async with spanner_async_config.provide_session() as session:
        row = await session.select_one_or_none(f"SELECT name FROM {test_users_table} WHERE id = @id", id=user_id)
        assert row is not None
        assert row["name"] == "Tx Runner"

    async with spanner_async_config.provide_write_session() as session:
        await session.execute(f"DELETE FROM {test_users_table} WHERE id = @id", id=user_id)
