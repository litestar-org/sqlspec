"""Spanner transaction completion tests for the sync and async adapters."""

from typing import Any
from uuid import uuid4

import pyarrow as pa
import pytest

from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerAsyncDriver, SpannerSyncDriver
from tests.integration.adapters.spanner.spanner._modes import SpannerModeConfig, invoke, mode_session

pytestmark = [pytest.mark.spanner, pytest.mark.anyio]


@pytest.mark.parametrize("commit", [False, True])
async def test_explicit_transaction_completion(
    spanner_mode_config: SpannerModeConfig, test_users_table: str, commit: bool
) -> None:
    """Explicit commit or rollback is final, repeatable, and preserved on context exit."""
    user_id = str(uuid4())
    async with mode_session(spanner_mode_config, "write") as session:
        await invoke(
            session.execute(
                f"INSERT INTO {test_users_table} (id, name) VALUES (@id, @name)", id=user_id, name="completion"
            )
        )
        complete = session.commit if commit else session.rollback
        await invoke(complete())
        await invoke(complete())
    async with mode_session(spanner_mode_config, "read") as session:
        count = await invoke(
            session.select_value(f"SELECT COUNT(*) FROM {test_users_table} WHERE id = @id", id=user_id)
        )
    assert count == int(commit)


async def test_write_session_continues_after_commit(
    spanner_mode_config: SpannerModeConfig, test_users_table: str
) -> None:
    """Statements after an explicit commit run in a new transaction that commits on exit."""
    first_id = str(uuid4())
    second_id = str(uuid4())
    async with mode_session(spanner_mode_config, "write") as session:
        await invoke(
            session.execute(f"INSERT INTO {test_users_table} (id, name) VALUES (@id, @name)", id=first_id, name="one")
        )
        await invoke(session.commit())
        await invoke(
            session.execute(f"INSERT INTO {test_users_table} (id, name) VALUES (@id, @name)", id=second_id, name="two")
        )
    async with mode_session(spanner_mode_config, "read") as session:
        count = await invoke(
            session.select_value(
                f"SELECT COUNT(*) FROM {test_users_table} WHERE id IN UNNEST(@ids)", ids=[first_id, second_id]
            )
        )
    assert count == 2


async def test_rollback_discards_buffered_arrow_mutations(
    spanner_mode_config: SpannerModeConfig, test_arrow_table: str
) -> None:
    """Rolling back a write session discards Arrow rows buffered as mutations."""
    async with mode_session(spanner_mode_config, "write") as session:
        await invoke(
            session.load_from_arrow(test_arrow_table, pa.table({"id": [103], "name": ["rolled back"], "value": [1]}))
        )
        await invoke(session.rollback())
    async with mode_session(spanner_mode_config, "read") as session:
        count = await invoke(session.select_value(f"SELECT COUNT(*) FROM {test_arrow_table} WHERE id = @id", id=103))
    assert count == 0


async def test_config_run_in_transaction(spanner_mode_config: SpannerModeConfig, test_users_table: str) -> None:
    """run_in_transaction() commits the work and returns its result."""
    user_id = str(uuid4())
    insert_sql = f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)"
    params: dict[str, Any] = {"id": user_id, "name": "Tx Runner", "email": "tx@example.com", "age": 50}

    if isinstance(spanner_mode_config, SpannerAsyncConfig):

        async def _async_work(driver: SpannerAsyncDriver) -> str:
            await driver.execute(insert_sql, **params)
            return user_id

        returned_id = await spanner_mode_config.run_in_transaction(_async_work)
    else:

        def _sync_work(driver: SpannerSyncDriver) -> str:
            driver.execute(insert_sql, **params)
            return user_id

        returned_id = spanner_mode_config.run_in_transaction(_sync_work)

    assert returned_id == user_id
    async with mode_session(spanner_mode_config, "read") as session:
        row = await invoke(
            session.select_one_or_none(f"SELECT name FROM {test_users_table} WHERE id = @id", id=user_id)
        )
    assert row == {"name": "Tx Runner"}
