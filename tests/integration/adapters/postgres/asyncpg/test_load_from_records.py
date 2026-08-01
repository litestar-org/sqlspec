"""AsyncPG direct record COPY integration tests."""

from typing import Any
from uuid import uuid4

import pyarrow as pa
import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver

pytestmark = pytest.mark.xdist_group("postgres")


async def test_load_from_records_preserves_native_postgres_values(asyncpg_async_driver: AsyncpgDriver) -> None:
    table = '"sqlspec.bulk"."Record.Target"'
    await asyncpg_async_driver.execute_script(
        'CREATE SCHEMA IF NOT EXISTS "sqlspec.bulk"; '
        'DROP TABLE IF EXISTS "sqlspec.bulk"."Record.Target"; '
        'CREATE TABLE "sqlspec.bulk"."Record.Target" '
        "(id UUID PRIMARY KEY, payload JSONB, labels TEXT[], note TEXT)"
    )
    first_id = uuid4()
    second_id = uuid4()
    try:
        first = {"kind": "nested", "metadata": {"enabled": True}, "items": [1, {"name": "two"}]}
        second = {"kind": "different", "other": [False, None]}
        mapping_records: list[dict[str, Any]] = [
            {"id": first_id, "payload": first, "labels": ["a", "b"], "note": None},
            {"note": "second", "labels": [], "payload": second, "id": second_id},
        ]
        job = await asyncpg_async_driver.load_from_records(table, mapping_records)

        rows = await asyncpg_async_driver.select(
            'SELECT id, payload, labels, note FROM "sqlspec.bulk"."Record.Target" ORDER BY note NULLS FIRST'
        )
        assert [row["id"] for row in rows] == [first_id, second_id]
        assert rows[0]["payload"] == first
        assert rows[0]["labels"] == ["a", "b"]
        assert rows[1]["payload"] == second
        assert job.telemetry["rows_processed"] == 2

        await asyncpg_async_driver.load_from_records(
            table,
            [(first_id, {"replacement": True}, ["updated"], "overwritten")],
            columns=["id", "payload", "labels", "note"],
            overwrite=True,
        )
        assert await asyncpg_async_driver.select_value('SELECT count(*) FROM "sqlspec.bulk"."Record.Target"') == 1
    finally:
        await asyncpg_async_driver.execute_script('DROP SCHEMA IF EXISTS "sqlspec.bulk" CASCADE')


async def test_load_from_arrow_keeps_arrow_copy_path(asyncpg_async_driver: AsyncpgDriver) -> None:
    table = "asyncpg_arrow_path"
    await asyncpg_async_driver.execute_script(f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id INTEGER)")
    try:
        job = await asyncpg_async_driver.load_from_arrow(table, pa.table({"id": [1, 2]}), overwrite=True)
        assert job.telemetry["format"] == "arrow"
        assert await asyncpg_async_driver.select_value(f"SELECT count(*) FROM {table}") == 2
    finally:
        await asyncpg_async_driver.execute_script(f"DROP TABLE IF EXISTS {table}")
