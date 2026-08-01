"""Integration tests for psycopg record loading."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig

pytestmark = pytest.mark.xdist_group("postgres")

_SYNC_TABLE = "test_psycopg_sync_load_from_records"
_ASYNC_TABLE = "test_psycopg_async_load_from_records"
_RECORDS = [
    {
        "id": 1,
        "payload_json": {"name": "alpha", "items": [1, 2]},
        "payload_jsonb": {"status": "ready", "details": {"attempt": 1}},
        "metadata_jsonb": None,
        "tags": ["python", "sql"],
    },
    {
        "id": 2,
        "payload_json": {"name": "beta", "items": []},
        "payload_jsonb": {"status": "done", "details": {}},
        "metadata_jsonb": {"worker": "two"},
        "tags": [],
    },
]


def test_psycopg_sync_load_from_records_prepares_json_mappings(psycopg_sync_config: "PsycopgSyncConfig") -> None:
    """Psycopg COPY should serialize mapping records for JSON columns."""
    with psycopg_sync_config.provide_session() as session:
        session.execute_script(f"DROP TABLE IF EXISTS {_SYNC_TABLE}")
        session.execute_script(
            f"""
            CREATE TABLE {_SYNC_TABLE} (
                id INTEGER PRIMARY KEY,
                payload_json JSON NOT NULL,
                payload_jsonb JSONB NOT NULL,
                metadata_jsonb JSONB,
                tags TEXT[] NOT NULL
            )
            """
        )
        session.commit()
        try:
            job = session.load_from_records(_SYNC_TABLE, _RECORDS)
            rows = session.execute(f"SELECT * FROM {_SYNC_TABLE} ORDER BY id").get_data()

            assert rows == _RECORDS
            assert job.telemetry["rows_processed"] == 2
        finally:
            session.rollback()
            session.execute_script(f"DROP TABLE IF EXISTS {_SYNC_TABLE}")
            session.commit()


async def test_psycopg_async_load_from_records_prepares_json_mappings(
    psycopg_async_config: "PsycopgAsyncConfig",
) -> None:
    """Async psycopg COPY should serialize mapping records for JSON columns."""
    async with psycopg_async_config.provide_session() as session:
        await session.execute_script(f"DROP TABLE IF EXISTS {_ASYNC_TABLE}")
        await session.execute_script(
            f"""
            CREATE TABLE {_ASYNC_TABLE} (
                id INTEGER PRIMARY KEY,
                payload_json JSON NOT NULL,
                payload_jsonb JSONB NOT NULL,
                metadata_jsonb JSONB,
                tags TEXT[] NOT NULL
            )
            """
        )
        await session.commit()
        try:
            job = await session.load_from_records(_ASYNC_TABLE, _RECORDS)
            rows = (await session.execute(f"SELECT * FROM {_ASYNC_TABLE} ORDER BY id")).get_data()

            assert rows == _RECORDS
            assert job.telemetry["rows_processed"] == 2
        finally:
            await session.rollback()
            await session.execute_script(f"DROP TABLE IF EXISTS {_ASYNC_TABLE}")
            await session.commit()
