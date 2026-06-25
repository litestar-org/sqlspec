"""Integration tests for Spanner load_from_arrow mutations transport."""

from uuid import uuid4

import pyarrow as pa
import pytest

from sqlspec.adapters.spanner import SpannerSyncConfig

pytestmark = pytest.mark.spanner


def test_load_from_arrow_inserts_via_mutations(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    user_ids = [str(uuid4()) for _ in range(10)]
    arrow_table = pa.table({
        "id": user_ids,
        "name": [f"Arrow {i}" for i in range(10)],
        "email": [f"arrow{i}@example.com" for i in range(10)],
        "age": [20 + i for i in range(10)],
    })

    with spanner_config.provide_write_session() as session:
        job = session.load_from_arrow(test_users_table, arrow_table)
        assert job.telemetry["rows_processed"] == 10

    with spanner_config.provide_session() as session:
        rows = session.select(f"SELECT id FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids})
        assert len(rows) == 10


def test_load_from_arrow_rerun_upserts_idempotently(spanner_config: SpannerSyncConfig, test_users_table: str) -> None:
    user_ids = [str(uuid4()) for _ in range(5)]
    arrow_table = pa.table({
        "id": user_ids,
        "name": [f"Upsert {i}" for i in range(5)],
        "email": [f"upsert{i}@example.com" for i in range(5)],
        "age": [30 + i for i in range(5)],
    })

    with spanner_config.provide_write_session() as session:
        session.load_from_arrow(test_users_table, arrow_table)
    # Re-running the same rows must not raise on PK collision (insert_or_update is an upsert).
    with spanner_config.provide_write_session() as session:
        session.load_from_arrow(test_users_table, arrow_table)

    with spanner_config.provide_session() as session:
        rows = session.select(f"SELECT id FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids})
        assert len(rows) == 5
