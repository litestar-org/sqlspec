"""Spanner default-session DML residual (regression for #474).

Locks in two behaviours:

1. ``SpannerSyncConfig.provide_session()`` returns a write-capable Transaction
   by default. DML succeeds without the caller knowing about
   ``provide_write_session()``.
2. ``SpannerSyncConfig.provide_read_session()`` opens a read-only Snapshot,
   and DML through it raises ``SQLConversionError`` whose message names
   ``provide_read_session`` explicitly so the next reader can self-serve.

Spanner DDL is processed via the ``UpdateDatabaseDdl`` admin API rather
than session.execute(), so this test exercises only DML against a
fixture-created table.
"""

from typing import TYPE_CHECKING

import pytest

from sqlspec.exceptions import SQLConversionError

if TYPE_CHECKING:
    from sqlspec.adapters.spanner import SpannerSyncConfig

pytestmark = pytest.mark.spanner


def test_default_provide_session_runs_dml(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """provide_session() default is write-capable: INSERT + SELECT succeed."""
    with spanner_config.provide_session() as driver:
        driver.execute(
            f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": "u1", "name": "Alice", "email": "alice@example.com", "age": 30},
        )
        row = driver.execute(f"SELECT name FROM {test_users_table} WHERE id = @id", {"id": "u1"}).one()
        assert row["name"] == "Alice"


def test_provide_read_session_blocks_writes(spanner_config: "SpannerSyncConfig", test_users_table: str) -> None:
    """provide_read_session() yields a Snapshot; DML raises and names the entrypoint."""
    with spanner_config.provide_read_session() as driver:
        with pytest.raises(SQLConversionError, match="provide_read_session"):
            driver.execute(
                f"INSERT INTO {test_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": "u2", "name": "Bob", "email": "bob@example.com", "age": 25},
            )
