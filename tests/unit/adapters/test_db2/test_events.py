"""Unit tests for the Db2 event queue store."""

from typing import TYPE_CHECKING, cast

import pytest
import sqlglot

import sqlspec.dialects.db2  # noqa: F401  # pyright: ignore[reportUnusedImport]
from sqlspec.adapters.db2.events import Db2SyncEventQueueStore
from tests.unit.adapters.test_db2._fakes import FakeDb2SessionConfig

if TYPE_CHECKING:
    from sqlspec.adapters.db2.config import Db2SyncConfig


def _store(queue_table: "str | None" = None) -> Db2SyncEventQueueStore:
    events = {"queue_table": queue_table} if queue_table else {}
    config = FakeDb2SessionConfig(extension_config={"events": events})
    return Db2SyncEventQueueStore(cast("Db2SyncConfig", config))


def test_create_statements_are_valid_db2_ddl() -> None:
    """Queue DDL declares a NOT NULL key, has no existence clauses, and parses as Db2."""
    statements = _store().create_statements()

    assert len(statements) == 2
    assert all("IF NOT EXISTS" not in statement.upper() for statement in statements)
    assert "event_id VARCHAR(64) NOT NULL PRIMARY KEY" in statements[0]
    assert "DEFAULT CURRENT TIMESTAMP" in statements[0]
    assert statements[1] == (
        "CREATE INDEX idx_sqlspec_event_queue_channel_status ON sqlspec_event_queue(channel, status, available_at)"
    )
    for statement in statements:
        sqlglot.parse_one(statement, read="db2")


def test_drop_statements_are_plain_drop_table() -> None:
    """Dropping the queue uses Db2's plain DROP TABLE."""
    assert _store().drop_statements() == ["DROP TABLE sqlspec_event_queue"]


@pytest.mark.parametrize(
    ("queue_table", "expected"),
    [
        ("app.sqlspec_event_queue", ("APP", "SQLSPEC_EVENT_QUEUE")),
        (None, (None, "SQLSPEC_EVENT_QUEUE")),
        ("AppEvents", (None, "APPEVENTS")),
    ],
    ids=["qualified", "default", "mixed-case"],
)
def test_index_existence_target_uses_normalized_table(
    queue_table: "str | None", expected: "tuple[str | None, str]"
) -> None:
    """The catalog index check targets the upper-folded schema and table of the unquoted DDL."""
    assert _store(queue_table)._index_existence_target() == expected
