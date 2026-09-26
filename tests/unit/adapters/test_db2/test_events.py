"""Unit tests for the Db2 event queue stores.

DDL behaviors run against the sync and async stores through ``db2_mode``.
"""

import importlib
from types import SimpleNamespace
from typing import Any

import pytest
import sqlglot

import sqlspec.dialects.db2  # noqa: F401  # pyright: ignore[reportUnusedImport]
from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.events import Db2AsyncEventQueueStore, Db2SyncEventQueueStore
from tests.unit.adapters.test_db2._fakes import DriverMode


def _store(db2_mode: DriverMode, queue_table: "str | None" = None) -> Any:
    events = {"queue_table": queue_table} if queue_table else {}
    config = db2_mode.session_config(extension_config={"events": events})
    store_class = Db2AsyncEventQueueStore if db2_mode.is_async else Db2SyncEventQueueStore
    return store_class(config)


def test_create_statements_are_valid_db2_ddl(db2_mode: DriverMode) -> None:
    """Queue DDL declares a NOT NULL key, has no existence clauses, and parses as Db2."""
    statements = _store(db2_mode).create_statements()

    assert len(statements) == 2
    assert all("IF NOT EXISTS" not in statement.upper() for statement in statements)
    assert "event_id VARCHAR(64) NOT NULL PRIMARY KEY" in statements[0]
    assert "DEFAULT CURRENT TIMESTAMP" in statements[0]
    assert statements[1] == (
        "CREATE INDEX idx_sqlspec_event_queue_channel_status ON sqlspec_event_queue(channel, status, available_at)"
    )
    for statement in statements:
        sqlglot.parse_one(statement, read="db2")


def test_drop_statements_are_plain_drop_table(db2_mode: DriverMode) -> None:
    """Dropping the queue uses Db2's plain DROP TABLE."""
    assert _store(db2_mode).drop_statements() == ["DROP TABLE sqlspec_event_queue"]


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
    db2_mode: DriverMode, queue_table: "str | None", expected: "tuple[str | None, str]"
) -> None:
    """The catalog index check targets the upper-folded schema and table of the unquoted DDL."""
    assert _store(db2_mode, queue_table)._index_existence_target() == expected


@pytest.mark.parametrize(
    ("config_class", "store_class"),
    [(Db2SyncConfig, Db2SyncEventQueueStore), (Db2AsyncConfig, Db2AsyncEventQueueStore)],
)
def test_event_store_resolves_by_config_name(config_class: Any, store_class: Any) -> None:
    """The events migration loads the queue store matching each Db2 config."""
    migration = importlib.import_module("sqlspec.extensions.events.migrations.0001_create_event_queue")
    context = SimpleNamespace(config=config_class(connection_config={"database": "d"}))

    assert type(migration._load_store(context)) is store_class
