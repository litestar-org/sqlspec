"""Unit tests for Db2 Events extension integration."""

from unittest.mock import MagicMock

from sqlspec.adapters.db2.events import Db2EventsConfig, Db2SyncEventQueueStore


def test_db2_events_config() -> None:
    """Test Db2EventsConfig creation."""
    config = Db2EventsConfig()
    assert isinstance(config, dict)


def test_db2_event_queue_store_primitives() -> None:
    """Test Db2SyncEventQueueStore column types and timestamp default."""
    config = MagicMock()
    config.extension_config = {}
    store = Db2SyncEventQueueStore(config)
    assert store._column_types() == ("CLOB", "CLOB", "TIMESTAMP")
    assert store._timestamp_default() == "CURRENT TIMESTAMP"
    assert store._string_type(255) == "VARCHAR(255)"
    assert store._integer_type() == "INTEGER"
