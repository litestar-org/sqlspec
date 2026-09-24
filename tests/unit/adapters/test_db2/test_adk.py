"""Unit tests for Db2 ADK extension integration."""

from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.db2.adk import Db2ADKConfig, Db2ADKMemoryStore, Db2ADKStore


def _mock_config(adk_config: dict[str, Any] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _store_with_driver() -> tuple[Db2ADKStore, MagicMock, MagicMock]:
    config = _mock_config()
    store = Db2ADKStore(config)
    driver = MagicMock()
    config.provide_session.return_value.__enter__.return_value = driver
    config.provide_session.return_value.__exit__.return_value = False
    return store, driver, config


def _all_tables(store: Db2ADKStore) -> list[dict[str, Any]]:
    names = [
        store._session_table,
        store._events_table,
        store._app_state_table,
        store._user_state_table,
        store._metadata_table,
    ]
    return [{"table_name": name} for name in names]


def _all_indexes(store: Db2ADKStore) -> list[dict[str, Any]]:
    session_indexes = [f"idx_{store._session_table}_app_user", f"idx_{store._session_table}_update_time"]
    event_indexes = [
        f"idx_{store._events_table}_scope",
        f"idx_{store._events_table}_session",
        f"idx_{store._events_table}_invocation",
        f"idx_{store._events_table}_timestamp",
        f"idx_{store._events_table}_app_timestamp",
    ]
    return [{"index_name": name} for name in (*session_indexes, *event_indexes)]


def test_db2_adk_config() -> None:
    """Test Db2ADKConfig instantiability."""
    config = Db2ADKConfig()
    assert isinstance(config, dict)


def test_db2_adk_table_existence_uses_data_dictionary() -> None:
    """Test create_tables consults data_dictionary and skips existing tables."""
    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = _all_tables(store)
    driver.data_dictionary.get_indexes.return_value = _all_indexes(store)

    store.create_tables()

    driver.data_dictionary.get_tables.assert_called_once()
    assert driver.execute.call_count == 0


def test_db2_adk_creates_missing_tables() -> None:
    """Test create_tables issues DDL for missing tables and indexes."""
    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = []
    driver.data_dictionary.get_indexes.return_value = []

    store.create_tables()

    assert driver.execute.call_count > 0
    driver.commit.assert_called_once()


def test_db2_adk_memory_store_search_query() -> None:
    """Test Db2ADKMemoryStore searches memory with POSSTR case-insensitive match."""
    config = _mock_config()
    store = Db2ADKMemoryStore(config)
    driver = MagicMock()
    config.provide_session.return_value.__enter__.return_value = driver
    config.provide_session.return_value.__exit__.return_value = False
    driver.select.return_value = []

    results = store.search_entries(query="hello", app_name="test_app", user_id="user_1")

    assert results == []
    driver.select.assert_called_once()
    query_sql, query_params = driver.select.call_args[0]
    assert "POSSTR(LOWER(content_text), LOWER(?)) > 0" in query_sql
    assert "FETCH FIRST 20 ROWS ONLY" in query_sql
    assert query_params == ("test_app", "user_1", "hello")
