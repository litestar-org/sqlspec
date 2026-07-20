# pyright: reportPrivateUsage=false
"""Unit tests for arrow-odbc ADK store data-dictionary existence checks."""

from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.arrow_odbc.adk import ArrowOdbcADKStore


def _mock_config(adk_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _store_with_driver() -> "tuple[ArrowOdbcADKStore, MagicMock, MagicMock]":
    config = _mock_config()
    store = ArrowOdbcADKStore(config)
    driver = MagicMock()
    config.provide_session.return_value.__enter__.return_value = driver
    config.provide_session.return_value.__exit__.return_value = False
    return store, driver, config


def _all_tables(store: ArrowOdbcADKStore) -> "list[dict[str, Any]]":
    names = [
        store._session_table,
        store._events_table,
        store._app_state_table,
        store._user_state_table,
        store._metadata_table,
    ]
    return [{"table_name": name} for name in names]


def _all_indexes(store: ArrowOdbcADKStore) -> "list[dict[str, Any]]":
    session_indexes = [f"idx_{store._session_table}_app_user", f"idx_{store._session_table}_update_time"]
    event_indexes = [
        f"idx_{store._events_table}_scope",
        f"idx_{store._events_table}_session",
        f"idx_{store._events_table}_invocation",
        f"idx_{store._events_table}_timestamp",
    ]
    return [{"index_name": name} for name in (*session_indexes, *event_indexes)]


def test_mssql_adk_table_existence_uses_data_dictionary() -> None:
    """create_tables consults data_dictionary.get_tables and skips present tables."""

    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = _all_tables(store)
    driver.data_dictionary.get_indexes.return_value = _all_indexes(store)

    store.create_tables()

    driver.data_dictionary.get_tables.assert_called_once()
    issued = " ".join(str(call.args[0]) for call in driver.execute.call_args_list)
    assert "CREATE TABLE" not in issued.upper()
    assert "CREATE INDEX" not in issued.upper()


def test_mssql_adk_create_issues_ddl_for_absent_objects() -> None:
    """create_tables issues CREATE TABLE/INDEX when the dictionary reports nothing."""

    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = []
    driver.data_dictionary.get_indexes.return_value = []

    store.create_tables()

    issued = " ".join(str(call.args[0]) for call in driver.execute.call_args_list).upper()
    assert "CREATE TABLE" in issued
    assert "CREATE INDEX" in issued
    assert "SYS.TABLES" not in issued
    assert "SYS.INDEXES" not in issued


def test_existence_checks_bounded_query_count() -> None:
    """N existence checks in one create pass trigger a single domain load each."""

    store, driver, _ = _store_with_driver()
    driver.data_dictionary.get_tables.return_value = _all_tables(store)
    driver.data_dictionary.get_indexes.return_value = _all_indexes(store)

    store.create_tables()

    assert driver.data_dictionary.get_tables.call_count == 1
    assert driver.data_dictionary.get_indexes.call_count == 1
