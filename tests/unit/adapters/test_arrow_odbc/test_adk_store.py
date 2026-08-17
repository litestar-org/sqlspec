# pyright: reportPrivateUsage=false
"""Unit tests for arrow-odbc ADK store data-dictionary existence checks."""

from typing import Any
from unittest.mock import MagicMock

import pytest

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
        f"idx_{store._events_table}_app_timestamp",
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


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_capture(
    monkeypatch: "pytest.MonkeyPatch",
) -> "tuple[ArrowOdbcADKStore, list[tuple[str, tuple[Any, ...]]]]":
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: ArrowOdbcADKStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    monkeypatch.setattr(ArrowOdbcADKStore, "_execute_fetchall", capture)
    return ArrowOdbcADKStore(_mock_config()), calls


def test_arrow_odbc_list_sessions_binds_order_and_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """SQL Server row limiting binds the offset before the row count."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? "
        "ORDER BY create_time ASC, id ASC "
        "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    )
    assert params == ("app", "u1", 20, 10)


def test_arrow_odbc_list_sessions_defaults_to_recent_first_without_a_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """The default listing keeps recent-first ordering and emits no row limiting."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app")

    sql, params = calls[0]
    assert _normalized(sql).endswith("WHERE app_name = ? ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_arrow_odbc_list_sessions_pages_an_unfiltered_listing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Row limiting composes with an app-only listing."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", limit=5)

    sql, params = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY")
    assert params == ("app", 0, 5)


def test_arrow_odbc_list_sessions_zero_limit_never_queries(monkeypatch: "pytest.MonkeyPatch") -> None:
    """A zero limit short-circuits before any database work."""
    store, calls = _session_store_with_capture(monkeypatch)

    assert store.list_sessions("app", limit=0) == []
    assert calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_arrow_odbc_list_sessions_rejects_invalid_options(
    monkeypatch: "pytest.MonkeyPatch", options: "dict[str, Any]"
) -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, calls = _session_store_with_capture(monkeypatch)

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert calls == []
