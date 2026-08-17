# pyright: reportPrivateUsage=false
"""Unit tests for pymssql ADK store session listing."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.pymssql.adk import PymssqlADKStore


def _mock_config(adk_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_capture(
    monkeypatch: "pytest.MonkeyPatch",
) -> "tuple[PymssqlADKStore, list[tuple[str, tuple[Any, ...]]]]":
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: PymssqlADKStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    monkeypatch.setattr(PymssqlADKStore, "_execute_fetchall", capture)
    return PymssqlADKStore(_mock_config()), calls


def test_pymssql_list_sessions_binds_order_and_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """SQL Server row limiting binds the offset before the row count."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM [dbo].[adk_session] WHERE app_name = %s AND user_id = %s "
        "ORDER BY create_time ASC, id ASC "
        "OFFSET %s ROWS FETCH NEXT %s ROWS ONLY"
    )
    assert params == ("app", "u1", 20, 10)


def test_pymssql_list_sessions_defaults_to_recent_first_without_a_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """The default listing keeps recent-first ordering and emits no row limiting."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app")

    sql, params = calls[0]
    assert _normalized(sql).endswith("WHERE app_name = %s ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_pymssql_list_sessions_pages_an_unfiltered_listing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Row limiting composes with an app-only listing."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", limit=5)

    sql, params = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC OFFSET %s ROWS FETCH NEXT %s ROWS ONLY")
    assert params == ("app", 0, 5)


def test_pymssql_list_sessions_zero_limit_never_queries(monkeypatch: "pytest.MonkeyPatch") -> None:
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
def test_pymssql_list_sessions_rejects_invalid_options(
    monkeypatch: "pytest.MonkeyPatch", options: "dict[str, Any]"
) -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, calls = _session_store_with_capture(monkeypatch)

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert calls == []
