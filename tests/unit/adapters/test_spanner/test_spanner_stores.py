"""Unit tests for ADK and Litestar store session routing."""

from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.spanner.adk import SpannerSyncADKStore
from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.adapters.spanner.litestar import SpannerSyncStore


def _context_manager_yielding(value: Any) -> Any:
    class _Ctx:
        def __enter__(self) -> Any:
            return value

        def __exit__(self, *_: Any) -> None:
            pass

    return _Ctx()


def test_adk_store_run_write_routes_through_provide_session() -> None:
    """Verify that SpannerSyncADKStore._run_write executes via config.provide_session."""
    config = MagicMock(spec=SpannerSyncConfig)
    executed_statements: list[tuple[str, Any]] = []

    mock_driver = MagicMock(spec=SpannerSyncDriver)
    mock_driver.execute.side_effect = lambda sql, params=None, param_types=None, **kw: executed_statements.append((
        sql,
        params,
    ))
    config.provide_session.side_effect = lambda *a, **kw: _context_manager_yielding(mock_driver)

    store = SpannerSyncADKStore(config=config)
    statements = [
        ("INSERT INTO t (id) VALUES (@id)", {"id": "1"}, {"id": MagicMock()}),
        ("INSERT INTO t (id) VALUES (@id)", {"id": "2"}, {"id": MagicMock()}),
    ]
    store._run_write(statements)

    config.provide_session.assert_called_once()
    assert len(executed_statements) == 2


def test_litestar_store_writes_route_through_provide_session() -> None:
    """Verify that SpannerSyncStore write operations execute via config.provide_session."""
    config = MagicMock(spec=SpannerSyncConfig)
    config.extension_config = {"litestar": {"session_table": "sessions"}}
    executed_sqls: list[str] = []

    mock_driver = MagicMock(spec=SpannerSyncDriver)
    mock_result = MagicMock()
    mock_result.rowcount = 1
    mock_result.rows_affected = 1

    def mock_execute(sql: Any, *a: Any, **kw: Any) -> Any:
        executed_sqls.append(str(sql))
        return mock_result

    mock_driver.execute.side_effect = mock_execute
    config.provide_session.side_effect = lambda *a, **kw: _context_manager_yielding(mock_driver)

    store = SpannerSyncStore(config=config)

    store._set("session_1", b"payload", expires_in=3600)
    assert config.provide_session.call_count == 1

    store._delete("session_1")
    assert config.provide_session.call_count == 2

    store._delete_all()
    assert config.provide_session.call_count == 3

    expired_count = store._delete_expired()
    assert config.provide_session.call_count == 4
    assert expired_count == 1
