"""Unit tests for ADK and Litestar store transaction routing."""

from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.spanner.adk import SpannerSyncADKStore
from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.adapters.spanner.litestar import SpannerSyncStore


def test_adk_store_run_write_routes_through_config_run_in_transaction() -> None:
    """Verify that SpannerSyncADKStore._run_write executes via config.run_in_transaction."""
    config = MagicMock(spec=SpannerSyncConfig)
    executed_statements: list[tuple[str, Any]] = []

    def mock_run_in_transaction(func: Any, *args: Any, **kwargs: Any) -> Any:
        mock_driver = MagicMock(spec=SpannerSyncDriver)
        mock_driver.execute.side_effect = lambda sql, params=None, param_types=None, **kw: executed_statements.append((
            sql,
            params,
        ))
        return func(mock_driver, *args, **kwargs)

    config.run_in_transaction = MagicMock(side_effect=mock_run_in_transaction)

    store = SpannerSyncADKStore(config=config)
    statements = [
        ("INSERT INTO t (id) VALUES (@id)", {"id": "1"}, {"id": MagicMock()}),
        ("INSERT INTO t (id) VALUES (@id)", {"id": "2"}, {"id": MagicMock()}),
    ]
    store._run_write(statements)

    config.run_in_transaction.assert_called_once()
    assert len(executed_statements) == 2


def test_litestar_store_writes_route_through_config_run_in_transaction() -> None:
    """Verify that SpannerSyncStore write operations execute via config.run_in_transaction."""
    config = MagicMock(spec=SpannerSyncConfig)
    config.extension_config = {"litestar": {"session_table": "sessions"}}
    executed_sqls: list[str] = []

    def mock_run_in_transaction(func: Any, *args: Any, **kwargs: Any) -> Any:
        mock_driver = MagicMock(spec=SpannerSyncDriver)
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_driver.execute.side_effect = lambda sql, *a, **kw: (executed_sqls.append(str(sql)), mock_result)[1]
        return func(mock_driver, *args, **kwargs)

    config.run_in_transaction = MagicMock(side_effect=mock_run_in_transaction)

    store = SpannerSyncStore(config=config)

    store._set("session_1", b"payload", expires_in=3600)
    assert config.run_in_transaction.call_count == 1

    store._delete("session_1")
    assert config.run_in_transaction.call_count == 2

    store._delete_all()
    assert config.run_in_transaction.call_count == 3

    expired_count = store._delete_expired()
    assert config.run_in_transaction.call_count == 4
    assert expired_count == 1
