from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.spanner.litestar import SpannerSyncStore


def _mock_database() -> MagicMock:
    """Create a mock database that captures run_in_transaction calls."""
    db = MagicMock()
    db.run_in_transaction = MagicMock(side_effect=lambda func: func(MagicMock()))
    return db


def test_set_uses_run_in_transaction() -> None:
    """Verify _set uses database.run_in_transaction for write operations."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"table_name": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._table_name = "sess"  # type: ignore[attr-defined]

    store._set("s1", b"data", None)  # pyright: ignore

    mock_db.run_in_transaction.assert_called_once()


def test_delete_uses_run_in_transaction() -> None:
    """Verify _delete uses database.run_in_transaction for write operations."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"table_name": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._table_name = "sess"  # type: ignore[attr-defined]

    store._delete("s1")  # pyright: ignore

    mock_db.run_in_transaction.assert_called_once()


def test_delete_all_uses_run_in_transaction() -> None:
    """Verify _delete_all uses database.run_in_transaction for write operations."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"table_name": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._table_name = "sess"  # type: ignore[attr-defined]

    store._delete_all()  # pyright: ignore

    mock_db.run_in_transaction.assert_called_once()


def test_delete_expired_uses_run_in_transaction() -> None:
    """Verify _delete_expired uses database.run_in_transaction for write operations."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"table_name": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._table_name = "sess"  # type: ignore[attr-defined]

    store._delete_expired()  # pyright: ignore

    mock_db.run_in_transaction.assert_called_once()


def _context_manager_yielding(value: Any) -> Any:
    class _Ctx:
        def __enter__(self) -> Any:
            return value

        def __exit__(self, *_: Any) -> None:
            pass

    return _Ctx()


def test_get_uses_snapshot_session() -> None:
    """Verify _get uses snapshot session for read operations."""
    driver = MagicMock()
    driver.select_one_or_none.return_value = None
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"table_name": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._table_name = "sess"  # type: ignore[attr-defined]

    result = store._get("s1")  # pyright: ignore

    config.provide_session.assert_called_once_with()
    assert result is None


def test_exists_uses_snapshot_session() -> None:
    """Verify _exists uses snapshot session for read operations."""
    driver = MagicMock()
    driver.select_one_or_none.return_value = {"1": 1}
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"table_name": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._table_name = "sess"  # type: ignore[attr-defined]

    result = store._exists("s1")  # pyright: ignore

    config.provide_session.assert_called_once_with()
    assert result is True
