from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.spanner.litestar import SpannerSyncStore


def test_set_uses_session() -> None:
    """Verify _set uses config.provide_session for write operations."""
    driver = MagicMock()
    driver.execute.return_value = MagicMock(rows_affected=1)
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._set("s1", b"data", None)  # pyright: ignore

    config.provide_session.assert_called_once()


def test_delete_uses_session() -> None:
    """Verify _delete uses config.provide_session for write operations."""
    driver = MagicMock()
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._delete("s1")  # pyright: ignore

    config.provide_session.assert_called_once()


def test_delete_all_uses_session() -> None:
    """Verify _delete_all uses config.provide_session for write operations."""
    driver = MagicMock()
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._delete_all()  # pyright: ignore

    config.provide_session.assert_called_once()


def test_delete_expired_uses_session() -> None:
    """Verify _delete_expired uses config.provide_session for write operations."""
    driver = MagicMock()
    driver.execute.return_value = MagicMock(rows_affected=3)
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._delete_expired()  # pyright: ignore

    config.provide_session.assert_called_once()
    assert result == 3


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
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._get("s1")  # pyright: ignore

    config.provide_session.assert_called_once_with()
    assert result is None


def test_exists_uses_snapshot_session() -> None:
    """Verify _exists uses snapshot session for read operations."""
    driver = MagicMock()
    driver.select_one_or_none.return_value = {"1": 1}
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._exists("s1")  # pyright: ignore

    config.provide_session.assert_called_once_with()
    assert result is True
