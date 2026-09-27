from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, call

from sqlspec.adapters.spanner.litestar import SpannerSyncStore
from sqlspec.adapters.spanner.type_converter import bytes_to_spanner


def test_set_uses_session() -> None:
    """Verify _set uses config.provide_session(transaction=True) for write operations."""
    driver = MagicMock()
    driver.execute.return_value = MagicMock(rows_affected=1)
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._set("s1", b"data", None)

    config.provide_session.assert_called_once_with(transaction=True)


def test_delete_uses_session() -> None:
    """Verify _delete uses config.provide_session(transaction=True) for write operations."""
    driver = MagicMock()
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._delete("s1")

    config.provide_session.assert_called_once_with(transaction=True)


def test_delete_all_uses_session() -> None:
    """Verify _delete_all uses config.provide_session(transaction=True) for write operations."""
    driver = MagicMock()
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._delete_all()

    config.provide_session.assert_called_once_with(transaction=True)


def test_delete_expired_uses_session() -> None:
    """Verify _delete_expired uses config.provide_session(transaction=True) for write operations."""
    driver = MagicMock()
    driver.execute.return_value = MagicMock(rows_affected=3)
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._delete_expired()

    config.provide_session.assert_called_once_with(transaction=True)
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
    result = store._get("s1")

    config.provide_session.assert_called_once_with()
    assert result is None


def test_get_renewal_uses_transaction_session() -> None:
    """Verify _get token renewal uses provide_session(transaction=True) for the UPDATE."""
    driver = MagicMock()
    driver.select_one_or_none.return_value = {
        "data": bytes_to_spanner(b"val"),
        "expires_at": datetime(2030, 1, 1, tzinfo=timezone.utc),
    }
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._get("s1", renew_for=60)

    assert result == b"val"
    assert config.provide_session.call_args_list == [call(), call(transaction=True)]


def test_exists_uses_snapshot_session() -> None:
    """Verify _exists uses snapshot session for read operations."""
    driver = MagicMock()
    driver.select_one_or_none.return_value = {"1": 1}
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._exists("s1")

    config.provide_session.assert_called_once_with()
    assert result is True
