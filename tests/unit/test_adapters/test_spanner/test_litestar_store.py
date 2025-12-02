from unittest.mock import MagicMock

from sqlspec.adapters.spanner.litestar import SpannerSyncStore


def _context_manager_yielding(value):
    class _Ctx:
        def __enter__(self):
            return value

        def __exit__(self, *_):
            return False

    return _Ctx()


def test_set_uses_transaction_session() -> None:
    driver = MagicMock()
    driver.execute.return_value.rows_affected = 0
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"table_name": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._table_name = "sess"  # type: ignore[attr-defined]

    store._set("s1", b"data", None)  # pyright: ignore

    config.provide_session.assert_called_with(transaction=True)
    assert driver.execute.call_count == 2


def test_delete_uses_transaction_session() -> None:
    driver = MagicMock()
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"table_name": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    store._table_name = "sess"  # type: ignore[attr-defined]

    store._delete("s1")  # pyright: ignore

    config.provide_session.assert_called_with(transaction=True)
    driver.execute.assert_called_once()
