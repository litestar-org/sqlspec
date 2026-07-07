# pyright: reportPrivateUsage=false
"""Threaded regression tests for shared SQL statement state."""

import threading

from sqlspec.core.statement import SQL
from sqlspec.exceptions import SQLSpecError


def test_concurrent_compile_reset_vs_current_expression() -> None:
    raw = "SELECT id, name FROM users WHERE id = :id"
    sql = SQL(raw, {"id": 1})
    sql.compile()
    crashes: list[BaseException] = []
    iterations = 2000
    start = threading.Barrier(4)

    def writer() -> None:
        start.wait()
        for _ in range(iterations):
            try:
                sql.reset()
                sql._raw_sql = raw
                sql._named_parameters["id"] = 1
                sql.compile()
            except (AttributeError, TypeError) as exc:
                crashes.append(exc)
            except SQLSpecError:
                pass

    def reader() -> None:
        start.wait()
        for _ in range(iterations):
            try:
                sql._current_expression()
                sql.returns_rows()
                _ = sql.operation_type
            except (AttributeError, TypeError) as exc:
                crashes.append(exc)
            except SQLSpecError:
                pass

    threads = [threading.Thread(target=writer), threading.Thread(target=reader), threading.Thread(target=reader)]
    for thread in threads:
        thread.start()
    start.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=30)

    live_threads = [thread for thread in threads if thread.is_alive()]
    assert live_threads == []
    assert not crashes, f"sentinel/state crash under concurrency: {crashes[:3]}"
