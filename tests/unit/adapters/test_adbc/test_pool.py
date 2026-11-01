"""Exclusive native-shaped ADBC pool ownership contracts."""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, call

import pytest

from sqlspec.adapters.adbc.pool import AdbcConnectionPool
from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError, ImproperConfigurationError


def test_adbc_pool_reuses_only_after_full_native_session_reset() -> None:
    """A clean lease resets native session state before another borrower can reuse it."""
    seed, lease, replacement = MagicMock(), MagicMock(), MagicMock()
    lease.autocommit = False
    seed.adbc_clone.side_effect = [lease, replacement]
    pool = AdbcConnectionPool(lambda: seed, max_size=1, timeout=0)
    assert pool.acquire() is lease
    pool.release(lease)
    assert lease.mock_calls == [
        call.rollback(),
        call.adbc_connection.set_autocommit(True),
        call.cursor(),
        call.cursor().execute("DISCARD ALL"),
        call.cursor().close(),
        call.adbc_connection.set_autocommit(False),
    ]
    assert lease.autocommit is False
    assert pool.acquire() is lease
    pool.release(lease, discard=True)
    assert pool.acquire() is replacement
    pool.release(replacement)
    assert pool.close() is True


@pytest.mark.parametrize("stage", ["rollback", "enable", "cursor", "discard", "cursor_close", "restore"])
@pytest.mark.parametrize("close_fails", [False, True])
def test_adbc_pool_failed_reset_evicts_and_preserves_primary(stage: str, close_fails: bool) -> None:
    """Each reset failure evicts; a secondary native-close failure stays retryable."""
    seed, lease, replacement = MagicMock(), MagicMock(), MagicMock()
    primary = RuntimeError(f"reset {stage} failed")
    seed.adbc_clone.side_effect = [lease, replacement]
    cursor = lease.cursor.return_value
    handle = lease.adbc_connection
    if stage == "rollback":
        lease.rollback.side_effect = primary
    elif stage == "enable":
        handle.set_autocommit.side_effect = [primary, None]
    elif stage == "cursor":
        lease.cursor.side_effect = primary
    elif stage == "discard":
        cursor.execute.side_effect = primary
    elif stage == "cursor_close":
        cursor.close.side_effect = primary
    else:
        handle.set_autocommit.side_effect = [None, primary]
    if close_fails:
        lease.close.side_effect = [KeyboardInterrupt("close interrupted"), None]
    pool = AdbcConnectionPool(lambda: seed, max_size=1, timeout=0)
    assert pool.acquire() is lease
    with pytest.raises(RuntimeError) as caught:
        pool.release(lease)
    assert caught.value is primary
    assert handle.set_autocommit.call_args == call(False)
    if stage in {"discard", "cursor_close", "restore"}:
        cursor.close.assert_called_once_with()
    if close_fails:
        with pytest.raises(ConnectionTimeoutError):
            pool.acquire()
        seed.close.assert_not_called()
    else:
        assert pool.acquire() is replacement
        pool.release(replacement)
    assert pool.close() is True
    assert lease.close.call_count == (2 if close_fails else 1)


def test_adbc_pool_reset_primary_survives_all_secondary_cleanup_failures() -> None:
    """Cursor, autocommit and native-close cleanup cannot replace a reset exception."""
    seed, lease = MagicMock(), MagicMock()
    primary = KeyboardInterrupt("DISCARD interrupted")
    cleanup = RuntimeError("cleanup failed")
    seed.adbc_clone.return_value = lease
    lease.cursor.return_value.execute.side_effect = primary
    lease.cursor.return_value.close.side_effect = cleanup
    lease.adbc_connection.set_autocommit.side_effect = [None, cleanup]
    lease.close.side_effect = [cleanup, None]
    pool = AdbcConnectionPool(lambda: seed, max_size=1, timeout=0)
    assert pool.acquire() is lease
    with pytest.raises(KeyboardInterrupt) as caught:
        pool.release(lease)
    assert caught.value is primary
    lease.cursor.return_value.close.assert_called_once_with()
    assert lease.adbc_connection.set_autocommit.call_args == call(False)
    seed.close.assert_not_called()
    assert pool.close() is True


@pytest.mark.parametrize("rollback_fails", [False, True])
def test_adbc_pool_discard_rolls_back_and_closes_without_reset(rollback_fails: bool) -> None:
    """Body-failure disposal attempts rollback and close without DISCARD ALL or reuse."""
    seed, lease, replacement = MagicMock(), MagicMock(), MagicMock()
    primary = RuntimeError("rollback failed")
    seed.adbc_clone.side_effect = [lease, replacement]
    if rollback_fails:
        lease.rollback.side_effect = primary
        lease.close.side_effect = [KeyboardInterrupt("close interrupted"), None]
    pool = AdbcConnectionPool(lambda: seed, max_size=1, timeout=0)
    assert pool.acquire() is lease
    if rollback_fails:
        with pytest.raises(RuntimeError) as caught:
            pool.release(lease, discard=True)
        assert caught.value is primary
        with pytest.raises(ConnectionTimeoutError):
            pool.acquire()
    else:
        pool.release(lease, discard=True)
        assert pool.acquire() is replacement
        pool.release(replacement)
    lease.rollback.assert_called_once_with()
    lease.cursor.assert_not_called()
    lease.adbc_connection.set_autocommit.assert_not_called()
    assert pool.close() is True


@pytest.mark.parametrize("mode", ["reuse", "shutdown", "shutdown_close_failure"])
def test_adbc_pool_reset_retains_capacity_and_obeys_shutdown(mode: str) -> None:
    """A resetting lease stays owned until completion and never becomes idle after shutdown."""
    seed, lease = MagicMock(), MagicMock()
    entered, proceed = threading.Event(), threading.Event()
    cleanup_error = RuntimeError("shutdown close failed")
    shutdown = mode != "reuse"
    close_fails = mode == "shutdown_close_failure"

    def discard(_sql: str) -> None:
        entered.set()
        assert proceed.wait(3)

    seed.adbc_clone.return_value = lease
    lease.cursor.return_value.execute.side_effect = discard
    if close_fails:
        lease.close.side_effect = [cleanup_error, None]
    pool = AdbcConnectionPool(lambda: seed, max_size=1, timeout=0)
    assert pool.acquire() is lease
    with ThreadPoolExecutor(max_workers=1) as executor:
        resetting = executor.submit(pool.release, lease)
        assert entered.wait(3)
        try:
            with pytest.raises(ConnectionTimeoutError):
                pool.acquire()
            if shutdown:
                assert pool.close() is False
                seed.close.assert_not_called()
        finally:
            proceed.set()
        if close_fails:
            with pytest.raises(RuntimeError) as caught:
                resetting.result(timeout=3)
            assert caught.value is cleanup_error
            assert pool.is_closed is False
            seed.close.assert_not_called()
            assert pool.close() is True
        else:
            resetting.result(timeout=3)
    if shutdown:
        assert pool.is_closed is True
        with pytest.raises(DatabaseConnectionError):
            pool.acquire()
    else:
        assert pool.acquire() is lease
        pool.release(lease)
        assert pool.close() is True
    seed.adbc_clone.assert_called_once_with()
    assert lease.close.call_count == (2 if close_fails else 1)
    seed.close.assert_called_once_with()


def test_adbc_pool_exclusive_original_seed_leases_and_nested_timeout() -> None:
    """Every borrower owns a distinct original-seed clone within the configured bound."""
    seed, first, second, third = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    seed.adbc_clone.side_effect = [first, second, third]
    create = MagicMock(return_value=seed)
    pool = AdbcConnectionPool(create, max_size=2, timeout=0)
    create.assert_not_called()
    assert pool.acquire() is first
    assert pool.acquire() is second
    with pytest.raises(ConnectionTimeoutError):
        pool.acquire()
    pool.release(first, discard=True)
    assert pool.acquire() is third
    for connection in (second, third):
        pool.release(connection, discard=True)
    create.assert_called_once_with()
    assert seed.adbc_clone.call_count == 3
    first.close.assert_called_once_with()
    assert pool.close() is True
    assert pool.is_closed is True
    seed.close.assert_called_once_with()


@pytest.mark.parametrize("max_size", [0, -1, True, 1.5])
def test_adbc_pool_rejects_invalid_capacity(max_size: int) -> None:
    """Capacity is a positive integer and rejects bool before native acquisition."""
    create = MagicMock()
    with pytest.raises(ImproperConfigurationError):
        AdbcConnectionPool(create, max_size=max_size)
    create.assert_not_called()


@pytest.mark.parametrize("timeout", [-1, float("inf"), float("nan")])
def test_adbc_pool_rejects_invalid_timeout(timeout: float) -> None:
    """Timeout is finite and nonnegative."""
    with pytest.raises(ImproperConfigurationError):
        AdbcConnectionPool(MagicMock(), timeout=timeout)


@pytest.mark.parametrize("failure_stage", ["create", "clone"])
def test_adbc_pool_failed_acquisition_recovers_reserved_capacity(failure_stage: str) -> None:
    """A failed native open or clone releases its capacity reservation."""
    seed, lease = MagicMock(), MagicMock()
    failure = RuntimeError("native acquisition failed")
    create = MagicMock(return_value=seed)
    seed.adbc_clone.return_value = lease
    if failure_stage == "create":
        create.side_effect = [failure, seed]
    else:
        seed.adbc_clone.side_effect = [failure, lease]
    pool = AdbcConnectionPool(create, max_size=1, timeout=0)
    with pytest.raises(RuntimeError) as caught:
        pool.acquire()
    assert caught.value is failure
    assert pool.acquire() is lease
    pool.release(lease)
    assert pool.close() is True


def test_adbc_pool_rejects_foreign_and_duplicate_release() -> None:
    """Only the originating pool may release a currently active lease once."""
    seed, lease, foreign = MagicMock(), MagicMock(), MagicMock()
    seed.adbc_clone.return_value = lease
    pool = AdbcConnectionPool(lambda: seed)
    assert pool.acquire() is lease
    with pytest.raises(DatabaseConnectionError):
        pool.release(foreign)
    foreign.close.assert_not_called()
    pool.release(lease)
    with pytest.raises(DatabaseConnectionError):
        pool.release(lease)
    assert pool.close() is True
    lease.close.assert_called_once_with()


def test_adbc_pool_shutdown_drains_active_lease_without_forcing_close() -> None:
    """Terminal shutdown rejects new borrowers and leaves an executing lease alone."""
    seed, lease = MagicMock(), MagicMock()
    seed.adbc_clone.return_value = lease
    pool = AdbcConnectionPool(lambda: seed)
    assert pool.acquire() is lease
    assert pool.close() is False
    assert pool.is_closed is False
    lease.close.assert_not_called()
    seed.close.assert_not_called()
    with pytest.raises(DatabaseConnectionError):
        pool.acquire()
    pool.release(lease)
    assert pool.is_closed is True
    assert pool.close() is True
    lease.close.assert_called_once_with()
    seed.close.assert_called_once_with()


def test_adbc_pool_quarantined_close_retains_capacity_and_retries_seed_last() -> None:
    """A failed close retains the native lease until a later shutdown retries it."""
    seed, lease = MagicMock(), MagicMock()
    failure = RuntimeError("native close failed")
    seed.adbc_clone.return_value = lease
    lease.close.side_effect = [failure, None]
    pool = AdbcConnectionPool(lambda: seed, max_size=1, timeout=0)
    assert pool.acquire() is lease
    with pytest.raises(RuntimeError) as caught:
        pool.release(lease, discard=True)
    assert caught.value is failure
    with pytest.raises(ConnectionTimeoutError):
        pool.acquire()
    seed.close.assert_not_called()
    assert pool.close() is True
    assert lease.close.call_count == 2
    seed.close.assert_called_once_with()


def test_adbc_pool_failed_seed_close_is_retryable() -> None:
    """CLOSED is published only after the original seed actually closes."""
    seed, lease = MagicMock(), MagicMock()
    failure = RuntimeError("seed close failed")
    seed.adbc_clone.return_value = lease
    seed.close.side_effect = [failure, None]
    pool = AdbcConnectionPool(lambda: seed)
    assert pool.acquire() is lease
    pool.release(lease)
    with pytest.raises(RuntimeError) as caught:
        pool.close()
    assert caught.value is failure
    assert pool.is_closed is False
    assert pool.close() is True
    assert seed.close.call_count == 2


def test_adbc_pool_shutdown_during_clone_closes_clone_before_seed() -> None:
    """Shutdown retains an in-flight reservation and closes its eventual clone first."""
    seed, lease = MagicMock(), MagicMock()
    entered, proceed = threading.Event(), threading.Event()
    closed: list[str] = []

    def clone() -> MagicMock:
        entered.set()
        assert proceed.wait(3)
        return lease

    seed.adbc_clone.side_effect = clone
    seed.close.side_effect = lambda: closed.append("seed")
    lease.close.side_effect = lambda: closed.append("lease")
    pool = AdbcConnectionPool(lambda: seed, max_size=1)
    with ThreadPoolExecutor(max_workers=2) as executor:
        acquiring = executor.submit(pool.acquire)
        assert entered.wait(3)
        try:
            assert executor.submit(pool.close).result(timeout=2) is False
            assert closed == []
        finally:
            proceed.set()
        with pytest.raises(DatabaseConnectionError):
            acquiring.result(timeout=3)
    assert closed == ["lease", "seed"]
    assert pool.is_closed is True


def test_adbc_pool_native_io_does_not_hold_bookkeeping_lock() -> None:
    """Native open, clone and close permit another thread to inspect pool state."""
    seed, lease = MagicMock(), MagicMock()
    with ThreadPoolExecutor(max_workers=1) as executor:

        def inspect_pool() -> None:
            assert executor.submit(lambda: pool.is_closed).result(timeout=2) is False

        def create() -> MagicMock:
            inspect_pool()
            return seed

        def clone() -> MagicMock:
            inspect_pool()
            return lease

        seed.adbc_clone.side_effect = clone
        seed.close.side_effect = inspect_pool
        lease.close.side_effect = inspect_pool
        lease.rollback.side_effect = inspect_pool
        lease.adbc_connection.set_autocommit.side_effect = lambda _enabled: inspect_pool()
        lease.cursor.return_value.execute.side_effect = lambda _sql: inspect_pool()
        lease.cursor.return_value.close.side_effect = inspect_pool
        pool = AdbcConnectionPool(create)
        assert pool.acquire() is lease
        pool.release(lease)
        assert pool.close() is True


def test_adbc_pool_serializes_original_seed_cloning() -> None:
    """Concurrent checkouts serialize original-seed cloning and yield distinct handles."""
    seed = MagicMock()
    entered, proceed = threading.Event(), threading.Event()
    start = threading.Barrier(3, timeout=3)
    leases: list[MagicMock] = []
    native_guard = threading.Lock()

    def clone() -> MagicMock:
        assert native_guard.acquire(blocking=False), "concurrent original-seed clone"
        try:
            entered.set()
            assert proceed.wait(3)
            lease = MagicMock()
            lease.adbc_clone.side_effect = AssertionError("cloned a clone")
            leases.append(lease)
            return lease
        finally:
            native_guard.release()

    def acquire() -> object:
        start.wait()
        return pool.acquire()

    seed.adbc_clone.side_effect = clone
    pool = AdbcConnectionPool(lambda: seed, max_size=2, timeout=0)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.submit(acquire), executor.submit(acquire)
        start.wait()
        assert entered.wait(3)
        proceed.set()
        first_lease, second_lease = first.result(timeout=3), second.result(timeout=3)
    assert first_lease is not second_lease
    assert len(leases) == 2
    for lease in leases:
        pool.release(lease)
    assert pool.close() is True


def test_adbc_pool_closing_lease_retains_capacity_until_native_close_finishes() -> None:
    """Native cleanup remains owned and counted while another thread is closing it."""
    seed, lease = MagicMock(), MagicMock()
    entered, proceed = threading.Event(), threading.Event()

    def native_close() -> None:
        entered.set()
        assert proceed.wait(3)

    seed.adbc_clone.return_value = lease
    lease.close.side_effect = native_close
    pool = AdbcConnectionPool(lambda: seed, max_size=1, timeout=0)
    assert pool.acquire() is lease
    with ThreadPoolExecutor(max_workers=1) as executor:
        releasing = executor.submit(pool.release, lease, discard=True)
        assert entered.wait(3)
        try:
            with pytest.raises(ConnectionTimeoutError):
                pool.acquire()
            assert pool.close() is False
            seed.close.assert_not_called()
        finally:
            proceed.set()
        releasing.result(timeout=3)
    assert pool.is_closed is True
    seed.close.assert_called_once_with()


@pytest.mark.parametrize("timeout", [30.0, 1e100])
def test_adbc_pool_shutdown_wakes_waiting_borrower(monkeypatch: pytest.MonkeyPatch, timeout: float) -> None:
    """A borrower waiting for capacity wakes on shutdown rather than its long timeout."""
    seed, lease = MagicMock(), MagicMock()
    waiting = threading.Event()
    native_clock = time.monotonic
    worker_id: list[int] = []
    clock_reads = 0

    def clock() -> float:
        nonlocal clock_reads
        if worker_id and threading.get_ident() == worker_id[0]:
            clock_reads += 1
            if clock_reads == 2:
                waiting.set()
        return native_clock()

    def acquire() -> object:
        worker_id.append(threading.get_ident())
        return pool.acquire()

    seed.adbc_clone.return_value = lease
    pool = AdbcConnectionPool(lambda: seed, max_size=1, timeout=timeout)
    assert pool.acquire() is lease
    monkeypatch.setattr(time, "monotonic", clock)
    with ThreadPoolExecutor(max_workers=1) as executor:
        borrower = executor.submit(acquire)
        assert waiting.wait(3)
        assert pool.close() is False
        with pytest.raises(DatabaseConnectionError):
            borrower.result(timeout=2)
    pool.release(lease)
    assert pool.is_closed is True


def test_adbc_pool_rejects_fork_before_inherited_locks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every operation refuses a changed PID before touching a poisoned lock or native handle."""
    condition = MagicMock()
    condition.__enter__.side_effect = AssertionError("inherited condition accessed")
    monkeypatch.setattr(threading, "Condition", lambda *_args: condition)
    create, lease = MagicMock(), MagicMock()
    pool = AdbcConnectionPool(create)
    child_pid = os.getpid() + 1
    monkeypatch.setattr(os, "getpid", lambda: child_pid)
    for operation in (pool.acquire, lambda: pool.release(lease), pool.close, lambda: pool.is_closed, pool.check_pid):
        with pytest.raises(DatabaseConnectionError, match="fork"):
            operation()
    create.assert_not_called()
    lease.close.assert_not_called()
