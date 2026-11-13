"""Oracle transaction-honesty: _connection_in_transaction reflects begin/commit/rollback."""

from typing import Any, cast

import pytest

from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.exceptions import TransactionError


class _FakeSyncConnection:
    def __init__(self, transaction_in_progress: bool = False) -> None:
        self.transaction_in_progress = transaction_in_progress

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


class _FakeAsyncConnection:
    def __init__(self, transaction_in_progress: bool = False) -> None:
        self.transaction_in_progress = transaction_in_progress

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass


def test_oracle_sync_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    """The sync transaction predicate should follow the real begin/commit/rollback state."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection()))
    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    driver.commit()
    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    driver.rollback()
    assert driver._connection_in_transaction() is False


@pytest.mark.anyio
async def test_oracle_async_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    """The async transaction predicate should follow the real begin/commit/rollback state."""
    driver = OracleAsyncDriver(cast("Any", _FakeAsyncConnection()))
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.commit()
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.rollback()
    assert driver._connection_in_transaction() is False


def test_oracle_sync_release_savepoint_validates_without_sql() -> None:
    """Oracle has no RELEASE SAVEPOINT statement, so release only validates the name."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection()))

    driver.release_savepoint("sqlspec_sp_1")
    with pytest.raises(TransactionError):
        driver.release_savepoint("sp; DROP TABLE users")


@pytest.mark.anyio
async def test_oracle_async_release_savepoint_validates_without_sql() -> None:
    """Oracle has no RELEASE SAVEPOINT statement, so release only validates the name."""
    driver = OracleAsyncDriver(cast("Any", _FakeAsyncConnection()))

    await driver.release_savepoint("sqlspec_sp_1")
    with pytest.raises(TransactionError):
        await driver.release_savepoint("sp; DROP TABLE users")


def test_sync_predicate_sees_an_implicitly_opened_transaction() -> None:
    """DML without an explicit begin still opens a transaction Oracle reports natively."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection(transaction_in_progress=True)))

    assert driver._connection_in_transaction() is True


async def test_async_predicate_sees_an_implicitly_opened_transaction() -> None:
    """The async twin must consult the same native signal."""
    driver = OracleAsyncDriver(cast("Any", _FakeAsyncConnection(transaction_in_progress=True)))

    assert driver._connection_in_transaction() is True


def test_sync_predicate_sees_an_explicit_begin_before_any_dml() -> None:
    """begin() issues no SQL, so only the ownership flag can report it."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection(transaction_in_progress=False)))
    driver.begin()

    assert driver._connection_in_transaction() is True


def test_execute_stack_does_not_commit_after_begin_without_dml() -> None:
    """Ownership after an explicit begin must not flip just because no DML has run."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection(transaction_in_progress=False)))
    driver.begin()

    assert driver._connection_in_transaction() is True

    driver.commit()

    assert driver._connection_in_transaction() is False


def test_failed_commit_resets_transaction_active() -> None:
    """A failed commit must reset _transaction_active in finally."""
    import oracledb

    from sqlspec.exceptions import SQLSpecError

    class _FailingCommitConn(_FakeSyncConnection):
        def commit(self) -> None:
            raise oracledb.DatabaseError("ORA-02091: transaction rolled back")

    driver = OracleSyncDriver(cast("Any", _FailingCommitConn()))
    driver.begin()
    assert driver._connection_in_transaction() is True
    with pytest.raises(SQLSpecError):
        driver.commit()
    assert driver._connection_in_transaction() is False


def test_connection_context_rolls_back_on_block_exception() -> None:
    """Connection context rolls back on unhandled block exceptions."""
    from unittest.mock import MagicMock

    from sqlspec.adapters.oracledb.config import OracleSyncConnectionContext

    conn = MagicMock()
    config = MagicMock()
    config._create_tenant_lease.return_value = None
    config.driver_features = {}
    config.connection_config = {}
    config.connection_instance.acquire.return_value = conn
    config.provide_pool.return_value = config.connection_instance
    config._build_pool_acquire_kwargs.return_value = {}
    conn.supports_opentelemetry_tracing = False
    ctx = OracleSyncConnectionContext(config)

    with pytest.raises(RuntimeError):
        with ctx:
            raise RuntimeError("boom")

    conn.rollback.assert_called_once()


@pytest.mark.anyio
async def test_async_connection_context_rolls_back_on_block_exception() -> None:
    """Async connection context rolls back on unhandled block exceptions."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.adapters.oracledb.config import OracleAsyncConnectionContext

    conn = AsyncMock()
    conn.supports_opentelemetry_tracing = False
    pool = MagicMock()
    pool.acquire = AsyncMock(return_value=conn)
    pool.release = AsyncMock()
    config = MagicMock()
    config._create_tenant_lease.return_value = None
    config.driver_features = {}
    config.connection_config = {}
    config.provide_pool = AsyncMock(return_value=pool)
    ctx = OracleAsyncConnectionContext(config)

    with pytest.raises(RuntimeError):
        async with ctx:
            raise RuntimeError("boom")

    conn.rollback.assert_called_once()


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_sessionless_transaction_scope_begin_suspend_and_resume(mode: str) -> None:
    from inspect import isawaitable
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    native = AsyncMock if mode == "async" else Mock
    connection = SimpleNamespace(
        transaction_in_progress=False,
        begin_sessionless_transaction=native(return_value=b"transaction"),
        resume_sessionless_transaction=native(return_value=b"transaction"),
        suspend_sessionless_transaction=native(),
        commit=native(),
        rollback=native(),
    )
    driver = (OracleAsyncDriver if mode == "async" else OracleSyncDriver)(cast("Any", connection))
    context = driver.sessionless_transaction("transaction")
    if mode == "async":
        async with cast("Any", context) as txid:
            assert txid == b"transaction"
            assert driver.sessionless_transaction_id == txid
    else:
        with cast("Any", context) as txid:
            assert txid == b"transaction"
            assert driver.sessionless_transaction_id == txid
    assert driver.sessionless_transaction_id is None
    connection.begin_sessionless_transaction.assert_called_once_with(b"transaction", timeout=60, defer_round_trip=True)
    connection.suspend_sessionless_transaction.assert_called_once_with()
    result = driver.resume_sessionless_transaction(b"transaction", timeout=0)
    resumed = await result if isawaitable(result) else result
    assert resumed == b"transaction"
    result = driver.commit()
    if isawaitable(result):
        await result
    assert driver.sessionless_transaction_id is None


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_sessionless_transaction_error_preserves_body_error(mode: str) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    native = AsyncMock if mode == "async" else Mock
    connection = SimpleNamespace(
        transaction_in_progress=False,
        begin_sessionless_transaction=native(return_value=b"transaction"),
        suspend_sessionless_transaction=native(),
        rollback=native(side_effect=RuntimeError("network cleanup")),
    )
    driver = (OracleAsyncDriver if mode == "async" else OracleSyncDriver)(cast("Any", connection))
    with pytest.raises(ValueError, match="body"):
        if mode == "async":
            async with cast("Any", driver.sessionless_transaction()):
                raise ValueError("body")
        else:
            with cast("Any", driver.sessionless_transaction()):
                raise ValueError("body")
    connection.rollback.assert_called_once_with()


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(("transaction_id", "resume"), [(b"", False), ("界" * 22, False), (None, True)])
async def test_sessionless_transaction_validates_before_native_call(
    mode: str, transaction_id: str | bytes | None, resume: bool
) -> None:
    from types import SimpleNamespace

    from sqlspec.exceptions import ImproperConfigurationError

    driver = (OracleAsyncDriver if mode == "async" else OracleSyncDriver)(cast("Any", SimpleNamespace()))
    with pytest.raises(ImproperConfigurationError):
        driver.sessionless_transaction(transaction_id, resume=resume)


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("operation", ["begin", "resume", "suspend"])
@pytest.mark.parametrize("failure", ["absent", "non_callable", "native_error"])
async def test_sessionless_native_api_failure_preserves_attached_transaction(
    mode: str, operation: str, failure: str
) -> None:
    from inspect import isawaitable
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    import oracledb

    from sqlspec.exceptions import DeadlockError

    native = AsyncMock if mode == "async" else Mock
    connection = SimpleNamespace(
        transaction_in_progress=False,
        begin_sessionless_transaction=native(return_value=b"attached"),
        resume_sessionless_transaction=native(return_value=b"attached"),
        suspend_sessionless_transaction=native(),
    )
    driver = (OracleAsyncDriver if mode == "async" else OracleSyncDriver)(cast("Any", connection))
    attached = driver.begin_sessionless_transaction()
    if isawaitable(attached):
        attached = await attached
    assert attached == b"attached"
    method_name = f"{operation}_sessionless_transaction"
    native_error = oracledb.DatabaseError("ORA-00060: deadlock detected while waiting for resource")
    if failure == "absent":
        delattr(connection, method_name)
    elif failure == "non_callable":
        setattr(connection, method_name, None)
    else:
        setattr(connection, method_name, native(side_effect=native_error))
    expected_error = DeadlockError if failure == "native_error" else oracledb.NotSupportedError
    with pytest.raises(expected_error) as caught:
        method = getattr(driver, method_name)
        result = method() if operation == "suspend" else method(b"replacement")
        if isawaitable(result):
            await result
    assert driver.sessionless_transaction_id == b"attached"
    if failure == "native_error":
        assert caught.value.__cause__ is native_error
    else:
        assert "26.1" in str(caught.value)
