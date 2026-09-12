"""psqlpy transaction state follows begin/commit/rollback."""

from typing import Any, cast

import pytest

from sqlspec.adapters.psqlpy._typing import PsqlpyDatabaseError
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.anyio


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self.statements.append(sql)

    def in_transaction(self) -> Any:
        msg = "in_transaction() is not a reliable transaction predicate"
        raise AssertionError(msg)


async def test_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    connection = _FakeConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.commit()
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.rollback()
    assert driver._connection_in_transaction() is False
    assert connection.statements == ["BEGIN", "COMMIT", "BEGIN", "ROLLBACK"]


@pytest.mark.parametrize("operation", ["commit", "rollback"])
async def test_failed_transaction_end_clears_the_flag(operation: str) -> None:
    connection = _FakeConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    await driver.begin()

    async def fail(sql: str, *args: Any, **kwargs: Any) -> None:
        raise PsqlpyDatabaseError(sql)

    connection.execute = fail  # type: ignore[method-assign]
    with pytest.raises(SQLSpecError):
        await getattr(driver, operation)()
    assert driver._connection_in_transaction() is False
