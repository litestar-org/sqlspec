"""CockroachDB AsyncPG driver regression tests."""

import pytest

from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver


class _RecordingCockroachAsyncpgDriver(CockroachAsyncpgDriver):
    def __init__(self) -> None:
        super().__init__(connection=object(), driver_features={"enable_auto_retry": False})
        self.calls: list[str] = []

    async def _dispatch_execute_many_impl(self, cursor: object, statement: object) -> str:
        self.calls.append("many")
        return "many"

    async def _dispatch_execute_script_impl(self, cursor: object, statement: object) -> str:
        self.calls.append("script")
        return "script"


@pytest.mark.anyio
async def test_cockroach_asyncpg_non_retry_execute_many_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachAsyncpgDriver()

    result = await driver.dispatch_execute_many(object(), object())  # type: ignore[arg-type]

    assert result == "many"
    assert driver.calls == ["many"]


@pytest.mark.anyio
async def test_cockroach_asyncpg_non_retry_execute_script_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachAsyncpgDriver()

    result = await driver.dispatch_execute_script(object(), object())  # type: ignore[arg-type]

    assert result == "script"
    assert driver.calls == ["script"]
