"""CockroachDB psycopg driver regression tests."""

import pytest

from sqlspec.adapters.cockroach_psycopg.data_dictionary import (
    CockroachPsycopgAsyncDataDictionary,
    CockroachPsycopgSyncDataDictionary,
)
from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver


class _RecordingCockroachPsycopgSyncDriver(CockroachPsycopgSyncDriver):
    def __init__(self) -> None:
        super().__init__(connection=object(), driver_features={"enable_auto_retry": False})
        self.calls: list[str] = []

    def _dispatch_execute_many_impl(self, cursor: object, statement: object) -> str:
        self.calls.append("many")
        return "many"

    def _dispatch_execute_script_impl(self, cursor: object, statement: object) -> str:
        self.calls.append("script")
        return "script"


class _RecordingCockroachPsycopgAsyncDriver(CockroachPsycopgAsyncDriver):
    def __init__(self) -> None:
        super().__init__(connection=object(), driver_features={"enable_auto_retry": False})
        self.calls: list[str] = []

    async def _dispatch_execute_many_impl(self, cursor: object, statement: object) -> str:
        self.calls.append("many")
        return "many"

    async def _dispatch_execute_script_impl(self, cursor: object, statement: object) -> str:
        self.calls.append("script")
        return "script"


def test_cockroach_psycopg_sync_non_retry_execute_many_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachPsycopgSyncDriver()

    result = driver.dispatch_execute_many(object(), object())  # type: ignore[arg-type]

    assert result == "many"
    assert driver.calls == ["many"]


def test_cockroach_psycopg_sync_non_retry_execute_script_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachPsycopgSyncDriver()

    result = driver.dispatch_execute_script(object(), object())  # type: ignore[arg-type]

    assert result == "script"
    assert driver.calls == ["script"]


@pytest.mark.anyio
async def test_cockroach_psycopg_async_non_retry_execute_many_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachPsycopgAsyncDriver()

    result = await driver.dispatch_execute_many(object(), object())  # type: ignore[arg-type]

    assert result == "many"
    assert driver.calls == ["many"]


@pytest.mark.anyio
async def test_cockroach_psycopg_async_non_retry_execute_script_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachPsycopgAsyncDriver()

    result = await driver.dispatch_execute_script(object(), object())  # type: ignore[arg-type]

    assert result == "script"
    assert driver.calls == ["script"]


def test_cockroach_psycopg_sync_data_dictionary_uses_parent_slot() -> None:
    driver = CockroachPsycopgSyncDriver(connection=object())

    assert isinstance(driver.data_dictionary, CockroachPsycopgSyncDataDictionary)


def test_cockroach_psycopg_async_data_dictionary_uses_parent_slot() -> None:
    driver = CockroachPsycopgAsyncDriver(connection=object())

    assert isinstance(driver.data_dictionary, CockroachPsycopgAsyncDataDictionary)
