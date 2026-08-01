"""AsyncPG direct record COPY tests."""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
from sqlspec.exceptions import ImproperConfigurationError


@pytest.fixture
def connection() -> AsyncMock:
    return AsyncMock()


@pytest.mark.parametrize("driver_type", [AsyncpgDriver, CockroachAsyncpgDriver])
async def test_load_from_records_copies_mapping_rows_directly(
    driver_type: type[AsyncpgDriver], connection: AsyncMock
) -> None:
    driver = driver_type(connection)  # type: ignore[arg-type]
    payload = {"nested": {"enabled": True}, "items": [1, 2]}
    mapping_records: list[dict[str, Any]] = [{"id": 1, "payload": payload}, {"payload": None, "id": 2}]

    job = await driver.load_from_records('"audit.schema"."Event.Log"', mapping_records, columns=["payload", "id"])

    connection.copy_records_to_table.assert_awaited_once_with(
        "Event.Log", records=[(payload, 1), (None, 2)], columns=["payload", "id"], schema_name="audit.schema"
    )
    assert job.status == "completed"
    assert job.telemetry == {
        "bytes_processed": 0,
        "destination": '"audit.schema"."Event.Log"',
        "format": "records",
        "rows_processed": 2,
    }


async def test_load_from_records_preserves_first_mapping_key_order(connection: AsyncMock) -> None:
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    await driver.load_from_records("events", [{"b": 2, "a": 1}, {"a": 3, "b": 4}])

    connection.copy_records_to_table.assert_awaited_once_with(
        "events", records=[(2, 1), (4, 3)], columns=["b", "a"], schema_name=None
    )


async def test_load_from_records_truncates_quoted_target_before_copy(connection: AsyncMock) -> None:
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    await driver.load_from_records('"audit.schema"."Event.Log"', [(1,)], columns=["id"], overwrite=True)

    assert connection.method_calls[0].args == ('TRUNCATE TABLE "audit.schema"."Event.Log"',)
    assert connection.method_calls[1].args[0] == "Event.Log"


@pytest.mark.parametrize(
    "records,columns,match",
    [
        ([], None, "at least one record"),
        ([(1,)], None, "requires columns"),
        ([(1, 2)], ["id"], "number of columns"),
        ([{"id": 1}, (2,)], None, "all be mappings"),
        ([{"id": 1}, {"id": 2, "extra": 3}], None, "share the same keys"),
        ([(1,), {"id": 2}], ["id"], "same shape"),
    ],
)
async def test_load_from_records_validates_before_database_calls(
    connection: AsyncMock, records: list[object], columns: list[str] | None, match: str
) -> None:
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    with pytest.raises(ImproperConfigurationError, match=match):
        await driver.load_from_records("events", records, columns=columns)  # type: ignore[arg-type]

    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()


async def test_load_from_records_does_not_use_arrow_normalization(
    connection: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    def fail_if_called(*args: object) -> None:
        raise AssertionError("Arrow normalization was called")

    monkeypatch.setattr(AsyncpgDriver, "_records_to_arrow_table", fail_if_called)

    await driver.load_from_records("events", [(1,)], columns=["id"])

    connection.copy_records_to_table.assert_awaited_once()
