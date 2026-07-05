"""Unit tests for adapter streaming contract deviations."""

from typing import Any, cast

from typing_extensions import Self

from tests.integration.adapters.contracts._cases import DriverCase
from tests.integration.adapters.contracts._schema import DEFAULT_CONTRACT_TABLE
from tests.integration.adapters.contracts.behaviors import assert_sync_streaming_contract


class _FakeRowStream:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self._index = 0
        self._buffer: list[dict[str, Any]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __iter__(self) -> "_FakeRowStream":
        return self

    def __next__(self) -> dict[str, Any]:
        if self._index >= len(self._rows):
            self.close()
            raise StopIteration
        self._buffer = self._rows[self._index : self._index + 100]
        row = self._rows[self._index]
        self._index += 1
        return row

    def close(self) -> None:
        self._buffer = []


class _RaisingRowStream:
    _buffer: list[dict[str, Any]] = []

    def __iter__(self) -> "_RaisingRowStream":
        return self

    def __next__(self) -> dict[str, Any]:
        msg = "missing table"
        raise RuntimeError(msg)


class _FakeStreamingDriver:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.streamed_statements: list[object] = []

    def execute(self, statement: object, /, *parameters: object, **kwargs: Any) -> object:
        del statement, parameters, kwargs
        return object()

    def execute_many(self, statement: object, parameters: object, /, **kwargs: Any) -> object:
        del statement, kwargs
        rows = cast("list[tuple[str, int, object | None]]", parameters)
        self.rows = [{"name": name, "value": value, "note": note} for name, value, note in rows]
        return object()

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def select_value(self, statement: object, /, *parameters: object, **kwargs: Any) -> object:
        del statement, parameters, kwargs
        return len(self.rows)

    def select_stream(self, statement: object, /, *parameters: object, **kwargs: Any) -> object:
        del parameters, kwargs
        self.streamed_statements.append(statement)
        if statement == DEFAULT_CONTRACT_TABLE.select_ordered_sql:
            return _FakeRowStream(self.rows)
        return _RaisingRowStream()


def test_bigquery_streaming_deviation_avoids_emulator_reopen_paths() -> None:
    driver = _FakeStreamingDriver()
    case = DriverCase(
        id="bigquery-sync",
        fixture_name="contract_bigquery_driver",
        adapter="bigquery",
        dialect="bigquery",
        mode="sync",
        supports_native_row_streaming=True,
        streaming_row_count=60,
        supports_stream_reopen_after_partial_iteration=False,
        stream_chunk_policy="advisory",
        invalid_sql_error_policy="emulator_retries",
    )

    assert_sync_streaming_contract(driver, case)

    assert driver.streamed_statements == [DEFAULT_CONTRACT_TABLE.select_ordered_sql]
