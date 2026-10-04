"""Unit tests for SpannerAsyncDriver, SpannerAsyncExceptionHandler, and _SpannerAsyncSelectStreamSource."""

from types import SimpleNamespace
from typing import Any, cast

from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner.core import resolve_row_plan
from sqlspec.adapters.spanner.driver import SpannerAsyncExceptionHandler, _SpannerAsyncSelectStreamSource
from sqlspec.driver import AsyncRowStream
from sqlspec.exceptions import DeadlockError, UniqueViolationError
from sqlspec.utils.serializers import from_json


def _field(name: str, code: int) -> SimpleNamespace:
    return SimpleNamespace(name=name, type_=SimpleNamespace(code=code))


class _FakeAsyncResultSet:
    def __init__(self, rows: list[tuple[Any, ...]], fields: list[Any]) -> None:
        self._rows = rows
        self._fields = fields
        self.metadata: Any = None
        self.closed = False

    def __aiter__(self) -> "_FakeAsyncResultSet":
        self._index = 0
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        if self._index >= len(self._rows):
            raise StopAsyncIteration
        if self._index == 0:
            self.metadata = SimpleNamespace(row_type=SimpleNamespace(fields=self._fields))
        row = self._rows[self._index]
        self._index += 1
        return row

    async def close(self) -> None:
        self.closed = True


async def test_spanner_async_exception_handler_maps_google_api_errors() -> None:
    """Verify SpannerAsyncExceptionHandler maps GoogleAPICallError subclasses to SQLSpecError."""
    handler = SpannerAsyncExceptionHandler()
    async with handler:
        raise cast("Any", api_exceptions.Aborted)("Transaction was aborted.")

    assert isinstance(handler.pending_exception, DeadlockError)
    assert isinstance(handler.pending_exception.__cause__, api_exceptions.Aborted)

    handler2 = SpannerAsyncExceptionHandler()
    async with handler2:
        raise cast("Any", api_exceptions.AlreadyExists)("Row already exists.")

    assert isinstance(handler2.pending_exception, UniqueViolationError)


async def test_spanner_async_select_stream_source_chunks_and_resolves_metadata() -> None:
    """Verify _SpannerAsyncSelectStreamSource streams chunks via AsyncRowStream and resolves metadata lazily."""
    json_cls = cast("Any", JsonObject)
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.JSON)]
    fake_rs = _FakeAsyncResultSet(
        rows=[(1, json_cls({"k": "v1"})), (2, json_cls({"k": "v2"})), (3, json_cls({"k": "v3"}))],
        fields=fields,
    )

    class _FakeConnection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, Any, Any, dict[str, Any]]] = []

        async def execute_sql(
            self,
            sql: str,
            params: dict[str, Any] | None = None,
            param_types: dict[str, Any] | None = None,
            **kwargs: Any,
        ) -> _FakeAsyncResultSet:
            self.calls.append((sql, params, param_types, kwargs))
            return fake_rs

    conn = _FakeConnection()

    class _FakeDriver:
        def __init__(self) -> None:
            self.connection = conn

        def handle_database_exceptions(self) -> SpannerAsyncExceptionHandler:
            return SpannerAsyncExceptionHandler()

        def _check_pending_exception(self, exc_handler: SpannerAsyncExceptionHandler) -> None:
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception

        def _resolve_row_plan(self, result_fields: Any) -> tuple[list[str], tuple[tuple[int, Any], ...] | None]:
            return resolve_row_plan(result_fields, {}, json_deserializer=from_json)

    source = _SpannerAsyncSelectStreamSource(
        cast("Any", _FakeDriver()),
        "SELECT id, payload FROM items",
        {"p": 1},
        {},
        2,
        {"timeout": 5.0},
    )
    stream: AsyncRowStream[dict[str, Any]] = AsyncRowStream(source)
    async with stream as active_stream:
        collected = [item async for item in active_stream]

    assert collected == [
        {"id": 1, "payload": {"k": "v1"}},
        {"id": 2, "payload": {"k": "v2"}},
        {"id": 3, "payload": {"k": "v3"}},
    ]
    assert fake_rs.closed is True
