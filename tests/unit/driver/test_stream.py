"""Unit tests for row streaming primitives (sqlspec/driver/_stream.py)."""

from typing import Any

import pytest

from sqlspec.driver._stream import (
    AsyncRowSource,
    AsyncRowStream,
    EagerAsyncRowSource,
    EagerSyncRowSource,
    SyncRowSource,
    SyncRowStream,
    rows_to_dicts,
)


def _rows(start: int, stop: int) -> "list[dict[str, Any]]":
    return [{"id": i} for i in range(start, stop)]


def _sync_stream(source: SyncRowSource) -> "SyncRowStream[dict[str, Any]]":
    return SyncRowStream(source)


def _async_stream(source: AsyncRowSource) -> "AsyncRowStream[dict[str, Any]]":
    return AsyncRowStream(source)


class FakeSyncSource:
    """Records calls; serves predefined chunks then empty list to signal exhaustion."""

    def __init__(self, chunks: "list[list[dict[str, Any]]]") -> None:
        self._chunks = [list(chunk) for chunk in chunks]
        self.start_calls = 0
        self.fetch_calls = 0
        self.close_calls = 0
        self.close_errors: list[bool] = []

    def start(self) -> None:
        self.start_calls += 1

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        self.fetch_calls += 1
        if self._chunks:
            return self._chunks.pop(0)
        return []

    def close(self, error: bool = False) -> None:
        self.close_calls += 1
        self.close_errors.append(error)


class FakeAsyncSource:
    """Async mirror of FakeSyncSource."""

    def __init__(self, chunks: "list[list[dict[str, Any]]]") -> None:
        self._chunks = [list(chunk) for chunk in chunks]
        self.start_calls = 0
        self.fetch_calls = 0
        self.close_calls = 0
        self.close_errors: list[bool] = []

    async def start(self) -> None:
        self.start_calls += 1

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        self.fetch_calls += 1
        if self._chunks:
            return self._chunks.pop(0)
        return []

    async def close(self, error: bool = False) -> None:
        self.close_calls += 1
        self.close_errors.append(error)


# --------------------------------------------------------------------------- #
# SyncRowStream
# --------------------------------------------------------------------------- #


def test_sync_iterates_all_rows_in_order_across_chunks() -> None:
    source = FakeSyncSource([_rows(0, 10), _rows(10, 20), _rows(20, 25)])
    stream = _sync_stream(source)

    collected = list(stream)

    assert collected == _rows(0, 25)
    assert source.start_calls == 1
    assert source.fetch_calls == 4  # ceil(25/10) data chunks + 1 terminating empty


def test_sync_buffer_is_bounded_to_chunk_size() -> None:
    source = FakeSyncSource([_rows(0, 10), _rows(10, 20)])
    stream = _sync_stream(source)

    iterator = iter(stream)
    first = next(iterator)

    assert first == {"id": 0}
    assert len(stream._buffer) == 10  # pyright: ignore[reportPrivateUsage]


def test_sync_close_mid_iteration_is_idempotent_and_stops() -> None:
    source = FakeSyncSource([_rows(0, 5)])
    stream = _sync_stream(source)
    iterator = iter(stream)
    next(iterator)

    stream.close()
    assert source.close_calls == 1

    with pytest.raises(StopIteration):
        next(iterator)

    stream.close()
    assert source.close_calls == 1


def test_sync_fetch_chunk_exception_propagates_and_closes() -> None:
    class RaisingFetch(FakeSyncSource):
        def fetch_chunk(self) -> "list[dict[str, Any]]":
            self.fetch_calls += 1
            raise RuntimeError("fetch boom")

    source = RaisingFetch([])
    stream = _sync_stream(source)

    with pytest.raises(RuntimeError, match="fetch boom"):
        list(stream)

    assert source.close_calls == 1


def test_sync_start_exception_propagates_and_closes() -> None:
    class RaisingStart(FakeSyncSource):
        def start(self) -> None:
            self.start_calls += 1
            raise RuntimeError("start boom")

    source = RaisingStart([_rows(0, 5)])
    stream = _sync_stream(source)

    with pytest.raises(RuntimeError, match="start boom"):
        next(iter(stream))

    assert source.close_calls == 1


def test_sync_context_manager_normal_exit_closes() -> None:
    source = FakeSyncSource([_rows(0, 5)])
    with _sync_stream(source) as stream:
        assert next(iter(stream)) == {"id": 0}
    assert source.close_calls == 1
    assert source.close_errors == [False]


def test_sync_context_manager_exception_exit_closes() -> None:
    source = FakeSyncSource([_rows(0, 5)])
    with pytest.raises(ValueError, match="boom"), _sync_stream(source):
        raise ValueError("boom")
    assert source.close_calls == 1
    assert source.close_errors == [True]


def test_sync_close_does_not_retry_when_source_raises_type_error() -> None:
    class RaisingClose(FakeSyncSource):
        def close(self, error: bool = False) -> None:
            super().close(error=error)
            raise TypeError("close boom")

    source = RaisingClose([])
    _sync_stream(source).close()

    assert source.close_calls == 1


def test_sync_close_supports_legacy_no_argument_source() -> None:
    class LegacyCloseSource:
        def __init__(self) -> None:
            self.close_calls = 0

        def start(self) -> None:
            pass

        def fetch_chunk(self) -> "list[dict[str, Any]]":
            return []

        def close(self) -> None:
            self.close_calls += 1

    source = LegacyCloseSource()
    _sync_stream(source).close()

    assert source.close_calls == 1


# --------------------------------------------------------------------------- #
# AsyncRowStream
# --------------------------------------------------------------------------- #


async def test_async_iterates_all_rows_in_order_across_chunks() -> None:
    source = FakeAsyncSource([_rows(0, 10), _rows(10, 20), _rows(20, 25)])
    stream = _async_stream(source)

    collected = [row async for row in stream]

    assert collected == _rows(0, 25)
    assert source.start_calls == 1
    assert source.fetch_calls == 4


async def test_async_buffer_is_bounded_to_chunk_size() -> None:
    source = FakeAsyncSource([_rows(0, 10), _rows(10, 20)])
    stream = _async_stream(source)

    iterator = stream.__aiter__()
    first = await iterator.__anext__()

    assert first == {"id": 0}
    assert len(stream._buffer) == 10  # pyright: ignore[reportPrivateUsage]


async def test_async_aclose_mid_iteration_is_idempotent_and_stops() -> None:
    source = FakeAsyncSource([_rows(0, 5)])
    stream = _async_stream(source)
    iterator = stream.__aiter__()
    await iterator.__anext__()

    await stream.aclose()
    assert source.close_calls == 1

    with pytest.raises(StopAsyncIteration):
        await iterator.__anext__()

    await stream.aclose()
    assert source.close_calls == 1


async def test_async_fetch_chunk_exception_propagates_and_closes() -> None:
    class RaisingFetch(FakeAsyncSource):
        async def fetch_chunk(self) -> "list[dict[str, Any]]":
            self.fetch_calls += 1
            raise RuntimeError("fetch boom")

    source = RaisingFetch([])
    stream = _async_stream(source)

    with pytest.raises(RuntimeError, match="fetch boom"):
        async for _ in stream:
            pass

    assert source.close_calls == 1


async def test_async_start_exception_propagates_and_closes() -> None:
    class RaisingStart(FakeAsyncSource):
        async def start(self) -> None:
            self.start_calls += 1
            raise RuntimeError("start boom")

    source = RaisingStart([_rows(0, 5)])
    stream = _async_stream(source)

    with pytest.raises(RuntimeError, match="start boom"):
        await stream.__aiter__().__anext__()

    assert source.close_calls == 1


async def test_async_context_manager_normal_exit_closes() -> None:
    source = FakeAsyncSource([_rows(0, 5)])
    async with _async_stream(source) as stream:
        assert await stream.__aiter__().__anext__() == {"id": 0}
    assert source.close_calls == 1
    assert source.close_errors == [False]


async def test_async_context_manager_exception_exit_closes() -> None:
    source = FakeAsyncSource([_rows(0, 5)])
    with pytest.raises(ValueError, match="boom"):
        async with _async_stream(source):
            raise ValueError("boom")
    assert source.close_calls == 1
    assert source.close_errors == [True]


async def test_async_close_does_not_retry_when_source_raises_type_error() -> None:
    class RaisingClose(FakeAsyncSource):
        async def close(self, error: bool = False) -> None:
            await super().close(error=error)
            raise TypeError("close boom")

    source = RaisingClose([])
    await _async_stream(source).aclose()

    assert source.close_calls == 1


async def test_async_close_supports_legacy_no_argument_source() -> None:
    class LegacyCloseSource:
        def __init__(self) -> None:
            self.close_calls = 0

        async def start(self) -> None:
            pass

        async def fetch_chunk(self) -> "list[dict[str, Any]]":
            return []

        async def close(self) -> None:
            self.close_calls += 1

    source = LegacyCloseSource()
    await _async_stream(source).aclose()

    assert source.close_calls == 1


# --------------------------------------------------------------------------- #
# Eager sources
# --------------------------------------------------------------------------- #


def test_eager_sync_source_chunks_then_signals_exhaustion() -> None:
    source = EagerSyncRowSource(_rows(0, 25), 10)
    source.start()

    assert source.fetch_chunk() == _rows(0, 10)
    assert source.fetch_chunk() == _rows(10, 20)
    assert source.fetch_chunk() == _rows(20, 25)
    assert source.fetch_chunk() == []

    source.close()


async def test_eager_async_source_chunks_then_signals_exhaustion() -> None:
    source = EagerAsyncRowSource(_rows(0, 25), 10)
    await source.start()

    assert await source.fetch_chunk() == _rows(0, 10)
    assert await source.fetch_chunk() == _rows(10, 20)
    assert await source.fetch_chunk() == _rows(20, 25)
    assert await source.fetch_chunk() == []

    await source.close()


def test_eager_sync_source_drives_sync_stream_end_to_end() -> None:
    stream = _sync_stream(EagerSyncRowSource(_rows(0, 25), 10))
    assert list(stream) == _rows(0, 25)


async def test_eager_async_source_drives_async_stream_end_to_end() -> None:
    stream = _async_stream(EagerAsyncRowSource(_rows(0, 25), 10))
    assert [row async for row in stream] == _rows(0, 25)


# --------------------------------------------------------------------------- #
# rows_to_dicts
# --------------------------------------------------------------------------- #


def test_rows_to_dicts_zips_tuple_rows_with_column_names() -> None:
    result = rows_to_dicts([(1, "a"), (2, "b")], ["id", "name"])
    assert result == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_rows_to_dicts_empty_column_names_returns_empty() -> None:
    assert rows_to_dicts([(1,), (2,)], []) == []
