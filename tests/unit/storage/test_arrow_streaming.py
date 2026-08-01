"""Cross-backend tests for bounded Parquet streaming."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from sqlspec.storage._arrow_stream import iter_parquet_row_groups
from sqlspec.storage.backends.local import LocalStore
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED, PYARROW_INSTALLED


class _TrackedParquetFile:
    num_row_groups = 3

    def __init__(self) -> None:
        self.calls: list[tuple[int, list[int]]] = []

    def iter_batches(self, *, batch_size: int, row_groups: list[int], **kwargs: Any) -> Iterator[Any]:
        _ = kwargs
        self.calls.append((batch_size, row_groups))
        yield row_groups[0]


def test_row_group_iterator_does_not_touch_later_groups_before_first_batch() -> None:
    parquet_file = _TrackedParquetFile()
    batches = iter_parquet_row_groups(parquet_file, batch_size=17)

    assert next(batches) == 0
    assert parquet_file.calls == [(17, [0])]

    assert list(batches) == [1, 2]
    assert parquet_file.calls == [(17, [0]), (17, [1]), (17, [2])]


@pytest.mark.parametrize("batch_size", [0, -1])
def test_local_stream_rejects_nonpositive_batch_size_before_glob(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, batch_size: int
) -> None:
    store = LocalStore(str(tmp_path))
    monkeypatch.setattr(LocalStore, "glob_sync", lambda *_args, **_kwargs: pytest.fail("storage accessed"))

    with pytest.raises(ValueError, match="batch_size must be greater than zero"):
        list(store.stream_arrow_sync("*.parquet", batch_size=batch_size))


@pytest.mark.parametrize("pattern", ["*.csv", "data.jsonl", "data.arrow", "data.ipc"])
def test_local_stream_rejects_recognized_non_parquet_suffix_before_glob(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pattern: str
) -> None:
    store = LocalStore(str(tmp_path))
    monkeypatch.setattr(LocalStore, "glob_sync", lambda *_args, **_kwargs: pytest.fail("storage accessed"))

    with pytest.raises(ValueError, match="supports only Parquet"):
        list(store.stream_arrow_sync(pattern))


def test_local_stream_rejects_non_parquet_format_before_glob(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = LocalStore(str(tmp_path))
    monkeypatch.setattr(LocalStore, "glob_sync", lambda *_args, **_kwargs: pytest.fail("storage accessed"))

    with pytest.raises(ValueError, match="file_format='csv'"):
        list(store.stream_arrow_sync("*", file_format=cast("Any", "csv")))


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow missing")
def test_local_stream_preserves_multi_file_multi_row_group_order(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"value": [0, 1, 2]}), tmp_path / "a.parquet", row_group_size=2)
    pq.write_table(pa.table({"value": [3, 4, 5]}), tmp_path / "b.parquet", row_group_size=2)
    store = LocalStore(str(tmp_path))

    batches = list(store.stream_arrow_sync("*.parquet", batch_size=1))

    assert [value for batch in batches for value in batch.column(0).to_pylist()] == list(range(6))
    assert [batch.num_rows for batch in batches] == [1, 1, 1, 1, 1, 1]


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow missing")
async def test_local_async_stream_matches_sync(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"value": [0, 1, 2, 3]}), tmp_path / "data.parquet", row_group_size=2)
    store = LocalStore(str(tmp_path))

    sync_values = [
        value for batch in store.stream_arrow_sync("*.parquet", batch_size=1) for value in batch[0].to_pylist()
    ]
    async_values = [
        value async for batch in store.stream_arrow_async("*.parquet", batch_size=1) for value in batch[0].to_pylist()
    ]

    assert async_values == sync_values == [0, 1, 2, 3]


@pytest.mark.skipif(not FSSPEC_INSTALLED or not PYARROW_INSTALLED, reason="fsspec or PyArrow missing")
def test_fsspec_stream_preserves_base_path_and_order(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sqlspec.storage.backends.fsspec import FSSpecBackend

    data_path = tmp_path / "nested"
    data_path.mkdir()
    pq.write_table(pa.table({"value": [1, 2, 3]}), data_path / "data.parquet", row_group_size=2)

    store = FSSpecBackend(f"file://{tmp_path}", base_path="nested")
    values = [value for batch in store.stream_arrow_sync("*.parquet", batch_size=1) for value in batch[0].to_pylist()]

    assert values == [1, 2, 3]


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_obstore_stream_preserves_base_path_without_full_object_drain(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sqlspec.storage.backends.obstore import ObStoreBackend

    data_path = tmp_path / "nested"
    data_path.mkdir()
    pq.write_table(pa.table({"value": [1, 2, 3]}), data_path / "data.parquet", row_group_size=2)

    store = ObStoreBackend(f"file://{tmp_path}", base_path="nested")
    values = [value for batch in store.stream_arrow_sync("*.parquet", batch_size=1) for value in batch[0].to_pylist()]

    assert values == [1, 2, 3]


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_stream_closes_reader_on_early_generator_close(monkeypatch: pytest.MonkeyPatch) -> None:
    import obstore

    from sqlspec.storage.backends import obstore as backend_module
    from sqlspec.storage.backends.obstore import ObStoreBackend

    class Reader:
        closed = False

        def close(self) -> None:
            self.closed = True

        def seekable(self) -> bool:
            return True

    class ParquetFile:
        num_row_groups = 2

        def __init__(self, _stream: Any) -> None:
            pass

        def iter_batches(self, **kwargs: Any) -> Iterator[Any]:
            yield kwargs["row_groups"][0]

    reader = Reader()
    opened_paths: list[str] = []

    def open_reader(_store: Any, path: str) -> Reader:
        opened_paths.append(path)
        return reader

    monkeypatch.setattr(obstore, "open_reader", open_reader)
    monkeypatch.setattr(backend_module, "import_pyarrow_parquet", lambda: type("PQ", (), {"ParquetFile": ParquetFile}))
    monkeypatch.setattr(ObStoreBackend, "glob_sync", lambda *_args, **_kwargs: ["mybase/data.parquet"])
    store = ObStoreBackend("memory://", base_path="mybase")

    batches = store.stream_arrow_sync("*.parquet")
    assert next(batches) == 0
    cast("Any", batches).close()

    assert reader.closed
    assert opened_paths == ["mybase/data.parquet"]


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_stream_closes_reader_when_batch_iteration_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    import obstore

    from sqlspec.storage.backends import obstore as backend_module
    from sqlspec.storage.backends.obstore import ObStoreBackend

    class Reader:
        closed = False

        def close(self) -> None:
            self.closed = True

        def seekable(self) -> bool:
            return True

    class ParquetFile:
        num_row_groups = 1

        def __init__(self, _stream: Any) -> None:
            pass

        def iter_batches(self, **kwargs: Any) -> Iterator[Any]:
            _ = kwargs
            msg = "reader failed"
            raise OSError(msg)
            yield

    reader = Reader()
    monkeypatch.setattr(obstore, "open_reader", lambda *_args, **_kwargs: reader)
    monkeypatch.setattr(backend_module, "import_pyarrow_parquet", lambda: type("PQ", (), {"ParquetFile": ParquetFile}))
    monkeypatch.setattr(ObStoreBackend, "glob_sync", lambda *_args, **_kwargs: ["data.parquet"])
    store = ObStoreBackend("memory://")

    with pytest.raises(OSError, match="reader failed"):
        list(store.stream_arrow_sync("*.parquet"))

    assert reader.closed


def test_async_arrow_iterator_close_closes_active_sync_generator() -> None:
    from sqlspec.storage.backends.base import AsyncArrowBatchIterator

    closed = False

    def batches() -> Iterator[Any]:
        nonlocal closed
        try:
            yield object()
            yield object()
        finally:
            closed = True

    async def exercise() -> None:
        iterator = AsyncArrowBatchIterator(batches())
        await anext(iterator)
        await iterator.aclose()

    import asyncio

    asyncio.run(exercise())
    assert closed
