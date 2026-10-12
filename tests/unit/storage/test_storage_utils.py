"""Unit tests for storage utilities."""

import asyncio
import io
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, get_args

import pyarrow as pa
import pytest

import sqlspec.storage as storage_pkg
import sqlspec.storage.backends as storage_backends_pkg
from sqlspec.exceptions import (
    FileNotFoundInStorageError,
    MissingDependencyError,
    StorageOperationFailedError,
    StoragePathTraversalError,
)
from sqlspec.storage import StorageFormat as PublicStorageFormat
from sqlspec.storage import resolve_storage_path
from sqlspec.storage._arrow_payload import StorageFormat as ArrowPayloadStorageFormat
from sqlspec.storage._arrow_payload import decode_arrow_payload, encode_arrow_payload
from sqlspec.storage._paths import parse_file_uri_path, strip_windows_drive_prefix
from sqlspec.storage._paths import resolve_storage_path as resolve_compiled_storage_path
from sqlspec.storage._utils import import_pyarrow, import_pyarrow_csv, import_pyarrow_parquet
from sqlspec.storage.backends.base import (
    AsyncArrowBatchIterator,
    AsyncObStoreStreamIterator,
    AsyncThreadedBytesIterator,
    ObjectStoreBase,
)
from sqlspec.storage.backends.fsspec import FSSpecBackend
from sqlspec.storage.backends.local import LocalStore
from sqlspec.storage.backends.obstore import ObStoreBackend
from sqlspec.storage.errors import execute_sync_storage_operation, raise_storage_error
from sqlspec.storage.pipeline import (
    AsyncStoragePipeline,
    StorageTelemetry,
    SyncStoragePipeline,
    get_recent_storage_events,
    record_storage_diagnostic_event,
    reset_storage_bridge_events,
)
from sqlspec.storage.pipeline import StorageFormat as PipelineStorageFormat
from sqlspec.storage.registry import StorageRegistry
from sqlspec.typing import OBSTORE_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.sync_tools import set_default_async_executor


def test_ensure_pyarrow_succeeds_when_installed() -> None:
    """Test ensure_pyarrow succeeds when pyarrow is available."""
    if not PYARROW_INSTALLED:
        pytest.skip("pyarrow not installed")

    ensure_pyarrow()


def test_import_pyarrow_helpers_return_modules_when_installed() -> None:
    """The storage import helpers should return the guarded PyArrow modules."""
    if not PYARROW_INSTALLED:
        pytest.skip("pyarrow not installed")

    assert import_pyarrow().__name__ == "pyarrow"
    assert import_pyarrow_parquet().__name__ == "pyarrow.parquet"
    assert import_pyarrow_csv().__name__ == "pyarrow.csv"


def test_ensure_pyarrow_raises_when_not_installed() -> None:
    """Test ensure_pyarrow raises error when pyarrow not available."""
    if PYARROW_INSTALLED:
        pytest.skip("pyarrow is installed")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        ensure_pyarrow()


def test_resolve_storage_path_file_protocol_absolute() -> None:
    """Test path resolution for file protocol with absolute path."""
    result = resolve_storage_path("/data/file.txt", base_path="", protocol="file")
    assert result == "data/file.txt"


def test_resolve_storage_path_file_protocol_with_base() -> None:
    """Test path resolution for file protocol with base_path."""
    result = resolve_storage_path("file.txt", base_path="/base", protocol="file")
    assert result == "/base/file.txt"


def test_resolve_storage_path_file_scheme_stripping() -> None:
    """Test file:// scheme stripping."""
    result = resolve_storage_path("file:///data/file.txt", base_path="", protocol="file", strip_file_scheme=True)
    assert result == "data/file.txt"


def test_resolve_storage_path_file_scheme_preserved() -> None:
    """Test file:// scheme preserved when strip_file_scheme=False."""
    result = resolve_storage_path("file:///data/file.txt", base_path="", protocol="file", strip_file_scheme=False)
    assert result == "file:///data/file.txt"


def test_resolve_storage_path_cloud_protocol() -> None:
    """Test path resolution for cloud protocols."""
    result = resolve_storage_path("data/file.txt", base_path="bucket/prefix", protocol="s3")
    assert result == "bucket/prefix/data/file.txt"


def test_resolve_storage_path_cloud_absolute() -> None:
    """Test absolute path handling for cloud protocols."""
    result = resolve_storage_path("/data/file.txt", base_path="", protocol="s3")
    assert result == "/data/file.txt"


def test_resolve_storage_path_no_base_path() -> None:
    """Test path resolution without base_path."""
    result = resolve_storage_path("data/file.txt", base_path="", protocol="s3")
    assert result == "data/file.txt"


def test_resolve_storage_path_pathlib_input() -> None:
    """Test path resolution with pathlib.Path input."""
    result = resolve_storage_path(Path("data") / "file.txt", base_path="", protocol="file")
    assert result == "data/file.txt"


def test_compiled_storage_path_helper_matches_public_reexport() -> None:
    """The compiled path helper should preserve the public storage re-export behavior."""
    path = "file:///tmp/data/file.txt"

    assert resolve_compiled_storage_path(path, base_path="/tmp", protocol="file", strip_file_scheme=True) == (
        resolve_storage_path(path, base_path="/tmp", protocol="file", strip_file_scheme=True)
    )


def test_pipeline_reexports_arrow_payload_storage_format_literal() -> None:
    assert PipelineStorageFormat is ArrowPayloadStorageFormat


def test_public_storage_format_matches_pipeline_reexport() -> None:
    assert PublicStorageFormat is PipelineStorageFormat


def test_arrow_payload_storage_format_only_advertises_encodable_formats() -> None:
    assert set(get_args(ArrowPayloadStorageFormat)) == {"jsonl", "json", "parquet", "arrow-ipc", "csv"}


def test_strip_windows_drive_prefix_handles_any_drive_letter() -> None:
    """Verify strip_windows_drive_prefix strips leading slash for any drive letter."""
    assert strip_windows_drive_prefix("/C:/data/file.txt") == "C:/data/file.txt"
    assert strip_windows_drive_prefix("/D:/archive/item.parquet") == "D:/archive/item.parquet"
    assert strip_windows_drive_prefix("/z:/tmp/a.json") == "z:/tmp/a.json"
    assert strip_windows_drive_prefix("/var/data/file.txt") == "/var/data/file.txt"


def test_parse_file_uri_path_handles_windows_and_fragments() -> None:
    """Verify parse_file_uri_path decodes percent-encoding, fragments, and Windows drives."""
    assert parse_file_uri_path("file:///D:/my%20data/part%231.parquet#frag") == "D:/my data/part#1.parquet#frag"
    assert parse_file_uri_path("file://C:/data/file.txt") == "C:/data/file.txt"
    assert parse_file_uri_path("file://tmp/payload.jsonl") == "tmp/payload.jsonl"


def test_resolve_storage_path_rejects_escape_from_base_path() -> None:
    """Verify resolve_storage_path rejects absolute paths escaping base_path."""
    with pytest.raises(StoragePathTraversalError):
        resolve_storage_path("/etc/passwd", base_path="/var/data", protocol="file")
    with pytest.raises(StoragePathTraversalError):
        resolve_storage_path("D:/secret/file.txt", base_path="C:/data", protocol="file")
    assert resolve_storage_path("C:/data/sub/file.txt", base_path="C:/data", protocol="file") == "C:/data/sub/file.txt"


def test_local_store_glob_and_list_reject_escape_outside_base_path(tmp_path: Path) -> None:
    """Verify LocalStore.glob_sync and list_objects_sync enforce base_path containment."""
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "inside.txt").write_text("ok", encoding="utf-8")
    secret = outside / "secret.txt"
    secret.write_text("secret", encoding="utf-8")
    (root / "escape_link.txt").symlink_to(secret)

    store = LocalStore(str(root))
    with pytest.raises(StoragePathTraversalError):
        store.glob_sync(f"{outside}/*.txt")
    with pytest.raises(StoragePathTraversalError):
        store.glob_sync("../*.txt")
    with pytest.raises(StoragePathTraversalError):
        store.list_objects_sync(prefix="../outside")

    assert store.glob_sync("*.txt") == ["inside.txt"]
    assert store.list_objects_sync() == ["inside.txt"]


def test_raise_storage_error_preserves_file_not_found_in_storage_error() -> None:
    """Verify raise_storage_error does not wrap FileNotFoundInStorageError into StorageOperationFailedError."""
    original = FileNotFoundInStorageError("missing.parquet")
    with pytest.raises(FileNotFoundInStorageError) as exc_info:
        execute_sync_storage_operation(
            lambda: (_ for _ in ()).throw(original), backend="local", operation="read_bytes", path="missing.parquet"
        )
    assert exc_info.value is original

    already_wrapped = StorageOperationFailedError("write failed for a.bin")
    with pytest.raises(StorageOperationFailedError) as op_exc_info:
        raise_storage_error(already_wrapped, backend="local", operation="outer", path="a.bin")
    assert op_exc_info.value is already_wrapped


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow not installed")
def test_arrow_ipc_compression_and_empty_json_decode() -> None:
    """Verify arrow-ipc honors compression and decode_arrow_payload handles empty JSON."""
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    compressed = encode_arrow_payload(table, "arrow-ipc", compression="zstd")
    decoded = decode_arrow_payload(compressed, "arrow-ipc")
    assert decoded.to_pydict() == {"id": [1, 2, 3], "name": ["a", "b", "c"]}

    empty_json_table = decode_arrow_payload(b"   ", "json")
    assert empty_json_table.num_rows == 0


def test_async_iterators_use_default_async_executor_and_convert_obstore_chunks() -> None:
    """Verify AsyncArrowBatchIterator, AsyncThreadedBytesIterator, and AsyncObStoreStreamIterator contracts."""

    async def _run() -> None:
        executor_calls = 0

        class TrackingExecutor(ThreadPoolExecutor):
            def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
                nonlocal executor_calls
                executor_calls += 1
                return super().submit(fn, *args, **kwargs)

        tracking_executor = TrackingExecutor(max_workers=2)
        set_default_async_executor(tracking_executor)
        try:
            bio = io.BytesIO(b"abcdefghijk")
            byte_iter = AsyncThreadedBytesIterator(bio, chunk_size=5)
            chunks = [chunk async for chunk in byte_iter]
            await byte_iter.aclose()
            assert chunks == [b"abcde", b"fghij", b"k"]
            assert executor_calls >= 4

            if PYARROW_INSTALLED:
                batch = pa.record_batch({"x": [1, 2]})
                arrow_iter = AsyncArrowBatchIterator(iter([batch]))
                batches = [item async for item in arrow_iter]
                assert len(batches) == 1
        finally:
            set_default_async_executor(None)
            tracking_executor.shutdown(wait=True)

        closed_flag = False

        class FakeObstoreStream:
            def __init__(self) -> None:
                self._items = iter([bytearray(b"abc"), memoryview(b"def")])

            async def __anext__(self) -> Any:
                try:
                    return next(self._items)
                except StopIteration as exc:
                    raise StopAsyncIteration from exc

            async def aclose(self) -> None:
                nonlocal closed_flag
                closed_flag = True

        async with AsyncObStoreStreamIterator(FakeObstoreStream(), chunk_size=3) as ob_iter:
            first = await anext(ob_iter)
            second = await anext(ob_iter)
            assert type(first) is bytes
            assert type(second) is bytes
            assert first + second == b"abcdef"
        assert closed_flag is True

    asyncio.run(_run())


def test_pipeline_direct_cloud_uri_strips_bucket_from_object_key() -> None:
    """Verify SyncStoragePipeline and AsyncStoragePipeline do not duplicate bucket name in object key."""
    written: list[tuple[str, str, bytes]] = []

    class FakeCloudStore(ObjectStoreBase):
        protocol = "s3"
        backend_type = "obstore"

        def __init__(self, uri: str, **kwargs: Any) -> None:
            self.store_uri = uri
            self.options = kwargs

        @property
        def supports_signing(self) -> bool:
            return True

        def resolve_uri(self, path: str | Path) -> str:
            return f"{self.store_uri}/{str(path).lstrip('/')}"

        def read_bytes_sync(self, path: str | Path, **kwargs: Any) -> bytes:
            del path, kwargs
            return b'{"id": 1}\n'

        def write_bytes_sync(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
            del kwargs
            written.append((self.store_uri, str(path), data))

        def stream_read_sync(self, path: str | Path, chunk_size: int | None = None, **kwargs: Any) -> Any:
            del path, chunk_size, kwargs
            return iter([b"data"])

        def read_text_sync(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
            del path, encoding, kwargs
            return ""

        def write_text_sync(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
            del path, data, encoding, kwargs

        def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
            del prefix, recursive, kwargs
            return []

        def exists_sync(self, path: str | Path, **kwargs: Any) -> bool:
            del path, kwargs
            return True

        def delete_sync(self, path: str | Path, **kwargs: Any) -> None:
            del path, kwargs

        def copy_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
            del source, destination, kwargs

        def move_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
            del source, destination, kwargs

        def glob_sync(self, pattern: str, **kwargs: Any) -> list[str]:
            del pattern, kwargs
            return []

        def get_metadata_sync(self, path: str | Path, **kwargs: Any) -> dict[str, object]:
            del path, kwargs
            return {}

        def is_object_sync(self, path: str | Path) -> bool:
            del path
            return True

        def is_path_sync(self, path: str | Path) -> bool:
            del path
            return False

        def read_arrow_sync(self, path: str | Path, **kwargs: Any) -> Any:
            raise NotImplementedError

        def write_arrow_sync(self, path: str | Path, table: Any, **kwargs: Any) -> None:
            raise NotImplementedError

        def stream_arrow_sync(
            self, pattern: str, *, file_format: Any = "parquet", batch_size: int = 65536, **kwargs: Any
        ) -> Any:
            raise NotImplementedError

        def sign_sync(self, paths: Any, expires_in: int = 3600, for_upload: bool = False) -> Any:
            del paths, expires_in, for_upload
            return ""

        async def read_bytes_async(self, path: str | Path, **kwargs: Any) -> bytes:
            del path, kwargs
            return b'{"id": 1}\n'

        async def write_bytes_async(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
            del kwargs
            written.append((self.store_uri, str(path), data))

        async def read_text_async(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
            del path, encoding, kwargs
            return ""

        async def write_text_async(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
            del path, data, encoding, kwargs

        async def stream_read_async(self, path: str | Path, chunk_size: int | None = None, **kwargs: Any) -> Any:
            del path, chunk_size, kwargs
            return AsyncObStoreStreamIterator(iter([]))

        async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
            del prefix, recursive, kwargs
            return []

        async def exists_async(self, path: str | Path, **kwargs: Any) -> bool:
            del path, kwargs
            return True

        async def delete_async(self, path: str | Path, **kwargs: Any) -> None:
            del path, kwargs

        async def copy_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
            del source, destination, kwargs

        async def move_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
            del source, destination, kwargs

        async def get_metadata_async(self, path: str | Path, **kwargs: Any) -> dict[str, object]:
            del path, kwargs
            return {}

        async def read_arrow_async(self, path: str | Path, **kwargs: Any) -> Any:
            raise NotImplementedError

        async def write_arrow_async(self, path: str | Path, table: Any, **kwargs: Any) -> None:
            raise NotImplementedError

        def stream_arrow_async(
            self, pattern: str, *, file_format: Any = "parquet", batch_size: int = 65536, **kwargs: Any
        ) -> Any:
            raise NotImplementedError

        async def sign_async(self, paths: Any, expires_in: int = 3600, for_upload: bool = False) -> Any:
            del paths, expires_in, for_upload
            return ""

    class CustomRegistry(StorageRegistry):
        def _backend_from_uri(self, uri: str, **kwargs: Any) -> Any:
            return FakeCloudStore(uri, **kwargs)

    registry = CustomRegistry()

    sync_pipeline = SyncStoragePipeline(registry=registry)
    telemetry = sync_pipeline.write_rows([{"id": 1}], "s3://my-bucket/folder/items.jsonl")
    assert telemetry["destination"] == "folder/items.jsonl"
    assert written[-1][0] == "s3://my-bucket"
    assert written[-1][1] == "folder/items.jsonl"

    async def _run_async() -> None:
        async_pipeline = AsyncStoragePipeline(registry=registry)
        async_telemetry = await async_pipeline.write_rows([{"id": 2}], "s3://my-bucket/folder/async_items.jsonl")
        assert async_telemetry["destination"] == "folder/async_items.jsonl"
        assert written[-1][0] == "s3://my-bucket"
        assert written[-1][1] == "folder/async_items.jsonl"

    asyncio.run(_run_async())


def test_recent_storage_events_most_recent_first_and_registry_hashable() -> None:
    """Verify get_recent_storage_events order and StorageRegistry._make_hashable resilience."""
    reset_storage_bridge_events()
    first: StorageTelemetry = {"destination": "first.parquet", "bytes_processed": 10, "rows_processed": 1}
    second: StorageTelemetry = {"destination": "second.parquet", "bytes_processed": 20, "rows_processed": 2}
    record_storage_diagnostic_event(first)
    record_storage_diagnostic_event(second)
    events = get_recent_storage_events()
    assert [e["destination"] for e in events[:2]] == ["second.parquet", "first.parquet"]

    registry = StorageRegistry()
    hashed = registry._make_hashable({"mixed_set": {1, "two"}, "path": Path("/tmp/test")})
    assert isinstance(hashed, tuple)


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_obstore_write_arrow_async_casts_decimal64() -> None:
    """Verify ObStoreBackend.write_arrow_async casts decimal64 columns to decimal128."""

    async def _run() -> None:
        store = ObStoreBackend("memory://")
        dec64_type = getattr(pa, "decimal64", None)
        if dec64_type is None:
            pytest.skip("pyarrow.decimal64 not available")
        arr = pa.array([123, 456], type=dec64_type(10, 2))
        table = pa.table({"amount": arr})
        await store.write_arrow_async("dec64.parquet", table)
        loaded = await store.read_arrow_async("dec64.parquet")
        assert loaded.num_rows == 2

    asyncio.run(_run())


def test_storage_and_backends_pep562_lazy_exports() -> None:
    """Verify PEP 562 __getattr__ and __dir__ on sqlspec.storage and sqlspec.storage.backends."""
    assert "SyncStoragePipeline" in dir(storage_pkg)
    assert storage_pkg.__getattr__("SyncStoragePipeline") is SyncStoragePipeline
    with pytest.raises(AttributeError):
        storage_pkg.__getattr__("NonExistentExport")

    assert "LocalStore" in dir(storage_backends_pkg)
    assert storage_backends_pkg.__getattr__("LocalStore") is LocalStore
    assert storage_backends_pkg.__getattr__("FSSpecBackend") is FSSpecBackend
    assert storage_backends_pkg.__getattr__("ObStoreBackend") is ObStoreBackend
    assert storage_backends_pkg.__getattr__("ObjectStoreBase") is ObjectStoreBase
    with pytest.raises(AttributeError):
        storage_backends_pkg.__getattr__("NonExistentBackend")
