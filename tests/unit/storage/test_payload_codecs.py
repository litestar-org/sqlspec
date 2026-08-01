# pyright: reportPrivateUsage=false
"""Focused storage payload codec and write-format contract tests."""

from typing import Any

import pyarrow as pa
import pytest

from sqlspec.storage._arrow_payload import decode_arrow_payload, encode_arrow_payload
from sqlspec.storage.pipeline import (
    AsyncStoragePipeline,
    SyncStoragePipeline,
    get_recent_storage_events,
    reset_storage_bridge_events,
)


class _TrackingBackend:
    backend_type = "tracking"

    def __init__(self) -> None:
        self.writes: list[tuple[str, bytes]] = []

    def write_bytes_sync(self, path: str, payload: bytes) -> None:
        self.writes.append((path, payload))

    async def write_bytes_async(self, path: str, payload: bytes) -> None:
        self.writes.append((path, payload))


def _install_backend(monkeypatch: pytest.MonkeyPatch, pipeline_type: type[Any]) -> _TrackingBackend:
    backend = _TrackingBackend()
    monkeypatch.setattr(pipeline_type, "_backend", lambda *_args, **_kwargs: (backend, "payload", backend.backend_type))
    return backend


def test_decode_jsonl_uses_native_timestamp_inference() -> None:
    payload = b'{"id":1,"created_at":"2026-08-01T12:34:56"}\n'

    table = decode_arrow_payload(payload, "jsonl")

    assert table.schema.field("created_at").type == pa.timestamp("s")
    assert table.to_pylist() == [{"id": 1, "created_at": table.column("created_at")[0].as_py()}]


def test_decode_empty_jsonl_preserves_empty_table() -> None:
    table = decode_arrow_payload(b"", "jsonl")

    assert table.equals(pa.table({}))


def test_decode_whitespace_only_jsonl_is_delegated_to_pyarrow() -> None:
    table = decode_arrow_payload(b" \n\t", "jsonl")

    assert table.equals(pa.table({}))


def test_json_array_decode_keeps_existing_shape() -> None:
    table = decode_arrow_payload(b'[{"id":1},{"id":2}]', "json")

    assert table.to_pylist() == [{"id": 1}, {"id": 2}]


@pytest.mark.parametrize("format_choice", ["parquet", "arrow-ipc", "csv"])
def test_arrow_payload_round_trip_has_expected_signature(format_choice: str) -> None:
    table = pa.table({"id": [1, 2], "name": ["alpha", "beta"]})

    payload = encode_arrow_payload(table, format_choice, compression=None)  # type: ignore[arg-type]
    restored = decode_arrow_payload(payload, format_choice)  # type: ignore[arg-type]

    signatures = {"parquet": b"PAR1", "arrow-ipc": b"ARROW1", "csv": b'"id","name"'}
    assert payload.startswith(signatures[format_choice])
    assert restored.to_pylist() == table.to_pylist()


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
def test_sync_row_write_rejects_arrow_formats_before_encoding_or_io(
    monkeypatch: pytest.MonkeyPatch, format_hint: str
) -> None:
    pipeline = SyncStoragePipeline()
    backend = _install_backend(monkeypatch, SyncStoragePipeline)
    reset_storage_bridge_events()

    with pytest.raises(ValueError, match="Row storage writes support only JSON and JSONL"):
        pipeline.write_rows([{"id": 1}], "payload", format_hint=format_hint)  # type: ignore[arg-type]

    assert backend.writes == []
    assert get_recent_storage_events() == []


@pytest.mark.parametrize("format_hint", ["json", "jsonl"])
def test_sync_arrow_write_rejects_row_formats_before_encoding_or_io(
    monkeypatch: pytest.MonkeyPatch, format_hint: str
) -> None:
    pipeline = SyncStoragePipeline()
    backend = _install_backend(monkeypatch, SyncStoragePipeline)
    reset_storage_bridge_events()

    with pytest.raises(ValueError, match="Arrow storage writes support only Parquet, Arrow IPC, and CSV"):
        pipeline.write_arrow(pa.table({"id": [1]}), "payload", format_hint=format_hint)  # type: ignore[arg-type]

    assert backend.writes == []
    assert get_recent_storage_events() == []


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
async def test_async_row_write_rejects_arrow_formats_before_io(
    monkeypatch: pytest.MonkeyPatch, format_hint: str
) -> None:
    pipeline = AsyncStoragePipeline()
    backend = _install_backend(monkeypatch, AsyncStoragePipeline)

    with pytest.raises(ValueError, match="Row storage writes support only JSON and JSONL"):
        await pipeline.write_rows([{"id": 1}], "payload", format_hint=format_hint)  # type: ignore[arg-type]

    assert backend.writes == []


@pytest.mark.parametrize("format_hint", ["json", "jsonl"])
async def test_async_arrow_write_rejects_row_formats_before_io(
    monkeypatch: pytest.MonkeyPatch, format_hint: str
) -> None:
    pipeline = AsyncStoragePipeline()
    backend = _install_backend(monkeypatch, AsyncStoragePipeline)

    with pytest.raises(ValueError, match="Arrow storage writes support only Parquet, Arrow IPC, and CSV"):
        await pipeline.write_arrow(pa.table({"id": [1]}), "payload", format_hint=format_hint)  # type: ignore[arg-type]

    assert backend.writes == []
