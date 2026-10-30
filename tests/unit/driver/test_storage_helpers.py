"""Unit tests for driver storage helper utilities."""

import pytest

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver._storage_helpers import arrow_batch_to_rows, coerce_arrow_batches, ingest_telemetry_from_batches
from sqlspec.typing import PYARROW_INSTALLED

pytestmark = pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")


def test_ingest_telemetry_from_batches_populates_fields() -> None:
    """ingest_telemetry_from_batches builds a StorageTelemetry dictionary with stage metrics."""
    telemetry = ingest_telemetry_from_batches(
        1024,
        65536,
        duration_s=1.25,
        format_label="parquet",
        read_duration_s=0.25,
        encode_duration_s=0.35,
        write_duration_s=0.50,
        commit_duration_s=0.15,
        shards_processed=4,
        shards_skipped=1,
        wal_bytes=4096,
    )
    assert telemetry["rows_processed"] == 1024
    assert telemetry["bytes_processed"] == 65536
    assert telemetry["duration_s"] == 1.25
    assert telemetry["format"] == "parquet"
    assert telemetry.get("read_duration_s") == 0.25
    assert telemetry.get("encode_duration_s") == 0.35
    assert telemetry.get("write_duration_s") == 0.50
    assert telemetry.get("commit_duration_s") == 0.15
    assert telemetry.get("shards_processed") == 4
    assert telemetry.get("shards_skipped") == 1
    assert telemetry.get("wal_bytes") == 4096


def test_driver_common_exposes_streaming_arrow_batch_helpers() -> None:
    """CommonDriverAttributesMixin exposes _coerce_arrow_batches and _arrow_batch_to_rows."""
    import pyarrow as pa

    table = pa.table({"k": [10, 20, 30], "v": ["x", "y", "z"]})
    cols, batches = CommonDriverAttributesMixin._coerce_arrow_batches(table, batch_size=2)
    batch_list = list(batches)
    assert cols == ["k", "v"]
    assert len(batch_list) == 2
    assert CommonDriverAttributesMixin._arrow_batch_to_rows(batch_list[0]) == [(10, "x"), (20, "y")]
    assert coerce_arrow_batches is not None
    assert arrow_batch_to_rows is not None
