"""Local fallback coverage; cloud EXPORT DATA/readback is intentionally unverified."""

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.driver import BigQueryDriver

pytestmark = pytest.mark.xdist_group("bigquery")


def test_emulator_storage_export_preserves_bound_values(
    bigquery_session: "BigQueryDriver", tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The local emulator uses the existing Arrow writer, not cloud EXPORT DATA."""
    destination = tmp_path / "result.parquet"
    monkeypatch.setitem(bigquery_session.driver_features, "storage_capabilities", {"arrow_export_enabled": True})
    result = bigquery_session.select_to_storage("SELECT :value AS value", destination, {"value": "local export"})
    assert pq.read_table(destination).to_pylist() == [{"value": "local export"}]
    assert result.telemetry["rows_processed"] == 1
    assert not result.telemetry.get("extra", {}).get("native_export", False)
