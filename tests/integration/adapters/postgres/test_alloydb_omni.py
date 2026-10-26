"""Live AlloyDB Omni 18 integration tests for enable_alloydb_columnar across PostgreSQL drivers."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from tests.integration.fixtures.postgres import AlloydbOmniHarness

pytestmark = [pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("alloydb")]


async def test_alloydb_omni_session_columnar_guc_and_crud(alloydb_omni_harness: "AlloydbOmniHarness") -> None:
    """Verify enable_alloydb_columnar=True activates google_columnar_engine.enable_columnar_scan in session mode."""
    engine_enabled, scan_enabled = await alloydb_omni_harness.show_session_columnar_settings()
    assert engine_enabled == "on"
    assert scan_enabled == "on"

    rows = await alloydb_omni_harness.verify_session_ddl_and_dml()
    assert rows == [{"id": 1, "label": "alpha", "metric": 10}, {"id": 2, "label": "beta", "metric": 25}]


async def test_alloydb_omni_transaction_pooler_columnar_guc(alloydb_omni_harness: "AlloydbOmniHarness") -> None:
    """Verify pooler_mode='transaction' applies SET LOCAL google_columnar_engine.enable_columnar_scan inside begin()."""
    in_tx_scan, after_commit_scan = await alloydb_omni_harness.show_transaction_columnar_transition()
    assert in_tx_scan == "on"
    assert after_commit_scan == "off"


async def test_alloydb_omni_vector_extension_roundtrip(alloydb_omni_harness: "AlloydbOmniHarness") -> None:
    """Verify AlloyDB Omni 18 exposes the vector extension and executes L2 distance queries."""
    extension_name, distance = await alloydb_omni_harness.verify_vector_extension()
    assert extension_name == "vector"
    assert distance == pytest.approx(0.0)
