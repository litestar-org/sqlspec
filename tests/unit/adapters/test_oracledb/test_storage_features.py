# pyright: reportPrivateUsage=false
"""Tests for Oracle extension storage feature gating."""

import logging
from types import SimpleNamespace

import pytest

from sqlspec.adapters.oracledb._storage import _oracle_table_feature_report
from sqlspec.adapters.oracledb.data_dictionary import OracleVersionCache


def test_oracle_optimization_clauses_gated_gracefully(caplog: "pytest.LogCaptureFixture") -> None:
    """Unsupported requested features are reported and omitted from table DDL."""
    cache = OracleVersionCache()
    cache.storage_capabilities_resolved = True
    config = SimpleNamespace(_oracle_version_cache=cache)

    with caplog.at_level(logging.WARNING, logger="sqlspec.adapters.oracledb.storage"):
        report = _oracle_table_feature_report(
            config,
            "events",
            {
                "compression": {"enabled": True, "algorithm": "advanced"},
                "in_memory": True,
                "partitioning": {"strategy": "hash", "partition_count": 8},
            },
            "queue",
            in_memory=True,
            hash_partition_key="event_id",
            range_partition_key="available_at",
        )

    assert report["clause"] == ""
    assert report["applied"] == ()
    assert report["degraded"] == (
        {"optimization": "advanced_compression", "reason": "unlicensed_or_unavailable"},
        {"optimization": "in_memory", "reason": "unlicensed_or_unavailable"},
        {"optimization": "partitioning", "reason": "unlicensed_or_unavailable"},
    )
    assert caplog.messages.count("oracle.storage.optimization.degraded") == 3
