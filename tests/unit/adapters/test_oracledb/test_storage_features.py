# pyright: reportPrivateUsage=false
"""Tests for Oracle extension storage clause generation."""

from types import SimpleNamespace

from sqlspec.adapters.oracledb._storage import _oracle_table_feature_report


def test_oracle_configured_clauses_are_emitted() -> None:
    """Every configured storage option reaches the table DDL."""
    report = _oracle_table_feature_report(
        SimpleNamespace(),
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

    assert "ROW STORE COMPRESS ADVANCED" in report["clause"]
    assert "INMEMORY PRIORITY HIGH" in report["clause"]
    assert "PARTITION BY HASH (event_id) PARTITIONS 8" in report["clause"]
    assert report["applied"] == ("advanced_compression", "in_memory", "partitioning")


def test_oracle_unconfigured_options_emit_nothing() -> None:
    """Nothing is added to the DDL when no storage options are configured."""
    report = _oracle_table_feature_report(
        SimpleNamespace(),
        "events",
        {},
        "queue",
        in_memory=False,
        hash_partition_key="event_id",
        range_partition_key="available_at",
    )

    assert report["clause"] == ""
    assert report["applied"] == ()


def test_oracle_range_partitioning_uses_interval() -> None:
    """Range partitioning emits an INTERVAL so the server creates partitions on demand."""
    report = _oracle_table_feature_report(
        SimpleNamespace(),
        "events",
        {"partitioning": {"strategy": "range", "interval": "month"}},
        "events",
        in_memory=False,
        hash_partition_key="session_id",
        range_partition_key="timestamp",
    )

    assert "PARTITION BY RANGE (timestamp)" in report["clause"]
    assert "INTERVAL (NUMTOYMINTERVAL(1, ''MONTH''))" in report["clause"]
