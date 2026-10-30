"""Unit tests for DuckDB container-aware memory limits, tmpfs spill validation, and post-connect parquet_metadata_cache."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.duckdb.core import (
    build_connection_config,
    resolve_duckdb_memory_limit,
    validate_duckdb_temp_directory,
)
from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool
from sqlspec.exceptions import ImproperConfigurationError


def test_resolve_duckdb_memory_limit_cgroup_v2_and_clamping(tmp_path: Path) -> None:
    """resolve_duckdb_memory_limit reads cgroup v2 memory.max and clamps oversized requests."""
    cgroup_v2 = tmp_path / "memory.max"
    cgroup_v2.write_text(str(2 * 1024 * 1024 * 1024), encoding="utf-8")

    default_limit = resolve_duckdb_memory_limit(None, memory_ceiling_fraction=0.8, cgroup_v2_path=cgroup_v2)
    assert default_limit == "1638MB"

    clamped_limit = resolve_duckdb_memory_limit("64GB", memory_ceiling_fraction=0.8, cgroup_v2_path=cgroup_v2)
    assert clamped_limit == "1638MB"

    within_limit = resolve_duckdb_memory_limit("512MB", memory_ceiling_fraction=0.8, cgroup_v2_path=cgroup_v2)
    assert within_limit == "512MB"


def test_resolve_duckdb_memory_limit_cgroup_v1_and_host_fallback(tmp_path: Path) -> None:
    """resolve_duckdb_memory_limit falls back from unbounded cgroup v2 to cgroup v1 or host memory."""
    cgroup_v2 = tmp_path / "memory.max"
    cgroup_v2.write_text("max\n", encoding="utf-8")
    cgroup_v1 = tmp_path / "memory.limit_in_bytes"
    cgroup_v1.write_text(str(4 * 1024 * 1024 * 1024), encoding="utf-8")

    v1_limit = resolve_duckdb_memory_limit(
        None, memory_ceiling_fraction=0.75, cgroup_v2_path=cgroup_v2, cgroup_v1_path=cgroup_v1
    )
    assert v1_limit == "3072MB"

    cgroup_v1.write_text(str(1 << 62), encoding="utf-8")
    host_limit = resolve_duckdb_memory_limit(
        None,
        memory_ceiling_fraction=0.5,
        cgroup_v2_path=cgroup_v2,
        cgroup_v1_path=cgroup_v1,
        host_memory_bytes=8 * 1024 * 1024 * 1024,
    )
    assert host_limit == "4096MB"


def test_validate_duckdb_temp_directory_detects_tmpfs() -> None:
    """validate_duckdb_temp_directory warns or raises when temp_directory is mounted on tmpfs/ramfs."""
    fake_mounts = (
        "overlay / overlay rw,relatime 0 0\n"
        "tmpfs /tmp tmpfs rw,nosuid,nodev 0 0\n"
        "/dev/sda1 /var/data ext4 rw,relatime 0 0\n"
    )
    assert validate_duckdb_temp_directory("/tmp/duckdb_spill", reject_tmpfs=False, mounts_content=fake_mounts) is False
    with pytest.raises(ImproperConfigurationError, match="tmpfs"):
        validate_duckdb_temp_directory("/tmp/duckdb_spill", reject_tmpfs=True, mounts_content=fake_mounts)
    assert (
        validate_duckdb_temp_directory("/var/data/duckdb_spill", reject_tmpfs=True, mounts_content=fake_mounts) is True
    )


def test_build_connection_config_applies_memory_ceiling_fraction() -> None:
    """build_connection_config resolves memory_limit when memory_ceiling_fraction is set."""
    cfg = build_connection_config({"database": ":memory:", "memory_ceiling_fraction": 0.75})
    assert "memory_limit" in cfg["config"]
    assert "memory_ceiling_fraction" not in cfg["config"]


def test_pool_defers_parquet_metadata_cache_after_extensions_load(monkeypatch: pytest.MonkeyPatch) -> None:
    """DuckDBConnectionPool applies SET parquet_metadata_cache after extensions load."""
    events: list[str] = []

    class _Conn:
        def install_extension(self, name: str, **_: Any) -> None:
            events.append(f"install:{name}")

        def load_extension(self, name: str) -> None:
            events.append(f"load:{name}")

        def execute(self, sql: str, *_: Any) -> "_Conn":
            events.append(f"sql:{sql}")
            return self

        def list_filesystems(self) -> list[str]:
            return []

    captured_connect: dict[str, Any] = {}

    def fake_connect(**kwargs: Any) -> _Conn:
        captured_connect.update(kwargs)
        return _Conn()

    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.duckdb.connect", fake_connect)
    pool = DuckDBConnectionPool(
        {"database": ":memory:", "config": {"parquet_metadata_cache": True, "threads": 2}},
        extensions=[{"name": "httpfs"}],
    )
    pool._create_connection()

    assert "parquet_metadata_cache" not in captured_connect.get("config", {})
    assert events == ["load:httpfs", "sql:SET parquet_metadata_cache = true"]
