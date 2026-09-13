"""Integration tests for DuckDB exception mapping."""

from pathlib import Path

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.exceptions import SerializationConflictError

pytestmark = pytest.mark.xdist_group("duckdb")


def test_duckdb_concurrent_update_conflict_raises_serialization_conflict(tmp_path: Path) -> None:
    """Two connections in concurrent transactions updating the same row raise SerializationConflictError."""
    db_file = tmp_path / "conflict.duckdb"
    config1 = DuckDBConfig(connection_config={"database": str(db_file)})
    config2 = DuckDBConfig(connection_config={"database": str(db_file)})
    try:
        with config1.provide_session() as s1, config2.provide_session() as s2:
            s1.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            s1.execute("INSERT INTO t VALUES (1, 'initial')")
            s1.begin()
            s2.begin()
            s1.execute("UPDATE t SET val = 'first' WHERE id = 1")
            with pytest.raises(SerializationConflictError):
                s2.execute("UPDATE t SET val = 'second' WHERE id = 1")
                s2.commit()
    finally:
        config1.close_pool()
        config2.close_pool()
