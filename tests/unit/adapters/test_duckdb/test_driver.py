"""Unit tests for DuckDBDriver execution and materialization behavior."""

from pathlib import Path

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.core.result import SQLResult

__all__ = ("test_duckdb_unparseable_select_returns_rows_across_repeated_calls",)


def test_duckdb_unparseable_select_returns_rows_across_repeated_calls(tmp_path: Path) -> None:
    """Repeated SELECT queries with qmark casts that fail sqlglot parsing must return rows on every call."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "unparseable_select.duckdb")})
    try:
        with config.provide_session() as session:
            for value in (1, 2, 3):
                result = session.execute("SELECT ?::INTEGER AS i", (value,))
                assert isinstance(result, SQLResult)
                assert result.operation_type == "SELECT"
                assert result.get_data() == [{"i": value}]
    finally:
        config.close_pool()
