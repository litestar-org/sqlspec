"""Bulk INSERT preserves the semantics of its VALUES and column list."""

from typing import Any
from unittest.mock import patch

import pytest
from sqlglot import exp

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.core.result import DMLResult


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        ("INSERT INTO t (a, b) VALUES (?, ? + 100)", [{"a": 1, "b": 102, "c": 9}]),
        ("INSERT INTO t (a, b) VALUES ($2, $1)", [{"a": 2, "b": 1, "c": 9}]),
        ("INSERT INTO t (a, b) SELECT ?, ? WHERE 1 = 0", []),
        ("INSERT INTO t (b, a) VALUES (?, ?)", [{"a": 2, "b": 1, "c": 9}]),
        ("INSERT INTO t (a, b) VALUES (?, ?)", [{"a": 1, "b": 2, "c": 9}]),
        ("INSERT INTO t (a, b) VALUES (?, ?) ON CONFLICT DO NOTHING", [{"a": 1, "b": 2, "c": 9}]),
    ],
)
def test_execute_many_preserves_insert_semantics(statement: str, expected: list[dict[str, int]]) -> None:
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER DEFAULT 9)")
            for _ in range(3):
                session.execute("DELETE FROM t")
                session.execute_many(statement, [(1, 2)])
                if "CONFLICT" in statement:
                    session.execute_many(statement, [(1, 2)])
                assert session.select("SELECT * FROM t") == expected
    finally:
        config.close_pool()


def test_plain_values_retains_bulk_loading() -> None:
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    original = DuckDBDriver._execute_bulk_insert_many
    results: list[DMLResult | None] = []

    def record(driver: DuckDBDriver, expression: exp.Insert, parameters: Any) -> DMLResult | None:
        result = original(driver, expression, parameters)
        results.append(result)
        return result

    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE t (a INTEGER, b INTEGER)")
            with patch.object(DuckDBDriver, "_execute_bulk_insert_many", record):
                session.execute_many("INSERT INTO t (a, b) VALUES (?, ?)", [(1, 2)])
            assert len(results) == 1 and results[0] is not None
            assert session.select("SELECT * FROM t") == [{"a": 1, "b": 2}]
    finally:
        config.close_pool()
