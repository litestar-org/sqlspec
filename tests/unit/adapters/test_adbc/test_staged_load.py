"""Staged and upsert bulk loads through the ADBC sqlite and duckdb drivers."""

import importlib.util
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from sqlspec.adapters.adbc import AdbcConfig

_DRIVERS = {
    "sqlite": ("adbc_driver_sqlite", "sqlite_master", "temp.sqlite_master"),
    "duckdb": ("adbc_driver_duckdb", "duckdb_tables()", "duckdb_tables()"),
}


@pytest.fixture(params=["sqlite", "duckdb"])
def adbc_session(request: pytest.FixtureRequest, tmp_path: Path) -> "Iterator[tuple[Any, str]]":
    driver_name = _DRIVERS[request.param][0]
    if importlib.util.find_spec(driver_name) is None:
        pytest.skip(f"{driver_name} is not installed")
    config = AdbcConfig(connection_config={"driver_name": driver_name, "uri": str(tmp_path / "staged.db")})
    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(32))")
            yield session, request.param
    finally:
        config.close_pool()


def _rows(session: Any) -> "list[tuple[int, str]]":
    return [(row["id"], row["name"]) for row in session.select("SELECT id, name FROM t ORDER BY id")]


def _table_names(session: Any, dialect: str) -> "list[str]":
    if dialect == "sqlite":
        sql = (
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "UNION ALL SELECT name FROM temp.sqlite_master WHERE type = 'table'"
        )
    else:
        sql = "SELECT table_name AS name FROM duckdb_tables()"
    return [row["name"] for row in session.select(sql)]


def test_stage_table_load_reaches_target(adbc_session: "tuple[Any, str]") -> None:
    session, _ = adbc_session
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    session.load_from_arrow("t", table, stage_table=True)

    assert _rows(session) == [(1, "a"), (2, "b"), (3, "c")]


def test_conflict_keys_load_upserts_into_target(adbc_session: "tuple[Any, str]") -> None:
    session, _ = adbc_session
    session.execute("INSERT INTO t (id, name) VALUES (1, 'old'), (2, 'keep')")
    table = pa.table({"id": [1, 3], "name": ["new", "added"]})

    session.load_from_arrow("t", table, conflict_keys=["id"])

    assert _rows(session) == [(1, "new"), (2, "keep"), (3, "added")]


def test_staged_load_leaves_no_stage_table(adbc_session: "tuple[Any, str]") -> None:
    session, dialect = adbc_session
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    session.load_from_arrow("t", table, stage_table=True)

    assert _table_names(session, dialect) == ["t"]


def test_drop_indexes_load_without_stage_reaches_target(adbc_session: "tuple[Any, str]") -> None:
    session, dialect = adbc_session
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    session.load_from_arrow("t", table, drop_indexes=True)

    assert _rows(session) == [(1, "a"), (2, "b")]
    assert _table_names(session, dialect) == ["t"]
