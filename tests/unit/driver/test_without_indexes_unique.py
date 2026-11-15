"""``without_indexes`` keeps unique indexes in place on embedded engines."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig


@pytest.fixture(params=["sqlite", "duckdb"])
def config(request: pytest.FixtureRequest, tmp_path: Path) -> "Iterator[Any]":
    if request.param == "sqlite":
        cfg: Any = SqliteConfig(connection_config={"database": str(tmp_path / "wi.db")})
    else:
        cfg = DuckDBConfig(connection_config={"database": str(tmp_path / "wi.duckdb")})
    try:
        yield cfg
    finally:
        cfg.close_pool()


def _index_names(session: Any, table: str) -> set[str]:
    if "sqlite" in type(session).__module__:
        rows = session.select(f"SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '{table}'")
    else:
        rows = session.select(f"SELECT index_name AS name FROM duckdb_indexes() WHERE table_name = '{table}'")
    return {row["name"] for row in rows}


def _insert_fails(session: Any, sql: str) -> bool:
    try:
        session.execute(sql)
    except Exception:
        return True
    return False


def test_without_indexes_keeps_unique_index(config: Any) -> None:
    with config.provide_session() as session:
        session.execute("create table users_wi (id int, email varchar(64))")
        session.execute("create unique index uq_users_email on users_wi(email)")
        session.execute("insert into users_wi values (1, 'admin@corp')")

        with pytest.raises(ValueError, match="row 3 malformed"), session.without_indexes("users_wi"):
            assert "uq_users_email" in _index_names(session, "users_wi")
            assert _insert_fails(session, "insert into users_wi values (2, 'admin@corp')")
            msg = "row 3 malformed"
            raise ValueError(msg)

        assert "uq_users_email" in _index_names(session, "users_wi")
        assert _insert_fails(session, "insert into users_wi values (4, 'admin@corp')")


def test_without_indexes_clean_exit_keeps_unique_index(config: Any) -> None:
    with config.provide_session() as session:
        session.execute("create table t2 (id int, email varchar(64))")
        session.execute("create unique index uq_t2 on t2(email)")

        with session.without_indexes("t2"):
            session.execute("insert into t2 values (1, 'x')")
            assert _insert_fails(session, "insert into t2 values (2, 'x')")

        assert "uq_t2" in _index_names(session, "t2")


def test_without_indexes_still_drops_plain_index(config: Any) -> None:
    with config.provide_session() as session:
        session.execute("create table t3 (id int, name varchar(64))")
        session.execute("create index ix_t3_name on t3(name)")

        with session.without_indexes("t3"):
            assert "ix_t3_name" not in _index_names(session, "t3")

        assert "ix_t3_name" in _index_names(session, "t3")
