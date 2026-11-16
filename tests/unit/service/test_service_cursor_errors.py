"""Service cursor pagination maps forged cursor values to InvalidCursorError."""

from collections.abc import Iterator

import pytest

from sqlspec import SQL
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.core import CursorFilter, CursorKey
from sqlspec.core._cursor import encode_cursor
from sqlspec.exceptions import InvalidCursorError
from sqlspec.service import SQLSpecSyncService


@pytest.fixture
def duckdb_config() -> "Iterator[DuckDBConfig]":
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as session:
        session.execute("create table d(id int primary key, t text)")
        session.execute_many("insert into d values (?, ?)", [(index, f"t{index}") for index in range(10)])
    yield config
    config.close_pool()


def _forged_filter(value: object) -> CursorFilter:
    probe = CursorFilter((CursorKey("id"),), 2)
    token = encode_cursor([value], probe.fingerprint)
    return CursorFilter((CursorKey("id"),), 2, token)


def test_forged_string_cursor_value_raises_invalid_cursor_error(duckdb_config: DuckDBConfig) -> None:
    with duckdb_config.provide_session() as session:
        service = SQLSpecSyncService(session)
        with pytest.raises(InvalidCursorError):
            service.paginate(SQL("select id, t from d"), _forged_filter("x' or 1=1"))


def test_oversized_integer_cursor_value_returns_empty_page(duckdb_config: DuckDBConfig) -> None:
    with duckdb_config.provide_session() as session:
        page = SQLSpecSyncService(session).paginate(SQL("select id, t from d"), _forged_filter(2**100))
    assert page.items == []


def test_valid_cursor_token_pages_normally(duckdb_config: DuckDBConfig) -> None:
    with duckdb_config.provide_session() as session:
        service = SQLSpecSyncService(session)
        first = service.paginate(SQL("select id, t from d"), CursorFilter((CursorKey("id"),), 2))
        assert [row["id"] for row in first.items] == [0, 1]
        assert first.next_cursor is not None
        second = service.paginate(SQL("select id, t from d"), CursorFilter((CursorKey("id"),), 2, first.next_cursor))
    assert [row["id"] for row in second.items] == [2, 3]
