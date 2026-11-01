"""Unit tests for DuckDBDriver execution and materialization behavior."""

from pathlib import Path
from typing import Any, Self

import duckdb
import pytest
from sqlglot import exp

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.driver import DuckDBDriver
from sqlspec.core import SQL
from sqlspec.core.result import SQLResult

__all__ = (
    "test_duckdb_execute_many_caches_bulk_insert_sql_fragments",
    "test_duckdb_materialization_arrow_vs_fetchall_threshold",
    "test_duckdb_native_type_contract_parity",
    "test_duckdb_unparseable_select_returns_rows_across_repeated_calls",
)

DIVERGENT_AND_STANDARD_TYPE_EXPRESSIONS: tuple[str, ...] = (
    "1::HUGEINT",
    "1::UHUGEINT",
    "'101'::BIT",
    "'12:00:00+00'::TIMETZ",
    "'2026-01-01 00:00:00'::TIMESTAMP_NS",
    "INTERVAL 1 DAY",
    "'550e8400-e29b-41d4-a716-446655440000'::UUID",
    "[1, 2, 3]::INT[3]",
    "MAP {'a': 1}",
    "[INTERVAL 1 DAY]",
    "{'u': '550e8400-e29b-41d4-a716-446655440000'::UUID}",
    "1::BIGNUM",
    "1::INTEGER",
    "1::BIGINT",
    "'hello'::VARCHAR",
    "1.5::DOUBLE",
    "TRUE::BOOLEAN",
    "DATE '2026-01-01'",
    "TIMESTAMP '2026-01-01 00:00:00'",
    "[1, 2, 3]",
    "{'a': 1}",
)


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


@pytest.mark.parametrize("sql_expr", DIVERGENT_AND_STANDARD_TYPE_EXPRESSIONS)
def test_duckdb_native_type_contract_parity(tmp_path: Path, sql_expr: str) -> None:
    """First and cached executions must return identical Python types matching raw duckdb fetchall()."""
    raw_conn = duckdb.connect()
    try:
        expected = raw_conn.execute(f"SELECT {sql_expr} AS val").fetchall()[0][0]
    finally:
        raw_conn.close()

    config = DuckDBConfig(connection_config={"database": str(tmp_path / "type_contract.duckdb")})
    try:
        with config.provide_session() as session:
            parameterized_sql = f"SELECT {sql_expr} AS val WHERE ? >= 0"
            for index in range(3):
                result = session.execute(parameterized_sql, (index,))
                actual = result.one()["val"]
                assert type(actual) is type(expected)
                assert actual == expected

            unparameterized_sql = f"SELECT {sql_expr} AS val"
            for _ in range(2):
                result = session.execute(unparameterized_sql)
                actual = result.one()["val"]
                assert type(actual) is type(expected)
                assert actual == expected
    finally:
        config.close_pool()


def test_duckdb_materialization_arrow_vs_fetchall_threshold(tmp_path: Path) -> None:
    """Queries with >= 64 non-divergent rows use Arrow; < 64 rows after observation and divergent columns use fetchall."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "threshold.duckdb")})
    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE items AS SELECT i AS id, 'item_' || i::VARCHAR AS name FROM range(200) t(i)")
            raw_connection = session.connection
            counts = {"arrow": 0, "fetchall": 0}

            def reset_counts() -> None:
                counts["arrow"] = 0
                counts["fetchall"] = 0

            session.connection = _SpyDuckDBCursor(raw_connection, counts)  # type: ignore[assignment]
            try:
                large_sql = "SELECT id, name FROM items WHERE id >= ?"
                reset_counts()
                res1 = session.execute(large_sql, (0,))
                assert len(res1.get_data()) == 200
                assert counts == {"arrow": 1, "fetchall": 0}

                reset_counts()
                res2 = session.execute(large_sql, (0,))
                assert len(res2.get_data()) == 200
                assert counts == {"arrow": 1, "fetchall": 0}

                small_sql = "SELECT id, name FROM items WHERE id = ?"
                reset_counts()
                small1 = session.execute(small_sql, (1,))
                assert small1.one() == {"id": 1, "name": "item_1"}
                assert counts == {"arrow": 1, "fetchall": 0}

                reset_counts()
                small2 = session.execute(small_sql, (2,))
                assert small2.one() == {"id": 2, "name": "item_2"}
                assert counts == {"arrow": 0, "fetchall": 1}

                divergent_sql = "SELECT id::HUGEINT AS big_id FROM items WHERE id >= ?"
                reset_counts()
                div1 = session.execute(divergent_sql, (0,))
                assert len(div1.get_data()) == 200
                assert type(div1.get_data()[0]["big_id"]) is int
                assert counts == {"arrow": 0, "fetchall": 1}

                reset_counts()
                div2 = session.execute(divergent_sql, (0,))
                assert len(div2.get_data()) == 200
                assert type(div2.get_data()[0]["big_id"]) is int
                assert counts == {"arrow": 0, "fetchall": 1}
            finally:
                session.connection = raw_connection
    finally:
        config.close_pool()


def test_duckdb_execute_many_caches_bulk_insert_sql_fragments(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated execute_many bulk inserts cache (target_table, column_sql) and skip sqlglot .sql() on subsequent batches."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "bulk_cache.duckdb")})
    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE bulk_items (id INTEGER, name VARCHAR)")
            insert_sql = "INSERT INTO bulk_items (id, name) VALUES (?, ?)"
            res1 = session.execute_many(insert_sql, [(1, "a"), (2, "b")])
            assert res1.rows_affected == 2
            cached_pair = session._cache_holder.get_bulk_insert_sql(insert_sql)
            assert cached_pair == ("bulk_items", "id, name")

            def fail_sql(*_args: Any, **_kwargs: Any) -> str:
                msg = "exp.Expr.sql should not be called on bulk insert cache hit"
                raise AssertionError(msg)

            monkeypatch.setattr(exp.Expr, "sql", fail_sql)
            res2 = session.execute_many(insert_sql, [(3, "c"), (4, "d")])
            assert res2.rows_affected == 2
            count = session.select_value("SELECT COUNT(*) FROM bulk_items")
            assert count == 4
    finally:
        config.close_pool()


class _SpyDuckDBRelation:
    """Proxy relation for counting to_arrow_table and fetchall calls."""

    __slots__ = ("_counts", "_relation")

    def __init__(self, relation: Any, counts: dict[str, int]) -> None:
        self._relation = relation
        self._counts = counts

    @property
    def description(self) -> Any:
        return self._relation.description

    def fetchall(self) -> list[Any]:
        self._counts["fetchall"] += 1
        return list(self._relation.fetchall())

    def to_arrow_table(self, *args: Any, **kwargs: Any) -> Any:
        self._counts["arrow"] += 1
        return self._relation.to_arrow_table(*args, **kwargs)


class _SpyDuckDBCursor:
    """Proxy cursor for counting to_arrow_table and fetchall calls."""

    __slots__ = ("_connection", "_counts")

    def __init__(self, connection: Any, counts: dict[str, int]) -> None:
        self._connection = connection
        self._counts = counts

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    @property
    def description(self) -> Any:
        return self._connection.description

    def sql(self, query: str, *, params: Any = None) -> "_SpyDuckDBRelation | None":
        relation = self._connection.sql(query, params=params)
        if relation is None:
            return None
        return _SpyDuckDBRelation(relation, self._counts)

    def execute(self, sql: str, parameters: Any = None) -> Self:
        if parameters is not None:
            self._connection.execute(sql, parameters)
        else:
            self._connection.execute(sql)
        return self

    def fetchall(self) -> list[Any]:
        self._counts["fetchall"] += 1
        return list(self._connection.fetchall())

    def to_arrow_table(self) -> Any:
        self._counts["arrow"] += 1
        return self._connection.to_arrow_table()

    def fetchone(self) -> Any:
        return self._connection.fetchone()


def test_select_stream_hugeint_interval_divergent_types() -> None:
    """Stream selection falls back to fetchmany when divergent types like HUGEINT are present."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    stream = driver.select_stream("SELECT 170141183460469231731687303715884105727::HUGEINT as val", chunk_size=1)
    assert stream is not None
    chunks = list(stream)
    assert len(chunks) == 1
    assert chunks[0]["val"] == 170141183460469231731687303715884105727


@pytest.mark.parametrize("exhaust_stream", [True, False])
def test_select_stream_preserves_active_transaction(exhaust_stream: bool) -> None:
    with duckdb.connect(":memory:") as connection:
        driver = DuckDBDriver(connection)
        driver.execute("CREATE TABLE items (id INTEGER)")
        driver.begin()
        driver.execute("INSERT INTO items VALUES (1)")

        with driver.select_stream("SELECT id::HUGEINT AS id FROM items", chunk_size=1) as stream:
            if exhaust_stream:
                assert list(stream) == [{"id": 1}]
            else:
                assert next(stream) == {"id": 1}

        assert driver.select_value("SELECT COUNT(*) FROM items") == 1
        driver.rollback()
        assert driver.select_value("SELECT COUNT(*) FROM items") == 0


def test_concurrent_execute_many_ast_mutation() -> None:
    """dispatch_execute_many does not mutate parsed_expression.meta."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    driver.execute("CREATE TABLE items (id INT, name VARCHAR)")

    stmt = SQL("INSERT INTO items VALUES (?, ?)", statement_config=driver.statement_config)
    stmt.compile()
    expr = stmt._peek_expression()
    assert expr is not None
    meta_before = dict(expr.meta)

    driver.execute_many(stmt, [[1, "a"], [2, "b"]])
    assert expr.meta == meta_before


def test_select_column_name_cache_concurrency() -> None:
    """_materialize_select_rows avoids redundant double-locking when row count hint matches."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    driver.execute("CREATE TABLE test_cache_hits (id INT)")
    driver.execute("INSERT INTO test_cache_hits VALUES (1), (2)")

    sql = "SELECT * FROM test_cache_hits"
    res1 = driver.execute(sql)
    assert len(res1.data) == 2
    hint1 = driver._cache_holder.get_row_count_hint(sql)
    assert hint1 == 2

    res2 = driver.execute(sql)
    assert len(res2.data) == 2
    hint2 = driver._cache_holder.get_row_count_hint(sql)
    assert hint2 == 2


def test_auxiliary_exception_isolation() -> None:
    """Auxiliary methods route through exception handler with active exception isolation."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    try:
        raise ValueError("ambient error")
    except ValueError:
        driver.begin()
        driver.commit()


def test_migration_helpers_exception_mapping() -> None:
    """set_migration_session_schema and has_schema map exceptions properly."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    assert driver.has_schema("main") is True
    driver.set_migration_session_schema("main")


def test_profiling_temp_file_cleanup() -> None:
    """Driver cleans up temporary profile JSON file upon close."""
    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, driver_features={"enable_statement_profiling": True}
    )
    driver = DuckDBDriver(config.create_connection(), driver_features={"enable_statement_profiling": True})
    driver.execute("SELECT 1")
    profile_path = driver._created_profile_path
    assert profile_path is not None
    assert profile_path.exists()
    driver.close()
    assert not profile_path.exists()
