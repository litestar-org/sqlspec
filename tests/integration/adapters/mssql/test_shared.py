"""Shared Microsoft SQL Server-family integration contracts."""

from collections.abc import Generator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from sqlspec import sql
from sqlspec.adapters.arrow_odbc import ArrowOdbcDriver
from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.exceptions import SQLSpecError
from tests.integration.adapters._shared import install_shared_tests

install_shared_tests(globals(), "mssql")


@pytest.mark.parametrize(
    "case", [SimpleNamespace(adapter="pymssql", fixture_name="pymssql_connection_config")], ids=["pymssql"]
)
def test_pymssql_cached_dictionary_rows(case: Any, request: pytest.FixtureRequest, monkeypatch: Any) -> None:
    config = PymssqlConfig(connection_config={**request.getfixturevalue(case.fixture_name), "as_dict": True})
    executions = Mock()
    try:
        with config.provide_session() as driver:
            with_cursor = driver.with_cursor

            @contextmanager
            def counted_cursor(_driver: Any, connection: Any) -> Generator[Any, None, None]:
                with with_cursor(connection) as cursor:
                    proxy = SimpleNamespace(fetchall=cursor.fetchall, rowcount=cursor.rowcount)

                    def execute(sql: str, params: Any) -> None:
                        executions(sql, params)
                        cursor.execute(sql, params)
                        proxy.description = cursor.description
                        proxy.rowcount = cursor.rowcount

                    proxy.execute = execute
                    yield proxy

            monkeypatch.setattr(type(driver), "with_cursor", counted_cursor)
            for count in range(1, 4):
                result = driver.execute("SELECT 1 AS x UNION ALL SELECT 2 AS x ORDER BY x", ())
                assert result.get_data() == [{"x": 1}, {"x": 2}]
                assert executions.call_count == count
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    "case", [SimpleNamespace(adapter="arrow_odbc", fixture_name="arrow_odbc_mssql_config")], ids=["arrow-odbc"]
)
def test_arrow_odbc_cached_select_executes_once(case: Any, request: pytest.FixtureRequest) -> None:
    with request.getfixturevalue(case.fixture_name).provide_session() as session:
        connection = session.connection
        proxy = SimpleNamespace(
            execute=Mock(wraps=connection.execute), read_arrow_batches=Mock(wraps=connection.read_arrow_batches)
        )
        driver = ArrowOdbcDriver(
            connection=cast("ArrowOdbcConnection", proxy), driver_features={"dbms_name": "Microsoft SQL Server"}
        )
        for count in range(1, 4):
            result = driver.execute("SELECT 1 AS x", ())
            assert result.get_data() == [{"x": 1}]
            assert proxy.read_arrow_batches.call_count == count
            proxy.execute.assert_not_called()


_RETURNING_CONFIGS = [
    pytest.param(
        SimpleNamespace(adapter="mssql_python", fixture_name="mssql_python_config"),
        marks=pytest.mark.mssql_python,
        id="mssql_python",
    ),
    pytest.param(
        SimpleNamespace(adapter="pymssql", fixture_name="pymssql_config"), marks=pytest.mark.pymssql, id="pymssql"
    ),
]
_RETURNING_TARGET = "returning_target"
_RETURNING_SOURCE = "returning_source"
_RETURNING_SETUP = f"""
IF OBJECT_ID('{_RETURNING_TARGET}') IS NOT NULL DROP TABLE {_RETURNING_TARGET};
IF OBJECT_ID('{_RETURNING_SOURCE}') IS NOT NULL DROP TABLE {_RETURNING_SOURCE};
CREATE TABLE {_RETURNING_TARGET} (id INT PRIMARY KEY, a INT, [a b] INT);
CREATE TABLE {_RETURNING_SOURCE} (id INT, b INT);
INSERT INTO {_RETURNING_TARGET} (id, a) VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO {_RETURNING_SOURCE} VALUES (1, 7);
"""
_RETURNING_CLEANUP = f"""
IF OBJECT_ID('{_RETURNING_TARGET}') IS NOT NULL DROP TABLE {_RETURNING_TARGET};
IF OBJECT_ID('{_RETURNING_SOURCE}') IS NOT NULL DROP TABLE {_RETURNING_SOURCE};
"""


@pytest.mark.xdist_group("mssql")
@pytest.mark.parametrize("case", _RETURNING_CONFIGS)
def test_builder_returning_output_native(case: Any, request: pytest.FixtureRequest) -> None:
    target, source = _RETURNING_TARGET, _RETURNING_SOURCE
    cases = [
        (sql.insert(target, dialect="tsql").columns("id", "a").values(5, 50).returning("a"), [{"a": 50}]),
        (
            sql.insert(target, dialect="tsql").columns("id", "a").values(6, 60).returning("*"),
            [{"id": 6, "a": 60, "a b": None}],
        ),
        (
            sql.update(target, dialect="tsql").set(a=11).where_eq("id", 1).returning("a", f"{target}.id", "a + 1 AS x"),
            [{"a": 11, "id": 1, "x": 12}],
        ),
        (
            sql
            .update(target, dialect="tsql")
            .set(a=12)
            .where_eq("id", 1)
            .returning("DELETED.a AS old", "INSERTED.a AS new"),
            [{"old": 11, "new": 12}],
        ),
        (
            sql
            .update(target, dialect="tsql")
            .set(a=13)
            .from_(source, alias="s")
            .where(f"{target}.id = s.id")
            .returning("a", "s.b"),
            [{"a": 13, "b": 7}],
        ),
        (
            sql.update(target, dialect="tsql").set(a=14).where_eq("id", 2).returning("*"),
            [{"id": 2, "a": 14, "a b": None}],
        ),
        (sql.update(target, dialect="tsql").set(a=15).where_eq("id", 2).returning("[a b]"), [{"a b": None}]),
        (sql.delete(target, dialect="tsql").where_eq("id", 3).returning("a"), [{"a": 30}]),
        (sql.delete(target, dialect="tsql").where_eq("id", 4).returning("*"), [{"id": 4, "a": 40, "a b": None}]),
    ]
    config = request.getfixturevalue(case.fixture_name)
    with config.provide_session() as driver:
        driver.execute_script(_RETURNING_SETUP)
        try:
            for builder, expected in cases:
                assert driver.execute(builder).get_data() == expected
        finally:
            driver.execute_script(_RETURNING_CLEANUP)
            driver.commit()


@pytest.mark.xdist_group("mssql")
@pytest.mark.parametrize("case", _RETURNING_CONFIGS)
def test_builder_returning_output_rejected_on_triggered_table(case: Any, request: pytest.FixtureRequest) -> None:
    config = request.getfixturevalue(case.fixture_name)
    with config.provide_session() as driver:
        driver.execute_script(_RETURNING_SETUP)
        try:
            driver.execute_script(
                f"CREATE TRIGGER returning_target_trg ON {_RETURNING_TARGET} AFTER UPDATE AS BEGIN SET NOCOUNT ON; END"
            )
            builder = sql.update(_RETURNING_TARGET, dialect="tsql").set(a=9).where_eq("id", 1).returning("a")
            with pytest.raises(SQLSpecError, match="cannot have any enabled triggers"):
                driver.execute(builder)
        finally:
            driver.execute_script(_RETURNING_CLEANUP)
            driver.commit()
