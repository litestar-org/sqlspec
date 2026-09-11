"""Shared Microsoft SQL Server-family integration contracts."""

from collections.abc import Generator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from sqlspec.adapters.arrow_odbc import ArrowOdbcDriver
from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection
from sqlspec.adapters.pymssql import PymssqlConfig
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
