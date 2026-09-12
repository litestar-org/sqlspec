"""ADBC savepoint capability reporting per connected database."""

from typing import Any, cast

import pytest

from sqlspec.adapters.adbc.core import get_statement_config
from sqlspec.adapters.adbc.driver import AdbcDriver

pytestmark = pytest.mark.adbc


class _Cursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.description = None
        self.rowcount = -1

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.statements.append(sql)

    def fetchall(self) -> list[Any]:
        return []

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self, dialect: str) -> None:
        self.dialect = dialect
        self.cursor_obj = _Cursor()

    def adbc_get_info(self) -> dict[str, str]:
        return {"vendor_name": self.dialect, "driver_name": self.dialect}

    def cursor(self) -> _Cursor:
        return self.cursor_obj


def _driver(dialect: str) -> AdbcDriver:
    return AdbcDriver(
        cast("Any", _Connection(dialect)), statement_config=get_statement_config(dialect), dialect=dialect
    )


@pytest.mark.parametrize("dialect", ["duckdb", "bigquery", "snowflake"])
@pytest.mark.parametrize("method", ["create_savepoint", "release_savepoint", "rollback_to_savepoint"])
def test_savepoints_are_reported_unsupported(dialect: str, method: str) -> None:
    driver = _driver(dialect)

    with pytest.raises(NotImplementedError, match=dialect):
        getattr(driver, method)("sqlspec_sp_1")
    assert cast("_Connection", driver.connection).cursor_obj.statements == []
