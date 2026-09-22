"""Database adapters for SQLSpec."""

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlspec.adapters import (
        adbc,
        aiomysql,
        aiosqlite,
        arrow_odbc,
        asyncmy,
        asyncpg,
        bigquery,
        cockroach_asyncpg,
        cockroach_psycopg,
        db2,
        duckdb,
        mssql_python,
        mysqlconnector,
        oracledb,
        psqlpy,
        psycopg,
        pymssql,
        pymysql,
        spanner,
        sqlite,
    )

__all__ = (
    "adbc",
    "aiomysql",
    "aiosqlite",
    "arrow_odbc",
    "asyncmy",
    "asyncpg",
    "bigquery",
    "cockroach_asyncpg",
    "cockroach_psycopg",
    "db2",
    "duckdb",
    "mssql_python",
    "mysqlconnector",
    "oracledb",
    "psqlpy",
    "psycopg",
    "pymssql",
    "pymysql",
    "spanner",
    "sqlite",
)

_ADAPTERS = frozenset(__all__)


def __getattr__(name: str) -> Any:
    """Dynamically load adapter subpackages on demand."""
    if name in _ADAPTERS:
        module = importlib.import_module(f"sqlspec.adapters.{name}")
        globals()[name] = module
        return module
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """List exposed attributes."""
    return sorted(set(globals()) | _ADAPTERS)
