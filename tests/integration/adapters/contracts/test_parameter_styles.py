# pyright: reportUnknownMemberType=false
"""Cross-adapter parameter style execution contract tests."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tests.integration.adapters.contracts._helpers import SPANNER_LOCAL_SKIP, make_config, maybe_await, provide_driver


@dataclass(frozen=True)
class ParameterCase:
    """Adapter parameter style case."""

    adapter: str
    sql: str
    parameters: tuple[Any, ...] | dict[str, Any]
    expected: Any


PARAMETER_CASES = [
    pytest.param(
        ParameterCase("sqlite", "SELECT ? AS value", (42,), 42),
        marks=[pytest.mark.sqlite, pytest.mark.xdist_group("sqlite")],
        id="sqlite-qmark",
    ),
    pytest.param(
        ParameterCase("sqlite", "SELECT :value AS value", {"value": "sqlite"}, "sqlite"),
        marks=[pytest.mark.sqlite, pytest.mark.xdist_group("sqlite")],
        id="sqlite-named-colon",
    ),
    pytest.param(
        ParameterCase("aiosqlite", "SELECT ? AS value", (43,), 43),
        marks=[pytest.mark.sqlite, pytest.mark.aiosqlite, pytest.mark.xdist_group("sqlite")],
        id="aiosqlite-qmark",
    ),
    pytest.param(
        ParameterCase("aiosqlite", "SELECT :value AS value", {"value": "aiosqlite"}, "aiosqlite"),
        marks=[pytest.mark.sqlite, pytest.mark.aiosqlite, pytest.mark.xdist_group("sqlite")],
        id="aiosqlite-named-colon",
    ),
    pytest.param(
        ParameterCase("adbc-postgres", "SELECT $1::INTEGER AS value", (44,), 44),
        marks=[pytest.mark.postgres, pytest.mark.adbc, pytest.mark.xdist_group("postgres")],
        id="adbc-postgres-numeric",
    ),
    pytest.param(
        ParameterCase("duckdb", "SELECT ? AS value", (45,), 45),
        marks=[pytest.mark.duckdb, pytest.mark.xdist_group("duckdb")],
        id="duckdb-qmark",
    ),
    pytest.param(
        ParameterCase("duckdb", "SELECT :value AS value", {"value": "duckdb"}, "duckdb"),
        marks=[pytest.mark.duckdb, pytest.mark.xdist_group("duckdb")],
        id="duckdb-named-colon",
    ),
    pytest.param(
        ParameterCase("aiomysql", "SELECT %s AS value", (46,), 46),
        marks=[pytest.mark.mysql, pytest.mark.aiomysql, pytest.mark.xdist_group("mysql")],
        id="aiomysql-pyformat",
    ),
    pytest.param(
        ParameterCase("asyncmy", "SELECT %s AS value", (47,), 47),
        marks=[pytest.mark.mysql, pytest.mark.asyncmy, pytest.mark.xdist_group("mysql")],
        id="asyncmy-pyformat",
    ),
    pytest.param(
        ParameterCase("mysqlconnector-sync", "SELECT %s AS value", (48,), 48),
        marks=[pytest.mark.mysql, pytest.mark.mysql_connector, pytest.mark.xdist_group("mysql")],
        id="mysqlconnector-sync-pyformat",
    ),
    pytest.param(
        ParameterCase("mysqlconnector-async", "SELECT %s AS value", (49,), 49),
        marks=[pytest.mark.mysql, pytest.mark.mysql_connector, pytest.mark.xdist_group("mysql")],
        id="mysqlconnector-async-pyformat",
    ),
    pytest.param(
        ParameterCase("pymysql", "SELECT %s AS value", (50,), 50),
        marks=[pytest.mark.mysql, pytest.mark.pymysql, pytest.mark.xdist_group("mysql")],
        id="pymysql-pyformat",
    ),
    pytest.param(
        ParameterCase("asyncpg", "SELECT $1::INTEGER AS value", (51,), 51),
        marks=[pytest.mark.postgres, pytest.mark.asyncpg, pytest.mark.xdist_group("postgres")],
        id="asyncpg-numeric",
    ),
    pytest.param(
        ParameterCase("psqlpy", "SELECT $1::INTEGER AS value", (52,), 52),
        marks=[pytest.mark.postgres, pytest.mark.psqlpy, pytest.mark.xdist_group("postgres")],
        id="psqlpy-numeric",
    ),
    pytest.param(
        ParameterCase("psycopg-sync", "SELECT %s::INTEGER AS value", (53,), 53),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("postgres")],
        id="psycopg-sync-pyformat",
    ),
    pytest.param(
        ParameterCase("psycopg-async", "SELECT %s::INTEGER AS value", (54,), 54),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("postgres")],
        id="psycopg-async-pyformat",
    ),
    pytest.param(
        ParameterCase("cockroach-asyncpg", "SELECT $1::INTEGER AS value", (55,), 55),
        marks=[pytest.mark.postgres, pytest.mark.xdist_group("cockroachdb")],
        id="cockroach-asyncpg-numeric",
    ),
    pytest.param(
        ParameterCase("cockroach-psycopg-sync", "SELECT %s::INTEGER AS value", (56,), 56),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("cockroachdb")],
        id="cockroach-psycopg-sync-pyformat",
    ),
    pytest.param(
        ParameterCase("cockroach-psycopg-async", "SELECT %s::INTEGER AS value", (57,), 57),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("cockroachdb")],
        id="cockroach-psycopg-async-pyformat",
    ),
    pytest.param(
        ParameterCase("oracle-sync", "SELECT :1 AS value FROM dual", (58,), 58),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-sync-positional",
    ),
    pytest.param(
        ParameterCase("oracle-sync", "SELECT :value AS value FROM dual", {"value": "oracle"}, "oracle"),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-sync-named-colon",
    ),
    pytest.param(
        ParameterCase("oracle-async", "SELECT :1 AS value FROM dual", (59,), 59),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-async-positional",
    ),
    pytest.param(
        ParameterCase("oracle-async", "SELECT :value AS value FROM dual", {"value": "oracle-async"}, "oracle-async"),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-async-named-colon",
    ),
    pytest.param(
        ParameterCase("spanner", "SELECT @value", {"value": 60}, 60),
        marks=[pytest.mark.spanner, pytest.mark.google_spanner, pytest.mark.xdist_group("spanner"), SPANNER_LOCAL_SKIP],
        id="spanner-named-at",
    ),
    pytest.param(
        ParameterCase("bigquery", "SELECT @value AS value", {"value": 61}, 61),
        marks=[
            pytest.mark.bigquery,
            pytest.mark.google_bigquery,
            pytest.mark.xdist_group("bigquery"),
            pytest.mark.skip(reason="BigQuery emulator parameter contract is gated in adapter-local tests"),
        ],
        id="bigquery-named-at",
    ),
]


@pytest.mark.parametrize("case", PARAMETER_CASES)
async def test_adapter_parameter_style_executes(
    case: ParameterCase, tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    """Adapters execute their native parameter placeholder style through SQLSpec."""
    config = make_config(case.adapter, request, tmp_path)

    async with provide_driver(case.adapter, config) as driver:
        value = await maybe_await(driver.select_value(case.sql, case.parameters))

    assert value == case.expected
