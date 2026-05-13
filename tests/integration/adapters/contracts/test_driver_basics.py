# pyright: reportUnknownMemberType=false
"""Cross-adapter driver basics contract tests."""

import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from google.api_core import exceptions as api_exceptions

from tests.integration.adapters.contracts._helpers import SPANNER_LOCAL_SKIP, make_config, maybe_await, provide_driver


@dataclass(frozen=True)
class DriverCase:
    """Per-adapter driver basics setup."""

    adapter: str
    table_name: str
    id_type: str
    text_type: str
    placeholder_style: str
    cascade_drop: bool = False
    spanner_ddl: bool = False


DRIVER_CASES = [
    pytest.param(
        DriverCase("sqlite", "driver_basic_sqlite", "INTEGER", "TEXT", "qmark"),
        marks=[pytest.mark.sqlite, pytest.mark.xdist_group("sqlite")],
        id="sqlite",
    ),
    pytest.param(
        DriverCase("aiosqlite", "driver_basic_aiosqlite", "INTEGER", "TEXT", "qmark"),
        marks=[pytest.mark.sqlite, pytest.mark.aiosqlite, pytest.mark.xdist_group("sqlite")],
        id="aiosqlite",
    ),
    pytest.param(
        DriverCase("adbc-postgres", "driver_basic_adbc", "INTEGER", "TEXT", "numeric", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.adbc, pytest.mark.xdist_group("postgres")],
        id="adbc-postgres",
    ),
    pytest.param(
        DriverCase("duckdb", "driver_basic_duckdb", "INTEGER", "VARCHAR", "qmark"),
        marks=[pytest.mark.duckdb, pytest.mark.xdist_group("duckdb")],
        id="duckdb",
    ),
    pytest.param(
        DriverCase("aiomysql", "driver_basic_aiomysql", "INT", "VARCHAR(100)", "qmark"),
        marks=[pytest.mark.mysql, pytest.mark.aiomysql, pytest.mark.xdist_group("mysql")],
        id="aiomysql",
    ),
    pytest.param(
        DriverCase("asyncmy", "driver_basic_asyncmy", "INT", "VARCHAR(100)", "qmark"),
        marks=[pytest.mark.mysql, pytest.mark.asyncmy, pytest.mark.xdist_group("mysql")],
        id="asyncmy",
    ),
    pytest.param(
        DriverCase("mysqlconnector-sync", "driver_basic_mysqlconn_s", "INT", "VARCHAR(100)", "qmark"),
        marks=[pytest.mark.mysql, pytest.mark.mysql_connector, pytest.mark.xdist_group("mysql")],
        id="mysqlconnector-sync",
    ),
    pytest.param(
        DriverCase("mysqlconnector-async", "driver_basic_mysqlconn_a", "INT", "VARCHAR(100)", "qmark"),
        marks=[pytest.mark.mysql, pytest.mark.mysql_connector, pytest.mark.xdist_group("mysql")],
        id="mysqlconnector-async",
    ),
    pytest.param(
        DriverCase("pymysql", "driver_basic_pymysql", "INT", "VARCHAR(100)", "qmark"),
        marks=[pytest.mark.mysql, pytest.mark.pymysql, pytest.mark.xdist_group("mysql")],
        id="pymysql",
    ),
    pytest.param(
        DriverCase("asyncpg", "driver_basic_asyncpg", "INTEGER", "TEXT", "numeric", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.asyncpg, pytest.mark.xdist_group("postgres")],
        id="asyncpg",
    ),
    pytest.param(
        DriverCase("psqlpy", "driver_basic_psqlpy", "INTEGER", "TEXT", "numeric", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.psqlpy, pytest.mark.xdist_group("postgres")],
        id="psqlpy",
    ),
    pytest.param(
        DriverCase("psycopg-sync", "driver_basic_psycopg_s", "INTEGER", "TEXT", "pyformat", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("postgres")],
        id="psycopg-sync",
    ),
    pytest.param(
        DriverCase("psycopg-async", "driver_basic_psycopg_a", "INTEGER", "TEXT", "pyformat", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("postgres")],
        id="psycopg-async",
    ),
    pytest.param(
        DriverCase("cockroach-asyncpg", "driver_basic_crdb_apg", "INTEGER", "STRING", "numeric", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.xdist_group("cockroachdb")],
        id="cockroach-asyncpg",
    ),
    pytest.param(
        DriverCase(
            "cockroach-psycopg-sync", "driver_basic_crdb_ps_s", "INTEGER", "STRING", "pyformat", cascade_drop=True
        ),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("cockroachdb")],
        id="cockroach-psycopg-sync",
    ),
    pytest.param(
        DriverCase(
            "cockroach-psycopg-async", "driver_basic_crdb_ps_a", "INTEGER", "STRING", "pyformat", cascade_drop=True
        ),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("cockroachdb")],
        id="cockroach-psycopg-async",
    ),
    pytest.param(
        DriverCase("oracle-sync", "DRIVER_BASIC_ORA_S", "NUMBER", "VARCHAR2(100)", "oracle"),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-sync",
    ),
    pytest.param(
        DriverCase("oracle-async", "DRIVER_BASIC_ORA_A", "NUMBER", "VARCHAR2(100)", "oracle"),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-async",
    ),
    pytest.param(
        DriverCase("spanner", "driver_basic_spanner", "INT64", "STRING(100)", "named_at", spanner_ddl=True),
        marks=[pytest.mark.spanner, pytest.mark.google_spanner, pytest.mark.xdist_group("spanner"), SPANNER_LOCAL_SKIP],
        id="spanner",
    ),
    pytest.param(
        DriverCase("bigquery", "driver_basic_bigquery", "INT64", "STRING", "named_at"),
        marks=[
            pytest.mark.bigquery,
            pytest.mark.google_bigquery,
            pytest.mark.xdist_group("bigquery"),
            pytest.mark.skip(reason="BigQuery driver basics remain emulator-gated in adapter-local tests"),
        ],
        id="bigquery",
    ),
]


def _placeholders(style: str) -> str:
    if style == "numeric":
        return "$1, $2, $3"
    if style == "pyformat":
        return "%s, %s, %s"
    if style == "oracle":
        return ":1, :2, :3"
    if style == "named_at":
        return "@id, @name, @value"
    return "?, ?, ?"


def _select_id_sql(case: DriverCase) -> str:
    if case.placeholder_style == "numeric":
        return f"SELECT name, value FROM {case.table_name} WHERE id = $1"
    if case.placeholder_style == "pyformat":
        return f"SELECT name, value FROM {case.table_name} WHERE id = %s"
    if case.placeholder_style == "oracle":
        return f"SELECT name, value FROM {case.table_name} WHERE id = :1"
    if case.placeholder_style == "named_at":
        return f"SELECT name, value FROM {case.table_name} WHERE id = @id"
    return f"SELECT name, value FROM {case.table_name} WHERE id = ?"


def _select_id_params(case: DriverCase, row_id: int) -> tuple[int] | dict[str, int]:
    if case.placeholder_style == "named_at":
        return {"id": row_id}
    return (row_id,)


def _rows(case: DriverCase) -> list[tuple[int, str, int]] | list[dict[str, Any]]:
    data = [(idx, f"user_{idx:03d}", idx * 10) for idx in range(1, 101)]
    if case.placeholder_style == "named_at":
        return [{"id": row_id, "name": name, "value": value} for row_id, name, value in data]
    return data


async def _execute(driver: Any, sql: str, parameters: Any | None = None) -> Any:
    result = driver.execute(sql) if parameters is None else driver.execute(sql, parameters)
    return await maybe_await(result)


async def _execute_many(driver: Any, sql: str, parameters: Any) -> Any:
    return await maybe_await(driver.execute_many(sql, parameters))


async def _create_table(driver: Any, case: DriverCase) -> None:
    if case.spanner_ddl:
        return
    await _execute(
        driver, f"CREATE TABLE {case.table_name} (id {case.id_type} PRIMARY KEY, name {case.text_type}, value INT)"
    )


async def _drop_table(driver: Any, case: DriverCase) -> None:
    if case.spanner_ddl:
        return
    if case.adapter.startswith("oracle"):
        with contextlib.suppress(Exception):
            await _execute(driver, f"DROP TABLE {case.table_name} PURGE")
        return
    suffix = " CASCADE" if case.cascade_drop else ""
    await _execute(driver, f"DROP TABLE IF EXISTS {case.table_name}{suffix}")


def _spanner_database(request: pytest.FixtureRequest) -> Any:
    service = request.getfixturevalue("spanner_service")
    connection = request.getfixturevalue("spanner_connection")
    instance = connection.instance(service.instance_name)
    return instance.database(service.database_name)


def _spanner_create_table(request: pytest.FixtureRequest, case: DriverCase) -> None:
    database = _spanner_database(request)
    with contextlib.suppress(api_exceptions.GoogleAPICallError):
        database.update_ddl([f"DROP TABLE {case.table_name}"]).result(300)
    database.update_ddl([
        f"""
            CREATE TABLE {case.table_name} (
                id INT64 NOT NULL,
                name STRING(100),
                value INT64
            ) PRIMARY KEY (id)
            """
    ]).result(300)


def _spanner_drop_table(request: pytest.FixtureRequest, case: DriverCase) -> None:
    with contextlib.suppress(api_exceptions.GoogleAPICallError):
        _spanner_database(request).update_ddl([f"DROP TABLE {case.table_name}"]).result(300)


@pytest.mark.parametrize("case", DRIVER_CASES)
async def test_driver_basic_operations(case: DriverCase, tmp_path: Path, request: pytest.FixtureRequest) -> None:
    """Adapters support the shared SQLSpec driver basics contract."""
    config = make_config(case.adapter, request, tmp_path)
    insert_sql = f"INSERT INTO {case.table_name} (id, name, value) VALUES ({_placeholders(case.placeholder_style)})"

    if case.spanner_ddl:
        _spanner_create_table(request, case)

    try:
        async with provide_driver(case.adapter, config, write=case.spanner_ddl) as driver:
            await _drop_table(driver, case)
            await _create_table(driver, case)
            result = await _execute_many(driver, insert_sql, _rows(case))
            assert result.rows_affected in {100, -1}

            one = await maybe_await(driver.select_one(_select_id_sql(case), _select_id_params(case, 42)))
            assert one["name"] == "user_042"
            assert one["value"] == 420

            many_result = await _execute(driver, f"SELECT id, name, value FROM {case.table_name} ORDER BY id")
            many = many_result.get_data()
            assert len(many) == 100
            assert many[0]["name"] == "user_001"
            assert many[-1]["name"] == "user_100"

            scalar = await maybe_await(driver.select_value(f"SELECT COUNT(*) FROM {case.table_name}"))
            assert scalar == 100

            missing = await maybe_await(driver.select_one_or_none(_select_id_sql(case), _select_id_params(case, 999)))
            assert missing is None

            await _drop_table(driver, case)
    finally:
        if case.spanner_ddl:
            _spanner_drop_table(request, case)
