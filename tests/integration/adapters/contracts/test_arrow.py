# pyright: reportUnknownMemberType=false
"""Cross-adapter Arrow result contract tests."""

import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from google.api_core import exceptions as api_exceptions

from sqlspec.typing import PYARROW_INSTALLED
from tests.integration.adapters.contracts._helpers import SPANNER_LOCAL_SKIP, make_config, maybe_await, provide_driver

pytestmark = pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")


@dataclass(frozen=True)
class ArrowCase:
    """Per-adapter Arrow setup."""

    adapter: str
    table_name: str
    id_type: str
    text_type: str
    placeholder_style: str
    cascade_drop: bool = False
    spanner_ddl: bool = False


ARROW_CASES = [
    pytest.param(
        ArrowCase("sqlite", "arrow_contract_sqlite", "INTEGER", "TEXT", "qmark"),
        marks=[pytest.mark.sqlite, pytest.mark.xdist_group("sqlite")],
        id="sqlite",
    ),
    pytest.param(
        ArrowCase("aiosqlite", "arrow_contract_aiosqlite", "INTEGER", "TEXT", "qmark"),
        marks=[pytest.mark.sqlite, pytest.mark.aiosqlite, pytest.mark.xdist_group("sqlite")],
        id="aiosqlite",
    ),
    pytest.param(
        ArrowCase("adbc-postgres", "arrow_contract_adbc", "INTEGER", "TEXT", "numeric", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.adbc, pytest.mark.xdist_group("postgres")],
        id="adbc-postgres",
    ),
    pytest.param(
        ArrowCase("duckdb", "arrow_contract_duckdb", "INTEGER", "VARCHAR", "qmark"),
        marks=[pytest.mark.duckdb, pytest.mark.xdist_group("duckdb")],
        id="duckdb",
    ),
    pytest.param(
        ArrowCase("aiomysql", "arrow_contract_aiomysql", "INT", "VARCHAR(100)", "pyformat"),
        marks=[pytest.mark.mysql, pytest.mark.aiomysql, pytest.mark.xdist_group("mysql")],
        id="aiomysql",
    ),
    pytest.param(
        ArrowCase("asyncmy", "arrow_contract_asyncmy", "INT", "VARCHAR(100)", "pyformat"),
        marks=[pytest.mark.mysql, pytest.mark.asyncmy, pytest.mark.xdist_group("mysql")],
        id="asyncmy",
    ),
    pytest.param(
        ArrowCase("asyncpg", "arrow_contract_asyncpg", "INTEGER", "TEXT", "numeric", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.asyncpg, pytest.mark.xdist_group("postgres")],
        id="asyncpg",
    ),
    pytest.param(
        ArrowCase("psqlpy", "arrow_contract_psqlpy", "INTEGER", "TEXT", "numeric", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.psqlpy, pytest.mark.xdist_group("postgres")],
        id="psqlpy",
    ),
    pytest.param(
        ArrowCase("psycopg-async", "arrow_contract_psycopg_a", "INTEGER", "TEXT", "pyformat", cascade_drop=True),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("postgres")],
        id="psycopg-async",
    ),
    pytest.param(
        ArrowCase("oracle-async", "ARROW_CONTRACT_ORA_A", "NUMBER", "VARCHAR2(100)", "oracle"),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-async",
    ),
    pytest.param(
        ArrowCase("spanner", "arrow_contract_spanner", "INT64", "STRING(100)", "named_at", spanner_ddl=True),
        marks=[pytest.mark.spanner, pytest.mark.google_spanner, pytest.mark.xdist_group("spanner"), SPANNER_LOCAL_SKIP],
        id="spanner",
    ),
    pytest.param(
        ArrowCase("bigquery", "arrow_contract_bigquery", "INT64", "STRING", "named_at"),
        marks=[
            pytest.mark.bigquery,
            pytest.mark.google_bigquery,
            pytest.mark.xdist_group("bigquery"),
            pytest.mark.skip(reason="BigQuery Arrow contract remains emulator-gated in adapter-local tests"),
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


def _parameter_sql(case: ArrowCase) -> str:
    if case.placeholder_style == "numeric":
        return f"SELECT id, name, value FROM {case.table_name} WHERE value > $1 ORDER BY id"
    if case.placeholder_style == "pyformat":
        return f"SELECT id, name, value FROM {case.table_name} WHERE value > %s ORDER BY id"
    if case.placeholder_style == "oracle":
        return f"SELECT id, name, value FROM {case.table_name} WHERE value > :1 ORDER BY id"
    if case.placeholder_style == "named_at":
        return f"SELECT id, name, value FROM {case.table_name} WHERE value > @min_value ORDER BY id"
    return f"SELECT id, name, value FROM {case.table_name} WHERE value > ? ORDER BY id"


def _parameter_values(case: ArrowCase) -> tuple[int] | dict[str, int]:
    if case.placeholder_style == "named_at":
        return {"min_value": 15}
    return (15,)


def _rows(case: ArrowCase) -> list[tuple[int, str, int]] | list[dict[str, Any]]:
    data = [(1, "Alice", 10), (2, "Bob", 20), (3, "Charlie", 30)]
    if case.placeholder_style == "named_at":
        return [{"id": row_id, "name": name, "value": value} for row_id, name, value in data]
    return data


async def _execute(driver: Any, sql: str, parameters: Any | None = None) -> Any:
    result = driver.execute(sql) if parameters is None else driver.execute(sql, parameters)
    return await maybe_await(result)


async def _execute_many(driver: Any, sql: str, parameters: Any) -> Any:
    return await maybe_await(driver.execute_many(sql, parameters))


async def _select_to_arrow(driver: Any, sql: str, parameters: Any | None = None, **kwargs: Any) -> Any:
    if parameters is None:
        return await maybe_await(driver.select_to_arrow(sql, **kwargs))
    return await maybe_await(driver.select_to_arrow(sql, parameters, **kwargs))


async def _drop_table(driver: Any, case: ArrowCase) -> None:
    if case.spanner_ddl:
        return
    if case.adapter.startswith("oracle"):
        with contextlib.suppress(Exception):
            await _execute(driver, f"DROP TABLE {case.table_name} PURGE")
        return
    suffix = " CASCADE" if case.cascade_drop else ""
    await _execute(driver, f"DROP TABLE IF EXISTS {case.table_name}{suffix}")


async def _create_table(driver: Any, case: ArrowCase) -> None:
    if case.spanner_ddl:
        return
    await _execute(
        driver, f"CREATE TABLE {case.table_name} (id {case.id_type} PRIMARY KEY, name {case.text_type}, value INT)"
    )


def _spanner_database(request: pytest.FixtureRequest) -> Any:
    service = request.getfixturevalue("spanner_service")
    connection = request.getfixturevalue("spanner_connection")
    return connection.instance(service.instance_name).database(service.database_name)


def _spanner_create_table(request: pytest.FixtureRequest, case: ArrowCase) -> None:
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


def _spanner_drop_table(request: pytest.FixtureRequest, case: ArrowCase) -> None:
    with contextlib.suppress(api_exceptions.GoogleAPICallError):
        _spanner_database(request).update_ddl([f"DROP TABLE {case.table_name}"]).result(300)


def _records(result: Any) -> list[dict[str, Any]]:
    return [{key.lower(): value for key, value in row.items()} for row in result.to_pandas().to_dict("records")]


@pytest.mark.parametrize("case", ARROW_CASES)
async def test_select_to_arrow_table_result(case: ArrowCase, tmp_path: Path, request: pytest.FixtureRequest) -> None:
    """Adapters return Arrow tables for a basic ordered result set."""
    config = make_config(case.adapter, request, tmp_path)
    insert_sql = f"INSERT INTO {case.table_name} (id, name, value) VALUES ({_placeholders(case.placeholder_style)})"

    if case.spanner_ddl:
        _spanner_create_table(request, case)

    try:
        async with provide_driver(case.adapter, config, write=case.spanner_ddl) as driver:
            await _drop_table(driver, case)
            await _create_table(driver, case)
            await _execute_many(driver, insert_sql, _rows(case))

            result = await _select_to_arrow(
                driver, f"SELECT id, name, value FROM {case.table_name} ORDER BY id", return_format="table"
            )

            assert isinstance(result.data, pa.Table)
            assert result.rows_affected == 3
            assert _records(result) == [
                {"id": 1, "name": "Alice", "value": 10},
                {"id": 2, "name": "Bob", "value": 20},
                {"id": 3, "name": "Charlie", "value": 30},
            ]

            filtered = await _select_to_arrow(driver, _parameter_sql(case), _parameter_values(case))
            assert [row["name"] for row in _records(filtered)] == ["Bob", "Charlie"]

            empty = await _select_to_arrow(
                driver, _parameter_sql(case), {"min_value": 999} if case.placeholder_style == "named_at" else (999,)
            )
            assert empty.rows_affected == 0
            assert len(empty.to_pandas()) == 0

            await _drop_table(driver, case)
    finally:
        if case.spanner_ddl:
            _spanner_drop_table(request, case)
