"""Unit tests for the driver lifecycle (pooling / connection-hook) contract wiring."""

from contextlib import suppress
from importlib import import_module
from typing import Any

import pytest

from sqlspec.core import StatementConfig as SQLSpecStatementConfig
from tests.integration.adapters._shared._cases import DRIVER_CASES, get_driver_case

_SESSION_CONTEXTS = (
    ("sqlspec.adapters.sqlite._typing", "SqliteSessionContext"),
    ("sqlspec.adapters.duckdb._typing", "DuckDBSessionContext"),
    ("sqlspec.adapters.pymysql._typing", "PyMysqlSessionContext"),
    ("sqlspec.adapters.pymssql._typing", "PymssqlSessionContext"),
    ("sqlspec.adapters.mssql_python._typing", "MssqlPythonSessionContext"),
    ("sqlspec.adapters.adbc._typing", "AdbcSessionContext"),
    ("sqlspec.adapters.arrow_odbc._typing", "ArrowOdbcSessionContext"),
    ("sqlspec.adapters.bigquery._typing", "BigQuerySessionContext"),
)

POOLING_CASES = (
    "sqlite-sync",
    "duckdb-sync",
    "aiosqlite-async",
    "psycopg-sync",
    "cockroach-psycopg-sync",
    "asyncpg-async",
    "psqlpy-async",
    "psycopg-async",
    "cockroach-asyncpg-async",
    "cockroach-psycopg-async",
    "mysqlconnector-sync",
    "pymysql-sync",
    "aiomysql-async",
    "asyncmy-async",
    "oracledb-sync",
    "oracledb-async",
)
# mysqlconnector-async and bigquery-sync are NoPool configs: connection-hook only, no pooling.
CONNECTION_HOOK_CASES = (*POOLING_CASES, "mysqlconnector-async", "bigquery-sync")


@pytest.mark.parametrize("case_id", POOLING_CASES)
def test_pooling_case_declares_factory(case_id: str) -> None:
    """A case that opts into the pooling contract must provide a config factory fixture."""
    case = get_driver_case(case_id)
    assert case.supports_pooling
    assert case.config_factory_fixture is not None


@pytest.mark.parametrize("case_id", CONNECTION_HOOK_CASES)
def test_connection_hook_case_declares_factory(case_id: str) -> None:
    """A case that opts into the connection-hook contract must provide a config factory fixture."""
    case = get_driver_case(case_id)
    assert case.supports_connection_hook
    assert case.config_factory_fixture is not None


def test_lifecycle_flags_require_config_factory() -> None:
    """No case may declare a config-factory-driven feature without a config factory (no untestable claim)."""
    for case in DRIVER_CASES:
        needs_factory = (
            case.supports_pooling
            or case.supports_connection_hook
            or case.supports_lowercase_columns
            or case.supports_uuid_feature
            or case.supports_custom_json_serializer
            or case.supports_custom_type_adapters
        )
        if needs_factory:
            assert case.config_factory_fixture is not None, (
                f"{case.id} declares a config-factory feature without a config_factory_fixture"
            )


def test_session_contexts_forward_exception_info_to_release() -> None:
    """Every adapter session context must hand its exception triple to connection release."""
    offenders: list[str] = []
    for module_name, context_name in _SESSION_CONTEXTS:
        context_class = getattr(import_module(module_name), context_name)
        recorded: list[dict[str, Any]] = []
        context = context_class(
            acquire_connection=lambda: object(),
            release_connection=lambda _conn, **kwargs: recorded.append(kwargs),
            statement_config=SQLSpecStatementConfig(),
            driver_features={},
            prepare_driver=lambda driver: driver,
        )
        with suppress(RuntimeError):
            with context:
                raise RuntimeError
        if not recorded or recorded[0].get("exc_type") is not RuntimeError:
            offenders.append(f"{module_name}.{context_name}")

    assert offenders == []
