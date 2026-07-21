"""Unit tests for the driver lifecycle (pooling / connection-hook) contract wiring."""

import pytest

from tests.integration.adapters._shared._cases import DRIVER_CASES, get_driver_case

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
