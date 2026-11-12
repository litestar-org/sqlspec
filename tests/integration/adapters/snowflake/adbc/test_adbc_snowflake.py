"""Containerized Snowflake ADBC integration tests against Embucket using adbc-driver-snowflake."""

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.adbc import AdbcDriver

pytestmark = [pytest.mark.snowflake, pytest.mark.adbc, pytest.mark.integration, pytest.mark.xdist_group("snowflake")]


def test_adbc_snowflake_scalar_probe(adbc_snowflake_session: "AdbcDriver") -> None:
    """Snowflake ADBC executes a scalar SELECT probe over HTTP against Embucket."""
    result = adbc_snowflake_session.execute("SELECT 1 AS v")
    rows = [_lowercase_keys(row) for row in result.get_data()]
    assert len(rows) == 1
    assert int(rows[0]["v"]) == 1


def test_adbc_snowflake_parameterized_crud_and_arrow(adbc_snowflake_session: "AdbcDriver") -> None:
    """Snowflake ADBC executes scratch table DDL, parameter binding, and Arrow fetches against Embucket."""
    adbc_snowflake_session.execute(
        "INSERT INTO test_table_adbc_snowflake (id, name, amount) VALUES (?, ?, ?)", (1, "alpha", 40)
    )
    adbc_snowflake_session.execute(
        "INSERT INTO test_table_adbc_snowflake (id, name, amount) VALUES (?, ?, ?)", (2, "beta", 80)
    )

    filtered = adbc_snowflake_session.execute(
        "SELECT id, name, amount FROM test_table_adbc_snowflake WHERE id = ?", (2,)
    )
    filtered_rows = [_lowercase_keys(row) for row in filtered.get_data()]
    assert len(filtered_rows) == 1
    assert int(filtered_rows[0]["id"]) == 2
    assert filtered_rows[0]["name"] == "beta"
    assert int(filtered_rows[0]["amount"]) == 80

    arrow_result = adbc_snowflake_session.select_to_arrow(
        "SELECT id, name, amount FROM test_table_adbc_snowflake ORDER BY id"
    )
    assert arrow_result.num_rows == 2
    assert [col.lower() for col in arrow_result.column_names] == ["id", "name", "amount"]
    arrow_rows = [_lowercase_keys(row) for row in arrow_result]
    assert [row["name"] for row in arrow_rows] == ["alpha", "beta"]
    assert [int(row["amount"]) for row in arrow_rows] == [40, 80]


def _lowercase_keys(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize uppercase database column identifiers to lowercase keys."""
    return {key.lower(): value for key, value in row.items()}


def test_adbc_snowflake_named_parameters_and_script(adbc_snowflake_session: "AdbcDriver") -> None:
    """Top-level connection options support named binds and independently bound scripts."""
    session = adbc_snowflake_session
    session.execute(
        "INSERT INTO test_table_adbc_snowflake (id, name, amount) VALUES (:id, :name, :amount)",
        {"id": 10, "name": "delta", "amount": 95},
    )
    rows = session.select(
        "SELECT id, name, amount FROM test_table_adbc_snowflake WHERE name = :name", {"name": "delta"}
    )
    assert rows[0]["name"] == "delta"
    assert int(rows[0]["id"]) == 10
    assert int(rows[0]["amount"]) == 95
    result = session.execute_script("SELECT 's3://bucket/key?x=1'; SELECT :value", {"value": 7})
    assert result.successful_statements == 2


def test_adbc_snowflake_query_tag_and_observability(adbc_snowflake_config: Any) -> None:
    """The Snowflake query tag reaches the wire alongside correlated statement events."""
    from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime
    from sqlspec.utils.correlation import CorrelationContext
    from tests.fixtures.snowflake import _EmbucketSnowflakeProxyHandler

    events: list[Any] = []
    runtime = ObservabilityRuntime(ObservabilityConfig(statement_observers=[events.append]), config_name="AdbcConfig")
    with adbc_snowflake_config.provide_session() as session:
        session.attach_observability(runtime)
        with CorrelationContext.context("corr-embucket-1"):
            assert int(session.select_value("SELECT 1 AS ok")) == 1
            assert (
                _EmbucketSnowflakeProxyHandler.last_query_parameters["QUERY_TAG"] == "sqlspec-embucket:corr-embucket-1"
            )
    assert events[-1].db_system == "snowflake"
    assert events[-1].correlation_id == "corr-embucket-1"


def test_adbc_snowflake_live_cloud_smoke() -> None:
    """An explicitly configured cloud account exercises real Snowflake authentication and stage ordinals."""
    import os

    from sqlspec.adapters.adbc import AdbcConfig

    account = os.getenv("SNOWFLAKE_ADBC_ACCOUNT")
    if not account:
        pytest.skip("SNOWFLAKE_ADBC_ACCOUNT is not configured")
    options: dict[str, Any] = {"driver": "snowflake", "account": account, "query_tag": "sqlspec-adbc-cloud"}
    for suffix, field in [
        ("USER", "username"),
        ("PASSWORD", "password"),
        ("TOKEN", "token"),
        ("AUTH_TYPE", "auth_type"),
        ("PRIVATE_KEY", "private_key"),
        ("WAREHOUSE", "warehouse"),
        ("DATABASE", "database"),
        ("SCHEMA", "schema"),
    ]:
        value = os.getenv("SNOWFLAKE_ADBC_" + suffix)
        if value is not None:
            options[field] = value
    config = AdbcConfig(connection_config=options)
    try:
        with config.provide_session() as session:
            version = session.select_one("SELECT CURRENT_VERSION() AS ver, CURRENT_SESSION() AS sess")
            assert version["ver"]
            assert int(version["sess"]) > 0
            assert (
                session.select_value(
                    "SELECT $1 FROM VALUES ('stage_safe') WHERE $1 = :expected", {"expected": "stage_safe"}
                )
                == "stage_safe"
            )
    finally:
        config.close_pool()
