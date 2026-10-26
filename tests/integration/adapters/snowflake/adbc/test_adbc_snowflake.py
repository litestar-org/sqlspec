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
