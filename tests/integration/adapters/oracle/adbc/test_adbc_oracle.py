"""Containerized Oracle Database ADBC integration tests using pure-Go adbc-driver-oracle."""

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.adbc import AdbcDriver

pytestmark = [pytest.mark.oracle, pytest.mark.adbc, pytest.mark.integration, pytest.mark.xdist_group("oracle")]


def test_adbc_oracle_dual_scalar_probe(adbc_oracle_session: "AdbcDriver") -> None:
    """Pure-Go Oracle ADBC executes a scalar probe against DUAL without Oracle Instant Client."""
    result = adbc_oracle_session.execute("SELECT 1 AS v FROM DUAL")
    rows = [_lowercase_keys(row) for row in result.get_data()]
    assert len(rows) == 1
    assert int(rows[0]["v"]) == 1


def test_adbc_oracle_parameterized_crud_and_arrow(adbc_oracle_session: "AdbcDriver") -> None:
    """Oracle ADBC executes scratch table DDL, positional and named parameter binding, and Arrow fetches."""
    adbc_oracle_session.execute(
        "INSERT INTO test_table_adbc_oracle (id, name, amount) VALUES (:1, :2, :3)", (1, "alpha", 15)
    )
    adbc_oracle_session.execute(
        "INSERT INTO test_table_adbc_oracle (id, name, amount) VALUES (:1, :2, :3)", (2, "beta", 30)
    )
    adbc_oracle_session.commit()

    filtered = adbc_oracle_session.execute(
        "SELECT id, name, amount FROM test_table_adbc_oracle WHERE id = :item_id", {"item_id": 2}
    )
    filtered_rows = [_lowercase_keys(row) for row in filtered.get_data()]
    assert len(filtered_rows) == 1
    assert int(filtered_rows[0]["id"]) == 2
    assert filtered_rows[0]["name"] == "beta"
    assert int(filtered_rows[0]["amount"]) == 30

    arrow_result = adbc_oracle_session.select_to_arrow(
        "SELECT id, name, amount FROM test_table_adbc_oracle ORDER BY id"
    )
    assert arrow_result.num_rows == 2
    assert [col.lower() for col in arrow_result.column_names] == ["id", "name", "amount"]
    arrow_rows = [_lowercase_keys(row) for row in arrow_result]
    assert [row["name"] for row in arrow_rows] == ["alpha", "beta"]
    assert [int(row["amount"]) for row in arrow_rows] == [15, 30]


def _lowercase_keys(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize uppercase database column identifiers to lowercase keys."""
    return {key.lower(): value for key, value in row.items()}
