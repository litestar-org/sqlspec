"""Containerized IBM Db2 ADBC integration tests using pure-Go adbc-driver-db2."""

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.adbc import AdbcDriver

pytestmark = [pytest.mark.db2, pytest.mark.adbc, pytest.mark.integration, pytest.mark.xdist_group("db2")]


def test_adbc_db2_sysdummy1_scalar_probe(adbc_db2_session: "AdbcDriver") -> None:
    """Pure-Go Db2 ADBC executes a scalar probe against SYSIBM.SYSDUMMY1 over DRDA without clidriver."""
    result = adbc_db2_session.execute("SELECT 1 AS v FROM SYSIBM.SYSDUMMY1")
    rows = [_lowercase_keys(row) for row in result.get_data()]
    assert len(rows) == 1
    assert int(rows[0]["v"]) == 1


def test_adbc_db2_parameterized_crud_and_arrow(adbc_db2_session: "AdbcDriver") -> None:
    """Db2 ADBC executes scratch table DDL, positional and named parameter binding, and Arrow fetches."""
    adbc_db2_session.execute("INSERT INTO test_table_adbc_db2 (id, name, amount) VALUES (?, ?, ?)", (1, "alpha", 25))
    adbc_db2_session.execute("INSERT INTO test_table_adbc_db2 (id, name, amount) VALUES (?, ?, ?)", (2, "beta", 50))
    adbc_db2_session.commit()

    filtered = adbc_db2_session.execute(
        "SELECT id, name, amount FROM test_table_adbc_db2 WHERE id = :item_id", {"item_id": 1}
    )
    filtered_rows = [_lowercase_keys(row) for row in filtered.get_data()]
    assert len(filtered_rows) == 1
    assert int(filtered_rows[0]["id"]) == 1
    assert filtered_rows[0]["name"] == "alpha"
    assert int(filtered_rows[0]["amount"]) == 25

    arrow_result = adbc_db2_session.select_to_arrow("SELECT id, name, amount FROM test_table_adbc_db2 ORDER BY id")
    assert arrow_result.num_rows == 2
    assert [col.lower() for col in arrow_result.column_names] == ["id", "name", "amount"]
    arrow_rows = [_lowercase_keys(row) for row in arrow_result]
    assert [row["name"] for row in arrow_rows] == ["alpha", "beta"]
    assert [int(row["amount"]) for row in arrow_rows] == [25, 50]


def _lowercase_keys(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize uppercase database column identifiers to lowercase keys."""
    return {key.lower(): value for key, value in row.items()}
