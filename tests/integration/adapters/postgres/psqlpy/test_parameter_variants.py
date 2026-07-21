"""PSQLPy-specific parameter and row-count coverage."""

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver

pytestmark = pytest.mark.xdist_group("postgres")


async def test_psqlpy_execute_reports_exact_rows_affected(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy should report exact counts for single and batch DML."""
    first_insert = await psqlpy_session.execute("INSERT INTO test_table_psqlpy (name) VALUES (?)", ("single",))
    second_insert = await psqlpy_session.execute("INSERT INTO test_table_psqlpy (name) VALUES (?)", ("cached",))
    update_result = await psqlpy_session.execute(
        "UPDATE test_table_psqlpy SET name = ? WHERE name = ?", ("updated", "single")
    )
    zero_result = await psqlpy_session.execute(
        "UPDATE test_table_psqlpy SET name = ? WHERE name = ?", ("missing", "absent")
    )
    delete_result = await psqlpy_session.execute("DELETE FROM test_table_psqlpy WHERE name = ?", ("updated",))
    many_result = await psqlpy_session.execute_many(
        "INSERT INTO test_table_psqlpy (name) VALUES ($1)", [("batch1",), ("batch2",), ("batch3",)]
    )

    assert first_insert.rows_affected == 1
    assert second_insert.rows_affected == 1
    assert update_result.rows_affected == 1
    assert zero_result.rows_affected == 0
    assert delete_result.rows_affected == 1
    assert many_result.rows_affected == 3
