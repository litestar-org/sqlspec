"""PSQLPy-specific parameter variant coverage.

Only the psqlpy rows_affected deviation remains here (single execute DML reports -1 while
execute_many reports the actual affected count). Generic binding, native placeholder styles, and
codec/type-fidelity behavior are covered by the shared parameter, parameter-style, and
``param_codecs:psqlpy`` contracts.
"""

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver

pytestmark = pytest.mark.xdist_group("postgres")


async def test_psqlpy_execute_rows_affected_deviation(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy single execute DML reports -1, while execute_many reports actual affected rows."""
    insert_result = await psqlpy_session.execute("INSERT INTO test_table_psqlpy (name) VALUES (?)", ("single",))
    many_result = await psqlpy_session.execute_many(
        "INSERT INTO test_table_psqlpy (name) VALUES ($1)", [("batch1",), ("batch2",), ("batch3",)]
    )

    assert insert_result.rows_affected == -1
    assert many_result.rows_affected == 3
