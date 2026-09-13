"""ADBC driver residuals not owned by the shared contract matrix.

The contract suite owns CRUD, parameter styles, execute_many, execute_script,
sequential StatementStack execution, SQLResult helpers, mapped errors, bulk
operations, and multi-backend consistency. This module keeps ADBC-specific
StatementStack continue-on-error recovery and SQLite locking rejection.
"""

import pytest

from sqlspec import StatementStack, sql
from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.exceptions import SQLBuilderError
from tests.conftest import requires_interpreted


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
@requires_interpreted
def test_adbc_postgresql_statement_stack_continue_on_error(adbc_postgresql_session: AdbcDriver) -> None:
    """continue_on_error should surface failures but execute remaining operations."""
    adbc_postgresql_session.execute("DELETE FROM test_table_adbc")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table_adbc (id, name, value) VALUES ($1, $2, $3)", (1, "adbc-initial", 5))
        .push_execute("INSERT INTO test_table_adbc (id, name, value) VALUES ($1, $2, $3)", (1, "adbc-duplicate", 15))
        .push_execute("INSERT INTO test_table_adbc (id, name, value) VALUES ($1, $2, $3)", (2, "adbc-final", 25))
    )

    results = adbc_postgresql_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[1].error is not None

    verify = adbc_postgresql_session.execute("SELECT COUNT(*) AS total FROM test_table_adbc")
    assert verify.get_data()[0]["total"] == 2


@pytest.mark.xdist_group("sqlite")
@pytest.mark.adbc
@pytest.mark.parametrize("lock_method", ["for_update", "for_share", "for_update_skip_locked"])
def test_adbc_unsupported_lock_raises(adbc_sqlite_session: AdbcDriver, lock_method: str) -> None:
    """SQLite-backed ADBC rejects locking instead of executing an unlocked query."""
    query = sql.select("*").from_("test_table_adbc")
    if lock_method == "for_share":
        query = query.for_share()
    else:
        query = query.for_update(skip_locked=lock_method == "for_update_skip_locked")
    with pytest.raises(SQLBuilderError, match="does not support FOR UPDATE / row locking"):
        adbc_sqlite_session.execute(query)
