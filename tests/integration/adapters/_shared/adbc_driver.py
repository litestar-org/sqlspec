"""ADBC driver residuals not owned by the shared contract matrix.

The contract suite owns CRUD, parameter styles, execute_many, execute_script,
sequential StatementStack execution, SQLResult helpers, mapped errors, bulk
operations, and multi-backend consistency. This module keeps ADBC-specific
StatementStack continue-on-error recovery and exact SQLite lock SQL generation.
"""

import pytest

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.adbc import AdbcDriver
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
def test_adbc_for_update_generates_sql(adbc_sqlite_session: AdbcDriver) -> None:
    """SQLite-backed ADBC strips unsupported FOR UPDATE while preserving the query."""
    adbc_sqlite_session.execute("INSERT INTO test_table_adbc (name, value) VALUES (?, ?)", ("adbc_lock", 100))

    query = sql.select("*").from_("test_table_adbc").where_eq("name", "adbc_lock").for_update()
    stmt = query.build()

    assert "FOR UPDATE" not in stmt.sql
    assert "SELECT" in stmt.sql

    result = adbc_sqlite_session.execute(query)
    assert isinstance(result, SQLResult)
    assert result.get_data()[0]["name"] == "adbc_lock"


@pytest.mark.xdist_group("sqlite")
@pytest.mark.adbc
def test_adbc_for_share_generates_sql(adbc_sqlite_session: AdbcDriver) -> None:
    """SQLite-backed ADBC strips unsupported FOR SHARE while preserving the query."""
    adbc_sqlite_session.execute("INSERT INTO test_table_adbc (name, value) VALUES (?, ?)", ("adbc_share", 200))

    query = sql.select("*").from_("test_table_adbc").where_eq("name", "adbc_share").for_share()
    stmt = query.build()

    assert "FOR SHARE" not in stmt.sql
    assert "SELECT" in stmt.sql

    result = adbc_sqlite_session.execute(query)
    assert isinstance(result, SQLResult)
    assert result.get_data()[0]["name"] == "adbc_share"


@pytest.mark.xdist_group("sqlite")
@pytest.mark.adbc
def test_adbc_for_update_skip_locked_generates_sql(adbc_sqlite_session: AdbcDriver) -> None:
    """SQLite-backed ADBC can compile a SKIP LOCKED builder path without backend locking support."""
    adbc_sqlite_session.execute("INSERT INTO test_table_adbc (name, value) VALUES (?, ?)", ("adbc_skip", 300))

    query = sql.select("*").from_("test_table_adbc").where_eq("name", "adbc_skip").for_update(skip_locked=True)
    stmt = query.build()

    assert stmt.sql is not None

    result = adbc_sqlite_session.execute(query)
    assert isinstance(result, SQLResult)
    assert result.get_data()[0]["name"] == "adbc_skip"
