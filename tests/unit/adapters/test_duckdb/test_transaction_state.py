"""DuckDB transaction-honesty: honest _connection_in_transaction and no double-BEGIN."""

from collections.abc import Generator

import pytest

from sqlspec import StatementStack
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.service import SQLSpecSyncService

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture()
def duckdb_stack_session() -> "Generator[DuckDBDriver, None, None]":
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as session:
        session.execute_script(
            "CREATE TABLE IF NOT EXISTS stack_txn_table (id INTEGER PRIMARY KEY); DELETE FROM stack_txn_table;"
        )
        session.commit()
        yield session
    config.close_pool()


def _table_count(session: "DuckDBDriver") -> int:
    result = session.execute("SELECT COUNT(*) AS total FROM stack_txn_table")
    assert result.data is not None
    return int(result.get_data()[0]["total"])


def test_connection_in_transaction_tracks_begin_commit_rollback(duckdb_stack_session: "DuckDBDriver") -> None:
    """_connection_in_transaction should reflect the real begin/commit/rollback state."""
    session = duckdb_stack_session
    assert session._connection_in_transaction() is False

    session.begin()
    assert session._connection_in_transaction() is True
    session.commit()
    assert session._connection_in_transaction() is False

    session.begin()
    assert session._connection_in_transaction() is True
    session.rollback()
    assert session._connection_in_transaction() is False


def test_execute_stack_inside_user_transaction_does_not_double_begin(duckdb_stack_session: "DuckDBDriver") -> None:
    """A stack run inside a user transaction must not re-BEGIN or commit the outer transaction."""
    session = duckdb_stack_session
    session.begin()

    stack = StatementStack().push_execute("INSERT INTO stack_txn_table (id) VALUES (1)")
    results = session.execute_stack(stack)

    assert len(results) == 1
    assert session._connection_in_transaction() is True

    session.rollback()
    assert _table_count(session) == 0


@pytest.mark.parametrize("method", ["create_savepoint", "release_savepoint", "rollback_to_savepoint"])
def test_savepoints_are_reported_unsupported(duckdb_stack_session: "DuckDBDriver", method: str) -> None:
    """DuckDB rejects SAVEPOINT, so savepoint helpers raise NotImplementedError before sending SQL."""
    duckdb_stack_session.begin()
    with pytest.raises(NotImplementedError, match="DuckDB"):
        getattr(duckdb_stack_session, method)("sqlspec_sp_1")
    assert duckdb_stack_session._connection_in_transaction() is True
    duckdb_stack_session.rollback()


def test_nested_service_transaction_reports_missing_savepoints(duckdb_stack_session: "DuckDBDriver") -> None:
    """A nested begin_transaction() block on DuckDB fails clearly and leaves the outer block usable."""
    service = SQLSpecSyncService(duckdb_stack_session)
    with service.begin_transaction() as session:
        with pytest.raises(ImproperConfigurationError, match="savepoints"):
            with service.begin_transaction():
                pytest.fail("nested block entered")
        session.execute("INSERT INTO stack_txn_table (id) VALUES (1)")
    assert _table_count(duckdb_stack_session) == 1
