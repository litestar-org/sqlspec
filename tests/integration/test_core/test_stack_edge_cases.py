"""Cross-adapter StatementStack edge cases exercised against SQLite."""

from collections.abc import Generator

import pytest

from sqlspec import StatementStack
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.exceptions import StackExecutionError
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.fixture()
def sqlite_stack_session() -> "Generator[SqliteDriver, None, None]":
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as session:
        session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS stack_edge_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                notes TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            DELETE FROM stack_edge_table;
            """
        )
        session.commit()
        yield session
    config.close_pool()


def _table_count(session: "SqliteDriver") -> int:
    result = session.execute("SELECT COUNT(*) AS total FROM stack_edge_table")
    assert result.data is not None
    return int(result.data[0]["total"])


def test_execute_stack_requires_operations(sqlite_stack_session: "SqliteDriver") -> None:
    with pytest.raises(ValueError, match="Cannot execute an empty StatementStack"):
        sqlite_stack_session.execute_stack(StatementStack())


def test_single_operation_stack_matches_execute(sqlite_stack_session: "SqliteDriver") -> None:
    stack = StatementStack().push_execute(
        "INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (1, "solo", None)
    )

    results = sqlite_stack_session.execute_stack(stack)

    assert len(results) == 1
    assert results[0].rows_affected == 1
    assert _table_count(sqlite_stack_session) == 1


def test_stack_with_only_select_operations(sqlite_stack_session: "SqliteDriver") -> None:
    sqlite_stack_session.execute(
        "INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (1, "alpha", "note")
    )
    sqlite_stack_session.execute("INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (2, "beta", "note"))

    stack = (
        StatementStack()
        .push_execute("SELECT name FROM stack_edge_table WHERE id = ?", (1,))
        .push_execute("SELECT COUNT(*) AS total FROM stack_edge_table", ())
    )

    results = sqlite_stack_session.execute_stack(stack)

    first_result = results[0].result
    second_result = results[1].result
    assert first_result is not None
    assert second_result is not None
    assert first_result.data is not None
    assert second_result.data is not None
    assert first_result.data[0]["name"] == "alpha"
    assert second_result.data[0]["total"] == 2


def test_large_stack_of_mixed_operations(sqlite_stack_session: "SqliteDriver") -> None:
    stack = StatementStack()
    for idx in range(1, 51):
        stack = stack.push_execute(
            "INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (idx, f"user-{idx}", None)
        )
    stack = stack.push_execute("SELECT COUNT(*) AS total FROM stack_edge_table", ())

    results = sqlite_stack_session.execute_stack(stack)

    assert len(results) == 51
    final_result = results[-1].result
    assert final_result is not None
    assert final_result.data is not None
    assert final_result.data[0]["total"] == 50


def test_fail_fast_rolls_back_new_transaction(sqlite_stack_session: "SqliteDriver") -> None:
    stack = (
        StatementStack()
        .push_execute("INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (1, "first", None))
        .push_execute("INSERT INTO missing_table VALUES (1)")
    )

    with pytest.raises(StackExecutionError):
        sqlite_stack_session.execute_stack(stack)

    assert _table_count(sqlite_stack_session) == 0


@requires_interpreted
def test_continue_on_error_commits_successes(sqlite_stack_session: "SqliteDriver") -> None:
    stack = (
        StatementStack()
        .push_execute("INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (1, "ok", None))
        .push_execute("INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (1, "duplicate", None))
        .push_execute("INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (2, "ok", None))
    )

    results = sqlite_stack_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[1].error is not None
    assert _table_count(sqlite_stack_session) == 2


def test_parameter_edge_cases(sqlite_stack_session: "SqliteDriver") -> None:
    stack = (
        StatementStack()
        .push_execute("INSERT INTO stack_edge_table (id, name, notes) VALUES (?, ?, ?)", (1, "nullable", None))
        .push_execute(
            "INSERT INTO stack_edge_table (id, name, notes) VALUES (:id, :name, :notes)",
            {"id": 2, "name": "dict", "notes": ""},
        )
        .push_execute("SELECT notes FROM stack_edge_table WHERE id = ?", (1,))
    )

    results = sqlite_stack_session.execute_stack(stack)
    third_result = results[2].result
    assert third_result is not None
    assert third_result.data is not None
    assert third_result.data[0]["notes"] is None


def test_stack_with_existing_transaction(sqlite_stack_session: "SqliteDriver") -> None:
    sqlite_stack_session.begin()
    stack = (
        StatementStack()
        .push_execute("INSERT INTO stack_edge_table (id, name) VALUES (?, ?)", (1, "tx"))
        .push_execute("INSERT INTO stack_edge_table (id, name) VALUES (?, ?)", (2, "tx"))
    )

    sqlite_stack_session.execute_stack(stack)
    assert sqlite_stack_session.connection.in_transaction is True

    sqlite_stack_session.rollback()
    assert _table_count(sqlite_stack_session) == 0


def test_stack_creates_transaction_when_needed(sqlite_stack_session: "SqliteDriver") -> None:
    stack = (
        StatementStack()
        .push_execute("INSERT INTO stack_edge_table (id, name) VALUES (?, ?)", (1, "auto"))
        .push_execute("INSERT INTO stack_edge_table (id, name) VALUES (?, ?)", (2, "auto"))
    )

    sqlite_stack_session.execute_stack(stack)
    assert sqlite_stack_session.connection.in_transaction is False
    assert _table_count(sqlite_stack_session) == 2


def test_stack_single_statement_selects_inside_existing_transaction(sqlite_stack_session: "SqliteDriver") -> None:
    sqlite_stack_session.begin()
    sqlite_stack_session.execute("INSERT INTO stack_edge_table (id, name) VALUES (?, ?)", (1, "pre"))

    stack = StatementStack().push_execute("SELECT name FROM stack_edge_table WHERE id = ?", (1,))

    results = sqlite_stack_session.execute_stack(stack)
    select_result = results[0].result
    assert select_result is not None
    assert select_result.data is not None
    assert select_result.data[0]["name"] == "pre"

    sqlite_stack_session.rollback()
