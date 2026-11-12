"""Caller-owned pymssql transaction boundaries around statement stacks."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec import StatementStack
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.exceptions import SQLParsingError, StackExecutionError
from tests.integration.adapters.mssql._migration_failure import assert_failed_migration_leaves_no_partial_work

pytestmark = [pytest.mark.pymssql, pytest.mark.xdist_group("mssql")]


@pytest.mark.parametrize("fails", [False, True])
def test_execute_stack_preserves_caller_transaction(pymssql_connection_config: dict[str, Any], fails: bool) -> None:
    config = PymssqlConfig(connection_config=pymssql_connection_config)
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS sqlspec_pymssql_stack_txn")
            driver.execute_script("CREATE TABLE sqlspec_pymssql_stack_txn (id INT PRIMARY KEY)")
            driver.commit()
            try:
                driver.begin()
                driver.execute("INSERT INTO sqlspec_pymssql_stack_txn (id) VALUES (?)", (1,))
                transaction_count = driver.select_value("SELECT @@TRANCOUNT")
                stack = StatementStack().push_execute("INSERT INTO sqlspec_pymssql_stack_txn (id) VALUES (?)", (2,))
                if fails:
                    stack = stack.push_execute("INSERT INTO sqlspec_pymssql_stack_txn (id) VALUES (?)", (2,))
                    with pytest.raises(StackExecutionError):
                        driver.execute_stack(stack)
                else:
                    results = driver.execute_stack(stack)
                    assert len(results) == 1
                    assert results[0].rows_affected == 1
                assert driver.select_value("SELECT @@TRANCOUNT") == transaction_count
                assert driver.select_value("SELECT COUNT(*) FROM sqlspec_pymssql_stack_txn") == 2
                driver.rollback()
                assert driver.select_value("SELECT COUNT(*) FROM sqlspec_pymssql_stack_txn") == 0
            finally:
                driver.rollback()
                driver.execute_script("DROP TABLE IF EXISTS sqlspec_pymssql_stack_txn")
                driver.commit()
    finally:
        config.close_pool()


@pytest.mark.parametrize("autocommit", [None, True], ids=["default", "autocommit"])
def test_pymssql_failed_migration_leaves_no_partial_work(
    tmp_path: Path, mssql_migration_connection_config: dict[str, Any], autocommit: bool | None
) -> None:
    """A failed transactional migration persists no work and restores the user's default schema."""
    connection_config = dict(mssql_migration_connection_config)
    if autocommit is not None:
        connection_config["autocommit"] = autocommit
    assert_failed_migration_leaves_no_partial_work(
        tmp_path, PymssqlConfig, connection_config, SQLParsingError, "Invalid object name"
    )


@pytest.mark.parametrize("autocommit", [False, True])
def test_pymssql_tenant_policy_and_transaction_ownership(
    pymssql_connection_config: dict[str, Any], autocommit: bool
) -> None:
    from tests.integration.adapters.mssql._tenant import assert_native_tenant_acceptance

    assert_native_tenant_acceptance("pymssql", pymssql_connection_config, autocommit)
