"""Caller-owned pymssql transaction boundaries around statement stacks."""

from typing import Any

import pytest

from sqlspec import StatementStack
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.exceptions import StackExecutionError

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
