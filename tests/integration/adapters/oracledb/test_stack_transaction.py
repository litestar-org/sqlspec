"""Oracle native pipeline stack transaction durability.

Verifies that continue-on-error stack execution commits the operations that
succeeded, so partial results survive after the connection is released.
"""

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver
from sqlspec.core import StatementStack

pytestmark = pytest.mark.xdist_group("oracle")

_TABLE = "test_stack_txn_oracledb_async"


async def _drop_table(driver: OracleAsyncDriver) -> None:
    await driver.execute_script(
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {_TABLE}';"
        " EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_native_stack_continue_on_error_commits_successes(oracle_async_config: OracleAsyncConfig) -> None:
    """Successful operations in a continue-on-error stack must persist across sessions."""

    async with oracle_async_config.provide_session() as setup:
        if not await setup._pipeline_native_supported():
            pytest.skip("Oracle native pipeline path unavailable (thin async mode required)")
        await _drop_table(setup)
        await setup.execute_script(f"CREATE TABLE {_TABLE} (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
        await setup.commit()

    try:
        async with oracle_async_config.provide_session() as driver:
            stack = (
                StatementStack()
                .push_execute(f"INSERT INTO {_TABLE} (id, name) VALUES (:1, :2)", (1, "first"))
                .push_execute(f"INSERT INTO {_TABLE} (id, name) VALUES (:1, :2)", (1, "duplicate"))
                .push_execute(f"INSERT INTO {_TABLE} (id, name) VALUES (:1, :2)", (2, "third"))
            )

            results = await driver.execute_stack(stack, continue_on_error=True)

            assert len(results) == 3
            assert results[0].error is None
            assert results[1].error is not None
            assert results[2].error is None

        async with oracle_async_config.provide_session() as verifier:
            verify = await verifier.execute(f"SELECT id FROM {_TABLE} ORDER BY id")
            assert verify.data is not None
            assert [row["id"] for row in verify.get_data()] == [1, 2]
    finally:
        async with oracle_async_config.provide_session() as cleanup:
            await _drop_table(cleanup)
            await cleanup.commit()
