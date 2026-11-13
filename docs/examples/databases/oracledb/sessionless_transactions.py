"""Suspend and resume a ledger transaction across two Oracle pool checkouts."""

# --8<-- [start:example]
import asyncio
import os

from sqlspec.adapters.oracledb import OracleAsyncConfig, OraclePoolParams


async def run_example() -> bytes:
    """Create a ledger table and commit one transaction across two checkouts.

    Returns:
        The identifier of the transaction committed on the second checkout.
    """
    config = OracleAsyncConfig(
        connection_config=OraclePoolParams(
            dsn=os.getenv("ORACLE_DSN", "localhost:1521/FREEPDB1"),
            user=os.getenv("ORACLE_USER", "app"),
            password=os.getenv("ORACLE_PASSWORD", "oracle"),
            min=1,
            max=2,
        )
    )
    created = False
    try:
        async with config.provide_session() as step_one:
            await step_one.execute(
                "CREATE TABLE sqlspec_checkout_ledger_example (id NUMBER PRIMARY KEY, status VARCHAR2(20))"
            )
            created = True
            async with step_one.sessionless_transaction(timeout=60) as transaction_id:
                await step_one.execute(
                    "INSERT INTO sqlspec_checkout_ledger_example (id, status) VALUES (:id, :status)",
                    {"id": 1, "status": "PENDING"},
                )
        async with config.provide_session() as step_two:
            async with step_two.sessionless_transaction(transaction_id, resume=True, timeout=60):
                step_two.set_transaction_priority("HIGH")
                await step_two.execute(
                    "UPDATE sqlspec_checkout_ledger_example SET status = :status WHERE id = :id",
                    {"id": 1, "status": "CONFIRMED"},
                )
                await step_two.commit()
            status = await step_two.select_value(
                "SELECT status FROM sqlspec_checkout_ledger_example WHERE id = :id", {"id": 1}
            )
            print(f"Committed ledger status: {status}")
        return transaction_id
    finally:
        try:
            if created:
                async with config.provide_session() as cleanup:
                    await cleanup.execute("DROP TABLE sqlspec_checkout_ledger_example PURGE")
        finally:
            await config.close_pool()


# --8<-- [end:example]

__all__ = ("main", "run_example")


def main() -> None:
    """Execute the Oracle sessionless transaction example."""
    if not os.getenv("ORACLE_DSN"):
        print("Set ORACLE_DSN, ORACLE_USER, and ORACLE_PASSWORD to run this example.")
        return
    asyncio.run(run_example())


if __name__ == "__main__":
    main()
