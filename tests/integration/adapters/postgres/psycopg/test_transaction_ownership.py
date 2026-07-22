"""Live psycopg transaction ownership and autocommit restoration tests."""

from typing import TYPE_CHECKING

import psycopg
import pytest

from sqlspec import StatementStack
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

pytestmark = pytest.mark.xdist_group("postgres")


def _connection_config(postgres_service: "PostgresService") -> dict[str, object]:
    return {
        "conninfo": (
            f"postgresql://{postgres_service.user}:{postgres_service.password}@"
            f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        ),
        "autocommit": True,
        "min_size": 1,
        "max_size": 1,
    }


def test_sync_transaction_ownership_and_autocommit_restoration(postgres_service: "PostgresService") -> None:
    subject_config = PsycopgSyncConfig(connection_config=_connection_config(postgres_service))
    observer_config = PsycopgSyncConfig(connection_config=_connection_config(postgres_service))

    try:
        subject_config.connection_instance = subject_config.create_pool()
        observer_config.connection_instance = observer_config.create_pool()

        with observer_config.provide_session() as observer:
            observer.execute_script("DROP TABLE IF EXISTS test_psycopg_sync_transaction_648")
            observer.execute_script(
                "CREATE TABLE test_psycopg_sync_transaction_648 (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
            )

        with subject_config.provide_session() as session:
            physical_connection = session.connection
            session.begin()
            session.execute("INSERT INTO test_psycopg_sync_transaction_648 (id, value) VALUES (%s, %s)", 1, "committed")
            session.commit()

            assert session.connection.autocommit is True
            assert session.connection.info.transaction_status is psycopg.pq.TransactionStatus.IDLE
            assert session.select_value("SELECT 1") == 1
            assert session.connection.info.transaction_status is psycopg.pq.TransactionStatus.IDLE

        with subject_config.provide_session() as reacquired:
            assert reacquired.connection is physical_connection
            assert reacquired.connection.autocommit is True

        with observer_config.provide_session() as observer:
            assert observer.select_value("SELECT COUNT(*) FROM test_psycopg_sync_transaction_648 WHERE id = %s", 1) == 1

        with subject_config.provide_session() as session:
            session.begin()
            session.execute(
                "INSERT INTO test_psycopg_sync_transaction_648 (id, value) VALUES (%s, %s)", 2, "rolled-back"
            )
            session.rollback()
            assert session.connection.autocommit is True
            assert session.connection.info.transaction_status is psycopg.pq.TransactionStatus.IDLE

        with observer_config.provide_session() as observer:
            assert observer.select_value("SELECT COUNT(*) FROM test_psycopg_sync_transaction_648 WHERE id = %s", 2) == 0

        with subject_config.provide_session() as session:
            session.begin()
            results = session.execute_stack(
                StatementStack().push_execute(
                    "INSERT INTO test_psycopg_sync_transaction_648 (id, value) VALUES (%s, %s)", (3, "caller-owned")
                )
            )
            assert len(results) == 1

            with observer_config.provide_session() as observer:
                assert (
                    observer.select_value("SELECT COUNT(*) FROM test_psycopg_sync_transaction_648 WHERE id = %s", 3)
                    == 0
                )

            session.commit()

        with observer_config.provide_session() as observer:
            assert observer.select_value("SELECT COUNT(*) FROM test_psycopg_sync_transaction_648 WHERE id = %s", 3) == 1

        with subject_config.provide_session() as session:
            session.driver_features["stack_native_disabled"] = True
            session.begin()
            results = session.execute_stack(
                StatementStack().push_execute(
                    "INSERT INTO test_psycopg_sync_transaction_648 (id, value) VALUES (%s, %s)",
                    (4, "caller-owned-fallback"),
                )
            )
            assert len(results) == 1

            with observer_config.provide_session() as observer:
                assert (
                    observer.select_value("SELECT COUNT(*) FROM test_psycopg_sync_transaction_648 WHERE id = %s", 4)
                    == 0
                )

            session.commit()

        with observer_config.provide_session() as observer:
            assert observer.select_value("SELECT COUNT(*) FROM test_psycopg_sync_transaction_648 WHERE id = %s", 4) == 1
            observer.execute_script("DROP TABLE IF EXISTS test_psycopg_sync_transaction_648")
    finally:
        subject_config.close_pool()
        observer_config.close_pool()


async def test_async_transaction_ownership_and_autocommit_restoration(postgres_service: "PostgresService") -> None:
    subject_config = PsycopgAsyncConfig(connection_config=_connection_config(postgres_service))
    observer_config = PsycopgAsyncConfig(connection_config=_connection_config(postgres_service))

    try:
        subject_config.connection_instance = await subject_config.create_pool()
        observer_config.connection_instance = await observer_config.create_pool()

        async with observer_config.provide_session() as observer:
            await observer.execute_script("DROP TABLE IF EXISTS test_psycopg_async_transaction_648")
            await observer.execute_script(
                "CREATE TABLE test_psycopg_async_transaction_648 (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
            )

        async with subject_config.provide_session() as session:
            physical_connection = session.connection
            await session.begin()
            await session.execute(
                "INSERT INTO test_psycopg_async_transaction_648 (id, value) VALUES (%s, %s)", 1, "committed"
            )
            await session.commit()

            assert session.connection.autocommit is True
            assert session.connection.info.transaction_status is psycopg.pq.TransactionStatus.IDLE
            assert await session.select_value("SELECT 1") == 1
            assert session.connection.info.transaction_status is psycopg.pq.TransactionStatus.IDLE

        async with subject_config.provide_session() as reacquired:
            assert reacquired.connection is physical_connection
            assert reacquired.connection.autocommit is True

        async with observer_config.provide_session() as observer:
            assert (
                await observer.select_value("SELECT COUNT(*) FROM test_psycopg_async_transaction_648 WHERE id = %s", 1)
                == 1
            )

        async with subject_config.provide_session() as session:
            await session.begin()
            await session.execute(
                "INSERT INTO test_psycopg_async_transaction_648 (id, value) VALUES (%s, %s)", 2, "rolled-back"
            )
            await session.rollback()
            assert session.connection.autocommit is True
            assert session.connection.info.transaction_status is psycopg.pq.TransactionStatus.IDLE

        async with observer_config.provide_session() as observer:
            assert (
                await observer.select_value("SELECT COUNT(*) FROM test_psycopg_async_transaction_648 WHERE id = %s", 2)
                == 0
            )

        async with subject_config.provide_session() as session:
            await session.begin()
            results = await session.execute_stack(
                StatementStack().push_execute(
                    "INSERT INTO test_psycopg_async_transaction_648 (id, value) VALUES (%s, %s)", (3, "caller-owned")
                )
            )
            assert len(results) == 1

            async with observer_config.provide_session() as observer:
                assert (
                    await observer.select_value(
                        "SELECT COUNT(*) FROM test_psycopg_async_transaction_648 WHERE id = %s", 3
                    )
                    == 0
                )

            await session.commit()

        async with observer_config.provide_session() as observer:
            assert (
                await observer.select_value("SELECT COUNT(*) FROM test_psycopg_async_transaction_648 WHERE id = %s", 3)
                == 1
            )

        async with subject_config.provide_session() as session:
            session.driver_features["stack_native_disabled"] = True
            await session.begin()
            results = await session.execute_stack(
                StatementStack().push_execute(
                    "INSERT INTO test_psycopg_async_transaction_648 (id, value) VALUES (%s, %s)",
                    (4, "caller-owned-fallback"),
                )
            )
            assert len(results) == 1

            async with observer_config.provide_session() as observer:
                assert (
                    await observer.select_value(
                        "SELECT COUNT(*) FROM test_psycopg_async_transaction_648 WHERE id = %s", 4
                    )
                    == 0
                )

            await session.commit()

        async with observer_config.provide_session() as observer:
            assert (
                await observer.select_value("SELECT COUNT(*) FROM test_psycopg_async_transaction_648 WHERE id = %s", 4)
                == 1
            )
            await observer.execute_script("DROP TABLE IF EXISTS test_psycopg_async_transaction_648")
    finally:
        await subject_config.close_pool()
        await observer_config.close_pool()
