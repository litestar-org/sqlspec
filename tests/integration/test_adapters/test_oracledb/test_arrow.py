"""Integration tests for OracleDB Arrow query support."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleAsyncConfig


@pytest.fixture
async def oracle_arrow_config(oracle_23ai_service: OracleService) -> AsyncGenerator[OracleAsyncConfig, None]:
    """Create Oracle async config for Arrow testing."""
    config = OracleAsyncConfig(
        pool_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
            "min": 1,
            "max": 5,
        }
    )
    try:
        yield config
    finally:
        await config.close_pool()


async def test_select_to_arrow_basic(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_users (id NUMBER, name VARCHAR2(100), age NUMBER)")
            await session.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30)")
            await session.execute("INSERT INTO arrow_users VALUES (2, 'Bob', 25)")
            await session.commit()

            result = await session.select_to_arrow("SELECT * FROM arrow_users ORDER BY id")

            assert result is not None
            assert isinstance(result.data, (pa.Table, pa.RecordBatch))
            assert result.rows_affected == 2

            df = result.to_pandas()
            assert len(df) == 2
            assert list(df["name"]) == ["Alice", "Bob"]
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_users CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_table_format(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_table_test (id NUMBER, value VARCHAR2(100))")
            await session.execute(
                "INSERT ALL INTO arrow_table_test VALUES (1, 'a') INTO arrow_table_test VALUES (2, 'b') INTO arrow_table_test VALUES (3, 'c') SELECT * FROM dual"
            )
            await session.commit()

            result = await session.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

            assert isinstance(result.data, pa.Table)
            assert result.rows_affected == 3
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_table_test CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_batch_format(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_batch_test (id NUMBER, value VARCHAR2(100))")
            await session.execute(
                "INSERT ALL INTO arrow_batch_test VALUES (1, 'a') INTO arrow_batch_test VALUES (2, 'b') SELECT * FROM dual"
            )
            await session.commit()

            result = await session.select_to_arrow(
                "SELECT * FROM arrow_batch_test ORDER BY id", return_format="batches"
            )

            assert isinstance(result.data, pa.RecordBatch)
            assert result.rows_affected == 2
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_batch_test CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_with_parameters(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test select_to_arrow with query parameters."""
    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_params_test (id NUMBER, value NUMBER)")
            await session.execute(
                "INSERT ALL INTO arrow_params_test VALUES (1, 100) INTO arrow_params_test VALUES (2, 200) INTO arrow_params_test VALUES (3, 300) SELECT * FROM dual"
            )
            await session.commit()

            result = await session.select_to_arrow(
                "SELECT * FROM arrow_params_test WHERE value > :1 ORDER BY id", (150,)
            )

            assert result.rows_affected == 2
            df = result.to_pandas()
            assert list(df["value"]) == [200, 300]
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_params_test CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_empty_result(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test select_to_arrow with empty result set."""
    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_empty_test (id NUMBER)")
            await session.commit()

            result = await session.select_to_arrow("SELECT * FROM arrow_empty_test")

            assert result.rows_affected == 0
            assert len(result.to_pandas()) == 0
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_empty_test CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_null_handling(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test select_to_arrow with NULL values."""
    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_null_test (id NUMBER, value VARCHAR2(100))")
            await session.execute(
                "INSERT ALL INTO arrow_null_test VALUES (1, 'a') INTO arrow_null_test VALUES (2, NULL) INTO arrow_null_test VALUES (3, 'c') SELECT * FROM dual"
            )
            await session.commit()

            result = await session.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 3
            assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_null_test CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_to_polars(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""
    pytest.importorskip("polars")

    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_polars_test (id NUMBER, value VARCHAR2(100))")
            await session.execute(
                "INSERT ALL INTO arrow_polars_test VALUES (1, 'a') INTO arrow_polars_test VALUES (2, 'b') SELECT * FROM dual"
            )
            await session.commit()

            result = await session.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
            df = result.to_polars()

            assert len(df) == 2
            assert df["value"].to_list() == ["a", "b"]
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_polars_test CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_large_dataset(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test select_to_arrow with larger dataset."""
    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_large_test (id NUMBER, value NUMBER)")
            await session.commit()

            # Insert 1000 rows using PL/SQL block
            await session.execute("""
                BEGIN
                    FOR i IN 1..1000 LOOP
                        INSERT INTO arrow_large_test VALUES (i, i * 10);
                    END LOOP;
                    COMMIT;
                END;
            """)

            result = await session.select_to_arrow("SELECT * FROM arrow_large_test ORDER BY id")

            assert result.rows_affected == 1000
            df = result.to_pandas()
            assert len(df) == 1000
            assert df["value"].sum() == sum(i * 10 for i in range(1, 1001))
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_large_test CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_type_preservation(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test that Oracle types are properly converted to Arrow types."""
    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute(
                """
                CREATE TABLE arrow_types_test (
                    id NUMBER,
                    name VARCHAR2(100),
                    price NUMBER(10, 2),
                    created_at TIMESTAMP,
                    is_active NUMBER(1)
                )
                """
            )
            await session.execute(
                """
                INSERT ALL
                INTO arrow_types_test VALUES (1, 'Item 1', 19.99, TIMESTAMP '2025-01-01 10:00:00', 1)
                INTO arrow_types_test VALUES (2, 'Item 2', 29.99, TIMESTAMP '2025-01-02 15:30:00', 0)
                SELECT * FROM dual
                """
            )
            await session.commit()

            result = await session.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 2
            assert df["name"].dtype == object
            assert df["is_active"].dtype in (int, "int64", "Int64", float, "float64")
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_types_test CASCADE CONSTRAINTS PURGE")


async def test_select_to_arrow_clob_handling(oracle_arrow_config: OracleAsyncConfig) -> None:
    """Test Oracle CLOB type handling in Arrow results."""
    try:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("CREATE TABLE arrow_clob_test (id NUMBER, data CLOB)")
            await session.execute(
                """
                INSERT ALL
                INTO arrow_clob_test VALUES (1, 'Large text content for CLOB testing')
                INTO arrow_clob_test VALUES (2, 'Another CLOB text value')
                SELECT * FROM dual
                """
            )
            await session.commit()

            result = await session.select_to_arrow("SELECT * FROM arrow_clob_test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 2
            assert isinstance(df["data"].iloc[0], str)
            assert "CLOB" in df["data"].iloc[0]
    finally:
        async with oracle_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE arrow_clob_test CASCADE CONSTRAINTS PURGE")
