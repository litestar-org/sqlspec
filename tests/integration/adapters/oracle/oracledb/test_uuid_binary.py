"""Oracle UUID binary residuals not covered by the shared UUID contract.

The contract suite verifies RAW(16) UUID round-trips with the feature enabled
and disabled. This module keeps Oracle-only negative/coexistence behavior:
RAW(32) stays bytes, VARCHAR2 UUID text stays text, and UUID handlers compose
with NumPy vector handlers.
"""

import uuid

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig
from sqlspec.typing import NUMPY_INSTALLED

pytestmark = [pytest.mark.xdist_group("oracle")]


@pytest.fixture
def oracle_uuid_async_config(oracle_async_config: OracleAsyncConfig) -> OracleAsyncConfig:
    """Create Oracle async config with UUID binary enabled (used by the RAW(32)/VARCHAR2 scope residuals)."""
    return OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, driver_features={"enable_uuid_binary": True}
    )


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")
async def test_uuid_numpy_coexistence(oracle_async_config: OracleAsyncConfig) -> None:
    """Test UUID and NumPy handlers work together via chaining."""
    import numpy as np

    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config,
        driver_features={"enable_numpy_vectors": True, "enable_uuid_binary": True},
    )

    async with config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_mixed_oracledb_async';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_mixed_oracledb_async (
                id NUMBER PRIMARY KEY,
                uuid_col RAW(16) NOT NULL,
                vector_col VECTOR(128, FLOAT32)
            )
        """)

        test_uuid = uuid.uuid4()
        rng = np.random.default_rng(42)
        test_vector = rng.random(128).astype(np.float32)

        await session.execute("INSERT INTO test_mixed_oracledb_async VALUES (:1, :2, :3)", (1, test_uuid, test_vector))

        result = await session.select_one("SELECT * FROM test_mixed_oracledb_async WHERE id = :1", (1,))

        assert result is not None
        assert isinstance(result["uuid_col"], uuid.UUID)
        assert result["uuid_col"] == test_uuid
        assert isinstance(result["vector_col"], np.ndarray)
        np.testing.assert_array_almost_equal(result["vector_col"], test_vector, decimal=5)


async def test_raw32_untouched(oracle_uuid_async_config: OracleAsyncConfig) -> None:
    """Test RAW(32) columns remain as bytes (not converted to UUID)."""
    async with oracle_uuid_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_raw32_oracledb_async';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_raw32_oracledb_async (
                id NUMBER PRIMARY KEY,
                binary_col RAW(32) NOT NULL
            )
        """)

        test_bytes = b"12345678901234567890123456789012"
        await session.execute("INSERT INTO test_raw32_oracledb_async VALUES (:1, :2)", (1, test_bytes))

        result = await session.select_one("SELECT * FROM test_raw32_oracledb_async WHERE id = :1", (1,))

        assert result is not None
        retrieved_value = result["binary_col"]

        assert isinstance(retrieved_value, bytes)
        assert retrieved_value == test_bytes


async def test_varchar_uuid_untouched(oracle_uuid_async_config: OracleAsyncConfig) -> None:
    """Test VARCHAR2 UUID columns remain as strings (not converted to UUID)."""
    async with oracle_uuid_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_varchar_uuid_oracledb_async';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_varchar_uuid_oracledb_async (
                id NUMBER PRIMARY KEY,
                uuid_str VARCHAR2(36) NOT NULL
            )
        """)

        test_uuid = uuid.uuid4()
        uuid_str = str(test_uuid)
        await session.execute("INSERT INTO test_varchar_uuid_oracledb_async VALUES (:1, :2)", (1, uuid_str))

        result = await session.select_one("SELECT * FROM test_varchar_uuid_oracledb_async WHERE id = :1", (1,))

        assert result is not None
        retrieved_value = result["uuid_str"]

        assert isinstance(retrieved_value, str)
        assert retrieved_value == uuid_str
