"""Integration tests for Oracle NumPy vector support with real database."""

import numpy as np
import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver, OracleSyncConfig
from sqlspec.typing import NUMPY_INSTALLED

rng = np.random.default_rng(42)

pytestmark = [pytest.mark.xdist_group("oracle"), pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")]


@pytest.fixture
def oracle_numpy_sync_config(oracle_sync_config: OracleSyncConfig) -> OracleSyncConfig:
    """Create Oracle sync config with NumPy vectors enabled."""
    return OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config, driver_features={"enable_numpy_vectors": True}
    )


@pytest.fixture
def oracle_numpy_async_config(oracle_async_config: OracleAsyncConfig) -> OracleAsyncConfig:
    """Create Oracle async config with NumPy vectors enabled."""
    return OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, driver_features={"enable_numpy_vectors": True}
    )


async def test_create_vector_table(oracle_async_session: OracleAsyncDriver) -> None:
    """Test creating table with VECTOR columns."""
    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_vectors';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN RAISE; END IF;
        END;
    """)

    await oracle_async_session.execute("""
        CREATE TABLE test_vectors (
            id NUMBER PRIMARY KEY,
            description VARCHAR2(1000),
            embedding_f32 VECTOR(128, FLOAT32),
            embedding_f64 VECTOR(128, FLOAT64),
            embedding_i8 VECTOR(128, INT8),
            embedding_bin VECTOR(128, BINARY)
        )
    """)

    result = await oracle_async_session.select_value(
        "SELECT COUNT(*) FROM user_tab_columns WHERE table_name = 'TEST_VECTORS'"
    )
    assert result == 6


async def test_numpy_float32_insert_and_select(oracle_numpy_async_config: OracleAsyncConfig) -> None:
    """Test NumPy float32 array INSERT and SELECT round-trip."""
    import numpy as np

    async with oracle_numpy_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_float32';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_float32 (
                id NUMBER PRIMARY KEY,
                vector_data VECTOR(128, FLOAT32)
            )
        """)

        original_vector = rng.random(128).astype(np.float32)

        await session.execute("INSERT INTO test_float32 VALUES (:1, :2)", (1, original_vector))

        result = await session.select_one("SELECT * FROM test_float32 WHERE id = :1", (1,))

        assert result is not None
        retrieved_vector = result["vector_data"]

        assert isinstance(retrieved_vector, np.ndarray)
        assert retrieved_vector.dtype == np.float32
        assert retrieved_vector.shape == (128,)
        np.testing.assert_array_almost_equal(retrieved_vector, original_vector, decimal=5)


async def test_numpy_float64_insert_and_select(oracle_numpy_async_config: OracleAsyncConfig) -> None:
    """Test NumPy float64 array INSERT and SELECT round-trip."""
    import numpy as np

    async with oracle_numpy_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_float64';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_float64 (
                id NUMBER PRIMARY KEY,
                vector_data VECTOR(64, FLOAT64)
            )
        """)

        original_vector = rng.random(64).astype(np.float64)

        await session.execute("INSERT INTO test_float64 VALUES (:1, :2)", (1, original_vector))

        result = await session.select_one("SELECT * FROM test_float64 WHERE id = :1", (1,))

        assert result is not None
        retrieved_vector = result["vector_data"]

        assert isinstance(retrieved_vector, np.ndarray)
        assert retrieved_vector.dtype == np.float64
        assert retrieved_vector.shape == (64,)
        np.testing.assert_array_almost_equal(retrieved_vector, original_vector)


async def test_numpy_uint8_binary_vector(oracle_numpy_async_config: OracleAsyncConfig) -> None:
    """Test NumPy uint8 array for BINARY vector type.

    Note: VECTOR(256, BINARY) stores 256 bits = 32 bytes.
    So we need 32 uint8 values to represent 256 bits.
    """
    import numpy as np

    async with oracle_numpy_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_binary';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_binary (
                id NUMBER PRIMARY KEY,
                vector_data VECTOR(256, BINARY)
            )
        """)

        original_vector = rng.integers(0, 256, size=32, dtype=np.uint8)

        await session.execute("INSERT INTO test_binary VALUES (:1, :2)", (1, original_vector))

        result = await session.select_one("SELECT * FROM test_binary WHERE id = :1", (1,))

        assert result is not None
        retrieved_vector = result["vector_data"]

        assert isinstance(retrieved_vector, np.ndarray)
        assert retrieved_vector.dtype == np.uint8
        assert retrieved_vector.shape == (32,)
        np.testing.assert_array_equal(retrieved_vector, original_vector)


async def test_numpy_int8_vector(oracle_numpy_async_config: OracleAsyncConfig) -> None:
    """Test NumPy int8 array for INT8 vector type."""
    import numpy as np

    async with oracle_numpy_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_int8';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_int8 (
                id NUMBER PRIMARY KEY,
                vector_data VECTOR(32, INT8)
            )
        """)

        original_vector = rng.integers(-128, 127, size=32, dtype=np.int8)

        await session.execute("INSERT INTO test_int8 VALUES (:1, :2)", (1, original_vector))

        result = await session.select_one("SELECT * FROM test_int8 WHERE id = :1", (1,))

        assert result is not None
        retrieved_vector = result["vector_data"]

        assert isinstance(retrieved_vector, np.ndarray)
        assert retrieved_vector.dtype == np.int8
        assert retrieved_vector.shape == (32,)
        np.testing.assert_array_equal(retrieved_vector, original_vector)


async def test_large_embedding_vector(oracle_numpy_async_config: OracleAsyncConfig) -> None:
    """Test large embedding vectors (1536 dimensions like OpenAI)."""
    import numpy as np

    async with oracle_numpy_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_embeddings';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_embeddings (
                id NUMBER PRIMARY KEY,
                text VARCHAR2(4000),
                embedding VECTOR(1536, FLOAT32)
            )
        """)

        original_vector = rng.random(1536).astype(np.float32)

        await session.execute(
            "INSERT INTO test_embeddings VALUES (:1, :2, :3)", (1, "sample text for embedding", original_vector)
        )

        result = await session.select_one("SELECT * FROM test_embeddings WHERE id = :1", (1,))

        assert result is not None
        retrieved_vector = result["embedding"]

        assert isinstance(retrieved_vector, np.ndarray)
        assert retrieved_vector.shape == (1536,)
        np.testing.assert_array_almost_equal(retrieved_vector, original_vector, decimal=5)


async def test_vector_null_handling(oracle_numpy_async_config: OracleAsyncConfig) -> None:
    """Test NULL handling for VECTOR columns."""
    async with oracle_numpy_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_nulls';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_nulls (
                id NUMBER PRIMARY KEY,
                vector_data VECTOR(64, FLOAT32)
            )
        """)

        await session.execute("INSERT INTO test_nulls VALUES (:1, NULL)", (1,))

        result = await session.select_one("SELECT * FROM test_nulls WHERE id = :1", (1,))

        assert result is not None
        assert result["vector_data"] is None


async def test_numpy_disabled_by_default(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that NumPy conversion can be explicitly disabled."""
    import array

    import numpy as np

    config_no_numpy = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, driver_features={"enable_numpy_vectors": False}
    )

    async with config_no_numpy.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_no_numpy';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_no_numpy (
                id NUMBER PRIMARY KEY,
                vector_data VECTOR(8, FLOAT32)
            )
        """)

        manual_array = array.array("f", [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])

        await session.execute("INSERT INTO test_no_numpy VALUES (:1, :2)", (1, manual_array))

        result = await session.select_one("SELECT * FROM test_no_numpy WHERE id = :1", (1,))

        assert result is not None
        retrieved = result["vector_data"]

        assert isinstance(retrieved, array.array)
        assert not isinstance(retrieved, np.ndarray)


def test_sync_numpy_vector_operations(oracle_numpy_sync_config: OracleSyncConfig) -> None:
    """Test synchronous NumPy vector operations."""
    import numpy as np

    with oracle_numpy_sync_config.provide_session() as session:
        session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_sync_vectors';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        session.execute("""
            CREATE TABLE test_sync_vectors (
                id NUMBER PRIMARY KEY,
                vector_data VECTOR(16, FLOAT32)
            )
        """)

        original_vector = np.array([float(i) for i in range(16)], dtype=np.float32)

        session.execute("INSERT INTO test_sync_vectors VALUES (:1, :2)", (1, original_vector))

        result = session.select_one("SELECT * FROM test_sync_vectors WHERE id = :1", (1,))

        assert result is not None
        retrieved_vector = result["vector_data"]

        assert isinstance(retrieved_vector, np.ndarray)
        np.testing.assert_array_almost_equal(retrieved_vector, original_vector)


async def test_batch_insert_numpy_vectors(oracle_numpy_async_config: OracleAsyncConfig) -> None:
    """Test batch inserting multiple NumPy vectors."""
    import numpy as np

    async with oracle_numpy_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_batch';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_batch (
                id NUMBER PRIMARY KEY,
                vector_data VECTOR(32, FLOAT32)
            )
        """)

        vectors = [rng.random(32).astype(np.float32) for _ in range(5)]

        for idx, vec in enumerate(vectors):
            await session.execute("INSERT INTO test_batch VALUES (:1, :2)", (idx + 1, vec))

        results = await session.select("SELECT * FROM test_batch ORDER BY id")

        assert len(results) == 5

        for idx, result in enumerate(results):
            retrieved_vector = result["vector_data"]
            assert isinstance(retrieved_vector, np.ndarray)
            np.testing.assert_array_almost_equal(retrieved_vector, vectors[idx], decimal=5)
