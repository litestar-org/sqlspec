"""Integration tests for Oracle UUID binary (RAW16) support with real database."""

import uuid

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver, OracleSyncConfig
from sqlspec.typing import NUMPY_INSTALLED

pytestmark = [pytest.mark.xdist_group("oracle")]


@pytest.fixture
def oracle_uuid_sync_config(oracle_sync_config: OracleSyncConfig) -> OracleSyncConfig:
    """Create Oracle sync config with UUID binary enabled."""
    return OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config, driver_features={"enable_uuid_binary": True}
    )


@pytest.fixture
def oracle_uuid_async_config(oracle_async_config: OracleAsyncConfig) -> OracleAsyncConfig:
    """Create Oracle async config with UUID binary enabled."""
    return OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, driver_features={"enable_uuid_binary": True}
    )


@pytest.fixture
def oracle_uuid_disabled_async_config(oracle_async_config: OracleAsyncConfig) -> OracleAsyncConfig:
    """Create Oracle async config with UUID binary explicitly disabled."""
    return OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, driver_features={"enable_uuid_binary": False}
    )


async def test_create_uuid_table(oracle_async_session: OracleAsyncDriver) -> None:
    """Test creating table with RAW(16) UUID columns."""
    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_uuid_binary';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN RAISE; END IF;
        END;
    """)

    await oracle_async_session.execute("""
        CREATE TABLE test_uuid_binary (
            id NUMBER PRIMARY KEY,
            user_id RAW(16) NOT NULL,
            session_id RAW(16),
            description VARCHAR2(1000)
        )
    """)

    result = await oracle_async_session.select_value(
        "SELECT COUNT(*) FROM user_tab_columns WHERE table_name = 'TEST_UUID_BINARY'"
    )
    assert result == 4


async def test_uuid_roundtrip_async(oracle_uuid_async_config: OracleAsyncConfig) -> None:
    """Test UUID INSERT and SELECT round-trip (async)."""
    async with oracle_uuid_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_uuid_async';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_uuid_async (
                id NUMBER PRIMARY KEY,
                uuid_col RAW(16) NOT NULL
            )
        """)

        test_uuid = uuid.uuid4()
        await session.execute("INSERT INTO test_uuid_async VALUES (:1, :2)", (1, test_uuid))

        result = await session.select_one("SELECT * FROM test_uuid_async WHERE id = :1", (1,))

        assert result is not None
        retrieved_uuid = result["uuid_col"]

        assert isinstance(retrieved_uuid, uuid.UUID)
        assert retrieved_uuid == test_uuid


def test_uuid_roundtrip_sync(oracle_uuid_sync_config: OracleSyncConfig) -> None:
    """Test UUID INSERT and SELECT round-trip (sync)."""
    with oracle_uuid_sync_config.provide_session() as session:
        session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_uuid_sync';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        session.execute("""
            CREATE TABLE test_uuid_sync (
                id NUMBER PRIMARY KEY,
                uuid_col RAW(16) NOT NULL
            )
        """)

        test_uuid = uuid.uuid4()
        session.execute("INSERT INTO test_uuid_sync VALUES (:1, :2)", (1, test_uuid))

        result = session.select_one("SELECT * FROM test_uuid_sync WHERE id = :1", (1,))

        assert result is not None
        retrieved_uuid = result["uuid_col"]

        assert isinstance(retrieved_uuid, uuid.UUID)
        assert retrieved_uuid == test_uuid


async def test_uuid_null_handling(oracle_uuid_async_config: OracleAsyncConfig) -> None:
    """Test NULL UUID values handled correctly."""
    async with oracle_uuid_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_uuid_null';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_uuid_null (
                id NUMBER PRIMARY KEY,
                uuid_col RAW(16)
            )
        """)

        await session.execute("INSERT INTO test_uuid_null VALUES (:1, :2)", (1, None))

        result = await session.select_one("SELECT * FROM test_uuid_null WHERE id = :1", (1,))

        assert result is not None
        assert result["uuid_col"] is None


async def test_uuid_variants(oracle_uuid_async_config: OracleAsyncConfig) -> None:
    """Test UUID variants (v1, v4, v5) all work correctly."""
    async with oracle_uuid_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_uuid_variants';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_uuid_variants (
                id NUMBER PRIMARY KEY,
                uuid_col RAW(16) NOT NULL
            )
        """)

        test_uuids = [(1, uuid.uuid1()), (2, uuid.uuid4()), (3, uuid.uuid5(uuid.NAMESPACE_DNS, "example.com"))]

        for row_id, test_uuid in test_uuids:
            await session.execute("INSERT INTO test_uuid_variants VALUES (:1, :2)", (row_id, test_uuid))

        results = await session.select("SELECT * FROM test_uuid_variants ORDER BY id")

        assert len(results) == 3
        for result, (row_id, original_uuid) in zip(results, test_uuids):
            assert result["id"] == row_id
            assert isinstance(result["uuid_col"], uuid.UUID)
            assert result["uuid_col"] == original_uuid


async def test_uuid_executemany(oracle_uuid_async_config: OracleAsyncConfig) -> None:
    """Test bulk operations with UUID parameters (executemany)."""
    async with oracle_uuid_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_uuid_bulk';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_uuid_bulk (
                id NUMBER PRIMARY KEY,
                uuid_col RAW(16) NOT NULL
            )
        """)

        test_data = [(i, uuid.uuid4()) for i in range(1, 101)]

        await session.execute_many("INSERT INTO test_uuid_bulk VALUES (:1, :2)", test_data)

        count = await session.select_value("SELECT COUNT(*) FROM test_uuid_bulk")
        assert count == 100

        results = await session.select("SELECT * FROM test_uuid_bulk ORDER BY id")
        assert len(results) == 100

        for result, (row_id, original_uuid) in zip(results, test_data):
            assert result["id"] == row_id
            assert isinstance(result["uuid_col"], uuid.UUID)
            assert result["uuid_col"] == original_uuid


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
                EXECUTE IMMEDIATE 'DROP TABLE test_mixed';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_mixed (
                id NUMBER PRIMARY KEY,
                uuid_col RAW(16) NOT NULL,
                vector_col VECTOR(128, FLOAT32)
            )
        """)

        test_uuid = uuid.uuid4()
        rng = np.random.default_rng(42)
        test_vector = rng.random(128).astype(np.float32)

        await session.execute("INSERT INTO test_mixed VALUES (:1, :2, :3)", (1, test_uuid, test_vector))

        result = await session.select_one("SELECT * FROM test_mixed WHERE id = :1", (1,))

        assert result is not None
        assert isinstance(result["uuid_col"], uuid.UUID)
        assert result["uuid_col"] == test_uuid
        assert isinstance(result["vector_col"], np.ndarray)
        np.testing.assert_array_almost_equal(result["vector_col"], test_vector, decimal=5)


async def test_uuid_disable(oracle_uuid_disabled_async_config: OracleAsyncConfig) -> None:
    """Test enable_uuid_binary=False disables automatic conversion."""
    async with oracle_uuid_disabled_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_uuid_disabled';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_uuid_disabled (
                id NUMBER PRIMARY KEY,
                uuid_col RAW(16) NOT NULL
            )
        """)

        test_uuid = uuid.uuid4()
        await session.execute("INSERT INTO test_uuid_disabled VALUES (:1, :2)", (1, test_uuid.bytes))

        result = await session.select_one("SELECT * FROM test_uuid_disabled WHERE id = :1", (1,))

        assert result is not None
        retrieved_value = result["uuid_col"]

        assert isinstance(retrieved_value, bytes)
        assert retrieved_value == test_uuid.bytes


async def test_raw32_untouched(oracle_uuid_async_config: OracleAsyncConfig) -> None:
    """Test RAW(32) columns remain as bytes (not converted to UUID)."""
    async with oracle_uuid_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_raw32';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_raw32 (
                id NUMBER PRIMARY KEY,
                binary_col RAW(32) NOT NULL
            )
        """)

        test_bytes = b"12345678901234567890123456789012"
        await session.execute("INSERT INTO test_raw32 VALUES (:1, :2)", (1, test_bytes))

        result = await session.select_one("SELECT * FROM test_raw32 WHERE id = :1", (1,))

        assert result is not None
        retrieved_value = result["binary_col"]

        assert isinstance(retrieved_value, bytes)
        assert retrieved_value == test_bytes


async def test_varchar_uuid_untouched(oracle_uuid_async_config: OracleAsyncConfig) -> None:
    """Test VARCHAR2 UUID columns remain as strings (not converted to UUID)."""
    async with oracle_uuid_async_config.provide_session() as session:
        await session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_varchar_uuid';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        await session.execute("""
            CREATE TABLE test_varchar_uuid (
                id NUMBER PRIMARY KEY,
                uuid_str VARCHAR2(36) NOT NULL
            )
        """)

        test_uuid = uuid.uuid4()
        uuid_str = str(test_uuid)
        await session.execute("INSERT INTO test_varchar_uuid VALUES (:1, :2)", (1, uuid_str))

        result = await session.select_one("SELECT * FROM test_varchar_uuid WHERE id = :1", (1,))

        assert result is not None
        retrieved_value = result["uuid_str"]

        assert isinstance(retrieved_value, str)
        assert retrieved_value == uuid_str
