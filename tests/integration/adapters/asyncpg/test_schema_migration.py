"""Integration tests for migration tracking table schema migration with PostgreSQL."""

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.migrations.tracker import AsyncMigrationTracker

pytestmark = pytest.mark.xdist_group("postgres")


def _create_config(postgres_service: "PostgresService") -> AsyncpgConfig:
    """Create AsyncpgConfig from PostgresService fixture."""
    return AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        }
    )


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_tracker_creates_full_schema(postgres_service: "PostgresService") -> None:
    """Test AsyncPG tracker creates complete schema with all columns."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await tracker.ensure_tracking_table(driver)

            result = await driver.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{tracker.version_table}'
            """)

            columns = {row["column_name"] for row in result.data or []}

            expected_columns = {
                "version_num",
                "version_type",
                "execution_sequence",
                "description",
                "applied_at",
                "execution_time_ms",
                "checksum",
                "applied_by",
            }

            assert columns == expected_columns
    finally:
        await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_tracker_migrates_legacy_schema(postgres_service: "PostgresService") -> None:
    """Test AsyncPG tracker adds missing columns to legacy schema."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {tracker.version_table}")
            await driver.execute(f"""
                CREATE TABLE {tracker.version_table} (
                    version_num VARCHAR(32) PRIMARY KEY,
                    description TEXT,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                )
            """)
            await driver.commit()

            await tracker.ensure_tracking_table(driver)

            result = await driver.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{tracker.version_table}'
            """)

            columns = {row["column_name"] for row in result.data or []}

            assert "version_type" in columns
            assert "execution_sequence" in columns
            assert "checksum" in columns
            assert "execution_time_ms" in columns
            assert "applied_by" in columns
    finally:
        await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_tracker_migration_preserves_data(postgres_service: "PostgresService") -> None:
    """Test AsyncPG schema migration preserves existing records."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {tracker.version_table}")
            await driver.execute(f"""
                CREATE TABLE {tracker.version_table} (
                    version_num VARCHAR(32) PRIMARY KEY,
                    description TEXT,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                )
            """)

            await driver.execute(f"""
                INSERT INTO {tracker.version_table} (version_num, description)
                VALUES ('0001', 'Initial migration')
            """)
            await driver.commit()

            await tracker.ensure_tracking_table(driver)

            result = await driver.execute(f"SELECT * FROM {tracker.version_table}")
            records = result.data or []

            assert len(records) == 1
            assert records[0]["version_num"] == "0001"
            assert records[0]["description"] == "Initial migration"
    finally:
        await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_tracker_version_type_recording(postgres_service: "PostgresService") -> None:
    """Test AsyncPG tracker correctly records version_type."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {tracker.version_table}")
            await tracker.ensure_tracking_table(driver)

            await tracker.record_migration(driver, "0001", "Sequential", 100, "checksum1")
            await tracker.record_migration(driver, "20251011120000", "Timestamp", 150, "checksum2")

            result = await driver.execute(f"""
                SELECT version_num, version_type
                FROM {tracker.version_table}
                ORDER BY execution_sequence
            """)
            records = result.data or []

            assert len(records) == 2
            assert records[0]["version_num"] == "0001"
            assert records[0]["version_type"] == "sequential"
            assert records[1]["version_num"] == "20251011120000"
            assert records[1]["version_type"] == "timestamp"
    finally:
        await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_tracker_execution_sequence(postgres_service: "PostgresService") -> None:
    """Test AsyncPG tracker execution_sequence auto-increments."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {tracker.version_table}")
            await tracker.ensure_tracking_table(driver)

            await tracker.record_migration(driver, "0001", "First", 100, "checksum1")
            await tracker.record_migration(driver, "0003", "Out of order", 100, "checksum3")
            await tracker.record_migration(driver, "0002", "Late merge", 100, "checksum2")

            result = await driver.execute(f"""
                SELECT version_num, execution_sequence
                FROM {tracker.version_table}
                ORDER BY execution_sequence
            """)
            records = result.data or []

            assert len(records) == 3
            assert records[0]["execution_sequence"] == 1
            assert records[1]["execution_sequence"] == 2
            assert records[2]["execution_sequence"] == 3

            assert records[0]["version_num"] == "0001"
            assert records[1]["version_num"] == "0003"
            assert records[2]["version_num"] == "0002"
    finally:
        await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_get_current_version_uses_execution_sequence(postgres_service: "PostgresService") -> None:
    """Test AsyncPG get_current_version uses execution order."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {tracker.version_table}")
            await tracker.ensure_tracking_table(driver)

            await tracker.record_migration(driver, "0001", "First", 100, "checksum1")
            await tracker.record_migration(driver, "0003", "Out of order", 100, "checksum3")
            await tracker.record_migration(driver, "0002", "Late merge", 100, "checksum2")

            current = await tracker.get_current_version(driver)

            assert current == "0002"
    finally:
        await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_update_version_record_preserves_metadata(postgres_service: "PostgresService") -> None:
    """Test AsyncPG update preserves execution_sequence and applied_at."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {tracker.version_table}")
            await tracker.ensure_tracking_table(driver)

            await tracker.record_migration(driver, "20251011120000", "Migration", 100, "checksum1")

            result_before = await driver.execute(f"""
                SELECT execution_sequence, applied_at
                FROM {tracker.version_table}
                WHERE version_num = '20251011120000'
            """)
            record_before = (result_before.data or [])[0]

            await tracker.update_version_record(driver, "20251011120000", "0001")

            result_after = await driver.execute(f"""
                SELECT version_num, version_type, execution_sequence, applied_at
                FROM {tracker.version_table}
                WHERE version_num = '0001'
            """)
            record_after = (result_after.data or [])[0]

            assert record_after["version_num"] == "0001"
            assert record_after["version_type"] == "sequential"
            assert record_after["execution_sequence"] == record_before["execution_sequence"]
    finally:
        await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_update_version_record_idempotent(postgres_service: "PostgresService") -> None:
    """Test AsyncPG update_version_record is idempotent."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {tracker.version_table}")
            await tracker.ensure_tracking_table(driver)

            await tracker.record_migration(driver, "20251011120000", "Migration", 100, "checksum1")

            await tracker.update_version_record(driver, "20251011120000", "0001")
            await tracker.update_version_record(driver, "20251011120000", "0001")

            result = await driver.execute(f"SELECT COUNT(*) as count FROM {tracker.version_table}")
            count = (result.data or [])[0]["count"]

            assert count == 1
    finally:
        await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_migration_schema_is_idempotent(postgres_service: "PostgresService") -> None:
    """Test AsyncPG schema migration can be run multiple times."""
    config = _create_config(postgres_service)
    tracker = AsyncMigrationTracker()

    try:
        async with config.provide_session() as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {tracker.version_table}")
            await driver.execute(f"""
                CREATE TABLE {tracker.version_table} (
                    version_num VARCHAR(32) PRIMARY KEY,
                    description TEXT
                )
            """)
            await driver.commit()

            await tracker.ensure_tracking_table(driver)

            result1 = await driver.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{tracker.version_table}'
            """)
            columns1 = {row["column_name"] for row in result1.data or []}

            await tracker.ensure_tracking_table(driver)

            result2 = await driver.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{tracker.version_table}'
            """)
            columns2 = {row["column_name"] for row in result2.data or []}

            assert columns1 == columns2
    finally:
        await config.close_pool()
