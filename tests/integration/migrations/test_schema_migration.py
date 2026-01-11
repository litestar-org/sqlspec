"""Integration tests for migration tracking table schema migration."""

from collections.abc import Generator

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.migrations.tracker import SyncMigrationTracker


@pytest.fixture
def sqlite_config() -> Generator[SqliteConfig, None, None]:
    """Create SQLite config for testing."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    yield config
    config.close_pool()


@pytest.fixture
def sqlite_session(sqlite_config: SqliteConfig) -> Generator[SqliteDriver, None, None]:
    """Create SQLite session for testing."""
    with sqlite_config.provide_session() as session:
        yield session


def test_tracker_creates_full_schema_on_fresh_install(sqlite_session: SqliteDriver) -> None:
    """Test tracker creates complete schema with all columns on new database."""
    tracker = SyncMigrationTracker()

    tracker.ensure_tracking_table(sqlite_session)

    result = sqlite_session.execute(f"PRAGMA table_info({tracker.version_table})")
    columns = {row["name"] if isinstance(row, dict) else row[1] for row in result.data or []}

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


def test_tracker_migrates_legacy_schema(sqlite_session: SqliteDriver) -> None:
    """Test tracker adds missing columns to legacy schema."""
    tracker = SyncMigrationTracker()

    sqlite_session.execute(f"""
        CREATE TABLE {tracker.version_table} (
            version_num VARCHAR(32) PRIMARY KEY,
            description TEXT,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        )
    """)
    sqlite_session.commit()

    tracker.ensure_tracking_table(sqlite_session)

    result = sqlite_session.execute(f"PRAGMA table_info({tracker.version_table})")
    columns = {row["name"] if isinstance(row, dict) else row[1] for row in result.data or []}

    assert "version_type" in columns
    assert "execution_sequence" in columns
    assert "checksum" in columns
    assert "execution_time_ms" in columns
    assert "applied_by" in columns


def test_tracker_migration_preserves_existing_data(sqlite_session: SqliteDriver) -> None:
    """Test schema migration preserves existing migration records."""
    tracker = SyncMigrationTracker()

    sqlite_session.execute(f"""
        CREATE TABLE {tracker.version_table} (
            version_num VARCHAR(32) PRIMARY KEY,
            description TEXT,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        )
    """)

    sqlite_session.execute(
        f"""
        INSERT INTO {tracker.version_table} (version_num, description)
        VALUES ('0001', 'Initial migration')
    """
    )
    sqlite_session.commit()

    tracker.ensure_tracking_table(sqlite_session)

    result = sqlite_session.execute(f"SELECT * FROM {tracker.version_table}")
    records = result.data or []

    assert len(records) == 1
    record = records[0]
    assert record["version_num"] == "0001"
    assert record["description"] == "Initial migration"
    assert "version_type" in record
    assert "execution_sequence" in record


def test_tracker_migration_is_idempotent(sqlite_session: SqliteDriver) -> None:
    """Test schema migration can be run multiple times safely."""
    tracker = SyncMigrationTracker()

    sqlite_session.execute(f"""
        CREATE TABLE {tracker.version_table} (
            version_num VARCHAR(32) PRIMARY KEY,
            description TEXT,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        )
    """)
    sqlite_session.commit()

    tracker.ensure_tracking_table(sqlite_session)

    result1 = sqlite_session.execute(f"PRAGMA table_info({tracker.version_table})")
    columns1 = {row["name"] if isinstance(row, dict) else row[1] for row in result1.data or []}

    tracker.ensure_tracking_table(sqlite_session)

    result2 = sqlite_session.execute(f"PRAGMA table_info({tracker.version_table})")
    columns2 = {row["name"] if isinstance(row, dict) else row[1] for row in result2.data or []}

    assert columns1 == columns2


def test_tracker_uses_version_type_for_recording(sqlite_session: SqliteDriver) -> None:
    """Test tracker correctly records version_type when recording migrations."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    tracker.record_migration(sqlite_session, "0001", "Sequential migration", 100, "checksum1")
    tracker.record_migration(sqlite_session, "20251011120000", "Timestamp migration", 150, "checksum2")

    result = sqlite_session.execute(
        f"SELECT version_num, version_type FROM {tracker.version_table} ORDER BY execution_sequence"
    )
    records = result.data or []

    assert len(records) == 2
    assert records[0]["version_num"] == "0001"
    assert records[0]["version_type"] == "sequential"
    assert records[1]["version_num"] == "20251011120000"
    assert records[1]["version_type"] == "timestamp"


def test_tracker_execution_sequence_auto_increments(sqlite_session: SqliteDriver) -> None:
    """Test execution_sequence auto-increments for tracking application order."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    tracker.record_migration(sqlite_session, "0001", "First", 100, "checksum1")
    tracker.record_migration(sqlite_session, "0002", "Second", 100, "checksum2")
    tracker.record_migration(sqlite_session, "0003", "Third", 100, "checksum3")

    result = sqlite_session.execute(
        f"SELECT version_num, execution_sequence FROM {tracker.version_table} ORDER BY execution_sequence"
    )
    records = result.data or []

    assert len(records) == 3
    assert records[0]["execution_sequence"] == 1
    assert records[1]["execution_sequence"] == 2
    assert records[2]["execution_sequence"] == 3


def test_tracker_get_current_version_uses_execution_sequence(sqlite_session: SqliteDriver) -> None:
    """Test get_current_version returns last applied migration by execution order."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    tracker.record_migration(sqlite_session, "0001", "First", 100, "checksum1")
    tracker.record_migration(sqlite_session, "0003", "Out of order", 100, "checksum3")
    tracker.record_migration(sqlite_session, "0002", "Late merge", 100, "checksum2")

    current = tracker.get_current_version(sqlite_session)

    assert current == "0002"


def test_tracker_update_version_record_preserves_execution_sequence(sqlite_session: SqliteDriver) -> None:
    """Test updating version preserves execution_sequence and applied_at."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    tracker.record_migration(sqlite_session, "20251011120000", "Timestamp migration", 100, "checksum1")

    result_before = sqlite_session.execute(
        f"SELECT execution_sequence, applied_at FROM {tracker.version_table} WHERE version_num = '20251011120000'"
    )
    record_before = (result_before.data or [])[0]

    tracker.update_version_record(sqlite_session, "20251011120000", "0001")

    result_after = sqlite_session.execute(
        f"SELECT version_num, version_type, execution_sequence, applied_at FROM {tracker.version_table} WHERE version_num = '0001'"
    )
    record_after = (result_after.data or [])[0]

    assert record_after["version_num"] == "0001"
    assert record_after["version_type"] == "sequential"
    assert record_after["execution_sequence"] == record_before["execution_sequence"]
    assert record_after["applied_at"] == record_before["applied_at"]


def test_tracker_update_version_record_idempotent(sqlite_session: SqliteDriver) -> None:
    """Test update_version_record is idempotent when version already updated."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    tracker.record_migration(sqlite_session, "20251011120000", "Migration", 100, "checksum1")

    tracker.update_version_record(sqlite_session, "20251011120000", "0001")

    tracker.update_version_record(sqlite_session, "20251011120000", "0001")

    result = sqlite_session.execute(f"SELECT COUNT(*) as count FROM {tracker.version_table}")
    count = (result.data or [])[0]["count"]

    assert count == 1


def test_tracker_update_version_record_raises_on_missing(sqlite_session: SqliteDriver) -> None:
    """Test update_version_record raises error when version not found."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    with pytest.raises(ValueError, match="Migration version .* not found"):  # noqa: RUF043
        tracker.update_version_record(sqlite_session, "nonexistent", "0001")


def test_tracker_migration_adds_columns_in_sorted_order(sqlite_session: SqliteDriver) -> None:
    """Test schema migration adds multiple missing columns consistently."""
    tracker = SyncMigrationTracker()

    sqlite_session.execute(f"""
        CREATE TABLE {tracker.version_table} (
            version_num VARCHAR(32) PRIMARY KEY,
            description TEXT
        )
    """)
    sqlite_session.commit()

    tracker.ensure_tracking_table(sqlite_session)

    result = sqlite_session.execute(f"PRAGMA table_info({tracker.version_table})")
    columns = [row["name"] if isinstance(row, dict) else row[1] for row in result.data or []]

    version_num_idx = columns.index("version_num")
    description_idx = columns.index("description")
    version_type_idx = columns.index("version_type")

    assert version_num_idx < description_idx < version_type_idx


def test_tracker_checksum_column_stores_md5_hashes(sqlite_session: SqliteDriver) -> None:
    """Test checksum column can store migration content checksums."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    import hashlib

    content = "CREATE TABLE users (id INTEGER PRIMARY KEY);"
    checksum = hashlib.md5(content.encode()).hexdigest()

    tracker.record_migration(sqlite_session, "0001", "Create users", 100, checksum)

    result = sqlite_session.execute(f"SELECT checksum FROM {tracker.version_table} WHERE version_num = '0001'")
    stored_checksum = (result.data or [])[0]["checksum"]

    assert stored_checksum == checksum
    assert len(stored_checksum) == 32


def test_tracker_applied_by_column_stores_user(sqlite_session: SqliteDriver) -> None:
    """Test applied_by column records who applied the migration."""
    import os

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    tracker.record_migration(sqlite_session, "0001", "Migration", 100, "checksum")

    result = sqlite_session.execute(f"SELECT applied_by FROM {tracker.version_table} WHERE version_num = '0001'")
    applied_by = (result.data or [])[0]["applied_by"]

    expected_user = os.environ.get("USER", "unknown")
    assert applied_by == expected_user


def test_tracker_get_applied_migrations_orders_by_execution_sequence(sqlite_session: SqliteDriver) -> None:
    """Test get_applied_migrations returns migrations in execution order."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    tracker.record_migration(sqlite_session, "0001", "First", 100, "checksum1")
    tracker.record_migration(sqlite_session, "0003", "Out of order", 100, "checksum3")
    tracker.record_migration(sqlite_session, "0002", "Late merge", 100, "checksum2")

    applied = tracker.get_applied_migrations(sqlite_session)

    assert len(applied) == 3
    assert applied[0]["version_num"] == "0001"
    assert applied[1]["version_num"] == "0003"
    assert applied[2]["version_num"] == "0002"
