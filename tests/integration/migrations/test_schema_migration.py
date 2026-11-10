"""Integration tests for migration tracking table schema migration."""

from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.exceptions import MigrationError
from sqlspec.migrations import create_migration_commands
from sqlspec.migrations.tracker import SyncMigrationTracker


@pytest.fixture
def sqlite_config() -> Generator[SqliteConfig, None, None]:
    """Create SQLite config for testing."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    yield config
    config.close_pool()


@pytest.fixture
def sqlite_session() -> Generator[SqliteDriver, None, None]:
    """Create SQLite session for testing with completely isolated database."""
    # Create fresh config for each test to ensure complete isolation
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as session:
        yield session
    config.close_pool()


def test_tracker_creates_full_schema_on_fresh_install(sqlite_session: SqliteDriver) -> None:
    """Test tracker creates complete schema with all columns on new database."""
    tracker = SyncMigrationTracker()

    tracker.ensure_tracking_table(sqlite_session)

    result = sqlite_session.execute(f"PRAGMA table_info({tracker.version_table})")
    columns = {row["name"] if isinstance(row, dict) else row[1] for row in result.get_data()}

    expected_columns = {
        "version_num",
        "version_type",
        "execution_sequence",
        "description",
        "applied_at",
        "execution_time_ms",
        "checksum",
        "applied_by",
        "replaces",
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
    columns = {row["name"] if isinstance(row, dict) else row[1] for row in result.get_data()}

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
    records = result.get_data()

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
    columns1 = {row["name"] if isinstance(row, dict) else row[1] for row in result1.get_data()}

    tracker.ensure_tracking_table(sqlite_session)

    result2 = sqlite_session.execute(f"PRAGMA table_info({tracker.version_table})")
    columns2 = {row["name"] if isinstance(row, dict) else row[1] for row in result2.get_data()}

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
    records = result.get_data()

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
    records = result.get_data()

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
    record_before = result_before.get_data()[0]

    tracker.update_version_record(sqlite_session, "20251011120000", "0001")

    result_after = sqlite_session.execute(
        f"SELECT version_num, version_type, execution_sequence, applied_at FROM {tracker.version_table} WHERE version_num = '0001'"
    )
    record_after = result_after.get_data()[0]

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
    count = result.get_data()[0]["count"]

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
    columns = [row["name"] if isinstance(row, dict) else row[1] for row in result.get_data()]

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
    stored_checksum = result.get_data()[0]["checksum"]

    assert stored_checksum == checksum
    assert len(stored_checksum) == 32


def test_tracker_applied_by_column_stores_user(sqlite_session: SqliteDriver) -> None:
    """Test applied_by column records who applied the migration."""
    import os

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    tracker.record_migration(sqlite_session, "0001", "Migration", 100, "checksum")

    result = sqlite_session.execute(f"SELECT applied_by FROM {tracker.version_table} WHERE version_num = '0001'")
    applied_by = result.get_data()[0]["applied_by"]

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


def test_sqlite_litestar_reconcile_mode_end_to_end(tmp_path: object) -> None:
    from pathlib import Path

    from sqlspec.migrations.commands import SyncMigrationCommands

    root = Path(str(tmp_path))
    migrations_dir = root / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    db_path = root / "litestar_e2e.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir)},
        extension_config={"litestar": {"session_table": "litestar_session", "migration_mode": "reconcile"}},
    )
    try:
        with config.provide_session() as session:
            session.execute_script(
                "CREATE TABLE litestar_session (session_id TEXT PRIMARY KEY, data BLOB NOT NULL); "
                "INSERT INTO litestar_session (session_id, data) VALUES ('keep-me', X'0102');"
            )
            session.commit()

        commands = SyncMigrationCommands(config)
        commands.upgrade()
        commands.upgrade()

        with config.provide_session() as session:
            cols = {
                str(col["column_name"]).casefold()
                for col in session.data_dictionary.get_columns(session, "litestar_session")
            }
            rows = session.select("SELECT session_id, data FROM litestar_session")
            ledger = session.select("SELECT version_num FROM ddl_migrations")

        assert "expires_at" in cols
        assert len(rows) == 1
        assert rows[0]["session_id"] == "keep-me"
        assert all(not str(r["version_num"]).startswith("ext_litestar_") for r in ledger)

        commands.downgrade()
        with config.provide_session() as session:
            remaining_tables = {
                str(tbl["table_name"]).casefold() for tbl in session.data_dictionary.get_tables(session)
            }
        assert "litestar_session" not in remaining_tables
    finally:
        config.close_pool()


def test_extension_reconciliation_acceptance(tmp_path: Path) -> None:
    """Verify public upgrade() reconciliation, dry-run isolation, ledger exclusion, and retry recovery."""
    preview_db = tmp_path / "preview.db"
    preview_migrations = tmp_path / "preview_migrations"
    preview_migrations.mkdir(parents=True, exist_ok=True)
    preview_ext = tmp_path / "preview_ext"
    preview_ext.mkdir(parents=True, exist_ok=True)
    (preview_ext / "reconcile.py").write_text(
        'raise RuntimeError("dry_run must not import reconcile")\n', encoding="utf-8"
    )

    preview_config = SqliteConfig(
        connection_config={"database": str(preview_db)}, migration_config={"script_location": str(preview_migrations)}
    )
    preview_config.add_extension_migrations("smoke_ext", preview_ext, migration_mode="reconcile")
    try:
        create_migration_commands(preview_config).upgrade(dry_run=True, echo=False)
        with preview_config.provide_session() as session:
            assert session.select("SELECT name FROM sqlite_master WHERE type = 'table'") == []
    finally:
        preview_config.close_pool()

    db_path = tmp_path / "acceptance.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    ext_dir = tmp_path / "smoke_ext"
    ext_dir.mkdir(parents=True, exist_ok=True)
    fail_marker = tmp_path / "fail"

    (ext_dir / "0001_shipped.sql").write_text(
        "-- name: migrate-0001-up\n"
        "CREATE TABLE smoke_numbered (id INTEGER PRIMARY KEY);\n"
        "-- name: migrate-0001-down\n"
        "DROP TABLE smoke_numbered;\n",
        encoding="utf-8",
    )
    (ext_dir / "reconcile.py").write_text(
        "from pathlib import Path\n\n\n"
        "def reconcile(context) -> None:\n"
        "    if Path(str(context.extension_config['fail_marker'])).exists():\n"
        "        raise RuntimeError('boom')\n"
        "    assert context.driver is not None\n"
        "    context.driver.execute_script(\n"
        "        'CREATE TABLE IF NOT EXISTS smoke_calls (id INTEGER PRIMARY KEY); '\n"
        "        'INSERT INTO smoke_calls DEFAULT VALUES;'\n"
        "    )\n",
        encoding="utf-8",
    )

    config = SqliteConfig(
        connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
    )
    config.add_extension_migrations("smoke_ext", ext_dir, {"fail_marker": str(fail_marker)}, migration_mode="reconcile")
    try:
        commands = create_migration_commands(config)
        commands.upgrade(echo=False)
        with config.provide_session() as session:
            assert session.select_value("SELECT COUNT(*) FROM smoke_calls") == 1
            assert (
                session.select_value(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'smoke_numbered'"
                )
                == 0
            )
            if session.select_value(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'ddl_migrations'"
            ):
                assert session.select_value("SELECT COUNT(*) FROM ddl_migrations") == 0

        commands.upgrade(echo=False)
        with config.provide_session() as session:
            assert session.select_value("SELECT COUNT(*) FROM smoke_calls") == 2
            assert (
                session.select_value(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'smoke_numbered'"
                )
                == 0
            )

        (migrations_dir / "0001_items.sql").write_text(
            "-- name: migrate-0001-up\n"
            "CREATE TABLE smoke_items AS SELECT COUNT(*) AS seen_calls FROM smoke_calls;\n"
            "-- name: migrate-0001-down\n"
            "DROP TABLE smoke_items;\n",
            encoding="utf-8",
        )
        commands.upgrade(echo=False)
        with config.provide_session() as session:
            assert session.select_value("SELECT COUNT(*) FROM smoke_calls") == 3
            assert session.select_value("SELECT seen_calls FROM smoke_items") == 3
            assert [
                row["version_num"]
                for row in session.select("SELECT version_num FROM ddl_migrations ORDER BY execution_sequence")
            ] == ["0001"]
            assert (
                session.select_value("SELECT COUNT(*) FROM ddl_migrations WHERE version_num LIKE 'ext_smoke_ext_%'")
                == 0
            )

        fail_marker.write_text("1", encoding="utf-8")
        (migrations_dir / "0002_after.sql").write_text(
            "-- name: migrate-0002-up\n"
            "CREATE TABLE smoke_after (id INTEGER PRIMARY KEY);\n"
            "-- name: migrate-0002-down\n"
            "DROP TABLE smoke_after;\n",
            encoding="utf-8",
        )
        with pytest.raises(MigrationError) as exc_info:
            commands.upgrade(echo=False)
        assert isinstance(exc_info.value.__cause__, RuntimeError)
        assert str(exc_info.value.__cause__) == "boom"

        with config.provide_session() as session:
            assert session.select_value("SELECT COUNT(*) FROM smoke_calls") == 3
            assert (
                session.select_value("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'smoke_after'")
                == 0
            )
            assert [
                row["version_num"]
                for row in session.select("SELECT version_num FROM ddl_migrations ORDER BY execution_sequence")
            ] == ["0001"]

        fail_marker.unlink()
        commands.upgrade(echo=False)
        with config.provide_session() as session:
            assert session.select_value("SELECT COUNT(*) FROM smoke_calls") == 4
            assert (
                session.select_value("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'smoke_after'")
                == 1
            )
            assert [
                row["version_num"]
                for row in session.select("SELECT version_num FROM ddl_migrations ORDER BY execution_sequence")
            ] == ["0001", "0002"]
            assert (
                session.select_value("SELECT COUNT(*) FROM ddl_migrations WHERE version_num LIKE 'ext_smoke_ext_%'")
                == 0
            )
    finally:
        config.close_pool()
