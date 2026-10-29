"""Unit tests for schema dump, verification, dry-run reconciliation, and schema/data revision bootstrap."""

import gzip
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.utils.serializers import to_json

USERS_UP = (
    "-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n\n"
    "-- name: migrate-0001-down\nDROP TABLE users;\n"
)
EMAIL_UP = (
    "-- name: migrate-0002-up\nALTER TABLE users ADD COLUMN email TEXT;\n\n"
    "-- name: migrate-0002-down\nALTER TABLE users DROP COLUMN email;\n"
)
POSTS_UP = (
    "-- name: migrate-0003-up\n"
    "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), body TEXT);\n\n"
    "-- name: migrate-0003-down\nDROP TABLE posts;\n"
)


def _write_three_migrations(migrations_dir: Path) -> None:
    migrations_dir.mkdir(exist_ok=True)
    (migrations_dir / "0001_users.sql").write_text(USERS_UP, encoding="utf-8")
    (migrations_dir / "0002_email.sql").write_text(EMAIL_UP, encoding="utf-8")
    (migrations_dir / "0003_posts.sql").write_text(POSTS_UP, encoding="utf-8")


def _sqlite_objects(db_path: Path) -> "list[tuple[str, str]]":
    with sqlite3.connect(db_path) as connection:
        return connection.execute("SELECT type, name FROM sqlite_master ORDER BY type, name").fetchall()


def _tracking_rows(db_path: Path, table: str = "ddl_migrations") -> "list[tuple[str, str | None]]":
    with sqlite3.connect(db_path) as connection:
        return connection.execute(f"SELECT version_num, replaces FROM {table} ORDER BY version_num").fetchall()


def _sync_commands(db_path: Path, migrations_dir: Path, **migration_config: object) -> SyncMigrationCommands:
    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "enabled": True, **migration_config},
    )
    return SyncMigrationCommands(config)


def _async_config(db_path: Path, migrations_dir: Path) -> AiosqliteConfig:
    return AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "enabled": True},
    )


def _replay(statements: "list[str]", connection: sqlite3.Connection) -> None:
    for statement in statements:
        connection.execute(statement)


def _migration_sections(file_path: Path) -> "tuple[list[str], list[str]]":
    """Return the UP and DOWN statements of a generated SQL migration file."""
    content = file_path.read_text(encoding="utf-8")
    up_text, down_text = content.split("-- name: migrate-", 2)[1:]
    up_body = up_text.split("\n", 1)[1]
    down_body = down_text.split("\n", 1)[1]
    return [s.strip() for s in up_body.split(";") if s.strip()], [s.strip() for s in down_body.split(";") if s.strip()]


def test_sync_dump_schema_writes_topological_ddl_and_excludes_migrations_table(tmp_path: Path) -> None:
    """dump_schema writes DDL that replays in order and leaves out the tracking table by default."""
    db_path = tmp_path / "sync_dump.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    schema_path = tmp_path / "schema.sql"
    (migrations_dir / "0001_init.sql").write_text(
        "-- name: migrate-0001-up\n"
        "CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n"
        "CREATE TABLE members (id INTEGER PRIMARY KEY, team_id INTEGER NOT NULL REFERENCES teams(id));\n\n"
        "-- name: migrate-0001-down\n"
        "DROP TABLE members;\n"
        "DROP TABLE teams;\n",
        encoding="utf-8",
    )
    commands = _sync_commands(db_path, migrations_dir)
    commands.upgrade(echo=False)

    rendered = commands.dump_schema(output_path=schema_path, echo=False)
    assert schema_path.read_text(encoding="utf-8") == rendered
    with sqlite3.connect(":memory:") as replay:
        replay.executescript(rendered)
        tables = [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    assert tables == ["members", "teams"]

    with_tracking = commands.dump_schema(include_migrations_table=True, echo=False)
    assert "ddl_migrations" in with_tracking


@pytest.mark.parametrize(
    ("version_table_name", "tracking_table"),
    [
        ("schema_versions", "schema_versions"),
        ('"quoted_versions"', "quoted_versions"),
        ("main.app_versions", "app_versions"),
    ],
)
def test_dump_schema_excludes_custom_version_table(
    tmp_path: Path, version_table_name: str, tracking_table: str
) -> None:
    """Custom, quoted, and schema-qualified version tables never leak into dump_schema output."""
    db_path = tmp_path / "custom.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_accounts.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE accounts (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    commands = _sync_commands(db_path, migrations_dir, version_table_name=version_table_name)
    commands.upgrade(echo=False)
    assert ("table", tracking_table) in _sqlite_objects(db_path)

    rendered = commands.dump_schema(echo=False)
    with sqlite3.connect(":memory:") as replay:
        replay.executescript(rendered)
        tables = [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert tables == ["accounts"]


def test_dump_schema_keeps_user_table_named_like_default_tracking_table(tmp_path: Path) -> None:
    """A user table called ddl_migrations is kept when the tracker uses a different version table."""
    db_path = tmp_path / "user_table.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_audit.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE ddl_migrations (id INTEGER PRIMARY KEY, note TEXT);\n", encoding="utf-8"
    )
    commands = _sync_commands(db_path, migrations_dir, version_table_name="versions")
    commands.upgrade(echo=False)

    rendered = commands.dump_schema(echo=False)
    with sqlite3.connect(":memory:") as replay:
        replay.executescript(rendered)
        tables = [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert tables == ["ddl_migrations"]


@pytest.mark.anyio
async def test_async_dump_schema_uses_configured_schema_dump_path(tmp_path: Path) -> None:
    """Async dump_schema writes to migration_config['schema_dump_path'] when output_path is omitted."""
    db_path = tmp_path / "async_dump.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    configured_dump = tmp_path / "configured_schema.sql"
    (migrations_dir / "0001_init.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE projects (id INTEGER PRIMARY KEY, title TEXT NOT NULL);\n",
        encoding="utf-8",
    )
    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migrations_dir),
            "enabled": True,
            "schema_dump_path": str(configured_dump),
        },
    )
    commands = AsyncMigrationCommands(config)
    await commands.upgrade(echo=False)

    rendered = await commands.dump_schema(echo=False)
    assert configured_dump.read_text(encoding="utf-8") == rendered
    with sqlite3.connect(":memory:") as replay:
        replay.executescript(rendered)
        tables = [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert tables == ["projects"]
    await config.close_pool()


def test_sync_verify_detects_checksum_drift_and_orphaned_records(tmp_path: Path) -> None:
    """verify reports pending, clean, checksum drift, and orphaned states; strict raises on drift and orphans."""
    db_path = tmp_path / "sync_verify.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    mig1 = migrations_dir / "0001_users.sql"
    mig2 = migrations_dir / "0002_posts.sql"
    mig1.write_text("-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n", encoding="utf-8")
    mig2.write_text("-- name: migrate-0002-up\nCREATE TABLE posts (id INTEGER PRIMARY KEY);\n", encoding="utf-8")
    commands = _sync_commands(db_path, migrations_dir)

    pending_report = commands.verify(strict=True, echo=False)
    assert pending_report["valid"] is True
    assert pending_report["pending_versions"] == ["0001", "0002"]
    with pytest.raises(MigrationError, match=r"pending migrations \[0001, 0002\]"):
        commands.verify(fail_on_pending=True, echo=False)

    commands.upgrade(echo=False)
    clean_report = commands.verify(strict=True, fail_on_pending=True, echo=False)
    assert clean_report == {
        "valid": True,
        "checksum_mismatches": [],
        "orphaned_records": [],
        "pending_versions": [],
        "partial_squash_errors": [],
    }

    mig1.write_text(
        "-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n", encoding="utf-8"
    )
    drift_report = commands.verify(echo=False)
    assert drift_report["valid"] is False
    assert [item["version"] for item in drift_report["checksum_mismatches"]] == ["0001"]
    with pytest.raises(MigrationError, match="checksum mismatch"):
        commands.verify(strict=True, echo=False)

    mig1.write_text("-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n", encoding="utf-8")
    mig2.unlink()
    orphan_report = commands.verify(echo=False)
    assert orphan_report["orphaned_records"] == ["0002"]
    with pytest.raises(MigrationError, match="orphaned applied records"):
        commands.verify(strict=True, echo=False)


def test_sync_verify_on_empty_database_creates_nothing(tmp_path: Path) -> None:
    """verify is read-only: without a tracking table it reports everything pending and creates no objects."""
    db_path = tmp_path / "empty.db"
    migrations_dir = tmp_path / "migrations"
    _write_three_migrations(migrations_dir)
    sqlite3.connect(db_path).close()
    before = _sqlite_objects(db_path)

    report = _sync_commands(db_path, migrations_dir).verify(strict=True, echo=False)

    assert report["pending_versions"] == ["0001", "0002", "0003"]
    assert report["valid"] is True
    assert before == [] == _sqlite_objects(db_path)


@pytest.mark.anyio
async def test_async_verify_on_empty_database_creates_nothing(tmp_path: Path) -> None:
    """Async verify never creates the tracking table on an empty database."""
    db_path = tmp_path / "empty_async.db"
    migrations_dir = tmp_path / "migrations"
    _write_three_migrations(migrations_dir)
    sqlite3.connect(db_path).close()
    config = _async_config(db_path, migrations_dir)

    report = await AsyncMigrationCommands(config).verify(strict=True, echo=False)
    await config.close_pool()

    assert report["pending_versions"] == ["0001", "0002", "0003"]
    assert _sqlite_objects(db_path) == []


@pytest.mark.anyio
async def test_async_verify_handles_squashed_migrations_and_strict_mode(tmp_path: Path) -> None:
    """Async verify passes after squash and reconciliation."""
    db_path = tmp_path / "async_verify.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_users.sql").write_text(USERS_UP, encoding="utf-8")
    (migrations_dir / "0002_email.sql").write_text(EMAIL_UP, encoding="utf-8")
    config = _async_config(db_path, migrations_dir)
    commands = AsyncMigrationCommands(config)
    await commands.upgrade(echo=False)
    await commands.squash(yes=True)

    squashed_report = await commands.verify(strict=True, echo=False)
    assert squashed_report["valid"] is True
    assert _tracking_rows(db_path) == [("0001", "0001,0002")]
    await config.close_pool()


def test_sync_upgrade_dry_run_does_not_reconcile_squashed_records(tmp_path: Path) -> None:
    """upgrade(dry_run=True) reports the squash reconciliation but leaves tracking rows untouched."""
    migrations_dir = tmp_path / "migrations"
    _write_three_migrations(migrations_dir)
    db_path = tmp_path / "full.db"
    _sync_commands(db_path, migrations_dir).upgrade(echo=False)
    _sync_commands(tmp_path / "author.db", migrations_dir).squash(yes=True, update_database=False)
    assert sorted(p.name for p in migrations_dir.glob("*.sql")) == ["0001_initial_schema.sql"]
    before_rows = _tracking_rows(db_path)
    before_bytes = db_path.read_bytes()

    commands = _sync_commands(db_path, migrations_dir)
    commands.upgrade(dry_run=True, echo=False)

    assert _tracking_rows(db_path) == before_rows == [("0001", None), ("0002", None), ("0003", None)]
    assert db_path.read_bytes() == before_bytes

    commands.upgrade(echo=False)
    assert _tracking_rows(db_path) == [("0001", "0001,0002,0003")]


def test_sync_upgrade_dry_run_on_empty_database_creates_nothing(tmp_path: Path) -> None:
    """A dry run on an empty database neither creates the tracking table nor applies migrations."""
    migrations_dir = tmp_path / "migrations"
    _write_three_migrations(migrations_dir)
    db_path = tmp_path / "dry.db"
    sqlite3.connect(db_path).close()

    _sync_commands(db_path, migrations_dir).upgrade(dry_run=True, echo=False)

    assert _sqlite_objects(db_path) == []


@pytest.mark.anyio
async def test_async_upgrade_dry_run_does_not_reconcile_squashed_records(tmp_path: Path) -> None:
    """Async dry-run upgrade leaves the pre-squash tracking rows untouched."""
    migrations_dir = tmp_path / "migrations"
    _write_three_migrations(migrations_dir)
    db_path = tmp_path / "full_async.db"
    config = _async_config(db_path, migrations_dir)
    await AsyncMigrationCommands(config).upgrade(echo=False)
    _sync_commands(tmp_path / "author.db", migrations_dir).squash(yes=True, update_database=False)

    commands = AsyncMigrationCommands(config)
    await commands.upgrade(dry_run=True, echo=False)
    assert _tracking_rows(db_path) == [("0001", None), ("0002", None), ("0003", None)]

    await commands.upgrade(echo=False)
    await config.close_pool()
    assert _tracking_rows(db_path) == [("0001", "0001,0002,0003")]


def test_sync_upgrade_without_auto_sync_rejects_partially_applied_squash(tmp_path: Path) -> None:
    """upgrade(auto_sync=False) on a partially applied pre-squash database raises instead of reporting success."""
    migrations_dir = tmp_path / "migrations"
    _write_three_migrations(migrations_dir)
    db_path = tmp_path / "partial.db"
    _sync_commands(db_path, migrations_dir).upgrade(revision="0002", echo=False)
    _sync_commands(tmp_path / "author.db", migrations_dir).squash(yes=True, update_database=False)

    with pytest.raises(MigrationError, match="Cannot reconcile squashed migration 0001"):
        _sync_commands(db_path, migrations_dir).upgrade(auto_sync=False, echo=False)
    assert _tracking_rows(db_path) == [("0001", None), ("0002", None)]


def test_sync_revision_from_schema_populates_sql_and_python_migrations(tmp_path: Path) -> None:
    """revision(from_schema=True) writes UP DDL that recreates the schema and DOWN DDL that drops it."""
    db_path = tmp_path / "from_schema.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    commands = _sync_commands(db_path, migrations_dir)
    with commands.config.provide_session() as driver:
        driver.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
        driver.execute(
            "CREATE TABLE articles (id INTEGER PRIMARY KEY, category_id INTEGER NOT NULL REFERENCES categories(id))"
        )
        driver.commit()

    sql_file = commands.revision("initial_baseline", file_type="sql", from_schema=True)
    up_statements, down_statements = _migration_sections(sql_file)
    with sqlite3.connect(":memory:") as replay:
        _replay(up_statements, replay)
        tables = [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        assert tables == ["articles", "categories"]
        _replay(down_statements, replay)
        assert replay.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone() == (0,)

    sql_file.unlink()
    py_file = commands.revision("initial_baseline_py", file_type="py", from_schema=True)
    py_content = py_file.read_text(encoding="utf-8")
    assert "CREATE TABLE categories" in py_content
    assert "ddl_migrations" not in py_content


@pytest.mark.parametrize(
    ("from_data", "message"),
    [
        ("seeds/", "is a directory"),
        ("seeds/users.csv", "Unsupported data file 'seeds/users.csv'"),
        ("users.csv", "Unsupported data file 'users.csv'"),
        ("public.json", "Data file not found: public.json"),
        ("seeds/missing.json.gz", "Data file not found: seeds/missing.json.gz"),
    ],
)
def test_revision_from_data_rejects_invalid_file_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, from_data: str, message: str
) -> None:
    """Path-like from_data values that are directories, missing, or unsupported raise MigrationError."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "seeds").mkdir()
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    commands = _sync_commands(tmp_path / "data.db", migrations_dir)

    with pytest.raises(MigrationError, match=message):
        commands.revision("seed", from_data=from_data)
    assert list(migrations_dir.iterdir()) == []


@pytest.mark.parametrize("file_name", ["users.json", "users.json.gz", "users.jsonl", "users.ndjson.gz"])
def test_revision_from_data_reads_supported_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, file_name: str
) -> None:
    """Existing data files, including gzipped and line-delimited JSON, produce replayable UP/DOWN DDL."""
    monkeypatch.chdir(tmp_path)
    rows = [{"id": 1, "email": "alice@example.com", "active": True}, {"id": 2, "email": None, "active": False}]
    if ".jsonl" in file_name or ".ndjson" in file_name:
        payload = "\n".join(to_json(row) for row in rows) + "\n"
    else:
        payload = to_json(rows)
    data_file = tmp_path / file_name
    if file_name.endswith(".gz"):
        with gzip.open(data_file, mode="wt", encoding="utf-8") as handle:
            handle.write(payload)
    else:
        data_file.write_text(payload, encoding="utf-8")
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    created = _sync_commands(tmp_path / "data.db", migrations_dir).revision("seed_users", from_data=file_name)

    up_statements, down_statements = _migration_sections(created)
    with sqlite3.connect(":memory:") as replay:
        _replay(up_statements, replay)
        columns = [row[1] for row in replay.execute('PRAGMA table_info("users")')]
        assert columns == ["id", "email", "active"]
        replay.execute("INSERT INTO users (id, email, active) VALUES (3, NULL, 1)")
        _replay(down_statements, replay)
        assert replay.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)


@pytest.mark.parametrize("target_table", ["users", 'we"ird', "Mixed Case"])
def test_revision_from_data_quotes_target_table_in_up_and_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target_table: str
) -> None:
    """target_table is a literal name: UP creates it and DOWN drops exactly that table, quoting as needed."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "rows.json").write_text(to_json([{"id": 1}]), encoding="utf-8")
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    created = _sync_commands(tmp_path / "data.db", migrations_dir).revision(
        "seed", from_data="rows.json", target_table=target_table
    )

    up_statements, down_statements = _migration_sections(created)
    with sqlite3.connect(":memory:") as replay:
        _replay(up_statements, replay)
        assert [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table'")] == [target_table]
        _replay(down_statements, replay)
        assert replay.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)


@pytest.mark.parametrize(("from_data", "schema"), [("raw_staging_users", None), ("main.raw_staging_users", None)])
def test_revision_from_data_staging_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, from_data: str, schema: "str | None"
) -> None:
    """Plain and schema-qualified table names profile the staging table; DOWN drops the created table."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "raw_staging_users").mkdir()
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    commands = _sync_commands(tmp_path / "staging.db", migrations_dir)
    with commands.config.provide_session() as driver:
        driver.execute("CREATE TABLE raw_staging_users (id TEXT, score TEXT)")
        driver.execute("INSERT INTO raw_staging_users (id, score) VALUES ('10', '98.5')")
        driver.commit()

    created = commands.revision("refine", from_data=from_data, schema=schema, target_table='users"v2')

    up_statements, down_statements = _migration_sections(created)
    with sqlite3.connect(":memory:") as replay:
        _replay(up_statements, replay)
        assert [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table'")] == ['users"v2']
        _replay(down_statements, replay)
        assert replay.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)


@pytest.mark.anyio
async def test_async_revision_from_data_file_and_staging_table(tmp_path: Path) -> None:
    """Async revision(from_data=...) generates replayable DDL from a JSON file or a staging table."""
    db_path = tmp_path / "from_data.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    fixture_file = tmp_path / "seed_users.json"
    fixture_file.write_text(
        to_json([
            {"id": 1, "email": "alice@example.com", "active": True},
            {"id": 2, "email": "bob@example.com", "active": False},
        ]),
        encoding="utf-8",
    )
    config = _async_config(db_path, migrations_dir)
    async with config.provide_session() as driver:
        await driver.execute("CREATE TABLE raw_staging_users (id TEXT, score TEXT)")
        await driver.execute("INSERT INTO raw_staging_users (id, score) VALUES ('10', '98.5')")
        await driver.execute("INSERT INTO raw_staging_users (id, score) VALUES ('20', '87.25')")
        await driver.commit()

    commands = AsyncMigrationCommands(config)
    for source in (fixture_file, "raw_staging_users"):
        created = await commands.revision("bootstrap", from_data=source, target_table="users")
        up_statements, down_statements = _migration_sections(created)
        with sqlite3.connect(":memory:") as replay:
            _replay(up_statements, replay)
            assert [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table'")] == ["users"]
            _replay(down_statements, replay)
            assert replay.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)
        created.unlink()
    await config.close_pool()


def test_upgrade_auto_dump_schema_writes_schema_sql(tmp_path: Path) -> None:
    """upgrade dumps schema.sql when auto_dump_schema is enabled."""
    db_path = tmp_path / "auto_dump.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_accounts.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE accounts (id INTEGER PRIMARY KEY, code TEXT NOT NULL);\n",
        encoding="utf-8",
    )
    _sync_commands(db_path, migrations_dir, auto_dump_schema=True).upgrade(echo=False)

    with sqlite3.connect(":memory:") as replay:
        replay.executescript((migrations_dir / "schema.sql").read_text(encoding="utf-8"))
        assert [row[0] for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table'")] == ["accounts"]


def test_upgrade_auto_dump_failure_does_not_fail_committed_upgrade(tmp_path: Path) -> None:
    """A failing automatic schema dump is reported, but upgrade returns and the migrations stay applied."""
    migrations_dir = tmp_path / "migrations"
    _write_three_migrations(migrations_dir)
    blocker = tmp_path / "not_a_directory"
    blocker.write_text("", encoding="utf-8")
    db_path = tmp_path / "auto_dump_fail.db"
    commands = _sync_commands(db_path, migrations_dir, auto_dump_schema=str(blocker / "schema.sql"))

    with patch("sqlspec.migrations.commands.logger.warning") as warning:
        commands.upgrade(echo=False)

    assert _tracking_rows(db_path) == [("0001", None), ("0002", None), ("0003", None)]
    assert warning.call_count == 1
    assert "automatic schema dump" in warning.call_args.args[0]


@pytest.mark.anyio
async def test_async_upgrade_auto_dump_failure_does_not_fail_committed_upgrade(tmp_path: Path) -> None:
    """Async upgrade keeps applied migrations when the automatic schema dump fails."""
    migrations_dir = tmp_path / "migrations"
    _write_three_migrations(migrations_dir)
    blocker = tmp_path / "not_a_directory"
    blocker.write_text("", encoding="utf-8")
    db_path = tmp_path / "auto_dump_fail_async.db"
    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migrations_dir),
            "enabled": True,
            "auto_dump_schema": str(blocker / "schema.sql"),
        },
    )

    await AsyncMigrationCommands(config).upgrade(echo=False)
    await config.close_pool()

    assert _tracking_rows(db_path) == [("0001", None), ("0002", None), ("0003", None)]


def test_squash_and_fix_use_rich_confirm_ask(tmp_path: Path) -> None:
    """squash and fix ask for confirmation when yes=False and leave files untouched when declined."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    mig_file = migrations_dir / "20251011120000_init.sql"
    mig_file.write_text("-- name: migrate-20251011120000-up\nCREATE TABLE items (id INT);\n", encoding="utf-8")
    commands = _sync_commands(tmp_path / "confirm.db", migrations_dir)

    with patch("rich.prompt.Confirm.ask", return_value=False) as mock_confirm:
        commands.squash(description="test_squash", yes=False, update_database=False)
        mock_confirm.assert_called_once()
        assert mig_file.exists()

    with patch("rich.prompt.Confirm.ask", return_value=False) as mock_confirm:
        commands.fix(yes=False, update_database=False)
        mock_confirm.assert_called_once()
        assert mig_file.exists()
