"""Click CLI migration tests: option forwarding, byte-exact dump-schema output, and real exit codes."""

import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.cli import add_migration_commands
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.serializers import to_json

USERS_UP = (
    "-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n"
    "INSERT INTO users (id, name) VALUES (1, 'seed');\n\n"
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


def _sqlite_config(db_path: Path, migrations_dir: Path) -> SqliteConfig:
    return SqliteConfig(
        bind_key="primary",
        connection_config={"database": str(db_path)},
        migration_config={"enabled": True, "script_location": str(migrations_dir)},
    )


def _invoke(config: object, *args: str) -> "object":
    with patch("sqlspec.cli.resolve_config_sync", return_value=config):
        return CliRunner().invoke(add_migration_commands(), ["--config", "test.config", *args])


def _write_migrations(migrations_dir: Path) -> None:
    migrations_dir.mkdir(exist_ok=True)
    (migrations_dir / "0001_users.sql").write_text(USERS_UP, encoding="utf-8")
    (migrations_dir / "0002_email.sql").write_text(EMAIL_UP, encoding="utf-8")
    (migrations_dir / "0003_posts.sql").write_text(POSTS_UP, encoding="utf-8")


def _tracking_rows(db_path: Path) -> "list[tuple[str, str | None]]":
    with sqlite3.connect(db_path) as connection:
        return connection.execute("SELECT version_num, replaces FROM ddl_migrations ORDER BY version_num").fetchall()


def test_squash_optional_range_and_message_with_no_prompt() -> None:
    """squash --no-prompt forwards every option, with the concat strategy by default."""
    config = SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    with (
        patch("sqlspec.cli.resolve_config_sync", return_value=config),
        patch("sqlspec.migrations.commands.create_migration_commands") as create_commands,
    ):
        result = CliRunner().invoke(
            add_migration_commands(), ["--config", "test.config", "squash", "--no-prompt", "--no-database"]
        )

    assert result.exit_code == 0, result.output
    create_commands.return_value.squash.assert_called_once_with(
        start_version=None,
        end_version=None,
        description=None,
        dry_run=False,
        update_database=False,
        yes=True,
        allow_gaps=False,
        output_format="sql",
        strategy="concat",
    )


def test_squash_with_explicit_range_message_and_strategy() -> None:
    """squash accepts a timestamp range, -m, --strategy ddl, and --yes."""
    config = SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    with (
        patch("sqlspec.cli.resolve_config_sync", return_value=config),
        patch("sqlspec.migrations.commands.create_migration_commands") as create_commands,
    ):
        result = CliRunner().invoke(
            add_migration_commands(),
            [
                "--config",
                "test.config",
                "squash",
                "20251011120000:20251012153000",
                "-m",
                "baseline",
                "--strategy",
                "DDL",
                "--yes",
            ],
        )

    assert result.exit_code == 0, result.output
    create_commands.return_value.squash.assert_called_once_with(
        start_version="20251011120000",
        end_version="20251012153000",
        description="baseline",
        dry_run=False,
        update_database=True,
        yes=True,
        allow_gaps=False,
        output_format="sql",
        strategy="ddl",
    )


def test_squash_failure_exits_non_zero_and_leaves_files(tmp_path: Path) -> None:
    """A squash that raises SquashValidationError exits 1 with the message and restores every migration file."""
    migrations_dir = tmp_path / "migrations"
    _write_migrations(migrations_dir)
    db_path = tmp_path / "partial.db"
    SyncMigrationCommands(_sqlite_config(db_path, migrations_dir)).upgrade(revision="0002", echo=False)
    before = {path.name: path.read_bytes() for path in migrations_dir.iterdir()}

    result = _invoke(_sqlite_config(db_path, migrations_dir), "squash", "--yes")

    assert result.exit_code == 1
    assert "Partial squash state detected for 0001" in result.output
    assert {path.name: path.read_bytes() for path in migrations_dir.iterdir()} == before
    assert _tracking_rows(db_path) == [("0001", None), ("0002", None)]


def test_squash_ddl_refusal_exits_non_zero_without_touching_files(tmp_path: Path) -> None:
    """squash --strategy ddl refuses DML followed by ALTER on the same table: exit 1, message, no file changes."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_users.sql").write_text(USERS_UP, encoding="utf-8")
    (migrations_dir / "0002_email.sql").write_text(EMAIL_UP, encoding="utf-8")
    before = {path.name: path.read_bytes() for path in migrations_dir.iterdir()}

    result = _invoke(
        _sqlite_config(tmp_path / "ddl.db", migrations_dir), "squash", "--yes", "--strategy", "ddl", "--no-database"
    )

    assert result.exit_code == 1
    assert "Cannot fold migration 0002" in result.output
    assert {path.name: path.read_bytes() for path in migrations_dir.iterdir()} == before


def test_squash_default_strategy_round_trip_through_cli(tmp_path: Path) -> None:
    """squash --yes with the default strategy rewrites files and collapses tracking rows on a fully applied DB."""
    migrations_dir = tmp_path / "migrations"
    _write_migrations(migrations_dir)
    db_path = tmp_path / "full.db"
    SyncMigrationCommands(_sqlite_config(db_path, migrations_dir)).upgrade(echo=False)

    result = _invoke(_sqlite_config(db_path, migrations_dir), "squash", "--yes")

    assert result.exit_code == 0, result.output
    assert sorted(path.name for path in migrations_dir.glob("*.sql")) == ["0001_initial_schema.sql"]
    assert _tracking_rows(db_path) == [("0001", "0001,0002,0003")]


def test_dump_schema_command_forwards_options(tmp_path: Path) -> None:
    """dump-schema forwards every option to commands.dump_schema."""
    schema_file = tmp_path / "schema.sql"
    config = SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": str(tmp_path / "migrations")},
    )
    with (
        patch("sqlspec.cli.resolve_config_sync", return_value=config),
        patch("sqlspec.migrations.commands.create_migration_commands") as create_commands,
    ):
        create_commands.return_value.dump_schema = Mock(return_value="CREATE TABLE t (id INT);\n")
        result = CliRunner().invoke(
            add_migration_commands(),
            [
                "--config",
                "test.config",
                "dump-schema",
                "-o",
                str(schema_file),
                "--schema",
                "main",
                "--include-migrations-table",
                "--synthesize",
                "--target-dialect",
                "postgres",
                "--no-echo",
            ],
        )

    assert result.exit_code == 0, result.output
    assert result.stdout == ""
    create_commands.return_value.dump_schema.assert_called_once_with(
        output_path=schema_file,
        schema="main",
        include_migrations_table=True,
        prefer_native=False,
        target_dialect="postgres",
        echo=False,
    )


@pytest.mark.parametrize("async_config", [False, True])
def test_dump_schema_stdout_is_byte_identical_to_dump_schema(tmp_path: Path, async_config: bool) -> None:
    """dump-schema stdout keeps Rich-like markup and long lines exactly as dump_schema() renders them."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    long_columns = ", ".join(f"column_number_{index:02d} TEXT" for index in range(8))
    (migrations_dir / "0001_markup.sql").write_text(
        "-- name: migrate-0001-up\n"
        f"CREATE TABLE notes (id INTEGER PRIMARY KEY, name TEXT CHECK (name <> '[bold]x[/bold]'), {long_columns});\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "markup.db"
    commands = SyncMigrationCommands(_sqlite_config(db_path, migrations_dir))
    commands.upgrade(echo=False)
    expected = commands.dump_schema(echo=False)
    assert "[bold]x[/bold]" in expected
    assert max(len(line) for line in expected.splitlines()) > 120

    config: object = _sqlite_config(db_path, migrations_dir)
    if async_config:
        config = AiosqliteConfig(
            bind_key="primary",
            connection_config={"database": str(db_path)},
            migration_config={"enabled": True, "script_location": str(migrations_dir)},
        )
    result = _invoke(config, "dump-schema")

    assert result.exit_code == 0, result.output
    assert result.stdout == expected


def test_verify_command_strict_and_fail_on_pending_exit_codes(tmp_path: Path) -> None:
    """verify exits 0 when clean, and non-zero for --fail-on-pending with pending work or --strict with drift."""
    migrations_dir = tmp_path / "migrations"
    _write_migrations(migrations_dir)
    db_path = tmp_path / "verify.db"
    sqlite3.connect(db_path).close()

    pending_strict = _invoke(_sqlite_config(db_path, migrations_dir), "verify", "--strict")
    assert pending_strict.exit_code == 0, pending_strict.output
    pending_gate = _invoke(_sqlite_config(db_path, migrations_dir), "verify", "--fail-on-pending")
    assert pending_gate.exit_code == 1
    assert "pending migrations [0001, 0002, 0003]" in pending_gate.output
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)

    SyncMigrationCommands(_sqlite_config(db_path, migrations_dir)).upgrade(echo=False)
    clean = _invoke(_sqlite_config(db_path, migrations_dir), "verify", "--strict", "--fail-on-pending")
    assert clean.exit_code == 0, clean.output

    (migrations_dir / "0002_email.sql").write_text(EMAIL_UP + "\n-- edited\n", encoding="utf-8")
    drifted = _invoke(_sqlite_config(db_path, migrations_dir), "verify", "--strict")
    assert drifted.exit_code == 1
    assert "checksum mismatch in [0002]" in drifted.output
    lenient = _invoke(_sqlite_config(db_path, migrations_dir), "verify")
    assert lenient.exit_code == 0


def test_verify_command_forwards_options() -> None:
    """verify forwards strict, fail_on_pending, use_logger, and echo, and maps MigrationError to exit 1."""
    config = SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    with (
        patch("sqlspec.cli.resolve_config_sync", return_value=config),
        patch("sqlspec.migrations.commands.create_migration_commands") as create_commands,
    ):
        ok_result = CliRunner().invoke(add_migration_commands(), ["--config", "test.config", "verify", "--strict"])
        assert ok_result.exit_code == 0, ok_result.output
        create_commands.return_value.verify.assert_called_once_with(
            strict=True, fail_on_pending=False, use_logger=False, echo=None
        )

        create_commands.return_value.verify = Mock(side_effect=MigrationError("Verification failed: checksum mismatch"))
        fail_result = CliRunner().invoke(add_migration_commands(), ["--config", "test.config", "verify", "--strict"])
        assert fail_result.exit_code == 1
        assert "Verification failed: checksum mismatch" in fail_result.output


def test_create_migration_forwards_all_options() -> None:
    """create-migration forwards every option explicitly, including defaults."""
    config = SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    with (
        patch("sqlspec.cli.resolve_config_sync", return_value=config),
        patch("sqlspec.migrations.commands.create_migration_commands") as create_commands,
    ):
        res_schema = CliRunner().invoke(
            add_migration_commands(),
            ["--config", "test.config", "create-migration", "-m", "initial", "--from-schema", "--schema", "public"],
        )
        assert res_schema.exit_code == 0, res_schema.output
        create_commands.return_value.revision.assert_called_once_with(
            message="initial", file_type=None, from_schema=True, from_data=None, schema="public", target_table=None
        )

        create_commands.return_value.revision.reset_mock()
        res_data = CliRunner().invoke(
            add_migration_commands(),
            [
                "--config",
                "test.config",
                "create-migration",
                "-m",
                "seed_users",
                "--format",
                "py",
                "--from-data",
                "users.json",
                "--target-table",
                "users",
            ],
        )
        assert res_data.exit_code == 0, res_data.output
        create_commands.return_value.revision.assert_called_once_with(
            message="seed_users",
            file_type="py",
            from_schema=False,
            from_data="users.json",
            schema=None,
            target_table="users",
        )


@pytest.mark.parametrize(
    ("from_data", "message"),
    [
        ("staging/", "is a directory"),
        ("seeds/users.csv", "Unsupported data file 'seeds/users.csv'"),
        ("public.json", "Data file not found: public.json"),
    ],
)
def test_create_migration_from_data_errors_exit_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, from_data: str, message: str
) -> None:
    """Invalid --from-data paths exit 1 with a message and no traceback, and create no migration file."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "staging").mkdir()
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    result = _invoke(
        _sqlite_config(tmp_path / "data.db", migrations_dir),
        "create-migration",
        "-m",
        "seed",
        "--from-data",
        from_data,
        "--no-prompt",
    )

    assert result.exit_code == 1
    assert message in result.output
    assert "Traceback" not in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert list(migrations_dir.iterdir()) == []


def test_create_migration_from_data_file_through_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """create-migration --from-data with an existing JSON file writes a migration whose UP/DOWN replay cleanly."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "users.json").write_text(to_json([{"id": 1, "email": "a@example.com"}]), encoding="utf-8")
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    result = _invoke(
        _sqlite_config(tmp_path / "data.db", migrations_dir),
        "create-migration",
        "-m",
        "seed",
        "--from-data",
        "users.json",
        "--target-table",
        'app"users',
        "--no-prompt",
    )

    assert result.exit_code == 0, result.output
    created = next(migrations_dir.glob("*_seed.sql"))
    content = created.read_text(encoding="utf-8")
    up_body, down_body = (part.split("\n", 1)[1] for part in content.split("-- name: migrate-")[1:])
    with sqlite3.connect(":memory:") as replay:
        replay.executescript(up_body)
        assert replay.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [('app"users',)]
        replay.executescript(down_body)
        assert replay.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)


def test_upgrade_forwards_all_options() -> None:
    """upgrade forwards allow_missing even when it is the default, and fix forwards its flags."""
    config = SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    with (
        patch("sqlspec.cli.resolve_config_sync", return_value=config),
        patch("sqlspec.migrations.commands.create_migration_commands") as create_commands,
    ):
        default_res = CliRunner().invoke(
            add_migration_commands(), ["--config", "test.config", "upgrade", "--no-prompt"]
        )
        assert default_res.exit_code == 0, default_res.output
        create_commands.return_value.upgrade.assert_called_once_with(
            revision="head",
            allow_missing=False,
            auto_sync=True,
            dry_run=False,
            use_logger=False,
            echo=None,
            summary_only=None,
        )

        create_commands.return_value.upgrade.reset_mock()
        up_res = CliRunner().invoke(
            add_migration_commands(), ["--config", "test.config", "upgrade", "--allow-missing", "--no-prompt"]
        )
        assert up_res.exit_code == 0, up_res.output
        create_commands.return_value.upgrade.assert_called_once_with(
            revision="head",
            allow_missing=True,
            auto_sync=True,
            dry_run=False,
            use_logger=False,
            echo=None,
            summary_only=None,
        )

        fix_res = CliRunner().invoke(
            add_migration_commands(), ["--config", "test.config", "fix", "--no-prompt", "--no-echo", "--use-logger"]
        )
        assert fix_res.exit_code == 0, fix_res.output
        create_commands.return_value.fix.assert_called_once_with(dry_run=False, update_database=True, yes=True)


def test_upgrade_dry_run_through_cli_leaves_tracking_rows(tmp_path: Path) -> None:
    """upgrade --dry-run on a fully applied pre-squash database exits 0 and changes no tracking rows."""
    migrations_dir = tmp_path / "migrations"
    _write_migrations(migrations_dir)
    db_path = tmp_path / "full.db"
    SyncMigrationCommands(_sqlite_config(db_path, migrations_dir)).upgrade(echo=False)
    SyncMigrationCommands(_sqlite_config(tmp_path / "author.db", migrations_dir)).squash(
        yes=True, update_database=False
    )

    result = _invoke(_sqlite_config(db_path, migrations_dir), "upgrade", "--dry-run", "--no-prompt")

    assert result.exit_code == 0, result.output
    assert "Would reconcile squashed migration" in result.output
    assert _tracking_rows(db_path) == [("0001", None), ("0002", None), ("0003", None)]


def test_upgrade_no_auto_sync_on_partial_squash_exits_non_zero(tmp_path: Path) -> None:
    """upgrade --no-auto-sync on a partially applied pre-squash database exits 1 instead of reporting success."""
    migrations_dir = tmp_path / "migrations"
    _write_migrations(migrations_dir)
    db_path = tmp_path / "partial.db"
    SyncMigrationCommands(_sqlite_config(db_path, migrations_dir)).upgrade(revision="0002", echo=False)
    SyncMigrationCommands(_sqlite_config(tmp_path / "author.db", migrations_dir)).squash(
        yes=True, update_database=False
    )

    result = _invoke(_sqlite_config(db_path, migrations_dir), "upgrade", "--no-auto-sync", "--no-prompt")

    assert result.exit_code == 1
    assert "Cannot reconcile squashed migration 0001" in result.output
    assert "Already at latest version" not in result.output
    assert _tracking_rows(db_path) == [("0001", None), ("0002", None)]
