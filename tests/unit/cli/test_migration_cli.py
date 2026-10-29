"""Unit tests for Click CLI migration enhancements (squash, dump-schema, verify, create-migration, upgrade, fix)."""

from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import CliRunner

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.cli import add_migration_commands
from sqlspec.exceptions import MigrationError


def test_squash_optional_range_and_message_with_no_prompt() -> None:
    """sqlspec squash --no-prompt works without VERSION_RANGE or -m and forwards None to commands.squash."""
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
    )


def test_squash_with_explicit_range_message_and_strategy() -> None:
    """sqlspec squash accepts timestamp range, -m, --strategy ddl, and -y/--yes."""
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
                "ddl",
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


def test_dump_schema_command(tmp_path: Path) -> None:
    """sqlspec dump-schema exports database schema DDL with output and dialect options."""
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
    create_commands.return_value.dump_schema.assert_called_once_with(
        output_path=schema_file,
        schema="main",
        include_migrations_table=True,
        prefer_native=False,
        target_dialect="postgres",
        echo=False,
    )


def test_verify_command_strict_and_failure_exit_code() -> None:
    """sqlspec verify --strict succeeds when valid and exits non-zero on MigrationError or pending migrations."""
    config = SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    with (
        patch("sqlspec.cli.resolve_config_sync", return_value=config),
        patch("sqlspec.migrations.commands.create_migration_commands") as create_commands,
    ):
        create_commands.return_value.verify = Mock(
            return_value={
                "valid": True,
                "checksum_mismatches": [],
                "orphaned_records": [],
                "pending_versions": [],
                "partial_squash_errors": [],
            }
        )
        ok_result = CliRunner().invoke(add_migration_commands(), ["--config", "test.config", "verify", "--strict"])
        assert ok_result.exit_code == 0, ok_result.output
        create_commands.return_value.verify.assert_called_once_with(strict=True, use_logger=False, echo=None)

        create_commands.return_value.verify = Mock(side_effect=MigrationError("Verification failed: checksum mismatch"))
        fail_result = CliRunner().invoke(add_migration_commands(), ["--config", "test.config", "verify", "--strict"])
        assert fail_result.exit_code != 0


def test_create_migration_from_schema_and_from_data() -> None:
    """sqlspec create-migration forwards --from-schema, --from-data, --schema, and --target-table."""
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
            message="initial", file_type=None, from_schema=True, schema="public"
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
                "--from-data",
                "users.json",
                "--target-table",
                "users",
            ],
        )
        assert res_data.exit_code == 0, res_data.output
        create_commands.return_value.revision.assert_called_once_with(
            message="seed_users", file_type=None, from_data="users.json", target_table="users"
        )


def test_upgrade_allow_missing_and_fix_flags() -> None:
    """sqlspec upgrade --allow-missing and sqlspec fix --no-prompt forward expected flags."""
    config = SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    with (
        patch("sqlspec.cli.resolve_config_sync", return_value=config),
        patch("sqlspec.migrations.commands.create_migration_commands") as create_commands,
    ):
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
