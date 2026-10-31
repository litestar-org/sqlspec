"""Unit tests for SQLSpec CLI squash, dump-schema, and create-migration options."""

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands


def test_squash_cli_forwards_single_file_and_split_flags(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """sqlspec squash forwards --single-file, --split, and omits single_file when unspecified."""
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.py"
    config_file.write_text(
        """
from sqlspec.adapters.sqlite import SqliteConfig

def get_config():
    return SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": "migrations", "enabled": True},
    )
""",
        encoding="utf-8",
    )

    runner = CliRunner()
    cli_group = add_migration_commands()

    for extra_args, expected_single_file in (([], None), (["--single-file"], True), (["--split"], False)):
        mock_commands = MagicMock()
        with patch("sqlspec.migrations.commands.create_migration_commands", return_value=mock_commands):
            result = runner.invoke(
                cli_group, ["--config", "config.get_config", "squash", "-m", "release", "--yes", *extra_args]
            )
        assert result.exit_code == 0, f"Output: {result.output}"
        mock_commands.squash.assert_called_once()
        _, kwargs = mock_commands.squash.call_args
        assert kwargs.get("single_file") is expected_single_file


def test_create_migration_from_data_table_requires_target_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """create-migration --from-data with a source table name requires --target-table."""
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.py"
    config_file.write_text(
        """
from sqlspec.adapters.sqlite import SqliteConfig

def get_config():
    return SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": "migrations", "enabled": True},
    )
""",
        encoding="utf-8",
    )

    runner = CliRunner()
    cli_group = add_migration_commands()
    mock_commands = MagicMock()

    with patch("sqlspec.migrations.commands.create_migration_commands", return_value=mock_commands):
        missing_target = runner.invoke(
            cli_group,
            ["--config", "config.get_config", "create-migration", "-m", "seed_users", "--from-data", "staging_users"],
        )
        assert missing_target.exit_code != 0
        cleaned_output = _strip_ansi(missing_target.output)
        assert "--target-table is required when --from-data specifies a source table name" in cleaned_output
        mock_commands.revision.assert_not_called()

        with_target = runner.invoke(
            cli_group,
            [
                "--config",
                "config.get_config",
                "create-migration",
                "-m",
                "seed_users",
                "--from-data",
                "staging_users",
                "--target-table",
                "users",
            ],
        )
        assert with_target.exit_code == 0, f"Output: {with_target.output}"
        mock_commands.revision.assert_called_once()

    mock_commands.reset_mock()
    with patch("sqlspec.migrations.commands.create_migration_commands", return_value=mock_commands):
        from_file = runner.invoke(
            cli_group,
            ["--config", "config.get_config", "create-migration", "-m", "seed_users", "--from-data", "users.jsonl"],
        )
        assert from_file.exit_code == 0, f"Output: {from_file.output}"
        mock_commands.revision.assert_called_once()


def test_dump_schema_multi_config_rejects_output_file_and_keeps_stdout_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """dump-schema across multiple configs rejects -o FILE and routes status output to stderr."""
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "multi_config.py"
    config_file.write_text(
        """
from sqlspec.adapters.sqlite import SqliteConfig

def get_configs():
    return [
        SqliteConfig(
            bind_key="primary",
            connection_config={"database": ":memory:"},
            migration_config={"script_location": "migrations_primary", "enabled": True},
        ),
        SqliteConfig(
            bind_key="analytics",
            connection_config={"database": ":memory:"},
            migration_config={"script_location": "migrations_analytics", "enabled": True},
        ),
    ]
""",
        encoding="utf-8",
    )

    runner = CliRunner()
    cli_group = add_migration_commands()

    mock_commands = MagicMock()
    mock_commands.dump_schema.return_value = "CREATE TABLE items (id INTEGER PRIMARY KEY);\n"

    with patch("sqlspec.migrations.commands.create_migration_commands", return_value=mock_commands):
        rejected = runner.invoke(
            cli_group, ["--config", "multi_config.get_configs", "dump-schema", "-o", str(tmp_path / "schema.sql")]
        )
        assert rejected.exit_code != 0
        cleaned_output = _strip_ansi(rejected.output)
        assert "Cannot use a single --output file" in cleaned_output
        mock_commands.dump_schema.assert_not_called()

        succeeded = runner.invoke(cli_group, ["--config", "multi_config.get_configs", "dump-schema"])
        assert succeeded.exit_code == 0, f"Output: {succeeded.output}"
        assert succeeded.stdout == (
            "CREATE TABLE items (id INTEGER PRIMARY KEY);\nCREATE TABLE items (id INTEGER PRIMARY KEY);\n"
        )
        cleaned_stderr = _strip_ansi(succeeded.stderr)
        assert "Dumping schema for configuration: primary" in cleaned_stderr
        assert "Dumping schema for configuration: analytics" in cleaned_stderr


ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from output text.

    Args:
        text: Input string with ANSI escape codes.

    Returns:
        String with ANSI escape codes removed.
    """
    return ANSI_ESCAPE_PATTERN.sub("", text)
