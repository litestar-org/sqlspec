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


def test_adk_memory_verify_quotes_qualified_keyword_table(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """sqlspec adk memory verify quotes database.table SQL keyword identifiers for sync and async configs."""
    pytest.importorskip("google.adk")
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "adk_verify_config.py"
    config_file.write_text(
        """
from contextlib import asynccontextmanager, contextmanager
from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.pymysql import PyMysqlConfig

EXECUTED_SQL = []


class _RecordingSyncDriver:
    dialect = "mysql"

    def execute(self, sql):
        EXECUTED_SQL.append(("sync", sql))


class _RecordingAsyncDriver:
    dialect = "mysql"

    async def execute(self, sql):
        EXECUTED_SQL.append(("async", sql))


class RecordingPyMysqlConfig(PyMysqlConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _RecordingSyncDriver()


class RecordingAiomysqlConfig(AiomysqlConfig):
    @asynccontextmanager
    async def provide_session(self, *args, **kwargs):
        yield _RecordingAsyncDriver()


def get_configs():
    return [
        RecordingPyMysqlConfig(
            bind_key="sync_mysql",
            connection_config={"host": "localhost", "user": "root", "database": "test"},
            extension_config={"adk": {"memory_table": "order.adk_memory", "enable_memory": True}},
        ),
        RecordingAiomysqlConfig(
            bind_key="async_mysql",
            connection_config={"host": "localhost", "user": "root", "db": "test"},
            extension_config={"adk": {"memory_table": "order.adk_memory", "enable_memory": True}},
        ),
    ]
""",
        encoding="utf-8",
    )

    import importlib

    runner = CliRunner()
    cli_group = add_migration_commands()
    result = runner.invoke(cli_group, ["--config", "adk_verify_config.get_configs", "adk", "memory", "verify"])
    assert result.exit_code == 0, f"Output: {result.output}"
    cleaned = _strip_ansi(result.output)
    assert "sync_mysql: memory table reachable" in cleaned
    assert "async_mysql: memory table reachable" in cleaned

    cfg_mod = importlib.import_module("adk_verify_config")
    assert cfg_mod.EXECUTED_SQL == [
        ("sync", "SELECT 1 FROM `order`.`adk_memory` WHERE 1 = 0"),
        ("async", "SELECT 1 FROM `order`.`adk_memory` WHERE 1 = 0"),
    ]


_ADK_VERIFY_CASES: tuple[tuple[str, str, str], ...] = (
    (
        "mysql_two_part_keyword",
        """
from contextlib import contextmanager
from sqlspec.adapters.pymysql import PyMysqlConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "mysql"

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(PyMysqlConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"host": "localhost", "user": "root", "database": "test"},
        extension_config={"adk": {"memory_table": "order.adk_memory", "enable_memory": True}},
    )
""",
        "SELECT 1 FROM `order`.`adk_memory` WHERE 1 = 0",
    ),
    (
        "bigquery_three_part",
        """
from contextlib import contextmanager
from sqlglot.dialects.bigquery import BigQuery
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.core import StatementConfig

EXECUTED_SQL = []


class _PassThroughMemoryStore:
    def __init__(self, config):
        self.config = config
        self.memory_table = config.extension_config["adk"]["memory_table"]


class _Driver:
    dialect = None
    statement_config = StatementConfig(dialect=BigQuery)

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(BigQueryConfig):
    adk_memory_store_class = _PassThroughMemoryStore

    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"project": "proj", "dataset_id": "ds"},
        extension_config={"adk": {"memory_table": "proj.ds.adk_memory", "enable_memory": True}},
    )
""",
        "SELECT 1 FROM `proj`.`ds`.`adk_memory` WHERE 1 = 0",
    ),
    (
        "spanner_googlesql",
        """
from contextlib import contextmanager
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.core import StatementConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "spanner"
    statement_config = StatementConfig(dialect="spanner")

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(SpannerSyncConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        "SELECT 1 FROM `adk_memory` WHERE 1 = 0",
    ),
    (
        "spanner_postgresql_spangres",
        """
from contextlib import contextmanager
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.core import StatementConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "spanner"
    statement_config = StatementConfig(dialect="spangres")

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(SpannerSyncConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        statement_config=StatementConfig(dialect="spangres"),
        extension_config={"adk": {"memory_table": "Adk_Memory", "enable_memory": True}},
    )
""",
        'SELECT 1 FROM "adk_memory" WHERE 1 = 0',
    ),
    (
        "oracle_uppercase",
        """
from contextlib import contextmanager
from sqlspec.adapters.oracledb import OracleSyncConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "oracle"

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(OracleSyncConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"dsn": "localhost/freepdb1"},
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        'SELECT 1 FROM "ADK_MEMORY" WHERE 1 = 0',
    ),
    (
        "db2_uppercase",
        """
from contextlib import contextmanager
from sqlspec.adapters.db2 import Db2SyncConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "db2"

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(Db2SyncConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"dsn": "DATABASE=testdb;HOSTNAME=localhost;PORT=50000;"},
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        'SELECT 1 FROM "ADK_MEMORY" WHERE 1 = 0',
    ),
    (
        "adbc_snowflake_dialect_name",
        """
from contextlib import contextmanager
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.core import StatementConfig

EXECUTED_SQL = []


class _Driver:
    _dialect_name = "snowflake"
    dialect = None
    statement_config = StatementConfig(dialect=None)

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(AdbcConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"driver_name": "libadbc_driver_unknown.so"},
        statement_config=StatementConfig(dialect=None),
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        'SELECT 1 FROM "ADK_MEMORY" WHERE 1 = 0',
    ),
    (
        "postgres_dialect_instance_mixed_case",
        """
from contextlib import asynccontextmanager
from sqlglot.dialects.postgres import Postgres
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.core import StatementConfig

EXECUTED_SQL = []


class _Driver:
    dialect = None
    statement_config = StatementConfig(dialect=Postgres())

    async def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(AsyncpgConfig):
    @asynccontextmanager
    async def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"dsn": "postgresql://localhost/test"},
        extension_config={"adk": {"memory_table": "AdkMemory", "enable_memory": True}},
    )
""",
        'SELECT 1 FROM "adkmemory" WHERE 1 = 0',
    ),
    (
        "pgvector_mixed_case",
        """
from contextlib import asynccontextmanager
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.core import StatementConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "postgres"
    statement_config = StatementConfig(dialect="pgvector")

    async def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(AsyncpgConfig):
    @asynccontextmanager
    async def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"dsn": "postgresql://localhost/test"},
        statement_config=StatementConfig(dialect="pgvector"),
        extension_config={"adk": {"memory_table": "AdkMemory", "enable_memory": True}},
    )
""",
        'SELECT 1 FROM "adkmemory" WHERE 1 = 0',
    ),
    (
        "mssql_python_one_part",
        """
from contextlib import contextmanager
from sqlspec.adapters.mssql_python import MssqlPythonConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "tsql"

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(MssqlPythonConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"connection_string": "Server=localhost;Database=master;"},
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        "SELECT 1 FROM [dbo].[adk_memory] WHERE 1 = 0",
    ),
    (
        "mssql_python_bracketed_two_part",
        """
from contextlib import contextmanager
from sqlspec.adapters.mssql_python import MssqlPythonConfig

EXECUTED_SQL = []


class _PassThroughMemoryStore:
    def __init__(self, config):
        self.config = config
        self.memory_table = config.extension_config["adk"]["memory_table"]


class _Driver:
    dialect = "tsql"

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(MssqlPythonConfig):
    adk_memory_store_class = _PassThroughMemoryStore

    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"connection_string": "Server=localhost;Database=master;"},
        extension_config={"adk": {"memory_table": "[auth].[mem]", "enable_memory": True}},
    )
""",
        "SELECT 1 FROM [auth].[mem] WHERE 1 = 0",
    ),
    (
        "arrow_odbc_tsql_one_part",
        """
from contextlib import contextmanager
from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.core import StatementConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "tsql"
    statement_config = StatementConfig(dialect="tsql")

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(ArrowOdbcConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"connection_string": "Driver={ODBC Driver 18};Server=localhost;"},
        statement_config=StatementConfig(dialect="tsql"),
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        "SELECT 1 FROM [dbo].[adk_memory] WHERE 1 = 0",
    ),
    (
        "adbc_mssql_one_part",
        """
from contextlib import contextmanager
from sqlspec.adapters.adbc import AdbcConfig

EXECUTED_SQL = []


class _Driver:
    _dialect_name = "mssql"
    dialect = "mssql"

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(AdbcConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"uri": "mssql://localhost:1433/master"},
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        "SELECT 1 FROM [adk_memory] WHERE 1 = 0",
    ),
    (
        "sybase_one_part",
        """
from contextlib import contextmanager
from sqlspec.adapters.sybase import SybaseConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "sybase"

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(SybaseConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"host": "localhost", "database": "master"},
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        "SELECT 1 FROM [dbo].[adk_memory] WHERE 1 = 0",
    ),
    (
        "sqlanywhere_one_part",
        """
from contextlib import contextmanager
from sqlspec.adapters.sqlanywhere import SqlanywhereConfig

EXECUTED_SQL = []


class _Driver:
    dialect = "sqlanywhere"

    def execute(self, sql):
        EXECUTED_SQL.append(sql)


class _Config(SqlanywhereConfig):
    @contextmanager
    def provide_session(self, *args, **kwargs):
        yield _Driver()


def get_configs():
    return _Config(
        bind_key="target",
        connection_config={"host": "localhost", "dbn": "test"},
        extension_config={"adk": {"memory_table": "adk_memory", "enable_memory": True}},
    )
""",
        'SELECT 1 FROM "DBA"."adk_memory" WHERE 1 = 0',
    ),
)


@pytest.mark.parametrize(
    ("case_id", "config_body", "expected_sql"), _ADK_VERIFY_CASES, ids=[case[0] for case in _ADK_VERIFY_CASES]
)
def test_adk_memory_verify_dialect_and_schema_quoting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case_id: str, config_body: str, expected_sql: str
) -> None:
    """sqlspec adk memory verify resolves runtime dialects, folds case, and qualifies 1-part default schemas."""
    pytest.importorskip("google.adk")
    monkeypatch.chdir(tmp_path)
    module_name = f"verify_{case_id}"
    (tmp_path / f"{module_name}.py").write_text(config_body, encoding="utf-8")

    import importlib

    runner = CliRunner()
    cli_group = add_migration_commands()
    result = runner.invoke(cli_group, ["--config", f"{module_name}.get_configs", "adk", "memory", "verify"])
    assert result.exit_code == 0, f"Output: {result.output}"
    cleaned = _strip_ansi(result.output)
    assert "target: memory table reachable" in cleaned

    cfg_mod = importlib.import_module(module_name)
    assert cfg_mod.EXECUTED_SQL == [expected_sql]


ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from output text.

    Args:
        text: Input string with ANSI escape codes.

    Returns:
        String with ANSI escape codes removed.
    """
    return ANSI_ESCAPE_PATTERN.sub("", text)
