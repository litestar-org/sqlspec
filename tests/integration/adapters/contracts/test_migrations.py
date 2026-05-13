# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
"""Cross-adapter migration workflow contract tests."""

import contextlib
import inspect
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias, cast
from uuid import uuid4

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
from sqlspec.adapters.mysqlconnector import default_statement_config as mysqlconnector_statement_config
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.pymysql import PyMysqlConfig
from sqlspec.adapters.pymysql import default_statement_config as pymysql_statement_config
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands, create_migration_commands

SyncMigrationConfig: TypeAlias = (
    SqliteConfig
    | AdbcConfig
    | DuckDBConfig
    | MysqlConnectorSyncConfig
    | PyMysqlConfig
    | PsycopgSyncConfig
    | OracleSyncConfig
)
AsyncMigrationConfig: TypeAlias = (
    AiosqliteConfig
    | AiomysqlConfig
    | AsyncmyConfig
    | MysqlConnectorAsyncConfig
    | AsyncpgConfig
    | PsqlpyConfig
    | PsycopgAsyncConfig
    | OracleAsyncConfig
)
MigrationConfig: TypeAlias = SyncMigrationConfig | AsyncMigrationConfig
MigrationCommands: TypeAlias = SyncMigrationCommands[SyncMigrationConfig] | AsyncMigrationCommands[AsyncMigrationConfig]


@dataclass(frozen=True)
class MigrationDialect:
    """Dialect-specific SQL fragments for the shared migration workflow."""

    kind: str
    id_column: str
    text_column: str
    created_at_column: str
    users_insert_columns: str
    users_insert_values: str
    users_insert_params: tuple[Any, ...]
    posts_insert_columns: str
    posts_insert_values: str
    posts_insert_params: tuple[Any, ...]
    drop_table: str
    bad_sql: str
    table_exists_sql: str
    all_tables_sql: str
    customer_insert_columns: str
    customer_insert_values: str
    customer_insert_params: tuple[Any, ...]
    rollback_insert_values: str
    rollback_insert_params: tuple[Any, ...]


@dataclass(frozen=True)
class MigrationCase:
    """Per-adapter migration contract setup."""

    adapter: str
    dialect: MigrationDialect
    is_async: bool = False
    sqlite_file: str | None = None


SQLITE = MigrationDialect(
    kind="sqlite",
    id_column="INTEGER PRIMARY KEY AUTOINCREMENT",
    text_column="TEXT",
    created_at_column="TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    users_insert_columns="name, email",
    users_insert_values="?, ?",
    users_insert_params=("John Doe", "john@example.com"),
    posts_insert_columns="title, content, user_id",
    posts_insert_values="?, ?, ?",
    posts_insert_params=("Test Post", "This is a test post", 1),
    drop_table="DROP TABLE IF EXISTS {table}",
    bad_sql="CREATE THAT TABLE invalid_sql",
    table_exists_sql="SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'",
    all_tables_sql="SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    customer_insert_columns="name",
    customer_insert_values="'Customer 1'), ('Customer 2'",
    customer_insert_params=(),
    rollback_insert_values="?",
    rollback_insert_params=("Rollback User",),
)

DUCKDB = MigrationDialect(
    kind="duckdb",
    id_column="INTEGER PRIMARY KEY",
    text_column="VARCHAR",
    created_at_column="TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    users_insert_columns="id, name, email",
    users_insert_values="?, ?, ?",
    users_insert_params=(1, "John Doe", "john@example.com"),
    posts_insert_columns="id, title, content, user_id",
    posts_insert_values="?, ?, ?, ?",
    posts_insert_params=(1, "Test Post", "This is a test post", 1),
    drop_table="DROP TABLE IF EXISTS {table}",
    bad_sql="CREATE BIG_TABLE invalid_sql",
    table_exists_sql="SELECT table_name FROM information_schema.tables WHERE table_name = '{table}'",
    all_tables_sql=(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name NOT LIKE 'sqlspec_%'"
    ),
    customer_insert_columns="id, name",
    customer_insert_values="1, 'Customer 1'), (2, 'Customer 2'",
    customer_insert_params=(),
    rollback_insert_values="?, ?",
    rollback_insert_params=(3, "Rollback User"),
)

MYSQL = MigrationDialect(
    kind="mysql",
    id_column="INT AUTO_INCREMENT PRIMARY KEY",
    text_column="VARCHAR(255)",
    created_at_column="TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    users_insert_columns="name, email",
    users_insert_values="%s, %s",
    users_insert_params=("John Doe", "john@example.com"),
    posts_insert_columns="title, content, user_id",
    posts_insert_values="%s, %s, %s",
    posts_insert_params=("Test Post", "This is a test post", 1),
    drop_table="DROP TABLE IF EXISTS {table}",
    bad_sql="CREATE THAT TABLE invalid_sql",
    table_exists_sql="SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{table}'",
    all_tables_sql="SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name NOT LIKE 'sqlspec_%'",
    customer_insert_columns="name",
    customer_insert_values="'Customer 1'), ('Customer 2'",
    customer_insert_params=(),
    rollback_insert_values="%s",
    rollback_insert_params=("Rollback User",),
)

POSTGRES = MigrationDialect(
    kind="postgres",
    id_column="SERIAL PRIMARY KEY",
    text_column="VARCHAR(255)",
    created_at_column="TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    users_insert_columns="name, email",
    users_insert_values="%s, %s",
    users_insert_params=("John Doe", "john@example.com"),
    posts_insert_columns="title, content, user_id",
    posts_insert_values="%s, %s, %s",
    posts_insert_params=("Test Post", "This is a test post", 1),
    drop_table="DROP TABLE IF EXISTS {table}",
    bad_sql="CREATE THAT TABLE invalid_sql",
    table_exists_sql=(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table}'"
    ),
    all_tables_sql=(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name NOT LIKE 'sqlspec_%'"
    ),
    customer_insert_columns="name",
    customer_insert_values="'Customer 1'), ('Customer 2'",
    customer_insert_params=(),
    rollback_insert_values="%s",
    rollback_insert_params=("Rollback User",),
)

POSTGRES_NUMERIC = MigrationDialect(
    kind="postgres",
    id_column=POSTGRES.id_column,
    text_column=POSTGRES.text_column,
    created_at_column=POSTGRES.created_at_column,
    users_insert_columns=POSTGRES.users_insert_columns,
    users_insert_values="$1, $2",
    users_insert_params=POSTGRES.users_insert_params,
    posts_insert_columns=POSTGRES.posts_insert_columns,
    posts_insert_values="$1, $2, $3",
    posts_insert_params=POSTGRES.posts_insert_params,
    drop_table=POSTGRES.drop_table,
    bad_sql=POSTGRES.bad_sql,
    table_exists_sql=POSTGRES.table_exists_sql,
    all_tables_sql=POSTGRES.all_tables_sql,
    customer_insert_columns=POSTGRES.customer_insert_columns,
    customer_insert_values=POSTGRES.customer_insert_values,
    customer_insert_params=POSTGRES.customer_insert_params,
    rollback_insert_values="$1",
    rollback_insert_params=POSTGRES.rollback_insert_params,
)

ORACLE = MigrationDialect(
    kind="oracle",
    id_column="NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY",
    text_column="VARCHAR2(255)",
    created_at_column="TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    users_insert_columns="name, email",
    users_insert_values=":1, :2",
    users_insert_params=("John Doe", "john@example.com"),
    posts_insert_columns="title, content, user_id",
    posts_insert_values=":1, :2, :3",
    posts_insert_params=("Test Post", "This is a test post", 1),
    drop_table="DROP TABLE {table}",
    bad_sql="CREATE THAT TABLE invalid_sql",
    table_exists_sql="SELECT table_name FROM user_tables WHERE table_name = '{table_upper}'",
    all_tables_sql="SELECT table_name FROM user_tables WHERE table_name NOT LIKE 'SQLSPEC_%'",
    customer_insert_columns="name",
    customer_insert_values="'Customer 1'), ('Customer 2'",
    customer_insert_params=(),
    rollback_insert_values=":1",
    rollback_insert_params=("Rollback User",),
)


MIGRATION_CASES = [
    pytest.param(
        MigrationCase("sqlite", SQLITE, sqlite_file="test.db"), marks=pytest.mark.xdist_group("sqlite"), id="sqlite"
    ),
    pytest.param(
        MigrationCase("aiosqlite", SQLITE, is_async=True, sqlite_file="test.db"),
        marks=pytest.mark.xdist_group("sqlite"),
        id="aiosqlite",
    ),
    pytest.param(
        MigrationCase("adbc-sqlite", SQLITE, sqlite_file="adbc.db"),
        marks=pytest.mark.xdist_group("sqlite"),
        id="adbc-sqlite",
    ),
    pytest.param(
        MigrationCase("duckdb", DUCKDB, sqlite_file="test.duckdb"), marks=pytest.mark.xdist_group("duckdb"), id="duckdb"
    ),
    pytest.param(
        MigrationCase("aiomysql", MYSQL, is_async=True),
        marks=[pytest.mark.xdist_group("mysql"), pytest.mark.aiomysql],
        id="aiomysql",
    ),
    pytest.param(
        MigrationCase("asyncmy", MYSQL, is_async=True),
        marks=[pytest.mark.xdist_group("mysql"), pytest.mark.asyncmy],
        id="asyncmy",
    ),
    pytest.param(
        MigrationCase("mysqlconnector-sync", MYSQL),
        marks=[pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector],
        id="mysqlconnector-sync",
    ),
    pytest.param(
        MigrationCase("mysqlconnector-async", MYSQL, is_async=True),
        marks=[pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector],
        id="mysqlconnector-async",
    ),
    pytest.param(
        MigrationCase("pymysql", MYSQL), marks=[pytest.mark.xdist_group("mysql"), pytest.mark.pymysql], id="pymysql"
    ),
    pytest.param(
        MigrationCase("asyncpg", POSTGRES_NUMERIC, is_async=True),
        marks=[pytest.mark.xdist_group("postgres"), pytest.mark.asyncpg],
        id="asyncpg",
    ),
    pytest.param(
        MigrationCase("psqlpy", POSTGRES_NUMERIC, is_async=True),
        marks=[pytest.mark.xdist_group("postgres"), pytest.mark.postgres],
        id="psqlpy",
    ),
    pytest.param(
        MigrationCase("psycopg-sync", POSTGRES),
        marks=[pytest.mark.xdist_group("postgres"), pytest.mark.postgres],
        id="psycopg-sync",
    ),
    pytest.param(
        MigrationCase("psycopg-async", POSTGRES, is_async=True),
        marks=[pytest.mark.xdist_group("postgres"), pytest.mark.postgres],
        id="psycopg-async",
    ),
    pytest.param(
        MigrationCase("oracle-sync", ORACLE),
        marks=[pytest.mark.xdist_group("oracle"), pytest.mark.oracle],
        id="oracle-sync",
    ),
    pytest.param(
        MigrationCase("oracle-async", ORACLE, is_async=True),
        marks=[pytest.mark.xdist_group("oracle"), pytest.mark.oracle],
        id="oracle-async",
    ),
]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _call(obj: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    return await _maybe_await(getattr(obj, name)(*args, **kwargs))


async def _execute(driver: Any, sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> Any:
    if params is None:
        return await _call(driver, "execute", sql)
    return await _call(driver, "execute", sql, params)


async def _close_config(config: Any) -> None:
    close_pool = getattr(config, "close_pool", None)
    if close_pool is not None:
        await _maybe_await(close_pool())
    elif (connection := getattr(config, "connection_instance", None)) is not None:
        close = getattr(connection, "close", None)
        if close is not None:
            await _maybe_await(close())

    if hasattr(config, "connection_instance"):
        config.connection_instance = None


@asynccontextmanager
async def _session(config: Any) -> AsyncGenerator[Any, None]:
    manager = config.provide_session()
    if hasattr(manager, "__aenter__"):
        async with manager as driver:
            yield driver
    else:
        with manager as driver:
            yield driver


def _postgres_url(request: pytest.FixtureRequest, *, scheme: str = "postgresql") -> str:
    service = request.getfixturevalue("postgres_service")
    return f"{scheme}://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


def _make_config(
    case: MigrationCase, request: pytest.FixtureRequest, tmp_path: Path, migration_dir: Path
) -> MigrationConfig:
    migration_config = {
        "script_location": str(migration_dir),
        "version_table_name": f"sm_{case.adapter.replace('-', '_')}_{uuid4().hex[:6]}",
    }
    if case.adapter == "sqlite":
        return SqliteConfig(
            connection_config={"database": str(tmp_path / str(case.sqlite_file))}, migration_config=migration_config
        )
    if case.adapter == "aiosqlite":
        return AiosqliteConfig(
            connection_config={"database": str(tmp_path / str(case.sqlite_file))}, migration_config=migration_config
        )
    if case.adapter == "adbc-sqlite":
        return AdbcConfig(
            connection_config={
                "driver_name": "adbc_driver_sqlite",
                "uri": f"file:{tmp_path / str(case.sqlite_file)}",
                "autocommit": True,
            },
            migration_config=migration_config,
        )
    if case.adapter == "duckdb":
        return DuckDBConfig(
            connection_config={"database": str(tmp_path / str(case.sqlite_file))}, migration_config=migration_config
        )
    if case.adapter in {"aiomysql", "asyncmy", "mysqlconnector-sync", "mysqlconnector-async", "pymysql"}:
        service = request.getfixturevalue("mysql_service")
        connection_config: dict[str, Any] = {
            "host": service.host,
            "port": service.port,
            "user": service.user,
            "password": service.password,
            "database": service.db,
            "autocommit": True,
        }
        if case.adapter == "aiomysql":
            connection_config["db"] = service.db
            connection_config.pop("database")
            return AiomysqlConfig(connection_config=connection_config, migration_config=migration_config)
        if case.adapter == "asyncmy":
            return AsyncmyConfig(connection_config=connection_config, migration_config=migration_config)
        if case.adapter == "mysqlconnector-sync":
            connection_config["use_pure"] = True
            return MysqlConnectorSyncConfig(
                connection_config=connection_config,
                statement_config=mysqlconnector_statement_config,
                migration_config=migration_config,
            )
        if case.adapter == "mysqlconnector-async":
            connection_config["use_pure"] = True
            return MysqlConnectorAsyncConfig(
                connection_config=connection_config,
                statement_config=mysqlconnector_statement_config,
                migration_config=migration_config,
            )
        return PyMysqlConfig(
            connection_config=connection_config,
            statement_config=pymysql_statement_config,
            migration_config=migration_config,
        )
    if case.adapter == "asyncpg":
        service = request.getfixturevalue("postgres_service")
        return AsyncpgConfig(
            connection_config={
                "host": service.host,
                "port": service.port,
                "user": service.user,
                "password": service.password,
                "database": service.database,
            },
            migration_config=migration_config,
        )
    if case.adapter == "psqlpy":
        return PsqlpyConfig(connection_config={"dsn": _postgres_url(request)}, migration_config=migration_config)
    if case.adapter == "psycopg-sync":
        return PsycopgSyncConfig(
            connection_config={"conninfo": _postgres_url(request)}, migration_config=migration_config
        )
    if case.adapter == "psycopg-async":
        return PsycopgAsyncConfig(
            connection_config={"conninfo": _postgres_url(request)}, migration_config=migration_config
        )
    if case.adapter in {"oracle-sync", "oracle-async"}:
        service = request.getfixturevalue("oracle_23ai_service")
        oracle_connection_config: dict[str, Any] = {
            "host": service.host,
            "port": service.port,
            "service_name": service.service_name,
            "user": service.user,
            "password": service.password,
        }
        if case.adapter == "oracle-async":
            oracle_connection_config.update({"min": 1, "max": 5})
            return OracleAsyncConfig(connection_config=oracle_connection_config, migration_config=migration_config)
        return OracleSyncConfig(connection_config=oracle_connection_config, migration_config=migration_config)
    msg = f"Unhandled migration adapter: {case.adapter}"
    raise ValueError(msg)


def _make_commands(config: MigrationConfig) -> MigrationCommands:
    return cast("MigrationCommands", create_migration_commands(config))


def _short_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _format_sql(template: str, table: str) -> str:
    return template.format(table=table, table_upper=table.upper())


def _user_migration(case: MigrationCase, users_table: str) -> str:
    dialect = case.dialect
    return f'''"""Create users table."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id {dialect.id_column},
            name {dialect.text_column} NOT NULL,
            email {dialect.text_column} UNIQUE NOT NULL,
            created_at {dialect.created_at_column}
        )
    """]


def down():
    """Drop users table."""
    return ["{_format_sql(dialect.drop_table, users_table)}"]
'''


def _post_migration(case: MigrationCase, users_table: str, posts_table: str) -> str:
    dialect = case.dialect
    fk_name = f"fk_{posts_table[:20]}"
    fk_sql = (
        f"CONSTRAINT {fk_name} FOREIGN KEY (user_id) REFERENCES {users_table}(id)"
        if dialect.kind == "oracle"
        else f"FOREIGN KEY (user_id) REFERENCES {users_table}(id)"
    )
    content_type = "CLOB" if dialect.kind == "oracle" else "TEXT"
    return f'''"""Create posts table."""


def up():
    """Create posts table."""
    return ["""
        CREATE TABLE {posts_table} (
            id {dialect.id_column},
            title {dialect.text_column} NOT NULL,
            content {content_type},
            user_id INTEGER,
            {fk_sql}
        )
    """]


def down():
    """Drop posts table."""
    return ["{_format_sql(dialect.drop_table, posts_table)}"]
'''


def _customer_migration(case: MigrationCase, customers_table: str) -> str:
    dialect = case.dialect
    return f'''"""Create customers table with seed data."""


def up():
    """Create customers table."""
    return [
        """CREATE TABLE {customers_table} (
            id {dialect.id_column},
            name {dialect.text_column} NOT NULL
        )""",
        "INSERT INTO {customers_table} ({dialect.customer_insert_columns}) VALUES ({dialect.customer_insert_values})"
    ]


def down():
    """Drop customers table."""
    return ["{_format_sql(dialect.drop_table, customers_table)}"]
'''


async def _table_exists(driver: Any, case: MigrationCase, table: str) -> bool:
    result = await _execute(driver, _format_sql(case.dialect.table_exists_sql, table))
    return len(result.data) == 1


async def _assert_table_exists(driver: Any, case: MigrationCase, table: str) -> None:
    assert await _table_exists(driver, case, table)


async def _assert_table_missing(driver: Any, case: MigrationCase, table: str) -> None:
    assert not await _table_exists(driver, case, table)


@pytest.mark.parametrize("case", MIGRATION_CASES)
async def test_migration_full_workflow(case: MigrationCase, tmp_path: Path, request: pytest.FixtureRequest) -> None:
    """Full migration workflow: init, upgrade, query, downgrade."""
    migration_dir = tmp_path / "migrations"
    users_table = _short_name("users")
    config = _make_config(case, request, tmp_path, migration_dir)
    commands = _make_commands(config)

    try:
        await _call(commands, "init", str(migration_dir), package=True)
        assert migration_dir.exists()
        assert (migration_dir / "__init__.py").exists()

        (migration_dir / "0001_create_users.py").write_text(_user_migration(case, users_table))
        await _call(commands, "upgrade")

        async with _session(config) as driver:
            await _assert_table_exists(driver, case, users_table)
            await _execute(
                driver,
                f"INSERT INTO {users_table} ({case.dialect.users_insert_columns}) VALUES ({case.dialect.users_insert_values})",
                case.dialect.users_insert_params,
            )
            users_result = await _execute(driver, f"SELECT * FROM {users_table}")
            assert len(users_result.data) == 1
            assert users_result.get_data()[0]["name"] == "John Doe"
            assert users_result.get_data()[0]["email"] == "john@example.com"

        await _call(commands, "downgrade", "base")

        async with _session(config) as driver:
            await _assert_table_missing(driver, case, users_table)
    finally:
        await _close_config(config)


@pytest.mark.parametrize("case", MIGRATION_CASES)
async def test_multiple_migrations_workflow(
    case: MigrationCase, tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    """Multiple migrations can upgrade, downgrade one step, and downgrade to base."""
    migration_dir = tmp_path / "migrations"
    users_table = _short_name("users")
    posts_table = _short_name("posts")
    config = _make_config(case, request, tmp_path, migration_dir)
    commands = _make_commands(config)

    try:
        await _call(commands, "init", str(migration_dir), package=True)
        (migration_dir / "0001_create_users.py").write_text(_user_migration(case, users_table))
        (migration_dir / "0002_create_posts.py").write_text(_post_migration(case, users_table, posts_table))

        await _call(commands, "upgrade")

        async with _session(config) as driver:
            await _assert_table_exists(driver, case, users_table)
            await _assert_table_exists(driver, case, posts_table)
            await _execute(
                driver,
                f"INSERT INTO {users_table} ({case.dialect.users_insert_columns}) VALUES ({case.dialect.users_insert_values})",
                case.dialect.users_insert_params,
            )
            await _execute(
                driver,
                f"INSERT INTO {posts_table} ({case.dialect.posts_insert_columns}) VALUES ({case.dialect.posts_insert_values})",
                case.dialect.posts_insert_params,
            )
            posts_result = await _execute(driver, f"SELECT * FROM {posts_table}")
            assert len(posts_result.data) == 1
            assert posts_result.get_data()[0]["title"] == "Test Post"

        await _call(commands, "downgrade", "0001")

        async with _session(config) as driver:
            await _assert_table_exists(driver, case, users_table)
            await _assert_table_missing(driver, case, posts_table)

        await _call(commands, "downgrade", "base")

        async with _session(config) as driver:
            await _assert_table_missing(driver, case, users_table)
    finally:
        await _close_config(config)


@pytest.mark.parametrize("case", MIGRATION_CASES)
async def test_migration_current_command(case: MigrationCase, tmp_path: Path, request: pytest.FixtureRequest) -> None:
    """The current migration command reports base and applied revisions."""
    migration_dir = tmp_path / "migrations"
    test_table = _short_name("current")
    config = _make_config(case, request, tmp_path, migration_dir)
    commands = _make_commands(config)

    try:
        await _call(commands, "init", str(migration_dir), package=True)
        current_version = await _call(commands, "current")
        assert current_version is None or current_version == "base"

        (migration_dir / "0001_current.py").write_text(_user_migration(case, test_table))
        await _call(commands, "upgrade")

        current_version = await _call(commands, "current", verbose=True)
        assert current_version == "0001"
    finally:
        await _close_config(config)


@pytest.mark.parametrize("case", MIGRATION_CASES)
async def test_migration_error_handling(case: MigrationCase, tmp_path: Path, request: pytest.FixtureRequest) -> None:
    """Failed migrations do not stamp the bad revision as applied."""
    migration_dir = tmp_path / "migrations"
    config = _make_config(case, request, tmp_path, migration_dir)
    commands = _make_commands(config)

    try:
        await _call(commands, "init", str(migration_dir), package=True)
        (migration_dir / "0001_bad.py").write_text(f'''"""Bad migration."""


def up():
    """Invalid SQL."""
    return ["{case.dialect.bad_sql}"]


def down():
    """No downgrade needed."""
    return []
''')

        with contextlib.suppress(Exception):
            await _call(commands, "upgrade")

        current_version = await _call(commands, "current")
        assert current_version is None or current_version == "base"
    finally:
        await _close_config(config)


@pytest.mark.parametrize("case", MIGRATION_CASES)
async def test_migration_with_transactions(case: MigrationCase, tmp_path: Path, request: pytest.FixtureRequest) -> None:
    """Migrations can apply multiple statements and the resulting session can roll back writes."""
    migration_dir = tmp_path / "migrations"
    customers_table = _short_name("customers")
    config = _make_config(case, request, tmp_path, migration_dir)
    commands = _make_commands(config)

    try:
        await _call(commands, "init", str(migration_dir), package=True)
        (migration_dir / "0001_customers.py").write_text(_customer_migration(case, customers_table))

        await _call(commands, "upgrade")

        async with _session(config) as driver:
            await _assert_table_exists(driver, case, customers_table)
            customers_result = await _execute(driver, f"SELECT * FROM {customers_table} ORDER BY name")
            assert len(customers_result.data) == 2
            assert customers_result.get_data()[0]["name"] == "Customer 1"
            assert customers_result.get_data()[1]["name"] == "Customer 2"

        async with _session(config) as driver:
            await _call(driver, "begin")
            try:
                values = case.dialect.rollback_insert_values
                columns = "id, name" if len(case.dialect.rollback_insert_params) == 2 else "name"
                await _execute(
                    driver,
                    f"INSERT INTO {customers_table} ({columns}) VALUES ({values})",
                    case.dialect.rollback_insert_params,
                )
                await _call(driver, "rollback")
            except Exception:
                await _call(driver, "rollback")
                raise

            result = await _execute(driver, f"SELECT * FROM {customers_table} WHERE name = 'Rollback User'")
            assert len(result.data) == 0

        await _call(commands, "downgrade", "base")

        async with _session(config) as driver:
            await _assert_table_missing(driver, case, customers_table)
    finally:
        await _close_config(config)


@pytest.mark.parametrize("case", MIGRATION_CASES)
async def test_config_migration_methods(case: MigrationCase, tmp_path: Path, request: pytest.FixtureRequest) -> None:
    """Config convenience migration methods work across adapter families."""
    migration_dir = tmp_path / "migrations"
    products_table = _short_name("products")
    stamped_table = _short_name("stamped")
    config = _make_config(case, request, tmp_path, migration_dir)

    try:
        await _call(config, "init_migrations")
        assert migration_dir.exists()

        (migration_dir / "0001_products.py").write_text(_user_migration(case, products_table))
        await _call(config, "migrate_up")

        async with _session(config) as driver:
            await _assert_table_exists(driver, case, products_table)

        assert await _call(config, "get_current_migration") == "0001"

        await _call(config, "migrate_down")
        async with _session(config) as driver:
            await _assert_table_missing(driver, case, products_table)

        await _call(config, "create_migration", "add users table", file_type="py")
        generated_files = [path for path in migration_dir.glob("*.py") if path.name != "__init__.py"]
        assert any("add_users_table" in path.name for path in generated_files)

        (migration_dir / "0003_stamped.py").write_text(_user_migration(case, stamped_table))
        await _call(config, "stamp_migration", "0003")
        assert await _call(config, "get_current_migration") == "0003"

        timestamp_file = migration_dir / "20251030120000_timestamp_migration.py"
        timestamp_file.write_text(_user_migration(case, _short_name("ts")))
        await _call(config, "fix_migrations", dry_run=True, yes=True)
        assert timestamp_file.exists()
        assert not any(
            path.name.endswith("timestamp_migration.py") and path.name != timestamp_file.name
            for path in migration_dir.glob("*.py")
        )
    finally:
        await _close_config(config)
