"""Comprehensive ORM comparison benchmark suite testing all databases and async/sync variants."""

import asyncio
import contextlib
import random
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from rich.panel import Panel
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert, pool, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.statement.sql import StatementConfig
from tools.benchmark.core.metrics import TimingResult
from tools.benchmark.infrastructure.containers import ContainerManager
from tools.benchmark.suites.base import BaseBenchmarkSuite


# Constants for benchmark queries - now randomized for fairness
def get_random_test_parameters() -> dict[str, int]:
    """Generate randomized test parameters for fair benchmarking."""
    return {
        "single_row_id": random.randint(100, 900),
        "batch_update_limit": random.choice([50, 100, 200]),
        "batch_select_limit": random.choice([50, 100, 200]),
    }


# Default constants (for backward compatibility)
SINGLE_ROW_ID = 500
BATCH_UPDATE_LIMIT = 100
BATCH_SELECT_LIMIT = 100


# SQLAlchemy setup
Base = declarative_base()


class User(Base):  # type: ignore[misc,valid-type]
    """SQLAlchemy ORM model for testing."""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    email = Column(String(100))
    status = Column(String(20))


# SQLAlchemy Core table definitions
metadata = MetaData()

users_table = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(50)),
    Column("email", String(100)),
    Column("status", String(20)),
)


class ORMComparisonBenchmark(BaseBenchmarkSuite):
    """Redesigned ORM benchmark for clear, apples-to-apples comparisons."""

    def __init__(self, config: Any, runner: Any, console: Any) -> None:
        super().__init__(config, runner, console)
        self.container_manager = ContainerManager(console)
        # Cache configs to ensure consistent schema names between setup and runtime
        self._cached_psycopg_configs: Optional[tuple] = None
        self._cached_psycopg_async_configs: Optional[tuple] = None

    @property
    def name(self) -> str:
        return "orm_comparison"

    @property
    def description(self) -> str:
        return "Redesigned ORM Performance Comparison"

    def run(self, adapter: str = "all", **kwargs: Any) -> dict[str, TimingResult]:
        """Run the redesigned ORM comparison benchmarks."""
        self.console.print(
            Panel.fit(
                f"[bold]Redesigned ORM Comparison Benchmark[/bold]\n"
                f"Iterations: [cyan]{self.config.iterations:,}[/cyan]",
                border_style="blue",
            )
        )

        results = {}

        # Database configurations to test - using unique database files to prevent locking
        # Note: We can add more databases here as needed (e.g., MySQL, MSSQL)

        # SQLAlchemy engines will use the same database files as SQLSpec configs
        # These will be set when SQLSpec configs are created

        databases = [
            {
                "name": "SQLite",
                "type": "sync",
                "get_sqlspec_config": self._get_sqlite_configs,
                "get_sqlalchemy_engine": self._get_sqlite_sqlalchemy_engine,
                "setup_func": self._setup_sync_db,
                "requires_container": False,
            },
            {
                "name": "AioSQLite",
                "type": "async",
                "get_sqlspec_config": self._get_aiosqlite_configs,
                "get_sqlalchemy_engine": self._get_aiosqlite_sqlalchemy_engine,
                "setup_func": self._setup_async_db,
                "requires_container": False,
            },
        ]

        # Dynamically add container-based databases if Docker is running
        if self.container_manager.is_docker_running() and not self.config.no_containers:
            self._add_containerized_databases(databases)

        # Filter databases based on the adapter argument
        if adapter != "all":
            # Allow partial matching so "psycopg" matches both "Psycopg Sync" and "PsycopgAsync"
            databases = [db for db in databases if adapter.lower() in db["name"].lower()]

        # Run benchmarks for each database with timeout and error handling
        for db_config in databases:
            self.console.print(f"\n[bold cyan]Testing {db_config['name']}...[/bold cyan]")

            try:
                if db_config["type"] == "sync":
                    db_results = self._run_sync_benchmarks(db_config)
                else:
                    db_results = asyncio.run(self._run_async_benchmarks(db_config))

                # Add results with database prefix
                for key, result in db_results.items():
                    full_key = f"{db_config['name'].lower()}_{key}"
                    results[full_key] = result

                self.console.print(f"[green]✅ {db_config['name']} benchmarks completed successfully[/green]")
            except Exception as e:
                self.console.print(f"[red]❌ Error testing {db_config['name']}: {e}[/red]")
                # Log the full traceback in verbose mode
                if hasattr(self.config, "verbose") and self.config.verbose:
                    import traceback

                    self.console.print(f"[red]Full traceback:\n{traceback.format_exc()}[/red]")
                continue

        return results

    def _add_containerized_databases(self, databases: list[dict[str, Any]]) -> None:
        """Add container-based databases to the list of databases to test."""
        try:
            host, port = self.container_manager.start_postgres(self.config.keep_containers)
            databases.extend(
                [
                    {
                        "name": "Psycopg Sync",
                        "type": "sync",
                        "get_sqlspec_config": lambda: self._get_psycopg_configs(host, port),
                        "get_sqlalchemy_engine": lambda: create_engine(
                            f"postgresql+psycopg://{self.container_manager.docker_config.POSTGRES_DEFAULT_USER}:"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD}@{host}:{port}/"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_DB}",
                            poolclass=QueuePool,
                            pool_size=20,
                            max_overflow=0,
                            pool_pre_ping=True,
                        ),
                        "setup_func": self._setup_sync_db,
                        "requires_container": True,
                    },
                    {
                        "name": "Asyncpg",
                        "type": "async",
                        "get_sqlspec_config": lambda: self._get_asyncpg_configs(host, port),
                        "get_sqlalchemy_engine": lambda: create_async_engine(
                            f"postgresql+asyncpg://{self.container_manager.docker_config.POSTGRES_DEFAULT_USER}:"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD}@{host}:{port}/"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_DB}",
                            poolclass=pool.AsyncAdaptedQueuePool,
                            pool_size=20,
                            max_overflow=0,
                            pool_pre_ping=True,
                        ),
                        "setup_func": self._setup_async_db,
                        "requires_container": True,
                    },
                    {
                        "name": "Psqlpy",
                        "type": "async",
                        "get_sqlspec_config": lambda: self._get_psqlpy_configs(host, port),
                        "get_sqlalchemy_engine": lambda: create_async_engine(
                            f"postgresql+asyncpg://{self.container_manager.docker_config.POSTGRES_DEFAULT_USER}:"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD}@{host}:{port}/"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_DB}",
                            poolclass=pool.AsyncAdaptedQueuePool,
                            pool_size=20,
                            max_overflow=0,
                            pool_pre_ping=True,
                        ),
                        "setup_func": self._setup_async_db,
                        "requires_container": True,
                    },
                    {
                        "name": "AdbcPostgres",
                        "type": "sync",
                        "get_sqlspec_config": lambda: self._get_adbc_postgres_configs(host, port),
                        "get_sqlalchemy_engine": lambda: create_engine(
                            f"postgresql+psycopg://{self.container_manager.docker_config.POSTGRES_DEFAULT_USER}:"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD}@{host}:{port}/"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_DB}",
                            poolclass=QueuePool,
                            pool_size=20,
                            max_overflow=0,
                            pool_pre_ping=True,
                        ),
                        "setup_func": self._setup_sync_db,
                        "requires_container": True,
                    },
                    {
                        "name": "PsycopgAsync",
                        "type": "async",
                        "get_sqlspec_config": lambda: self._get_psycopg_async_configs(host, port),
                        "get_sqlalchemy_engine": lambda: create_async_engine(
                            f"postgresql+psycopg://{self.container_manager.docker_config.POSTGRES_DEFAULT_USER}:"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD}@{host}:{port}/"
                            f"{self.container_manager.docker_config.POSTGRES_DEFAULT_DB}",
                            poolclass=pool.AsyncAdaptedQueuePool,
                            pool_size=20,
                            max_overflow=0,
                            pool_pre_ping=True,
                        ),
                        "setup_func": self._setup_async_db,
                        "requires_container": True,
                    },
                ]
            )
        except Exception as e:
            self.console.print(f"[yellow]Skipping PostgreSQL tests: {e}[/yellow]")

    def _generate_unique_db_path(self, base_name: str) -> str:
        """Generate unique database file path to prevent locking conflicts."""
        # Create unique identifier for this test run
        unique_id = str(uuid.uuid4())[:8]
        # Ensure benchmark directory exists
        Path(".benchmark").mkdir(exist_ok=True)
        return f".benchmark/{base_name}_{unique_id}.db"

    def _get_sqlite_sqlalchemy_engine(self) -> Any:
        """Get SQLAlchemy engine for SQLite - this method creates a separate database for SQLAlchemy benchmarks."""
        # SQLAlchemy gets its own isolated database file
        self._sqlite_sqlalchemy_db = self._generate_unique_db_path("sqlite_sqlalchemy")

        def _on_sqlite_connect(dbapi_connection, connection_record) -> None:
            """Enable WAL mode and optimizations to match SQLSpec adapter behavior."""
            dbapi_connection.execute("PRAGMA journal_mode=WAL")
            dbapi_connection.execute("PRAGMA foreign_keys=ON")
            dbapi_connection.execute("PRAGMA synchronous=NORMAL")
            dbapi_connection.execute("PRAGMA cache_size=-64000")
            dbapi_connection.execute("PRAGMA temp_store=MEMORY")
            dbapi_connection.execute("PRAGMA mmap_size=268435456")

        from sqlalchemy import event

        engine = create_engine(
            f"sqlite:///{self._sqlite_sqlalchemy_db}",
            poolclass=QueuePool,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False, "timeout": 20},
        )

        event.listen(engine, "connect", _on_sqlite_connect)
        return engine

    def _get_aiosqlite_sqlalchemy_engine(self) -> Any:
        """Get SQLAlchemy async engine for AioSQLite - this method creates a separate database for SQLAlchemy benchmarks."""
        # SQLAlchemy gets its own isolated database file
        self._aiosqlite_sqlalchemy_db = self._generate_unique_db_path("aiosqlite_sqlalchemy")

        return create_async_engine(
            f"sqlite+aiosqlite:///{self._aiosqlite_sqlalchemy_db}",
            poolclass=pool.AsyncAdaptedQueuePool,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            connect_args={"timeout": 20},
        )

        # Skip event listener for async - aiosqlite adapter handles WAL mode automatically

    def _get_sqlite_configs(self) -> tuple[SqliteConfig, SqliteConfig]:
        """Get SQLite configs with and without caching using separate database files for complete isolation."""
        # Each test scenario gets its own completely isolated database file
        self._sqlite_no_cache_db = self._generate_unique_db_path("sqlite_no_cache")
        self._sqlite_with_cache_db = self._generate_unique_db_path("sqlite_with_cache")

        return (
            SqliteConfig(
                pool_config={
                    "database": self._sqlite_no_cache_db,
                    "pool_min_size": 1,
                    "pool_max_size": 1,
                    "check_same_thread": False,
                    "timeout": 20,
                },
                statement_config=StatementConfig(enable_caching=False),
            ),
            SqliteConfig(
                pool_config={
                    "database": self._sqlite_with_cache_db,
                    "pool_min_size": 1,
                    "pool_max_size": 1,
                    "check_same_thread": False,
                    "timeout": 20,
                },
                statement_config=StatementConfig(enable_caching=True),
            ),
        )

    def _get_aiosqlite_configs(self) -> tuple[AiosqliteConfig, AiosqliteConfig]:
        """Get AioSQLite configs with and without caching using separate database files for complete isolation."""
        # Each test scenario gets its own completely isolated database file
        self._aiosqlite_no_cache_db = self._generate_unique_db_path("aiosqlite_no_cache")
        self._aiosqlite_with_cache_db = self._generate_unique_db_path("aiosqlite_with_cache")

        return (
            AiosqliteConfig(
                pool_config={
                    "database": self._aiosqlite_no_cache_db,
                    "pool_min_size": 1,
                    "pool_max_size": 1,
                    "timeout": 20,
                },
                statement_config=StatementConfig(enable_caching=False),
            ),
            AiosqliteConfig(
                pool_config={
                    "database": self._aiosqlite_with_cache_db,
                    "pool_min_size": 1,
                    "pool_max_size": 1,
                    "timeout": 20,
                },
                statement_config=StatementConfig(enable_caching=True),
            ),
        )

    def _get_psycopg_configs(self, host: str, port: int) -> tuple[PsycopgSyncConfig, PsycopgSyncConfig]:
        """Get Psycopg configs with and without caching using unique database schemas."""
        from sqlspec.adapters.psycopg.driver import psycopg_statement_config

        # Use cached configs to ensure consistent schema names between setup and runtime
        if self._cached_psycopg_configs is not None:
            return self._cached_psycopg_configs

        # Generate unique schema names to ensure complete isolation
        unique_id = str(uuid.uuid4())[:8]
        schema_no_cache = f"benchmark_no_cache_{unique_id}"
        schema_with_cache = f"benchmark_with_cache_{unique_id}"

        pool_parameters_no_cache = {
            "host": host,
            "port": port,
            "user": "postgres",
            "password": "postgres",
            "dbname": "postgres",
            "options": f"-c search_path={schema_no_cache}",
            "min_size": 5,
            "max_size": 10,
        }
        pool_parameters_with_cache = {
            "host": host,
            "port": port,
            "user": "postgres",
            "password": "postgres",
            "dbname": "postgres",
            "options": f"-c search_path={schema_with_cache}",
            "min_size": 5,
            "max_size": 10,
        }
        # Create and cache the configs
        self._cached_psycopg_configs = (
            PsycopgSyncConfig(
                pool_config=pool_parameters_no_cache,
                statement_config=psycopg_statement_config.replace(enable_caching=False),
            ),
            PsycopgSyncConfig(
                pool_config=pool_parameters_with_cache,
                statement_config=psycopg_statement_config.replace(enable_caching=True),
            ),
        )
        return self._cached_psycopg_configs

    def _get_asyncpg_configs(self, host: str, port: int) -> tuple[Any, Any]:
        """Get Asyncpg configs with and without caching using unique database schemas."""
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.adapters.asyncpg.driver import asyncpg_statement_config

        # Generate unique schema names to ensure complete isolation
        unique_id = str(uuid.uuid4())[:8]
        schema_no_cache = f"benchmark_no_cache_{unique_id}"
        schema_with_cache = f"benchmark_with_cache_{unique_id}"

        pool_parameters_no_cache = {
            "host": host,
            "port": port,
            "user": "postgres",
            "password": "postgres",
            "database": "postgres",
            "server_settings": {"search_path": schema_no_cache},
            "min_size": 5,
            "max_size": 10,
        }
        pool_parameters_with_cache = {
            "host": host,
            "port": port,
            "user": "postgres",
            "password": "postgres",
            "database": "postgres",
            "server_settings": {"search_path": schema_with_cache},
            "min_size": 5,
            "max_size": 10,
        }
        return (
            AsyncpgConfig(
                pool_config=pool_parameters_no_cache,
                statement_config=asyncpg_statement_config.replace(enable_caching=False),
            ),
            AsyncpgConfig(
                pool_config=pool_parameters_with_cache,
                statement_config=asyncpg_statement_config.replace(enable_caching=True),
            ),
        )

    def _get_psqlpy_configs(self, host: str, port: int) -> tuple[PsqlpyConfig, PsqlpyConfig]:
        """Get Psqlpy configs with and without caching using unique database schemas."""
        from sqlspec.adapters.psqlpy.driver import psqlpy_statement_config

        # Generate unique schema names to ensure complete isolation
        unique_id = str(uuid.uuid4())[:8]
        schema_no_cache = f"benchmark_no_cache_{unique_id}"
        schema_with_cache = f"benchmark_with_cache_{unique_id}"

        pool_parameters_no_cache = {
            "host": host,
            "port": port,
            "username": "postgres",
            "password": "postgres",
            "db_name": "postgres",
            "options": f"-c search_path={schema_no_cache}",
            "max_db_pool_size": 10,
        }
        pool_parameters_with_cache = {
            "host": host,
            "port": port,
            "username": "postgres",
            "password": "postgres",
            "db_name": "postgres",
            "options": f"-c search_path={schema_with_cache}",
            "max_db_pool_size": 10,
        }
        return (
            PsqlpyConfig(
                pool_config=pool_parameters_no_cache,
                statement_config=psqlpy_statement_config.replace(enable_caching=False),
            ),
            PsqlpyConfig(
                pool_config=pool_parameters_with_cache,
                statement_config=psqlpy_statement_config.replace(enable_caching=True),
            ),
        )

    def _get_adbc_postgres_configs(self, host: str, port: int) -> tuple[AdbcConfig, AdbcConfig]:
        """Get ADBC PostgreSQL configs with and without caching using unique database schemas."""
        from sqlspec.adapters.adbc.driver import get_adbc_statement_config

        # Generate unique schema names to ensure complete isolation
        unique_id = str(uuid.uuid4())[:8]
        schema_no_cache = f"benchmark_no_cache_{unique_id}"
        schema_with_cache = f"benchmark_with_cache_{unique_id}"

        connection_parameters_no_cache = {
            "uri": f"postgresql://postgres:postgres@{host}:{port}/postgres?options=-c%20search_path%3D{schema_no_cache}",
            "driver_name": "adbc_driver_postgresql",
        }
        connection_parameters_with_cache = {
            "uri": f"postgresql://postgres:postgres@{host}:{port}/postgres?options=-c%20search_path%3D{schema_with_cache}",
            "driver_name": "adbc_driver_postgresql",
        }
        adbc_statement_config = get_adbc_statement_config("postgres")
        return (
            AdbcConfig(
                connection_config=connection_parameters_no_cache,
                statement_config=adbc_statement_config.replace(enable_caching=False),
            ),
            AdbcConfig(
                connection_config=connection_parameters_with_cache,
                statement_config=adbc_statement_config.replace(enable_caching=True),
            ),
        )

    def _get_psycopg_async_configs(self, host: str, port: int) -> tuple[PsycopgAsyncConfig, PsycopgAsyncConfig]:
        """Get async psycopg configs with and without caching using unique database schemas."""
        from sqlspec.adapters.psycopg.driver import psycopg_statement_config

        # Use cached configs to ensure consistent schema names between setup and runtime
        if self._cached_psycopg_async_configs is not None:
            return self._cached_psycopg_async_configs

        # Generate unique schema names to ensure complete isolation
        unique_id = str(uuid.uuid4())[:8]
        schema_no_cache = f"benchmark_no_cache_{unique_id}"
        schema_with_cache = f"benchmark_with_cache_{unique_id}"

        pool_parameters_no_cache = {
            "host": host,
            "port": port,
            "user": "postgres",
            "password": "postgres",
            "dbname": "postgres",
            "options": f"-c search_path={schema_no_cache}",
            "min_size": 5,
            "max_size": 10,
        }
        pool_parameters_with_cache = {
            "host": host,
            "port": port,
            "user": "postgres",
            "password": "postgres",
            "dbname": "postgres",
            "options": f"-c search_path={schema_with_cache}",
            "min_size": 5,
            "max_size": 10,
        }
        # Create and cache the async configs
        self._cached_psycopg_async_configs = (
            PsycopgAsyncConfig(
                pool_config=pool_parameters_no_cache,
                statement_config=psycopg_statement_config.replace(enable_caching=False),
            ),
            PsycopgAsyncConfig(
                pool_config=pool_parameters_with_cache,
                statement_config=psycopg_statement_config.replace(enable_caching=True),
            ),
        )
        return self._cached_psycopg_async_configs

    def _run_sync_benchmarks(self, db_config: dict[str, Any]) -> dict[str, TimingResult]:
        """Run synchronous benchmarks for a database."""
        results = {}
        engine = db_config["get_sqlalchemy_engine"]()
        config_no_cache, config_with_cache = db_config["get_sqlspec_config"]()

        try:
            # Set up SQLAlchemy database
            db_config["setup_func"](engine)
            # Set up SQLSpec databases with the same data
            self._setup_sqlspec_sync_databases(config_no_cache, config_with_cache)
            self._warmup_sync_connections(engine, config_no_cache, config_with_cache)

            # Define benchmark operations
            operations = {
                "select_single": self._benchmark_sync_select_single,
                "select_bulk": self._benchmark_sync_select_bulk,
                "insert_bulk": self._benchmark_sync_insert_bulk,
                "update_bulk": self._benchmark_sync_update_bulk,
            }

            for op_func in operations.values():
                op_results = op_func(engine, config_no_cache, config_with_cache, db_config["name"])
                results.update(op_results)

        finally:
            engine.dispose()
            # Close pools if they exist - our configs should have this method
            with contextlib.suppress(AttributeError):
                config_no_cache._close_pool()
            with contextlib.suppress(AttributeError):
                config_with_cache._close_pool()

        return results

    async def _run_async_benchmarks(self, db_config: dict[str, Any]) -> dict[str, TimingResult]:
        """Run asynchronous benchmarks for a database."""
        results = {}
        engine = db_config["get_sqlalchemy_engine"]()
        config_no_cache, config_with_cache = db_config["get_sqlspec_config"]()

        try:
            # Set up SQLAlchemy database
            await db_config["setup_func"](engine)
            # Set up SQLSpec databases with the same data
            await self._setup_sqlspec_async_databases(config_no_cache, config_with_cache)
            await self._warmup_async_connections(engine, config_no_cache, config_with_cache)

            # Define benchmark operations
            operations = {
                "select_single": self._benchmark_async_select_single,
                "select_bulk": self._benchmark_async_select_bulk,
                "insert_bulk": self._benchmark_async_insert_bulk,
                "update_bulk": self._benchmark_async_update_bulk,
            }

            for op_func in operations.values():
                op_results = await op_func(engine, config_no_cache, config_with_cache, db_config["name"])
                results.update(op_results)

        finally:
            await engine.dispose()
            # Close pools if they exist - our configs should have this method
            with contextlib.suppress(AttributeError):
                await config_no_cache._close_pool()
            with contextlib.suppress(AttributeError):
                await config_with_cache._close_pool()

        return results

    def _setup_sync_db(self, engine: Any) -> None:
        """Set up a synchronous database using SQLAlchemy for setup, ensuring SQLSpec can access the same data."""
        try:
            # For all databases, use SQLAlchemy for the initial setup since it's what gets called
            # This ensures the data exists in the database that benchmarks will query
            # Note: SQLite adapter automatically handles WAL mode and optimizations
            metadata.drop_all(engine, checkfirst=True)
            metadata.create_all(engine, checkfirst=True)

            with engine.begin() as conn:
                users_data = [
                    {
                        "id": i,
                        "name": f"user_{i}",
                        "email": f"user{i}@example.com",
                        "status": "active" if i % 2 == 0 else "inactive",
                    }
                    for i in range(1, 1001)
                ]
                conn.execute(insert(users_table), users_data)

            # For PostgreSQL, also create schemas for isolated SQLSpec configs
            if "postgresql" in str(engine.url):
                with engine.begin() as conn:
                    # Extract host and port from engine URL
                    host = engine.url.host or "localhost"
                    port = engine.url.port or 5432

                    # We know these methods exist on our class
                    psycopg_configs = self._get_psycopg_configs(host, port)
                    for _i, config in enumerate(psycopg_configs):
                        schema_name = config.pool_config.get("options", "").split("search_path=")[-1]
                        if schema_name:
                            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

                            # Create table and data in each schema
                            conn.execute(text(f"CREATE TABLE {schema_name}.users AS SELECT * FROM users"))

                    asyncpg_configs = self._get_asyncpg_configs(host, port)
                    for config in asyncpg_configs:
                        schema_name = config.pool_config.get("server_settings", {}).get("search_path", "")
                        if schema_name:
                            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

                            # Create table and data in each schema
                            conn.execute(text(f"CREATE TABLE {schema_name}.users AS SELECT * FROM users"))

        except Exception:
            raise

    async def _setup_async_db(self, engine: Any) -> None:
        """Set up an asynchronous database using SQLAlchemy for setup, ensuring SQLSpec can access the same data."""
        try:
            # For all databases, use SQLAlchemy for the initial setup
            # Note: AioSQLite adapter automatically handles WAL mode and optimizations
            async with engine.begin() as conn:
                await conn.run_sync(metadata.drop_all, checkfirst=True)
                await conn.run_sync(metadata.create_all, checkfirst=True)
                users_data = [
                    {
                        "id": i,
                        "name": f"user_{i}",
                        "email": f"user{i}@example.com",
                        "status": "active" if i % 2 == 0 else "inactive",
                    }
                    for i in range(1, 1001)
                ]
                await conn.execute(insert(users_table), users_data)

            # For PostgreSQL, also create schemas for isolated SQLSpec configs
            if "postgresql" in str(engine.url):
                async with engine.begin() as conn:
                    # Extract host and port from engine URL
                    host = engine.url.host or "localhost"
                    port = engine.url.port or 5432

                    # We know these methods exist on our class
                    asyncpg_configs = self._get_asyncpg_configs(host, port)
                    for config in asyncpg_configs:
                        schema_name = config.pool_config.get("server_settings", {}).get("search_path", "")
                        if schema_name:
                            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                            # Create table and data in each schema
                            await conn.execute(text(f"CREATE TABLE {schema_name}.users AS SELECT * FROM users"))

                    psycopg_configs = self._get_psycopg_async_configs(host, port)
                    for config in psycopg_configs:
                        schema_name = config.pool_config.get("options", "").split("search_path=")[-1]
                        if schema_name:
                            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                            # Create table and data in each schema
                            await conn.execute(text(f"CREATE TABLE {schema_name}.users AS SELECT * FROM users"))
        except Exception as e:
            self.console.print(f"[red]Async setup failed: {e}[/red]")
            raise

    def _apply_psycopg_search_path_if_needed(self, session: Any, config: Any) -> None:
        """Apply search_path for Psycopg adapters if needed (sync version)."""
        if config.pool_config and "options" in config.pool_config:
            options = config.pool_config["options"]
            if "search_path=" in options:
                schema_name = options.split("search_path=")[-1]
                try:
                    session.execute(f"SET search_path = {schema_name}")
                except Exception:
                    pass  # Continue if setting search_path fails

    async def _apply_psycopg_search_path_if_needed_async(self, session: Any, config: Any) -> None:
        """Apply search_path for Psycopg adapters if needed (async version)."""
        if config.pool_config and "options" in config.pool_config:
            options = config.pool_config["options"]
            if "search_path=" in options:
                schema_name = options.split("search_path=")[-1]
                try:
                    await session.execute(f"SET search_path = {schema_name}")
                except Exception:
                    pass  # Continue if setting search_path fails

    def _setup_sqlspec_sync_databases(self, config_no_cache: Any, config_with_cache: Any) -> None:
        """Set up SQLSpec databases with the same data that SQLAlchemy has."""
        configs = [config_no_cache, config_with_cache]

        for config in configs:
            with config.provide_session() as session:
                # For PostgreSQL adapters, create schema first if needed
                schema_name = None

                # Check pool_config (for most adapters)
                if config.pool_config:
                    if "options" in config.pool_config:
                        # For psycopg: options="-c search_path=schema_name"
                        options = config.pool_config["options"]
                        if "search_path=" in options:
                            schema_name = options.split("search_path=")[-1]
                    elif "server_settings" in config.pool_config:
                        # For asyncpg: server_settings={'search_path': 'schema_name'}
                        schema_name = config.pool_config["server_settings"].get("search_path")
                    elif "uri" in config.pool_config and "search_path" in config.pool_config["uri"]:
                        # For ADBC: uri with search_path parameter
                        uri = config.pool_config["uri"]
                        if "search_path%3D" in uri:
                            schema_name = uri.split("search_path%3D")[-1].split("&")[0]

                # Check connection_config (for ADBC)
                elif config.connection_config:
                    if "uri" in config.connection_config and "search_path" in config.connection_config["uri"]:
                        # For ADBC: uri with search_path parameter
                        uri = config.connection_config["uri"]
                        if "search_path%3D" in uri:
                            schema_name = uri.split("search_path%3D")[-1].split("&")[0]

                if schema_name:
                    try:
                        session.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                    except Exception:
                        pass  # Schema might already exist

                    # For Psycopg adapters, we need to explicitly set search_path in each session
                    # because connection options don't persist across connection pool operations
                    if config.pool_config and "options" in config.pool_config:
                        try:
                            session.execute(f"SET search_path = {schema_name}")
                        except Exception:
                            pass  # Continue if setting search_path fails

                # Create table - DDL statements now properly skip parameterization
                session.execute("DROP TABLE IF EXISTS users")
                session.execute("""
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(50),
                        email VARCHAR(100),
                        status VARCHAR(20)
                    )
                """)

                # Insert test data
                users_data = [
                    (i, f"user_{i}", f"user{i}@example.com", "active" if i % 2 == 0 else "inactive")
                    for i in range(1, 1001)
                ]
                session.execute_many("INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)", users_data)

    async def _setup_sqlspec_async_databases(self, config_no_cache: Any, config_with_cache: Any) -> None:
        """Set up SQLSpec async databases with the same data that SQLAlchemy has."""
        configs = [config_no_cache, config_with_cache]

        for config in configs:
            async with config.provide_session() as session:
                # For PostgreSQL adapters, create schema first if needed
                schema_name = None

                # Check pool_config (for most adapters)
                if config.pool_config:
                    if "options" in config.pool_config:
                        # For psycopg: options="-c search_path=schema_name"
                        options = config.pool_config["options"]
                        if "search_path=" in options:
                            schema_name = options.split("search_path=")[-1]
                    elif "server_settings" in config.pool_config:
                        # For asyncpg: server_settings={'search_path': 'schema_name'}
                        schema_name = config.pool_config["server_settings"].get("search_path")
                    elif "uri" in config.pool_config and "search_path" in config.pool_config["uri"]:
                        # For ADBC: uri with search_path parameter
                        uri = config.pool_config["uri"]
                        if "search_path%3D" in uri:
                            schema_name = uri.split("search_path%3D")[-1].split("&")[0]

                # Check connection_config (for ADBC)
                elif config.connection_config:
                    if "uri" in config.connection_config and "search_path" in config.connection_config["uri"]:
                        # For ADBC: uri with search_path parameter
                        uri = config.connection_config["uri"]
                        if "search_path%3D" in uri:
                            schema_name = uri.split("search_path%3D")[-1].split("&")[0]

                if schema_name:
                    try:
                        await session.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                    except Exception:
                        pass  # Schema might already exist

                    # For Psycopg adapters, we need to explicitly set search_path in each session
                    # because connection options don't persist across connection pool operations
                    if config.pool_config and "options" in config.pool_config:
                        try:
                            await session.execute(f"SET search_path = {schema_name}")
                        except Exception:
                            pass  # Continue if setting search_path fails

                # Create table - DDL statements now properly skip parameterization
                await session.execute("DROP TABLE IF EXISTS users")
                await session.execute("""
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(50),
                        email VARCHAR(100),
                        status VARCHAR(20)
                    )
                """)

                # Insert test data
                users_data = [
                    (i, f"user_{i}", f"user{i}@example.com", "active" if i % 2 == 0 else "inactive")
                    for i in range(1, 1001)
                ]
                await session.execute_many(
                    "INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)", users_data
                )

    def _warmup_sync_connections(self, engine: Any, config_no_cache: Any, config_with_cache: Any) -> None:
        """Warm up synchronous connections for both SQLAlchemy and SQLSpec."""
        with engine.connect() as conn:
            for _ in range(10):
                conn.execute(text("SELECT 1"))
        with config_no_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_no_cache)
            for _ in range(10):
                session.execute("SELECT 1").all()
        with config_with_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_with_cache)
            for _ in range(10):
                session.execute("SELECT 1").all()

    async def _warmup_async_connections(self, engine: Any, config_no_cache: Any, config_with_cache: Any) -> None:
        """Warm up asynchronous connections for both SQLAlchemy and SQLSpec."""
        async with engine.connect() as conn:
            for _ in range(10):
                await conn.execute(text("SELECT 1"))
        async with config_no_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_no_cache)
            for _ in range(10):
                (await session.execute("SELECT 1")).all()
        async with config_with_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_with_cache)
            for _ in range(10):
                (await session.execute("SELECT 1")).all()

    def _benchmark_sync_select_single(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark selecting a single record by ID (sync)."""
        results = {}
        sql = f"SELECT * FROM users WHERE id = {SINGLE_ROW_ID}"
        stmt = select(users_table).where(users_table.c.id == SINGLE_ROW_ID)
        session_local = sessionmaker(bind=engine)

        # SQLSpec (no cache)
        with config_no_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_no_cache)
            times = self.runner.metrics.time_operation(lambda: session.execute(sql).one(), self.config.iterations)
            results["select_single_sqlspec_no_cache"] = TimingResult(
                "select_single_sqlspec_no_cache", len(times), times
            )

        # SQLSpec (with cache)
        with config_with_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_with_cache)
            times = self.runner.metrics.time_operation(lambda: session.execute(sql).one(), self.config.iterations)
            results["select_single_sqlspec_cache"] = TimingResult("select_single_sqlspec_cache", len(times), times)

        # SQLAlchemy Core
        with engine.connect() as conn:
            times = self.runner.metrics.time_operation(lambda: conn.execute(stmt).one(), self.config.iterations)
            results["select_single_sqlalchemy_core"] = TimingResult("select_single_sqlalchemy_core", len(times), times)

        # SQLAlchemy ORM
        with session_local() as session:
            times = self.runner.metrics.time_operation(lambda: session.get(User, 500), self.config.iterations)
            results["select_single_sqlalchemy_orm"] = TimingResult("select_single_sqlalchemy_orm", len(times), times)

        return results

    def _benchmark_sync_select_bulk(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark selecting 100 records (sync)."""
        results = {}
        sql = "SELECT * FROM users LIMIT 100"
        stmt = select(users_table).limit(100)
        session_local = sessionmaker(bind=engine)

        # SQLSpec (no cache)
        with config_no_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_no_cache)
            times = self.runner.metrics.time_operation(lambda: session.execute(sql).all(), self.config.iterations)
            results["select_bulk_sqlspec_no_cache"] = TimingResult("select_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        with config_with_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_with_cache)
            times = self.runner.metrics.time_operation(lambda: session.execute(sql).all(), self.config.iterations)
            results["select_bulk_sqlspec_cache"] = TimingResult("select_bulk_sqlspec_cache", len(times), times)

        # SQLAlchemy Core
        with engine.connect() as conn:
            times = self.runner.metrics.time_operation(lambda: conn.execute(stmt).fetchall(), self.config.iterations)
            results["select_bulk_sqlalchemy_core"] = TimingResult("select_bulk_sqlalchemy_core", len(times), times)

        # SQLAlchemy ORM
        with session_local() as session:
            times = self.runner.metrics.time_operation(
                lambda: session.query(User).limit(100).all(), self.config.iterations
            )
            results["select_bulk_sqlalchemy_orm"] = TimingResult("select_bulk_sqlalchemy_orm", len(times), times)

        return results

    def _benchmark_sync_insert_bulk(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark inserting 100 records (sync)."""
        results = {}
        # Generate unique data for each iteration to avoid conflicts
        # Start IDs after existing data (1-1000)
        base_id = 1001 + (int(time.time() * 1000) % 100000) * 100
        insert_data = [
            {"id": base_id + i, "name": f"insert_user_{i}", "email": f"insert{i}@example.com", "status": "pending"}
            for i in range(100)
        ]
        sql = "INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)"

        # SQLSpec (no cache)
        with config_no_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_no_cache)

            def insert_and_cleanup() -> None:
                session.execute_many(sql, [list(d.values()) for d in insert_data])
                # Clean up inserted data
                session.execute(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}")

            times = self.runner.metrics.time_operation(insert_and_cleanup, 10)
            results["insert_bulk_sqlspec_no_cache"] = TimingResult("insert_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        with config_with_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_with_cache)

            def insert_and_cleanup() -> None:
                session.execute_many(sql, [list(d.values()) for d in insert_data])
                # Clean up inserted data
                session.execute(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}")

            times = self.runner.metrics.time_operation(insert_and_cleanup, 10)
            results["insert_bulk_sqlspec_cache"] = TimingResult("insert_bulk_sqlspec_cache", len(times), times)

        # SQLAlchemy Core
        with engine.begin() as conn:

            def insert_and_cleanup() -> None:
                conn.execute(insert(users_table), insert_data)
                # Clean up inserted data
                conn.execute(text(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}"))

            times = self.runner.metrics.time_operation(insert_and_cleanup, 10)
            results["insert_bulk_sqlalchemy_core"] = TimingResult("insert_bulk_sqlalchemy_core", len(times), times)

        return results

    def _benchmark_sync_update_bulk(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark updating 100 records (sync)."""
        results = {}
        sql = f"UPDATE users SET status = 'updated' WHERE id <= {BATCH_UPDATE_LIMIT}"
        stmt = update(users_table).where(users_table.c.id <= BATCH_UPDATE_LIMIT).values(status="updated")

        # SQLSpec (no cache)
        with config_no_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_no_cache)
            times = self.runner.metrics.time_operation(lambda: session.execute(sql), self.config.iterations)
            results["update_bulk_sqlspec_no_cache"] = TimingResult("update_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        with config_with_cache.provide_session() as session:
            self._apply_psycopg_search_path_if_needed(session, config_with_cache)
            times = self.runner.metrics.time_operation(lambda: session.execute(sql), self.config.iterations)
            results["update_bulk_sqlspec_cache"] = TimingResult("update_bulk_sqlspec_cache", len(times), times)

        # SQLAlchemy Core
        with engine.begin() as conn:
            times = self.runner.metrics.time_operation(lambda: conn.execute(stmt), self.config.iterations)
            results["update_bulk_sqlalchemy_core"] = TimingResult("update_bulk_sqlalchemy_core", len(times), times)

        return results

    async def _benchmark_async_select_single(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark selecting a single record by ID (async)."""
        results = {}
        f"SELECT * FROM users WHERE id = {SINGLE_ROW_ID}"
        stmt = select(users_table).where(users_table.c.id == SINGLE_ROW_ID)
        async_session_local = async_sessionmaker(bind=engine)

        # SQLSpec (no cache)
        async with config_no_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_no_cache)

            async def op() -> Any:
                # Use literal SQL to avoid parameter binding issues
                session_sql = f"SELECT * FROM users WHERE id = {SINGLE_ROW_ID}"
                result = await session.execute(session_sql)
                return result.one()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_single_sqlspec_no_cache"] = TimingResult(
                "select_single_sqlspec_no_cache", len(times), times
            )

        # SQLSpec (with cache)
        async with config_with_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_with_cache)

            async def op() -> Any:
                # Use literal SQL to avoid parameter binding issues
                session_sql = f"SELECT * FROM users WHERE id = {SINGLE_ROW_ID}"
                result = await session.execute(session_sql)
                return result.one()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_single_sqlspec_cache"] = TimingResult("select_single_sqlspec_cache", len(times), times)

        # SQLAlchemy Core
        async with engine.connect() as conn:

            async def op() -> Any:
                result = await conn.execute(stmt)
                return result.one()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_single_sqlalchemy_core"] = TimingResult("select_single_sqlalchemy_core", len(times), times)

        # SQLAlchemy ORM
        async with async_session_local() as session:
            times = await self.runner.metrics.time_operation_async(
                lambda: session.get(User, 500), self.config.iterations
            )
            results["select_single_sqlalchemy_orm"] = TimingResult("select_single_sqlalchemy_orm", len(times), times)

        return results

    async def _benchmark_async_select_bulk(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark selecting 100 records (async)."""
        results = {}
        sql = "SELECT * FROM users LIMIT 100"
        stmt = select(users_table).limit(100)
        async_session_local = async_sessionmaker(bind=engine)

        # SQLSpec (no cache)
        async with config_no_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_no_cache)

            async def op() -> Any:
                result = await session.execute(sql)
                return result.all()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_bulk_sqlspec_no_cache"] = TimingResult("select_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        async with config_with_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_with_cache)

            async def op() -> Any:
                result = await session.execute(sql)
                return result.all()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_bulk_sqlspec_cache"] = TimingResult("select_bulk_sqlspec_cache", len(times), times)

        # SQLAlchemy Core
        async with engine.connect() as conn:

            async def op() -> Any:
                result = await conn.execute(stmt)
                return result.fetchall()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_bulk_sqlalchemy_core"] = TimingResult("select_bulk_sqlalchemy_core", len(times), times)

        # SQLAlchemy ORM
        async with async_session_local() as session:

            async def op() -> Any:
                result = await session.execute(select(User).limit(BATCH_SELECT_LIMIT))
                return result.scalars().all()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_bulk_sqlalchemy_orm"] = TimingResult("select_bulk_sqlalchemy_orm", len(times), times)

        return results

    async def _benchmark_async_insert_bulk(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark inserting 100 records (async)."""
        results = {}
        # Generate unique data for each iteration to avoid conflicts
        # Start IDs after existing data (1-1000)
        base_id = 1001 + (int(time.time() * 1000) % 100000) * 100
        insert_data = [
            {"id": base_id + i, "name": f"insert_user_{i}", "email": f"insert{i}@example.com", "status": "pending"}
            for i in range(100)
        ]
        sql = "INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)"

        # SQLSpec (no cache)
        async with config_no_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_no_cache)

            async def op() -> Any:
                await session.execute_many(sql, [list(d.values()) for d in insert_data])
                # Clean up inserted data
                await session.execute(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}")

            times = await self.runner.metrics.time_operation_async(op, 10)
            results["insert_bulk_sqlspec_no_cache"] = TimingResult("insert_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        async with config_with_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_with_cache)

            async def op() -> Any:
                await session.execute_many(sql, [list(d.values()) for d in insert_data])
                # Clean up inserted data
                await session.execute(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}")

            times = await self.runner.metrics.time_operation_async(op, 10)
            results["insert_bulk_sqlspec_cache"] = TimingResult("insert_bulk_sqlspec_cache", len(times), times)

        # SQLAlchemy Core
        async with engine.begin() as conn:

            async def op() -> Any:
                await conn.execute(insert(users_table), insert_data)
                # Clean up inserted data
                await conn.execute(text(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}"))

            times = await self.runner.metrics.time_operation_async(op, 10)
            results["insert_bulk_sqlalchemy_core"] = TimingResult("insert_bulk_sqlalchemy_core", len(times), times)

        return results

    async def _benchmark_async_update_bulk(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark updating 100 records (async)."""
        results = {}
        sql = f"UPDATE users SET status = 'updated' WHERE id <= {BATCH_UPDATE_LIMIT}"
        stmt = update(users_table).where(users_table.c.id <= BATCH_UPDATE_LIMIT).values(status="updated")

        # SQLSpec (no cache)
        async with config_no_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_no_cache)

            async def op() -> Any:
                return await session.execute(sql)

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["update_bulk_sqlspec_no_cache"] = TimingResult("update_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        async with config_with_cache.provide_session() as session:
            await self._apply_psycopg_search_path_if_needed_async(session, config_with_cache)

            async def op() -> Any:
                return await session.execute(sql)

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["update_bulk_sqlspec_cache"] = TimingResult("update_bulk_sqlspec_cache", len(times), times)

        # SQLAlchemy Core
        async with engine.begin() as conn:

            async def op() -> Any:
                return await conn.execute(stmt)

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["update_bulk_sqlalchemy_core"] = TimingResult("update_bulk_sqlalchemy_core", len(times), times)

        return results
