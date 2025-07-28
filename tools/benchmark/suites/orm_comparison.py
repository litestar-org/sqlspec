"""Comprehensive ORM comparison benchmark suite testing all databases and async/sync variants."""

import asyncio
import time
from typing import Any

from rich.panel import Panel
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert, pool, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.statement.sql import SQL, SQLConfig
from tools.benchmark.core.metrics import TimingResult
from tools.benchmark.infrastructure.containers import ContainerManager
from tools.benchmark.suites.base import BaseBenchmarkSuite

# Constants for benchmark queries to avoid magic values
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

        # Database configurations to test
        # Note: We can add more databases here as needed (e.g., MySQL, MSSQL)
        databases = [
            {
                "name": "SQLite",
                "type": "sync",
                "get_sqlspec_config": self._get_sqlite_configs,
                "get_sqlalchemy_engine": lambda: create_engine(
                    "sqlite:///.benchmark/test_sync.db",
                    poolclass=QueuePool,
                    pool_size=20,
                    max_overflow=0,
                    pool_pre_ping=True,
                ),
                "setup_func": self._setup_sync_db,
                "requires_container": False,
            },
            {
                "name": "AioSQLite",
                "type": "async",
                "get_sqlspec_config": self._get_aiosqlite_configs,
                "get_sqlalchemy_engine": lambda: create_async_engine(
                    "sqlite+aiosqlite:///.benchmark/test_async.db",
                    poolclass=pool.AsyncAdaptedQueuePool,
                    pool_size=20,
                    max_overflow=0,
                    pool_pre_ping=True,
                ),
                "setup_func": self._setup_async_db,
                "requires_container": False,
            },
        ]

        # Dynamically add container-based databases if Docker is running
        if self.container_manager.is_docker_running() and not self.config.no_containers:
            self._add_containerized_databases(databases)

        # Filter databases based on the adapter argument
        if adapter != "all":
            databases = [db for db in databases if db["name"].lower() == adapter.lower()]

        # Run benchmarks for each database
        for db_config in databases:
            self.console.print(f"\n[bold cyan]Testing {db_config['name']}...[/bold cyan]")

            if db_config["type"] == "sync":
                db_results = self._run_sync_benchmarks(db_config)
            else:
                db_results = asyncio.run(self._run_async_benchmarks(db_config))

            # Add results with database prefix
            for key, result in db_results.items():
                full_key = f"{db_config['name'].lower()}_{key}"
                results[full_key] = result

        return results

    def _add_containerized_databases(self, databases: list[dict[str, Any]]) -> None:
        """Add container-based databases to the list of databases to test."""
        try:
            host, port = self.container_manager.start_postgres(self.config.keep_containers)
            databases.extend(
                [
                    {
                        "name": "Psycopg",
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
                ]
            )
        except Exception as e:
            self.console.print(f"[yellow]Skipping PostgreSQL tests: {e}[/yellow]")

    def _get_sqlite_configs(self) -> tuple[SqliteConfig, SqliteConfig]:
        """Get SQLite configs with and without caching."""
        # Use separate database files to avoid locking issues
        return (
            SqliteConfig(
                connection_config={"database": ".benchmark/test_sync_no_cache.db"},
                statement_config=SQLConfig(enable_caching=False),
                min_pool_size=1,
                max_pool_size=1,  # Reduce pool size to avoid concurrent access
            ),
            SqliteConfig(
                connection_config={"database": ".benchmark/test_sync_with_cache.db"},
                statement_config=SQLConfig(enable_caching=True),
                min_pool_size=1,
                max_pool_size=1,  # Reduce pool size to avoid concurrent access
            ),
        )

    def _get_aiosqlite_configs(self) -> tuple[AiosqliteConfig, AiosqliteConfig]:
        """Get AioSQLite configs with and without caching."""
        # Use separate database files to avoid locking issues
        return (
            AiosqliteConfig(
                connection_config={"database": ".benchmark/test_async_no_cache.db"},
                statement_config=SQLConfig(enable_caching=False),
                min_pool=1,
                max_pool=1,  # Reduce pool size to avoid concurrent access
            ),
            AiosqliteConfig(
                connection_config={"database": ".benchmark/test_async_with_cache.db"},
                statement_config=SQLConfig(enable_caching=True),
                min_pool=1,
                max_pool=1,  # Reduce pool size to avoid concurrent access
            ),
        )

    def _get_psycopg_configs(self, host: str, port: int) -> tuple[PsycopgSyncConfig, PsycopgSyncConfig]:
        """Get Psycopg configs with and without caching."""
        pool_params = {
            "host": host,
            "port": port,
            "user": "postgres",
            "password": "postgres",
            "dbname": "postgres",
            "min_size": 10,
            "max_size": 20,
        }
        return (
            PsycopgSyncConfig(pool_config=pool_params, statement_config=SQLConfig(enable_caching=False)),
            PsycopgSyncConfig(pool_config=pool_params, statement_config=SQLConfig(enable_caching=True)),
        )

    def _get_asyncpg_configs(self, host: str, port: int) -> tuple[Any, Any]:
        """Get Asyncpg configs with and without caching."""
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        pool_params = {
            "host": host,
            "port": port,
            "user": "postgres",
            "password": "postgres",
            "database": "postgres",
            "min_size": 10,
            "max_size": 20,
        }
        return (
            AsyncpgConfig(pool_config=pool_params, statement_config=SQLConfig(enable_caching=False)),
            AsyncpgConfig(pool_config=pool_params, statement_config=SQLConfig(enable_caching=True)),
        )

    def _run_sync_benchmarks(self, db_config: dict[str, Any]) -> dict[str, TimingResult]:
        """Run synchronous benchmarks for a database."""
        results = {}
        engine = db_config["get_sqlalchemy_engine"]()
        config_no_cache, config_with_cache = db_config["get_sqlspec_config"]()

        try:
            db_config["setup_func"](engine)
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
            if hasattr(config_no_cache, "_close_pool"):
                config_no_cache._close_pool()
            if hasattr(config_with_cache, "_close_pool"):
                config_with_cache._close_pool()

        return results

    async def _run_async_benchmarks(self, db_config: dict[str, Any]) -> dict[str, TimingResult]:
        """Run asynchronous benchmarks for a database."""
        results = {}
        engine = db_config["get_sqlalchemy_engine"]()
        config_no_cache, config_with_cache = db_config["get_sqlspec_config"]()

        try:
            await db_config["setup_func"](engine)
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
            if hasattr(config_no_cache, "_close_pool"):
                await config_no_cache._close_pool()
            if hasattr(config_with_cache, "_close_pool"):
                await config_with_cache._close_pool()

        return results

    def _setup_sync_db(self, engine: Any) -> None:
        """Set up a synchronous database, creating tables and inserting test data."""
        # For SQLite, we need to setup all database files
        if "sqlite" in str(engine.url):
            # Setup all SQLite database files
            for db_file in [".benchmark/test_sync_no_cache.db", ".benchmark/test_sync_with_cache.db"]:
                temp_engine = create_engine(f"sqlite:///{db_file}")
                metadata.drop_all(temp_engine, checkfirst=True)
                metadata.create_all(temp_engine, checkfirst=True)
                with temp_engine.begin() as conn:
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
                temp_engine.dispose()
        else:
            # For other databases, use the provided engine
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

    async def _setup_async_db(self, engine: Any) -> None:
        """Set up an asynchronous database, creating tables and inserting test data."""
        # For SQLite (aiosqlite), we need to setup all database files
        if "sqlite" in str(engine.url):
            # Setup all async SQLite database files
            for db_file in [".benchmark/test_async_no_cache.db", ".benchmark/test_async_with_cache.db"]:
                temp_engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}")
                async with temp_engine.begin() as conn:
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
                await temp_engine.dispose()
        else:
            # For other databases, use the provided engine
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

    def _warmup_sync_connections(self, engine: Any, config_no_cache: Any, config_with_cache: Any) -> None:
        """Warm up synchronous connections for both SQLAlchemy and SQLSpec."""
        with engine.connect() as conn:
            for _ in range(10):
                conn.execute(text("SELECT 1"))
        with config_no_cache.provide_session() as session:
            for _ in range(10):
                session.execute(SQL("SELECT 1")).all()
        with config_with_cache.provide_session() as session:
            for _ in range(10):
                session.execute(SQL("SELECT 1")).all()

    async def _warmup_async_connections(self, engine: Any, config_no_cache: Any, config_with_cache: Any) -> None:
        """Warm up asynchronous connections for both SQLAlchemy and SQLSpec."""
        async with engine.connect() as conn:
            for _ in range(10):
                await conn.execute(text("SELECT 1"))
        async with config_no_cache.provide_session() as session:
            for _ in range(10):
                (await session.execute(SQL("SELECT 1"))).all()
        async with config_with_cache.provide_session() as session:
            for _ in range(10):
                (await session.execute(SQL("SELECT 1"))).all()

    def _benchmark_sync_select_single(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark selecting a single record by ID (sync)."""
        results = {}
        sql = SQL("SELECT * FROM users WHERE id = ?", SINGLE_ROW_ID)
        stmt = select(users_table).where(users_table.c.id == SINGLE_ROW_ID)
        session_local = sessionmaker(bind=engine)

        # SQLSpec (no cache)
        with config_no_cache.provide_session() as session:
            times = self.runner.metrics.time_operation(lambda: session.execute(sql).one(), self.config.iterations)
            results["select_single_sqlspec_no_cache"] = TimingResult(
                "select_single_sqlspec_no_cache", len(times), times
            )

        # SQLSpec (with cache)
        with config_with_cache.provide_session() as session:
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
        sql = SQL("SELECT * FROM users LIMIT 100")
        stmt = select(users_table).limit(100)
        session_local = sessionmaker(bind=engine)

        # SQLSpec (no cache)
        with config_no_cache.provide_session() as session:
            times = self.runner.metrics.time_operation(lambda: session.execute(sql).all(), self.config.iterations)
            results["select_bulk_sqlspec_no_cache"] = TimingResult("select_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        with config_with_cache.provide_session() as session:
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
        sql = SQL("INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)")

        # SQLSpec (no cache)
        with config_no_cache.provide_session() as session:

            def insert_and_cleanup() -> None:
                session.execute_many(sql, *[list(d.values()) for d in insert_data])
                # Clean up inserted data
                session.execute(SQL(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}"))

            times = self.runner.metrics.time_operation(insert_and_cleanup, 10)
            results["insert_bulk_sqlspec_no_cache"] = TimingResult("insert_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        with config_with_cache.provide_session() as session:

            def insert_and_cleanup() -> None:
                session.execute_many(sql, *[list(d.values()) for d in insert_data])
                # Clean up inserted data
                session.execute(SQL(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}"))

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
        sql = SQL(f"UPDATE users SET status = 'updated' WHERE id <= {BATCH_UPDATE_LIMIT}")
        stmt = update(users_table).where(users_table.c.id <= BATCH_UPDATE_LIMIT).values(status="updated")

        # SQLSpec (no cache)
        with config_no_cache.provide_session() as session:
            times = self.runner.metrics.time_operation(lambda: session.execute(sql), self.config.iterations)
            results["update_bulk_sqlspec_no_cache"] = TimingResult("update_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        with config_with_cache.provide_session() as session:
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
        SQL(f"SELECT * FROM users WHERE id = {SINGLE_ROW_ID}")
        stmt = select(users_table).where(users_table.c.id == SINGLE_ROW_ID)
        async_session_local = async_sessionmaker(bind=engine)

        # SQLSpec (no cache)
        async with config_no_cache.provide_session() as session:

            async def op() -> Any:
                # Use literal SQL to avoid parameter binding issues
                session_sql = SQL(f"SELECT * FROM users WHERE id = {SINGLE_ROW_ID}")
                result = await session.execute(session_sql)
                return result.one()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_single_sqlspec_no_cache"] = TimingResult(
                "select_single_sqlspec_no_cache", len(times), times
            )

        # SQLSpec (with cache)
        async with config_with_cache.provide_session() as session:

            async def op() -> Any:
                # Use literal SQL to avoid parameter binding issues
                session_sql = SQL(f"SELECT * FROM users WHERE id = {SINGLE_ROW_ID}")
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
        sql = SQL("SELECT * FROM users LIMIT 100")
        stmt = select(users_table).limit(100)
        async_session_local = async_sessionmaker(bind=engine)

        # SQLSpec (no cache)
        async with config_no_cache.provide_session() as session:

            async def op() -> Any:
                result = await session.execute(sql)
                return result.all()

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["select_bulk_sqlspec_no_cache"] = TimingResult("select_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        async with config_with_cache.provide_session() as session:

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
        sql = SQL("INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)")

        # SQLSpec (no cache)
        async with config_no_cache.provide_session() as session:

            async def op() -> Any:
                await session.execute_many(sql, *[list(d.values()) for d in insert_data])
                # Clean up inserted data
                await session.execute(SQL(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}"))

            times = await self.runner.metrics.time_operation_async(op, 10)
            results["insert_bulk_sqlspec_no_cache"] = TimingResult("insert_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        async with config_with_cache.provide_session() as session:

            async def op() -> Any:
                await session.execute_many(sql, *[list(d.values()) for d in insert_data])
                # Clean up inserted data
                await session.execute(SQL(f"DELETE FROM users WHERE id >= {base_id} AND id < {base_id + 100}"))

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
        sql = SQL(f"UPDATE users SET status = 'updated' WHERE id <= {BATCH_UPDATE_LIMIT}")
        stmt = update(users_table).where(users_table.c.id <= BATCH_UPDATE_LIMIT).values(status="updated")

        # SQLSpec (no cache)
        async with config_no_cache.provide_session() as session:

            async def op() -> Any:
                return await session.execute(sql)

            times = await self.runner.metrics.time_operation_async(op, self.config.iterations)
            results["update_bulk_sqlspec_no_cache"] = TimingResult("update_bulk_sqlspec_no_cache", len(times), times)

        # SQLSpec (with cache)
        async with config_with_cache.provide_session() as session:

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
