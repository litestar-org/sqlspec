"""Comprehensive ORM comparison benchmark suite testing all databases and async/sync variants."""

import asyncio
from typing import Any

from rich.panel import Panel
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    func,
    insert,
    pool,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.statement.sql import SQL, SQLConfig
from tools.benchmark.core.metrics import BenchmarkMetrics, TimingResult
from tools.benchmark.infrastructure.containers import ContainerManager
from tools.benchmark.suites.base import BaseBenchmarkSuite

# Constants
MIN_ID_THRESHOLD = 100

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
    """Comprehensive ORM benchmark testing all databases with sync/async variants."""

    def __init__(self, config: Any, runner: Any, console: Any) -> None:
        super().__init__(config, runner, console)
        self.container_manager = ContainerManager(console)

    @property
    def name(self) -> str:
        return "orm_comparison"

    @property
    def description(self) -> str:
        return "Comprehensive ORM Performance Comparison - All Databases"

    def run(self, adapter: str = "all", **kwargs: Any) -> dict[str, TimingResult]:
        """Run comprehensive ORM comparison benchmarks."""
        self.console.print(
            Panel.fit(
                f"[bold]Comprehensive ORM Comparison Benchmark[/bold]\n"
                f"Iterations: [cyan]{self.config.iterations:,}[/cyan]\n"
                f"Testing: SQLite, PostgreSQL, Oracle (sync + async)\n"
                f"SQLGlot Version: [yellow]Standard (Python)[/yellow]",
                border_style="blue",
            )
        )

        # Start run
        self.runner.start_run(
            benchmark_type=self.name,
            adapter="all",
            metadata={"test_type": "orm_comparison_full", "sqlglot_version": "standard"},
        )

        results = {}

        # Database configurations
        databases = []

        databases.append(
            {
                "name": "SQLite",
                "type": "sync",
                "sqlspec_config": self._get_sqlite_configs(),
                "sqlalchemy_url": "sqlite:///.benchmark/test_sync.db",
                "sqlalchemy_async_url": None,
                "setup_func": self._setup_sqlite,
                "requires_container": False,
            }
        )
        databases.append(
            {
                "name": "AioSQLite",
                "type": "async",
                "sqlspec_config": self._get_aiosqlite_configs(),
                "sqlalchemy_url": None,
                "sqlalchemy_async_url": "sqlite+aiosqlite:///.benchmark/test_async.db",
                "setup_func": self._setup_sqlite_async,  # type: ignore[dict-item]
                "requires_container": False,
            }
        )

        # PostgreSQL (sync + async)
        if self.container_manager.is_docker_running() and not self.config.no_containers:
            try:
                host, port = self.container_manager.start_postgres(self.config.keep_containers)

                databases.append(
                    {
                        "name": "Psycopg",
                        "type": "sync",
                        "sqlspec_config": self._get_psycopg_configs(host, port),
                        "sqlalchemy_url": f"postgresql+psycopg://{self.container_manager.docker_config.POSTGRES_DEFAULT_USER}:"
                        f"{self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD}@{host}:{port}/"
                        f"{self.container_manager.docker_config.POSTGRES_DEFAULT_DB}",
                        "sqlalchemy_async_url": None,
                        "setup_func": self._setup_postgres,
                        "requires_container": True,
                    }
                )
                databases.append(
                    {
                        "name": "Psycopg-Async",
                        "type": "async",
                        "sqlspec_config": self._get_psycopg_async_configs(host, port),
                        "sqlalchemy_url": None,
                        "sqlalchemy_async_url": f"postgresql+psycopg://{self.container_manager.docker_config.POSTGRES_DEFAULT_USER}:"
                        f"{self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD}@{host}:{port}/"
                        f"{self.container_manager.docker_config.POSTGRES_DEFAULT_DB}",
                        "setup_func": self._setup_postgres_async,  # type: ignore[dict-item]
                        "requires_container": True,
                    }
                )
                databases.append(
                    {
                        "name": "Asyncpg",
                        "type": "async",
                        "sqlspec_config": self._get_asyncpg_configs(host, port),
                        "sqlalchemy_url": None,
                        "sqlalchemy_async_url": f"postgresql+asyncpg://{self.container_manager.docker_config.POSTGRES_DEFAULT_USER}:"
                        f"{self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD}@{host}:{port}/"
                        f"{self.container_manager.docker_config.POSTGRES_DEFAULT_DB}",
                        "setup_func": self._setup_postgres_async,  # type: ignore[dict-item]
                        "requires_container": True,
                    }
                )
            except Exception as e:
                self.console.print(f"[yellow]Skipping PostgreSQL tests: {e}[/yellow]")

        # Oracle (sync + async)
        if self.container_manager.is_docker_running() and not self.config.no_containers:
            try:
                host, port = self.container_manager.start_oracle(self.config.keep_containers)

                databases.append(
                    {
                        "name": "OracleDB",
                        "type": "sync",
                        "sqlspec_config": self._get_oracledb_configs(host, port, mode="sync"),
                        "sqlalchemy_url": f"oracle+oracledb://system:{self.container_manager.docker_config.ORACLE_DEFAULT_PASSWORD}@{host}:{port}?service_name=FREEPDB1",
                        "sqlalchemy_async_url": None,
                        "setup_func": self._setup_oracle,
                        "requires_container": True,
                    }
                )
                databases.append(
                    {
                        "name": "OracleDB-Async",
                        "type": "async",
                        "sqlspec_config": self._get_oracledb_configs(host, port, mode="async"),
                        "sqlalchemy_url": None,
                        "sqlalchemy_async_url": f"oracle+oracledb://system:{self.container_manager.docker_config.ORACLE_DEFAULT_PASSWORD}@{host}:{port}?service_name=FREEPDB1",
                        "setup_func": self._setup_oracle_async,  # type: ignore[dict-item]
                        "requires_container": True,
                    }
                )
            except Exception as e:
                self.console.print(f"[yellow]Skipping Oracle tests: {e}[/yellow]")

        # Run benchmarks for each database
        for db_config in databases:
            self.console.print(f"\n[bold cyan]Testing {db_config['name']}...[/bold cyan]")

            if str(db_config["type"]) == "sync":
                db_results = self._run_sync_benchmarks(db_config)
            else:
                db_results = asyncio.run(self._run_async_benchmarks(db_config))

            # Add results with database prefix
            for key, result in db_results.items():
                full_key = f"{str(db_config['name']).lower()}_{key}"
                results[full_key] = result

        return results

    def _get_sqlite_configs(self) -> tuple[SqliteConfig, SqliteConfig]:
        """Get SQLite configs with and without caching."""
        db_path = ".benchmark/test_sync.db"

        config_no_cache = SqliteConfig(database=db_path, statement_config=SQLConfig(enable_caching=False))

        config_with_cache = SqliteConfig(database=db_path, statement_config=SQLConfig(enable_caching=True))

        return config_no_cache, config_with_cache

    def _get_aiosqlite_configs(self) -> tuple[AiosqliteConfig, AiosqliteConfig]:
        """Get AioSQLite configs with and without caching."""
        db_path = ".benchmark/test_async.db"

        # AioSQLite doesn't support connection pooling
        config_no_cache = AiosqliteConfig(database=db_path, statement_config=SQLConfig(enable_caching=False))

        config_with_cache = AiosqliteConfig(database=db_path, statement_config=SQLConfig(enable_caching=True))

        return config_no_cache, config_with_cache

    def _get_psycopg_configs(self, host: str, port: int) -> tuple[PsycopgSyncConfig, PsycopgSyncConfig]:
        """Get Psycopg configs with and without caching."""
        config_no_cache = PsycopgSyncConfig(
            host=host,
            port=port,
            user=self.container_manager.docker_config.POSTGRES_DEFAULT_USER,
            password=self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD,
            dbname=self.container_manager.docker_config.POSTGRES_DEFAULT_DB,
            min_size=10,
            max_size=20,
            statement_config=SQLConfig(enable_caching=False),
        )

        config_with_cache = PsycopgSyncConfig(
            host=host,
            port=port,
            user=self.container_manager.docker_config.POSTGRES_DEFAULT_USER,
            password=self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD,
            dbname=self.container_manager.docker_config.POSTGRES_DEFAULT_DB,
            min_size=10,
            max_size=20,
            statement_config=SQLConfig(enable_caching=True),
        )

        return config_no_cache, config_with_cache

    def _get_psycopg_async_configs(self, host: str, port: int) -> tuple[PsycopgAsyncConfig, PsycopgAsyncConfig]:
        """Get Psycopg Async configs with and without caching."""
        config_no_cache = PsycopgAsyncConfig(
            host=host,
            port=port,
            user=self.container_manager.docker_config.POSTGRES_DEFAULT_USER,
            password=self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD,
            dbname=self.container_manager.docker_config.POSTGRES_DEFAULT_DB,
            min_size=10,
            max_size=20,
            statement_config=SQLConfig(enable_caching=False),
        )

        config_with_cache = PsycopgAsyncConfig(
            host=host,
            port=port,
            user=self.container_manager.docker_config.POSTGRES_DEFAULT_USER,
            password=self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD,
            dbname=self.container_manager.docker_config.POSTGRES_DEFAULT_DB,
            min_size=10,
            max_size=20,
            statement_config=SQLConfig(enable_caching=True),
        )

        return config_no_cache, config_with_cache

    def _get_asyncpg_configs(self, host: str, port: int) -> tuple[AsyncpgConfig, AsyncpgConfig]:
        """Get Asyncpg configs with and without caching."""
        config_no_cache = AsyncpgConfig(
            host=host,
            port=port,
            user=self.container_manager.docker_config.POSTGRES_DEFAULT_USER,
            password=self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD,
            database=self.container_manager.docker_config.POSTGRES_DEFAULT_DB,
            min_size=10,
            max_size=20,
            statement_config=SQLConfig(enable_caching=False),
        )

        config_with_cache = AsyncpgConfig(
            host=host,
            port=port,
            user=self.container_manager.docker_config.POSTGRES_DEFAULT_USER,
            password=self.container_manager.docker_config.POSTGRES_DEFAULT_PASSWORD,
            database=self.container_manager.docker_config.POSTGRES_DEFAULT_DB,
            min_size=10,
            max_size=20,
            statement_config=SQLConfig(enable_caching=True),
        )

        return config_no_cache, config_with_cache

    def _get_oracledb_configs(self, host: str, port: int, mode: str) -> tuple:
        """Get OracleDB configs with and without caching."""
        config_class = OracleAsyncConfig if mode == "async" else OracleSyncConfig

        config_no_cache = config_class(
            user="system",
            password=self.container_manager.docker_config.ORACLE_DEFAULT_PASSWORD,
            dsn=f"{host}:{port}/FREEPDB1",
            min=10,
            max=20,
            statement_config=SQLConfig(enable_caching=False),
        )

        config_with_cache = config_class(
            user="system",
            password=self.container_manager.docker_config.ORACLE_DEFAULT_PASSWORD,
            dsn=f"{host}:{port}/FREEPDB1",
            min=10,
            max=20,
            statement_config=SQLConfig(enable_caching=True),
        )

        return config_no_cache, config_with_cache

    def _get_limit_clause(self, db_name: str, limit: int = 100) -> str:
        """Get database-specific LIMIT/FETCH clause."""
        db_name_lower = db_name.lower()
        if "oracle" in db_name_lower:
            return f"FETCH FIRST {limit} ROWS ONLY"
        return f"LIMIT {limit}"

    def _get_simple_select_sql(self, db_name: str) -> str:
        """Get database-specific simple SELECT query."""
        return f"SELECT * FROM users {self._get_limit_clause(db_name)}"

    def _setup_sqlite(self, engine: Any) -> None:
        """Set up SQLite database."""
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        # Insert test data
        with engine.begin() as conn:
            users_data = [
                {"name": f"user_{i}", "email": f"user{i}@example.com", "status": "active" if i % 2 == 0 else "inactive"}
                for i in range(1000)
            ]
            conn.execute(text("INSERT INTO users (name, email, status) VALUES (:name, :email, :status)"), users_data)

    async def _setup_sqlite_async(self, engine: Any) -> None:
        """Set up SQLite database asynchronously."""
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

            # Insert test data
            users_data = [
                {"name": f"user_{i}", "email": f"user{i}@example.com", "status": "active" if i % 2 == 0 else "inactive"}
                for i in range(1000)
            ]
            await conn.execute(
                text("INSERT INTO users (name, email, status) VALUES (:name, :email, :status)"), users_data
            )

    def _setup_postgres(self, engine: Any) -> None:
        """Set up PostgreSQL database."""
        self._setup_sqlite(engine)  # Same setup logic

    async def _setup_postgres_async(self, engine: Any) -> None:
        """Set up PostgreSQL database asynchronously."""
        await self._setup_sqlite_async(engine)  # Same setup logic

    def _setup_oracle(self, engine: Any) -> None:
        """Set up Oracle database."""
        # Oracle might need different syntax for table creation
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        # Insert test data with Oracle-specific syntax if needed
        with engine.begin() as conn:
            for i in range(1000):
                conn.execute(
                    text("INSERT INTO users (id, name, email, status) VALUES (:id, :name, :email, :status)"),
                    {
                        "id": i + 1,
                        "name": f"user_{i}",
                        "email": f"user{i}@example.com",
                        "status": "active" if i % 2 == 0 else "inactive",
                    },
                )

    async def _setup_oracle_async(self, engine: Any) -> None:
        """Set up Oracle database asynchronously."""
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

            # Insert test data
            for i in range(1000):
                await conn.execute(
                    text("INSERT INTO users (id, name, email, status) VALUES (:id, :name, :email, :status)"),
                    {
                        "id": i + 1,
                        "name": f"user_{i}",
                        "email": f"user{i}@example.com",
                        "status": "active" if i % 2 == 0 else "inactive",
                    },
                )

    def _run_sync_benchmarks(self, db_config: dict[str, Any]) -> dict[str, TimingResult]:
        """Run synchronous benchmarks for a database."""
        results = {}

        # Get configs
        config_no_cache, config_with_cache = db_config["sqlspec_config"]

        # Set up SQLAlchemy engine with optimized pooling
        engine = create_engine(
            db_config["sqlalchemy_url"], poolclass=QueuePool, pool_size=20, max_overflow=0, pool_pre_ping=True
        )

        # Set up database
        db_config["setup_func"](engine)

        # Warm up connections
        self._warmup_sync_connections(engine, config_no_cache, config_with_cache)

        # Test operations
        operations = [
            ("simple_select", self._benchmark_sync_simple_select),
            ("filtered_select", self._benchmark_sync_filtered_select),
            ("join_query", self._benchmark_sync_join),
            ("aggregation", self._benchmark_sync_aggregation),
            ("bulk_insert", self._benchmark_sync_bulk_insert),
            ("bulk_update", self._benchmark_sync_bulk_update),
        ]

        for _op_name, op_func in operations:
            op_results = op_func(engine, config_no_cache, config_with_cache, db_config["name"])
            results.update(op_results)

        # Cleanup
        engine.dispose()

        return results

    async def _run_async_benchmarks(self, db_config: dict[str, Any]) -> dict[str, TimingResult]:
        """Run asynchronous benchmarks for a database."""
        results = {}

        # Get configs
        config_no_cache, config_with_cache = db_config["sqlspec_config"]

        # Set up SQLAlchemy async engine with optimized pooling
        engine = create_async_engine(
            db_config["sqlalchemy_async_url"],
            poolclass=pool.AsyncAdaptedQueuePool,
            pool_size=20,
            max_overflow=0,
            pool_pre_ping=True,
        )

        # Set up database
        await db_config["setup_func"](engine)

        # Warm up connections
        await self._warmup_async_connections(engine, config_no_cache, config_with_cache)

        # Test operations
        operations = [
            ("simple_select", self._benchmark_async_simple_select),
            ("filtered_select", self._benchmark_async_filtered_select),
            ("join_query", self._benchmark_async_join),
            ("aggregation", self._benchmark_async_aggregation),
            ("bulk_insert", self._benchmark_async_bulk_insert),
            ("bulk_update", self._benchmark_async_bulk_update),
        ]

        for _op_name, op_func in operations:
            op_results = await op_func(engine, config_no_cache, config_with_cache, db_config["name"])
            results.update(op_results)

        # Cleanup
        await engine.dispose()

        return results

    def _warmup_sync_connections(self, engine: Any, config_no_cache: Any, config_with_cache: Any) -> None:
        """Warm up synchronous connections."""
        # SQLAlchemy warmup
        with engine.connect() as conn:
            for _ in range(10):
                list(conn.execute(text("SELECT 1")))

        # SQLSpec warmup
        with config_no_cache.provide_session() as session:
            for _ in range(10):
                list(session.execute(SQL("SELECT 1 as warmup")))

        with config_with_cache.provide_session() as session:
            for _ in range(10):
                list(session.execute(SQL("SELECT 1 as warmup")))

    async def _warmup_async_connections(self, engine: Any, config_no_cache: Any, config_with_cache: Any) -> None:
        """Warm up asynchronous connections."""
        # SQLAlchemy warmup
        async with engine.connect() as conn:
            for _ in range(10):
                await conn.execute(text("SELECT 1"))

        # SQLSpec warmup
        async with config_no_cache.provide_session() as session:
            for _ in range(10):
                await session.execute(SQL("SELECT 1 as warmup"))

        async with config_with_cache.provide_session() as session:
            for _ in range(10):
                await session.execute(SQL("SELECT 1 as warmup"))

    def _benchmark_sync_simple_select(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark simple SELECT operations (sync)."""
        results = {}

        # Pre-compile SQL with database-specific syntax
        simple_query = self._get_simple_select_sql(db_name)
        sql_simple = SQL(simple_query)

        # SQLSpec without cache
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                list(session.execute(sql_simple))

        times = BenchmarkMetrics.time_operation(sqlspec_no_cache, iterations=self.config.iterations, warmup=10)
        results["simple_select_sqlspec_no_cache"] = TimingResult(
            operation="simple_select_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                list(session.execute(sql_simple))

        times = BenchmarkMetrics.time_operation(sqlspec_with_cache, iterations=self.config.iterations, warmup=10)
        results["simple_select_sqlspec_cache"] = TimingResult(
            operation="simple_select_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core with proper constructs
        stmt = select(users_table).limit(100)

        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                list(conn.execute(stmt))

        times = BenchmarkMetrics.time_operation(sqlalchemy_core, iterations=self.config.iterations, warmup=10)
        results["simple_select_core"] = TimingResult(
            operation="simple_select_core", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy ORM with optimized query
        session_local = sessionmaker(bind=engine)

        def sqlalchemy_orm() -> None:
            with session_local() as session:
                # Use query.options() for optimized loading
                list(session.query(User).limit(100).all())

        times = BenchmarkMetrics.time_operation(sqlalchemy_orm, iterations=self.config.iterations, warmup=10)
        results["simple_select_orm"] = TimingResult(
            operation="simple_select_orm", iterations=self.config.iterations, times=times
        )

        return results

    def _benchmark_sync_filtered_select(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark filtered SELECT operations (sync)."""
        results = {}

        filtered_query = f"SELECT * FROM users WHERE status = :status {self._get_limit_clause(db_name, 50)}"
        sql_filtered = SQL(filtered_query, {"status": "active"})

        # SQLSpec without cache
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                list(session.execute(sql_filtered))

        times = BenchmarkMetrics.time_operation(sqlspec_no_cache, iterations=self.config.iterations, warmup=10)
        results["filtered_select_sqlspec_no_cache"] = TimingResult(
            operation="filtered_select_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                list(session.execute(sql_filtered))

        times = BenchmarkMetrics.time_operation(sqlspec_with_cache, iterations=self.config.iterations, warmup=10)
        results["filtered_select_sqlspec_cache"] = TimingResult(
            operation="filtered_select_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core with proper constructs
        stmt = select(users_table).where(users_table.c.status == "active").limit(50)

        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                list(conn.execute(stmt))

        times = BenchmarkMetrics.time_operation(sqlalchemy_core, iterations=self.config.iterations, warmup=10)
        results["filtered_select_core"] = TimingResult(
            operation="filtered_select_core", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy ORM
        session_local = sessionmaker(bind=engine)

        def sqlalchemy_orm() -> None:
            with session_local() as session:
                list(session.query(User).filter(User.status == "active").limit(50).all())

        times = BenchmarkMetrics.time_operation(sqlalchemy_orm, iterations=self.config.iterations, warmup=10)
        results["filtered_select_orm"] = TimingResult(
            operation="filtered_select_orm", iterations=self.config.iterations, times=times
        )

        return results

    def _benchmark_sync_join(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark JOIN operations (sync)."""
        results = {}

        join_query = f"""
            SELECT u1.name, COUNT(u2.id) as colleague_count
            FROM users u1
            LEFT JOIN users u2 ON u1.status = u2.status AND u1.id != u2.id
            WHERE u1.status = :status
            GROUP BY u1.name
            {self._get_limit_clause(db_name, 20)}
        """
        sql_join = SQL(join_query, {"status": "active"})

        # SQLSpec without cache
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                list(session.execute(sql_join))

        times = BenchmarkMetrics.time_operation(sqlspec_no_cache, iterations=min(self.config.iterations, 100), warmup=5)
        results["join_query_sqlspec_no_cache"] = TimingResult(
            operation="join_query_sqlspec_no_cache", iterations=min(self.config.iterations, 100), times=times
        )

        # SQLSpec with cache
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                list(session.execute(sql_join))

        times = BenchmarkMetrics.time_operation(
            sqlspec_with_cache, iterations=min(self.config.iterations, 100), warmup=5
        )
        results["join_query_sqlspec_cache"] = TimingResult(
            operation="join_query_sqlspec_cache", iterations=min(self.config.iterations, 100), times=times
        )

        # SQLAlchemy Core with proper constructs
        u1 = users_table.alias("u1")
        u2 = users_table.alias("u2")

        stmt = (
            select(u1.c.name, func.count(u2.c.id).label("colleague_count"))
            .select_from(u1.outerjoin(u2, (u1.c.status == u2.c.status) & (u1.c.id != u2.c.id)))
            .where(u1.c.status == "active")
            .group_by(u1.c.name)
            .limit(20)
        )

        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                list(conn.execute(stmt))

        times = BenchmarkMetrics.time_operation(sqlalchemy_core, iterations=min(self.config.iterations, 100), warmup=5)
        results["join_query_core"] = TimingResult(
            operation="join_query_core", iterations=min(self.config.iterations, 100), times=times
        )

        # SQLAlchemy ORM (simplified)
        session_local = sessionmaker(bind=engine)

        def sqlalchemy_orm() -> None:
            with session_local() as session:
                list(session.query(User.name).filter(User.status == "active").limit(20).all())

        times = BenchmarkMetrics.time_operation(sqlalchemy_orm, iterations=min(self.config.iterations, 100), warmup=5)
        results["join_query_orm"] = TimingResult(
            operation="join_query_orm", iterations=min(self.config.iterations, 100), times=times
        )

        return results

    def _benchmark_sync_aggregation(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark aggregation operations (sync)."""
        results = {}

        sql_agg = SQL("""
            SELECT status, COUNT(*) as count,
                   COUNT(DISTINCT email) as unique_emails
            FROM users
            GROUP BY status
        """)

        # SQLSpec without cache
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                list(session.execute(sql_agg))

        times = BenchmarkMetrics.time_operation(sqlspec_no_cache, iterations=self.config.iterations, warmup=10)
        results["aggregation_sqlspec_no_cache"] = TimingResult(
            operation="aggregation_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                list(session.execute(sql_agg))

        times = BenchmarkMetrics.time_operation(sqlspec_with_cache, iterations=self.config.iterations, warmup=10)
        results["aggregation_sqlspec_cache"] = TimingResult(
            operation="aggregation_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core
        stmt = text("""
            SELECT status, COUNT(*) as count,
                   COUNT(DISTINCT email) as unique_emails
            FROM users
            GROUP BY status
        """)

        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                list(conn.execute(stmt))

        times = BenchmarkMetrics.time_operation(sqlalchemy_core, iterations=self.config.iterations, warmup=10)
        results["aggregation_core"] = TimingResult(
            operation="aggregation_core", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy ORM
        from sqlalchemy import func

        session_local = sessionmaker(bind=engine)

        def sqlalchemy_orm() -> None:
            with session_local() as session:
                list(
                    session.query(User.status, func.count(User.id), func.count(func.distinct(User.email)))
                    .group_by(User.status)
                    .all()
                )

        times = BenchmarkMetrics.time_operation(sqlalchemy_orm, iterations=self.config.iterations, warmup=10)
        results["aggregation_orm"] = TimingResult(
            operation="aggregation_orm", iterations=self.config.iterations, times=times
        )

        return results

    def _benchmark_sync_bulk_insert(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark bulk INSERT operations (sync)."""
        results = {}

        # First, clean up any existing bulk insert data
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM users WHERE name LIKE 'bulk_user_%'"))

        # Test data - Oracle needs explicit IDs
        if "oracle" in db_name.lower():
            # Get next available ID
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COALESCE(MAX(id), 0) + 1 FROM users"))
                start_id = result.scalar()

            def get_oracle_insert_data(iteration: int) -> list:
                base_id = start_id + (iteration * 100)
                return [
                    {"id": base_id + i, "name": f"bulk_user_{i}", "email": f"bulk{i}@example.com", "status": "active"}
                    for i in range(100)
                ]

            insert_sql = "INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)"
        else:

            def get_insert_data() -> list:
                return [
                    {"name": f"bulk_user_{i}", "email": f"bulk{i}@example.com", "status": "active"} for i in range(100)
                ]

            insert_sql = "INSERT INTO users (name, email, status) VALUES (?, ?, ?)"

        # SQLSpec with cache (bulk operations typically benefit from caching)
        iteration_counter = 0

        def sqlspec_bulk_insert() -> None:
            nonlocal iteration_counter
            with config_with_cache.provide_session() as session:
                # Get fresh data for each iteration
                if "oracle" in db_name.lower():
                    insert_data = get_oracle_insert_data(iteration_counter)
                    tuple_data = [(d["id"], d["name"], d["email"], d["status"]) for d in insert_data]
                else:
                    insert_data = get_insert_data()
                    tuple_data = [(d["name"], d["email"], d["status"]) for d in insert_data]
                session.execute_many(insert_sql, tuple_data)

                # Clean up after each iteration
                session.execute(SQL("DELETE FROM users WHERE name LIKE 'bulk_user_%'"))
            iteration_counter += 1

        times = BenchmarkMetrics.time_operation(
            sqlspec_bulk_insert,
            iterations=min(10, self.config.iterations),  # Fewer iterations for writes
            warmup=2,
        )
        results["bulk_insert_sqlspec"] = TimingResult(
            operation="bulk_insert_sqlspec", iterations=min(10, self.config.iterations), times=times
        )

        # SQLAlchemy Core with bulk operations
        iteration_counter = 0

        def sqlalchemy_core_bulk() -> None:
            nonlocal iteration_counter
            with engine.begin() as conn:
                # Get fresh data for each iteration
                if "oracle" in db_name.lower():
                    insert_data = get_oracle_insert_data(iteration_counter)
                    stmt = insert(users_table).values(
                        id=insert_data[0]["id"],
                        name=insert_data[0]["name"],
                        email=insert_data[0]["email"],
                        status=insert_data[0]["status"],
                    )
                    conn.execute(stmt, insert_data)
                else:
                    insert_data = get_insert_data()
                    stmt = insert(users_table).values(
                        name=insert_data[0]["name"], email=insert_data[0]["email"], status=insert_data[0]["status"]
                    )
                    conn.execute(stmt, insert_data)

                # Clean up after each iteration
                delete_stmt = delete(users_table).where(users_table.c.name.like("bulk_user_%"))
                conn.execute(delete_stmt)
            iteration_counter += 1

        times = BenchmarkMetrics.time_operation(
            sqlalchemy_core_bulk, iterations=min(10, self.config.iterations), warmup=2
        )
        results["bulk_insert_core"] = TimingResult(
            operation="bulk_insert_core", iterations=min(10, self.config.iterations), times=times
        )

        return results

    def _benchmark_sync_bulk_update(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark bulk UPDATE operations (sync)."""
        results = {}

        # Simple WHERE clause for portability across databases
        sql_update = SQL(
            "UPDATE users SET status = :new_status WHERE status = :old_status AND id > :min_id",
            {"new_status": "updated", "old_status": "active", "min_id": 100},
        )

        # SQLSpec with cache
        def sqlspec_update() -> None:
            with config_with_cache.provide_session() as session:
                session.execute(sql_update)

        times = BenchmarkMetrics.time_operation(sqlspec_update, iterations=min(20, self.config.iterations), warmup=5)
        results["bulk_update_sqlspec"] = TimingResult(
            operation="bulk_update_sqlspec", iterations=min(20, self.config.iterations), times=times
        )

        # SQLAlchemy Core with proper constructs
        stmt = (
            update(users_table)
            .where((users_table.c.status == "active") & (users_table.c.id > MIN_ID_THRESHOLD))
            .values(status="updated")
        )

        def sqlalchemy_core_update() -> None:
            with engine.begin() as conn:
                conn.execute(stmt)

        times = BenchmarkMetrics.time_operation(
            sqlalchemy_core_update, iterations=min(20, self.config.iterations), warmup=5
        )
        results["bulk_update_core"] = TimingResult(
            operation="bulk_update_core", iterations=min(20, self.config.iterations), times=times
        )

        return results

    async def _benchmark_async_simple_select(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark simple SELECT operations (async)."""
        results = {}

        # Pre-compile SQL with database-specific syntax
        simple_query = self._get_simple_select_sql(db_name)
        sql_simple = SQL(simple_query)

        # SQLSpec without cache
        async def sqlspec_no_cache() -> None:
            async with config_no_cache.provide_session() as session:
                result = await session.execute(sql_simple)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_no_cache, iterations=self.config.iterations, warmup=10
        )
        results["simple_select_sqlspec_no_cache"] = TimingResult(
            operation="simple_select_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        async def sqlspec_with_cache() -> None:
            async with config_with_cache.provide_session() as session:
                result = await session.execute(sql_simple)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_with_cache, iterations=self.config.iterations, warmup=10
        )
        results["simple_select_sqlspec_cache"] = TimingResult(
            operation="simple_select_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core with proper constructs
        stmt = select(users_table).limit(100)

        async def sqlalchemy_core() -> None:
            async with engine.connect() as conn:
                result = await conn.execute(stmt)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlalchemy_core, iterations=self.config.iterations, warmup=10
        )
        results["simple_select_core"] = TimingResult(
            operation="simple_select_core", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy ORM with optimized async query
        async_session_local = async_sessionmaker(bind=engine)

        async def sqlalchemy_orm() -> None:
            async with async_session_local() as session:
                # Use query with async execution
                from sqlalchemy import select

                stmt = select(User).limit(100)
                result = await session.execute(stmt)
                list(result.scalars().all())

        times = await BenchmarkMetrics.time_operation_async(
            sqlalchemy_orm, iterations=self.config.iterations, warmup=10
        )
        results["simple_select_orm"] = TimingResult(
            operation="simple_select_orm", iterations=self.config.iterations, times=times
        )

        return results

    async def _benchmark_async_filtered_select(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark filtered SELECT operations (async)."""
        results = {}

        filtered_query = f"SELECT * FROM users WHERE status = :status {self._get_limit_clause(db_name, 50)}"
        sql_filtered = SQL(filtered_query, {"status": "active"})

        # SQLSpec without cache
        async def sqlspec_no_cache() -> None:
            async with config_no_cache.provide_session() as session:
                result = await session.execute(sql_filtered)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_no_cache, iterations=self.config.iterations, warmup=10
        )
        results["filtered_select_sqlspec_no_cache"] = TimingResult(
            operation="filtered_select_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        async def sqlspec_with_cache() -> None:
            async with config_with_cache.provide_session() as session:
                result = await session.execute(sql_filtered)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_with_cache, iterations=self.config.iterations, warmup=10
        )
        results["filtered_select_sqlspec_cache"] = TimingResult(
            operation="filtered_select_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core with proper constructs
        stmt = select(users_table).where(users_table.c.status == "active").limit(50)

        async def sqlalchemy_core() -> None:
            async with engine.connect() as conn:
                result = await conn.execute(stmt)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlalchemy_core, iterations=self.config.iterations, warmup=10
        )
        results["filtered_select_core"] = TimingResult(
            operation="filtered_select_core", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy ORM
        async_session_local = async_sessionmaker(bind=engine)

        async def sqlalchemy_orm() -> None:
            async with async_session_local() as session:
                from sqlalchemy import select

                stmt = select(User).where(User.status == "active").limit(50)
                result = await session.execute(stmt)
                list(result.scalars().all())

        times = await BenchmarkMetrics.time_operation_async(
            sqlalchemy_orm, iterations=self.config.iterations, warmup=10
        )
        results["filtered_select_orm"] = TimingResult(
            operation="filtered_select_orm", iterations=self.config.iterations, times=times
        )

        return results

    async def _benchmark_async_join(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark JOIN operations (async)."""
        results = {}

        join_query = f"""
            SELECT u1.name, COUNT(u2.id) as colleague_count
            FROM users u1
            LEFT JOIN users u2 ON u1.status = u2.status AND u1.id != u2.id
            WHERE u1.status = :status
            GROUP BY u1.name
            {self._get_limit_clause(db_name, 20)}
        """
        sql_join = SQL(join_query, {"status": "active"})

        # SQLSpec without cache
        async def sqlspec_no_cache() -> None:
            async with config_no_cache.provide_session() as session:
                result = await session.execute(sql_join)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_no_cache, iterations=min(self.config.iterations, 100), warmup=5
        )
        results["join_query_sqlspec_no_cache"] = TimingResult(
            operation="join_query_sqlspec_no_cache", iterations=min(self.config.iterations, 100), times=times
        )

        # SQLSpec with cache
        async def sqlspec_with_cache() -> None:
            async with config_with_cache.provide_session() as session:
                result = await session.execute(sql_join)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_with_cache, iterations=min(self.config.iterations, 100), warmup=5
        )
        results["join_query_sqlspec_cache"] = TimingResult(
            operation="join_query_sqlspec_cache", iterations=min(self.config.iterations, 100), times=times
        )

        # SQLAlchemy Core with proper constructs
        u1 = users_table.alias("u1")
        u2 = users_table.alias("u2")

        stmt = (
            select(u1.c.name, func.count(u2.c.id).label("colleague_count"))
            .select_from(u1.outerjoin(u2, (u1.c.status == u2.c.status) & (u1.c.id != u2.c.id)))
            .where(u1.c.status == "active")
            .group_by(u1.c.name)
            .limit(20)
        )

        async def sqlalchemy_core() -> None:
            async with engine.connect() as conn:
                result = await conn.execute(stmt)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlalchemy_core, iterations=min(self.config.iterations, 100), warmup=5
        )
        results["join_query_core"] = TimingResult(
            operation="join_query_core", iterations=min(self.config.iterations, 100), times=times
        )

        # SQLAlchemy ORM is complex for self-joins, skip for async

        return results

    async def _benchmark_async_aggregation(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark aggregation operations (async)."""
        results = {}

        sql_agg = SQL("""
            SELECT status, COUNT(*) as count,
                   COUNT(DISTINCT email) as unique_emails
            FROM users
            GROUP BY status
        """)

        # SQLSpec without cache
        async def sqlspec_no_cache() -> None:
            async with config_no_cache.provide_session() as session:
                result = await session.execute(sql_agg)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_no_cache, iterations=self.config.iterations, warmup=10
        )
        results["aggregation_sqlspec_no_cache"] = TimingResult(
            operation="aggregation_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        async def sqlspec_with_cache() -> None:
            async with config_with_cache.provide_session() as session:
                result = await session.execute(sql_agg)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_with_cache, iterations=self.config.iterations, warmup=10
        )
        results["aggregation_sqlspec_cache"] = TimingResult(
            operation="aggregation_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core
        stmt = text("""
            SELECT status, COUNT(*) as count,
                   COUNT(DISTINCT email) as unique_emails
            FROM users
            GROUP BY status
        """)

        async def sqlalchemy_core() -> None:
            async with engine.connect() as conn:
                result = await conn.execute(stmt)
                list(result)

        times = await BenchmarkMetrics.time_operation_async(
            sqlalchemy_core, iterations=self.config.iterations, warmup=10
        )
        results["aggregation_core"] = TimingResult(
            operation="aggregation_core", iterations=self.config.iterations, times=times
        )

        return results

    async def _benchmark_async_bulk_insert(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark bulk INSERT operations (async)."""
        results = {}

        # First, clean up any existing bulk insert data
        async with engine.begin() as conn:
            await conn.execute(text("DELETE FROM users WHERE name LIKE 'bulk_user_%'"))

        # Test data - Oracle needs explicit IDs
        if "oracle" in db_name.lower():
            # Get next available ID
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT COALESCE(MAX(id), 0) + 1 FROM users"))
                start_id = result.scalar()

            def get_oracle_insert_data(iteration: int) -> list:
                base_id = start_id + (iteration * 100)
                return [
                    {"id": base_id + i, "name": f"bulk_user_{i}", "email": f"bulk{i}@example.com", "status": "active"}
                    for i in range(100)
                ]

            insert_sql = "INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)"
        else:

            def get_insert_data() -> list:
                return [
                    {"name": f"bulk_user_{i}", "email": f"bulk{i}@example.com", "status": "active"} for i in range(100)
                ]

            insert_sql = "INSERT INTO users (name, email, status) VALUES (?, ?, ?)"

        # SQLSpec with cache (bulk operations typically benefit from caching)
        iteration_counter = 0

        async def sqlspec_bulk_insert() -> None:
            nonlocal iteration_counter
            async with config_with_cache.provide_session() as session:
                # Get fresh data for each iteration
                if "oracle" in db_name.lower():
                    insert_data = get_oracle_insert_data(iteration_counter)
                    tuple_data = [(d["id"], d["name"], d["email"], d["status"]) for d in insert_data]
                else:
                    insert_data = get_insert_data()
                    tuple_data = [(d["name"], d["email"], d["status"]) for d in insert_data]
                await session.execute_many(insert_sql, tuple_data)

                # Clean up after each iteration
                await session.execute(SQL("DELETE FROM users WHERE name LIKE 'bulk_user_%'"))
            iteration_counter += 1

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_bulk_insert,
            iterations=min(10, self.config.iterations),  # Fewer iterations for writes
            warmup=2,
        )
        results["bulk_insert_sqlspec"] = TimingResult(
            operation="bulk_insert_sqlspec", iterations=min(10, self.config.iterations), times=times
        )

        # SQLAlchemy Core with bulk operations
        iteration_counter = 0

        async def sqlalchemy_core_bulk() -> None:
            nonlocal iteration_counter
            async with engine.begin() as conn:
                # Get fresh data for each iteration
                if "oracle" in db_name.lower():
                    insert_data = get_oracle_insert_data(iteration_counter)
                    stmt = insert(users_table).values(
                        id=insert_data[0]["id"],
                        name=insert_data[0]["name"],
                        email=insert_data[0]["email"],
                        status=insert_data[0]["status"],
                    )
                    await conn.execute(stmt, insert_data)
                else:
                    insert_data = get_insert_data()
                    stmt = insert(users_table).values(
                        name=insert_data[0]["name"], email=insert_data[0]["email"], status=insert_data[0]["status"]
                    )
                    await conn.execute(stmt, insert_data)

                # Clean up after each iteration
                delete_stmt = delete(users_table).where(users_table.c.name.like("bulk_user_%"))
                await conn.execute(delete_stmt)
            iteration_counter += 1

        times = await BenchmarkMetrics.time_operation_async(
            sqlalchemy_core_bulk, iterations=min(10, self.config.iterations), warmup=2
        )
        results["bulk_insert_core"] = TimingResult(
            operation="bulk_insert_core", iterations=min(10, self.config.iterations), times=times
        )

        return results

    async def _benchmark_async_bulk_update(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark bulk UPDATE operations (async)."""
        results = {}

        # Simple WHERE clause for portability across databases
        sql_update = SQL(
            "UPDATE users SET status = :new_status WHERE status = :old_status AND id > :min_id",
            {"new_status": "updated", "old_status": "active", "min_id": 100},
        )

        # SQLSpec with cache
        async def sqlspec_update() -> None:
            async with config_with_cache.provide_session() as session:
                await session.execute(sql_update)

        times = await BenchmarkMetrics.time_operation_async(
            sqlspec_update, iterations=min(20, self.config.iterations), warmup=5
        )
        results["bulk_update_sqlspec"] = TimingResult(
            operation="bulk_update_sqlspec", iterations=min(20, self.config.iterations), times=times
        )

        # SQLAlchemy Core with proper constructs
        stmt = (
            update(users_table)
            .where((users_table.c.status == "active") & (users_table.c.id > MIN_ID_THRESHOLD))
            .values(status="updated")
        )

        async def sqlalchemy_core_update() -> None:
            async with engine.begin() as conn:
                await conn.execute(stmt)

        times = await BenchmarkMetrics.time_operation_async(
            sqlalchemy_core_update, iterations=min(20, self.config.iterations), warmup=5
        )
        results["bulk_update_core"] = TimingResult(
            operation="bulk_update_core", iterations=min(20, self.config.iterations), times=times
        )

        return results
