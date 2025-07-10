"""Optimized ORM comparison benchmark with pre-compiled statements."""

from typing import Any

from rich.panel import Panel
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, func, select, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

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


class OptimizedORMComparisonBenchmark(BaseBenchmarkSuite):
    """Optimized ORM benchmark with pre-compiled statements."""

    def __init__(self, config: Any, runner: Any, console: Any) -> None:
        super().__init__(config, runner, console)
        self.container_manager = ContainerManager(console)

        # Pre-compile SQL statements for SQLSpec
        self.sqlspec_statements = {
            "simple_select": SQL("SELECT * FROM users LIMIT 100"),
            "filtered_select": SQL("SELECT * FROM users WHERE status = :status LIMIT 50", {"status": "active"}),
            "join_query": SQL("""
                SELECT u1.name, COUNT(u2.id) as colleague_count
                FROM users u1
                LEFT JOIN users u2 ON u1.status = u2.status AND u1.id != u2.id
                WHERE u1.status = :status
                GROUP BY u1.name
                LIMIT 20
            """, {"status": "active"}),
            "aggregation": SQL("""
                SELECT status, COUNT(*) as count,
                       COUNT(DISTINCT email) as unique_emails
                FROM users
                GROUP BY status
            """),
            "delete_bulk": SQL("DELETE FROM users WHERE name LIKE 'bulk_user_%'"),
        }

        # Pre-compile SQLAlchemy Core statements
        self.core_statements = {
            "simple_select": select(users_table).limit(100),
            "filtered_select": select(users_table).where(users_table.c.status == "active").limit(50),
            "aggregation": text("""
                SELECT status, COUNT(*) as count,
                       COUNT(DISTINCT email) as unique_emails
                FROM users
                GROUP BY status
            """),
        }

        # Pre-compile join statement with aliases
        u1 = users_table.alias("u1")
        u2 = users_table.alias("u2")
        self.core_statements["join_query"] = (
            select(u1.c.name, func.count(u2.c.id).label("colleague_count"))
            .select_from(u1.outerjoin(u2, (u1.c.status == u2.c.status) & (u1.c.id != u2.c.id)))
            .where(u1.c.status == "active")
            .group_by(u1.c.name)
            .limit(20)
        )

    @property
    def name(self) -> str:
        return "orm_comparison_optimized"

    @property
    def description(self) -> str:
        return "Optimized ORM Performance Comparison with Pre-compiled Statements"

    def run(self, adapter: str = "all", **kwargs: Any) -> dict[str, TimingResult]:
        """Run optimized ORM comparison benchmarks."""
        self.console.print(
            Panel.fit(
                f"[bold]Optimized ORM Comparison Benchmark[/bold]\n"
                f"Iterations: [cyan]{self.config.iterations:,}[/cyan]\n"
                f"Testing: SQLite only with pre-compiled statements\n"
                f"Focus: Fair comparison with caching analysis",
                border_style="blue",
            )
        )

        # Start run
        self.runner.start_run(
            benchmark_type=self.name,
            adapter="sqlite",
            metadata={"test_type": "orm_comparison_optimized", "sqlglot_version": "standard"},
        )

        results = {}

        # Test SQLite sync only for focused comparison
        db_config = {
            "name": "SQLite",
            "type": "sync",
            "sqlalchemy_url": "sqlite:///.benchmark/test_optimized.db",
            "sqlspec_config": self._get_sqlite_configs(),
            "setup_func": self._setup_sqlite,
        }

        self.console.print(f"\n[bold cyan]Testing {db_config['name']}...[/bold cyan]")
        db_results = self._run_sync_benchmarks(db_config)

        # Add results with database prefix
        for key, result in db_results.items():
            full_key = f"sqlite_{key}"
            results[full_key] = result

        return results

    def _get_sqlite_configs(self) -> tuple[SqliteConfig, SqliteConfig]:
        """Get SQLite configs with and without caching."""
        db_path = ".benchmark/test_optimized.db"

        config_no_cache = SqliteConfig(
            connection_config={"database": db_path},
            statement_config=SQLConfig(
                enable_caching=False,
                enable_expression_simplification=False  # Disable optimization to reduce overhead
            )
        )

        config_with_cache = SqliteConfig(
            connection_config={"database": db_path},
            statement_config=SQLConfig(
                enable_caching=True,
                enable_expression_simplification=True  # Enable optimization for cached version
            )
        )

        return config_no_cache, config_with_cache

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

        # Test operations with pre-compiled statements
        operations = [
            ("simple_select", self._benchmark_sync_simple_select),
            ("filtered_select", self._benchmark_sync_filtered_select),
            ("join_query", self._benchmark_sync_join),
            ("aggregation", self._benchmark_sync_aggregation),
            ("complex_query", self._benchmark_sync_complex_query),
        ]

        for _op_name, op_func in operations:
            op_results = op_func(engine, config_no_cache, config_with_cache, db_config["name"])
            results.update(op_results)

        # Cleanup
        engine.dispose()

        return results

    def _warmup_sync_connections(self, engine: Any, config_no_cache: Any, config_with_cache: Any) -> None:
        """Warm up synchronous connections."""
        # SQLAlchemy warmup
        with engine.connect() as conn:
            for _ in range(10):
                list(conn.execute(text("SELECT 1")))

        # SQLSpec warmup - use pre-compiled statement
        warmup_sql = SQL("SELECT 1 as warmup")
        with config_no_cache.provide_session() as session:
            for _ in range(10):
                list(session.execute(warmup_sql))

        with config_with_cache.provide_session() as session:
            for _ in range(10):
                list(session.execute(warmup_sql))

    def _benchmark_sync_simple_select(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark simple SELECT operations with pre-compiled statements."""
        results = {}

        # Pre-compiled statements
        sqlspec_stmt = self.sqlspec_statements["simple_select"]
        core_stmt = self.core_statements["simple_select"]

        # SQLSpec without cache
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                list(session.execute(sqlspec_stmt))

        times = BenchmarkMetrics.time_operation(sqlspec_no_cache, iterations=self.config.iterations, warmup=10)
        results["simple_select_sqlspec_no_cache"] = TimingResult(
            operation="simple_select_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                list(session.execute(sqlspec_stmt))

        times = BenchmarkMetrics.time_operation(sqlspec_with_cache, iterations=self.config.iterations, warmup=10)
        results["simple_select_sqlspec_cache"] = TimingResult(
            operation="simple_select_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core with pre-compiled statement
        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                list(conn.execute(core_stmt))

        times = BenchmarkMetrics.time_operation(sqlalchemy_core, iterations=self.config.iterations, warmup=10)
        results["simple_select_core"] = TimingResult(
            operation="simple_select_core", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy ORM with optimized query
        session_local = sessionmaker(bind=engine)

        def sqlalchemy_orm() -> None:
            with session_local() as session:
                list(session.query(User).limit(100).all())

        times = BenchmarkMetrics.time_operation(sqlalchemy_orm, iterations=self.config.iterations, warmup=10)
        results["simple_select_orm"] = TimingResult(
            operation="simple_select_orm", iterations=self.config.iterations, times=times
        )

        return results

    def _benchmark_sync_filtered_select(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark filtered SELECT operations with pre-compiled statements."""
        results = {}

        # Pre-compiled statements
        sqlspec_stmt = self.sqlspec_statements["filtered_select"]
        core_stmt = self.core_statements["filtered_select"]

        # SQLSpec without cache
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                list(session.execute(sqlspec_stmt))

        times = BenchmarkMetrics.time_operation(sqlspec_no_cache, iterations=self.config.iterations, warmup=10)
        results["filtered_select_sqlspec_no_cache"] = TimingResult(
            operation="filtered_select_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                list(session.execute(sqlspec_stmt))

        times = BenchmarkMetrics.time_operation(sqlspec_with_cache, iterations=self.config.iterations, warmup=10)
        results["filtered_select_sqlspec_cache"] = TimingResult(
            operation="filtered_select_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core with pre-compiled statement
        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                list(conn.execute(core_stmt))

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
        """Benchmark JOIN operations with pre-compiled statements."""
        results = {}

        # Pre-compiled statements
        sqlspec_stmt = self.sqlspec_statements["join_query"]
        core_stmt = self.core_statements["join_query"]

        # SQLSpec without cache
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                list(session.execute(sqlspec_stmt))

        times = BenchmarkMetrics.time_operation(sqlspec_no_cache, iterations=min(self.config.iterations, 100), warmup=5)
        results["join_query_sqlspec_no_cache"] = TimingResult(
            operation="join_query_sqlspec_no_cache", iterations=min(self.config.iterations, 100), times=times
        )

        # SQLSpec with cache
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                list(session.execute(sqlspec_stmt))

        times = BenchmarkMetrics.time_operation(
            sqlspec_with_cache, iterations=min(self.config.iterations, 100), warmup=5
        )
        results["join_query_sqlspec_cache"] = TimingResult(
            operation="join_query_sqlspec_cache", iterations=min(self.config.iterations, 100), times=times
        )

        # SQLAlchemy Core with pre-compiled statement
        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                list(conn.execute(core_stmt))

        times = BenchmarkMetrics.time_operation(sqlalchemy_core, iterations=min(self.config.iterations, 100), warmup=5)
        results["join_query_core"] = TimingResult(
            operation="join_query_core", iterations=min(self.config.iterations, 100), times=times
        )

        return results

    def _benchmark_sync_aggregation(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark aggregation operations with pre-compiled statements."""
        results = {}

        # Pre-compiled statements
        sqlspec_stmt = self.sqlspec_statements["aggregation"]
        core_stmt = self.core_statements["aggregation"]

        # SQLSpec without cache
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                list(session.execute(sqlspec_stmt))

        times = BenchmarkMetrics.time_operation(sqlspec_no_cache, iterations=self.config.iterations, warmup=10)
        results["aggregation_sqlspec_no_cache"] = TimingResult(
            operation="aggregation_sqlspec_no_cache", iterations=self.config.iterations, times=times
        )

        # SQLSpec with cache
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                list(session.execute(sqlspec_stmt))

        times = BenchmarkMetrics.time_operation(sqlspec_with_cache, iterations=self.config.iterations, warmup=10)
        results["aggregation_sqlspec_cache"] = TimingResult(
            operation="aggregation_sqlspec_cache", iterations=self.config.iterations, times=times
        )

        # SQLAlchemy Core with pre-compiled statement
        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                list(conn.execute(core_stmt))

        times = BenchmarkMetrics.time_operation(sqlalchemy_core, iterations=self.config.iterations, warmup=10)
        results["aggregation_core"] = TimingResult(
            operation="aggregation_core", iterations=self.config.iterations, times=times
        )

        return results

    def _benchmark_sync_complex_query(
        self, engine: Any, config_no_cache: Any, config_with_cache: Any, db_name: str
    ) -> dict[str, TimingResult]:
        """Benchmark complex queries where caching shows more benefit."""
        results = {}

        # Complex query with multiple subqueries and CTEs
        complex_query_template = """
        WITH active_users AS (
            SELECT id, name, email
            FROM users
            WHERE status = 'active' AND email LIKE :pattern
        ),
        user_stats AS (
            SELECT
                status,
                COUNT(*) as user_count,
                COUNT(DISTINCT SUBSTR(email, INSTR(email, '@'))) as domain_count
            FROM users
            GROUP BY status
        )
        SELECT
            au.name,
            au.email,
            us.user_count,
            us.domain_count
        FROM active_users au
        CROSS JOIN user_stats us
        WHERE us.status = 'active'
        ORDER BY au.name
        LIMIT 50
        """

        # Test with different parameter values to show caching benefit
        patterns = ["%example.com", "%test.com", "%demo.com", "%sample.com", "%mail.com"]

        # SQLSpec without cache - parse each time
        def sqlspec_no_cache() -> None:
            with config_no_cache.provide_session() as session:
                for pattern in patterns:
                    # Create new SQL object each time (simulating real-world usage)
                    sql = SQL(complex_query_template, {"pattern": pattern})
                    list(session.execute(sql))

        times = BenchmarkMetrics.time_operation(
            sqlspec_no_cache, iterations=min(self.config.iterations // 5, 20), warmup=2
        )
        results["complex_query_sqlspec_no_cache"] = TimingResult(
            operation="complex_query_sqlspec_no_cache", iterations=len(times), times=times
        )

        # SQLSpec with cache - benefit from cached parsing
        def sqlspec_with_cache() -> None:
            with config_with_cache.provide_session() as session:
                for pattern in patterns:
                    # Create new SQL object each time (but parsing is cached)
                    sql = SQL(complex_query_template, {"pattern": pattern})
                    list(session.execute(sql))

        times = BenchmarkMetrics.time_operation(
            sqlspec_with_cache, iterations=min(self.config.iterations // 5, 20), warmup=2
        )
        results["complex_query_sqlspec_cache"] = TimingResult(
            operation="complex_query_sqlspec_cache", iterations=len(times), times=times
        )

        # SQLAlchemy Core - parse each time
        def sqlalchemy_core() -> None:
            with engine.connect() as conn:
                for pattern in patterns:
                    # Parse SQL each time
                    stmt = text(complex_query_template)
                    list(conn.execute(stmt, {"pattern": pattern}))

        times = BenchmarkMetrics.time_operation(
            sqlalchemy_core, iterations=min(self.config.iterations // 5, 20), warmup=2
        )
        results["complex_query_core"] = TimingResult(
            operation="complex_query_core", iterations=len(times), times=times
        )

        return results

    def cleanup(self) -> None:
        """Clean up after benchmarks."""
        # Close any containers if needed
        if hasattr(self.container_manager, "cleanup"):
            self.container_manager.cleanup()

