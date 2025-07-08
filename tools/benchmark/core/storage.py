"""DuckDB storage backend for benchmark results."""

import datetime as dt
import json
from pathlib import Path
from typing import Any, Optional

import duckdb

from tools.benchmark.config import BenchmarkConfig
from tools.benchmark.core.metrics import SystemInfo, TimingResult


class BenchmarkStorage:
    """Storage backend for benchmark results using DuckDB."""

    def __init__(self, config: BenchmarkConfig) -> None:
        self.config = config
        self.db_path = config.storage_path
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Ensure database schema exists."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with duckdb.connect(str(self.db_path)) as conn:
            # Benchmark runs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS benchmark_runs (
                    run_id VARCHAR PRIMARY KEY,
                    timestamp TIMESTAMP,
                    benchmark_type VARCHAR,
                    adapter VARCHAR,
                    iterations INTEGER,
                    system_info JSON,
                    sqlspec_info JSON,
                    sqlglot_info JSON,
                    metadata JSON
                )
            """)

            # Benchmark results table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS benchmark_results (
                    result_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR REFERENCES benchmark_runs(run_id),
                    operation VARCHAR,
                    min_ms DOUBLE,
                    max_ms DOUBLE,
                    avg_ms DOUBLE,
                    std_ms DOUBLE,
                    ops_per_sec DOUBLE,
                    metadata JSON
                )
            """)

            # Raw timing samples (optional, for detailed analysis)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS benchmark_samples (
                    sample_id VARCHAR PRIMARY KEY,
                    result_id VARCHAR REFERENCES benchmark_results(result_id),
                    iteration INTEGER,
                    time_ms DOUBLE
                )
            """)

            # Create views for analysis
            conn.execute("""
                CREATE OR REPLACE VIEW latest_results AS
                SELECT
                    br.*,
                    b.timestamp,
                    b.benchmark_type,
                    b.adapter
                FROM benchmark_results br
                JOIN benchmark_runs b ON br.run_id = b.run_id
                WHERE b.timestamp >= CURRENT_DATE - INTERVAL '30 days'
            """)

            conn.execute("""
                CREATE OR REPLACE VIEW performance_trends AS
                SELECT
                    benchmark_type,
                    adapter,
                    operation,
                    DATE_TRUNC('day', timestamp) as day,
                    AVG(avg_ms) as daily_avg_ms,
                    MIN(min_ms) as daily_min_ms,
                    MAX(max_ms) as daily_max_ms,
                    COUNT(*) as run_count
                FROM benchmark_results br
                JOIN benchmark_runs b ON br.run_id = b.run_id
                GROUP BY 1, 2, 3, 4
                ORDER BY 1, 2, 3, 4
            """)

    def save_run(
        self,
        run_id: str,
        benchmark_type: str,
        adapter: str,
        iterations: int,
        system_info: SystemInfo,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Save a benchmark run."""
        from tools.benchmark.core.metrics import BenchmarkMetrics

        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(
                """
                INSERT INTO benchmark_runs
                (run_id, timestamp, benchmark_type, adapter, iterations,
                 system_info, sqlspec_info, sqlglot_info, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                [
                    run_id,
                    dt.datetime.now(),
                    benchmark_type,
                    adapter,
                    iterations,
                    json.dumps(system_info.to_dict()),
                    json.dumps(BenchmarkMetrics.get_sqlspec_info()),
                    json.dumps(BenchmarkMetrics.get_sqlglot_info()),
                    json.dumps(metadata or {}),
                ],
            )

    def save_result(
        self,
        run_id: str,
        result: TimingResult,
        save_samples: bool = False,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Save a benchmark result."""
        import uuid

        result_id = str(uuid.uuid4())

        with duckdb.connect(str(self.db_path)) as conn:
            # Save result summary
            conn.execute(
                """
                INSERT INTO benchmark_results
                (result_id, run_id, operation, min_ms, max_ms, avg_ms, std_ms, ops_per_sec, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                [
                    result_id,
                    run_id,
                    result.operation,
                    result.min_ms,
                    result.max_ms,
                    result.avg_ms,
                    result.std_ms,
                    result.ops_per_sec,
                    json.dumps(metadata or {}),
                ],
            )

            # Optionally save raw samples
            if save_samples:
                samples = [
                    (str(uuid.uuid4()), result_id, i, t * 1000)
                    for i, t in enumerate(result.times)
                ]
                conn.executemany(
                    """
                    INSERT INTO benchmark_samples
                    (sample_id, result_id, iteration, time_ms)
                    VALUES ($1, $2, $3, $4)
                    """,
                    samples,
                )

    def get_comparison_baseline(
        self,
        benchmark_type: str,
        adapter: str,
        operation: str,
        days: int = 7,
    ) -> Optional[dict[str, float]]:
        """Get baseline metrics for comparison."""
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute(
                f"""
                SELECT
                    AVG(avg_ms) as baseline_avg_ms,
                    MIN(min_ms) as baseline_min_ms,
                    MAX(max_ms) as baseline_max_ms,
                    COUNT(*) as sample_count
                FROM benchmark_results br
                JOIN benchmark_runs b ON br.run_id = b.run_id
                WHERE b.benchmark_type = $1
                  AND b.adapter = $2
                  AND br.operation = $3
                  AND b.timestamp >= CURRENT_DATE - INTERVAL '{days}' DAY
                """,
                [benchmark_type, adapter, operation],
            ).fetchone()

            if result and result[3] > 0:  # sample_count > 0
                return {
                    "avg_ms": result[0],
                    "min_ms": result[1],
                    "max_ms": result[2],
                    "sample_count": result[3],
                }

            return None

    def cleanup_old_data(self, retention_days: int) -> int:
        """Clean up data older than retention period."""
        with duckdb.connect(str(self.db_path)) as conn:
            # Get count before deletion
            count_result = conn.execute(
                f"""
                SELECT COUNT(*) FROM benchmark_runs
                WHERE timestamp < CURRENT_DATE - INTERVAL '{retention_days}' DAY
                """
            ).fetchone()

            rows_to_delete = count_result[0] if count_result else 0

            if rows_to_delete > 0:
                # Delete old runs (cascades to results and samples)
                conn.execute(
                    f"""
                    DELETE FROM benchmark_runs
                    WHERE timestamp < CURRENT_DATE - INTERVAL '{retention_days}' DAY
                    """
                )

            return rows_to_delete

    def import_json_results(self, json_path: Path) -> None:
        """Import legacy JSON results into DuckDB."""
        import uuid

        with open(json_path) as f:
            data = json.load(f)

        # Generate run ID
        run_id = str(uuid.uuid4())

        # Save run
        self.save_run(
            run_id=run_id,
            benchmark_type=data.get("benchmark_type", "unknown"),
            adapter=data.get("adapter", "unknown"),
            iterations=data.get("iterations", 0),
            system_info=SystemInfo(),  # Will use current system info
            metadata={"imported_from": str(json_path)},
        )

        # Save results
        for operation, metrics in data.get("results", {}).items():
            # Create a minimal TimingResult
            result = TimingResult(
                operation=operation,
                iterations=data.get("iterations", 0),
                times=[metrics["avg_ms"] / 1000],  # Convert back to seconds
            )
            # Override calculated values with imported values
            result.min_ms = metrics.get("min_ms", result.min_ms)
            result.max_ms = metrics.get("max_ms", result.max_ms)
            result.avg_ms = metrics.get("avg_ms", result.avg_ms)
            result.std_ms = metrics.get("std_ms", 0.0)
            result.ops_per_sec = metrics.get("ops_per_sec", 1000 / result.avg_ms)

            self.save_result(run_id, result)
