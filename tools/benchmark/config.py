"""Configuration management for benchmark tool."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark execution."""

    # Execution settings
    iterations: int = 1000
    warmup_iterations: int = 10
    quick_mode: bool = False

    # Storage settings
    storage_path: Path = field(default_factory=lambda: Path(".benchmark/results.duckdb"))
    results_retention_days: int = 30

    # Container settings
    keep_containers: bool = False
    no_containers: bool = False
    container_timeout: int = 60  # seconds

    # Performance settings
    with_rust: bool = True  # Test with sqlglot[rs] if available
    enable_caching: bool = True

    # Analysis settings
    regression_threshold: float = 0.05  # 5% regression threshold
    comparison_days: int = 7  # Compare against 7-day average

    # Visual settings
    show_progress: bool = True
    verbose: bool = False

    @classmethod
    def from_env(cls) -> "BenchmarkConfig":
        """Create config from environment variables."""
        config = cls()

        # Override from environment
        if val := os.getenv("SQLSPEC_BENCHMARK_STORAGE"):
            config.storage_path = Path(val)
        if val := os.getenv("SQLSPEC_BENCHMARK_ITERATIONS"):
            config.iterations = int(val)
        if val := os.getenv("SQLSPEC_BENCHMARK_QUICK"):
            config.quick_mode = val.lower() in ("true", "1", "yes")

        return config


@dataclass
class DockerConfig:
    """Docker container configuration."""

    # PostgreSQL
    POSTGRES_IMAGE: Final[str] = "postgres:latest"
    POSTGRES_CONTAINER_NAME: Final[str] = "sqlspec-benchmark-postgres"
    POSTGRES_DEFAULT_PORT: Final[int] = 5432
    POSTGRES_DEFAULT_USER: Final[str] = "postgres"
    POSTGRES_DEFAULT_PASSWORD: Final[str] = "postgres"
    POSTGRES_DEFAULT_DB: Final[str] = "postgres"

    # Oracle
    ORACLE_IMAGE: Final[str] = "gvenzl/oracle-free:23-slim-faststart"
    ORACLE_CONTAINER_NAME: Final[str] = "sqlspec-benchmark-oracle"
    ORACLE_DEFAULT_PORT: Final[int] = 1521
    ORACLE_DEFAULT_USER: Final[str] = "system"
    ORACLE_DEFAULT_PASSWORD: Final[str] = "oracle"
    ORACLE_DEFAULT_SERVICE_NAME: Final[str] = "FREEPDB1"

    # MySQL
    MYSQL_IMAGE: Final[str] = "mysql:latest"
    MYSQL_CONTAINER_NAME: Final[str] = "sqlspec-benchmark-mysql"
    MYSQL_DEFAULT_PORT: Final[int] = 3306
    MYSQL_DEFAULT_USER: Final[str] = "root"
    MYSQL_DEFAULT_PASSWORD: Final[str] = "mysql"
    MYSQL_DEFAULT_DB: Final[str] = "test"


# Constants
PERFORMANCE_IMPROVEMENT_THRESHOLD: Final[float] = -5.0  # 5% improvement
PERFORMANCE_REGRESSION_THRESHOLD: Final[float] = 5.0  # 5% regression
MIN_COMPARISON_FILES: Final[int] = 2
