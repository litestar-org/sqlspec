import os
import time

from sqlspec.core.pipeline import get_statement_pipeline_metrics, reset_statement_pipeline_cache
from sqlspec.core.statement import SQL, get_default_config

__all__ = ("run_benchmark",)


# Enable metrics
os.environ["SQLSPEC_DEBUG_PIPELINE_CACHE"] = "1"


def run_benchmark() -> None:
    reset_statement_pipeline_cache()
    config = get_default_config()

    sql = "INSERT INTO table VALUES (?)"

    time.perf_counter()
    for i in range(10_000):
        # Create new SQL object every time (simulating driver.execute)
        stmt = SQL(sql, (i,), statement_config=config)
        stmt.compile()
    time.perf_counter()

    metrics = get_statement_pipeline_metrics()
    if metrics:
        metrics[0]
    else:
        pass


if __name__ == "__main__":
    run_benchmark()
