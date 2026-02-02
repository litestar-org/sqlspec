import os
import time
from sqlspec.core.statement import get_default_config, SQL
from sqlspec.core.pipeline import get_statement_pipeline_metrics, reset_statement_pipeline_cache

# Enable metrics
os.environ["SQLSPEC_DEBUG_PIPELINE_CACHE"] = "1"

def run_benchmark():
    reset_statement_pipeline_cache()
    config = get_default_config()
    
    sql = "INSERT INTO table VALUES (?)"
    
    start = time.perf_counter()
    for i in range(10_000):
        # Create new SQL object every time (simulating driver.execute)
        stmt = SQL(sql, (i,), statement_config=config)
        stmt.compile()
    end = time.perf_counter()
    
    print(f"Time: {end - start:.4f}s")
    
    metrics = get_statement_pipeline_metrics()
    if metrics:
        m = metrics[0]
        print(f"Hits: {m['hits']}")
        print(f"Misses: {m['misses']}")
        print(f"Parse Hits: {m['parse_hits']}")
        print(f"Parse Misses: {m['parse_misses']}")
    else:
        print("No metrics found")

if __name__ == "__main__":
    run_benchmark()
