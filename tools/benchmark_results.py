import time

ROWS = 10000
COLS = 5
COL_NAMES = [f"col_{i}" for i in range(COLS)]
DATA = [tuple(range(COLS)) for _ in range(ROWS)]

def bench_fetchall_sim():
    # Simulate fetchall() returning list of tuples
    start = time.perf_counter()
    res = list(DATA)
    time.perf_counter() - start
    return res

def bench_dict_construction():
    rows = list(DATA)
    names = COL_NAMES
    start = time.perf_counter()
    # This matches sqlspec/adapters/sqlite/core.py:collect_rows
    data = [dict(zip(names, row, strict=False)) for row in rows]
    time.perf_counter() - start
    return data

if __name__ == "__main__":
    bench_fetchall_sim()
    bench_dict_construction()
