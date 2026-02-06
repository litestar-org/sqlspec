import time

import sqlglot

SQL = "INSERT INTO notes (body) VALUES (?)"
DIALECT = "sqlite"
ITERATIONS = 10000


def bench_parse() -> float:
    start = time.perf_counter()
    for _ in range(ITERATIONS):
        sqlglot.parse_one(SQL, read=DIALECT)
    return time.perf_counter() - start


def bench_build() -> float:
    parsed = sqlglot.parse_one(SQL, read=DIALECT)
    start = time.perf_counter()
    for _ in range(ITERATIONS):
        parsed.sql(dialect=DIALECT)
    return time.perf_counter() - start


def bench_raw_string() -> float:
    start = time.perf_counter()
    for _ in range(ITERATIONS):
        _ = str(SQL)
    return time.perf_counter() - start


if __name__ == "__main__":
    parse_time = bench_parse()
    build_time = bench_build()
    raw_time = bench_raw_string()

    total_sqlglot = parse_time + build_time
