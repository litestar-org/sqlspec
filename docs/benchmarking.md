# SQLSpec Performance Benchmarking Guide

This document provides instructions on how to run the performance benchmarks for SQLSpec, compare its performance against SQLAlchemy, and measure the impact of SQLSpec's caching mechanism.

## Prerequisites

Before running the benchmarks, ensure you have `uv` installed and the project dependencies are set up:

```bash
uv sync
```

For benchmarks involving PostgreSQL or Oracle, you need to have Docker installed and the respective database containers running. If you don't have them running, you can start them using the following commands:

### Start PostgreSQL Container

```bash
docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
```

### Start Oracle Container

```bash
docker run --name some-oracle -e ORACLE_PASSWORD=mysecretpassword -p 1521:1521 -d gvenzl/oracle-free:23-slim-faststart
```

## Running Benchmarks

The benchmarking tool is located at `tools/benchmark_performance.py`. You can run specific benchmarks or all of them.

### Run All Benchmarks

To run all available benchmarks, including SQL compilation, parameter styles, typed parameters, and ORM comparison:

```bash
uv run tools/benchmark_performance.py run-all
```

### Run Specific Benchmarks

#### SQL Compilation Benchmark

Measures the performance of SQL compilation and caching within SQLSpec.

```bash
uv run tools/benchmark_performance.py sql-compilation --iterations 10000
```

#### Parameter Styles Benchmark

Compares the performance of different parameter styles for a given database adapter.

```bash
uv run tools/benchmark_performance.py parameter-styles --adapter sqlite --iterations 1000
```

Replace `sqlite` with any of the supported adapters: `sqlite`, `duckdb`, `psycopg`, `aiosqlite`, `asyncpg`, `oracledb`, `asyncmy`, `psqlpy`.

#### TypedParameter Performance Benchmark

Measures the overhead of `TypedParameter` wrapping.

```bash
uv run tools/benchmark_performance.py typed-parameters --iterations 1000
```

#### ORM Comparison Benchmark

Compares SQLSpec's query execution performance against SQLAlchemy Core and SQLAlchemy ORM. This benchmark will attempt to connect to PostgreSQL and Oracle containers. Ensure they are running as described in the Prerequisites section.

```bash
uv run tools/benchmark_performance.py orm-comparison --iterations 100
```

If you have existing PostgreSQL or Oracle databases, you can provide their DSNs:

```bash
uv run tools/benchmark_performance.py orm-comparison \
    --postgres-dsn "postgresql://user:pass@host:port/db" \
    --oracle-dsn "oracle://user:pass@host:port/service"
```

## Comparing Benchmark Results Over Time

To compare current benchmark results with previous runs, use the `compare` command. This will show performance changes over time.

```bash
uv run tools/benchmark_performance.py compare
```

Results are saved as JSON files in the `benchmarks/` directory.

