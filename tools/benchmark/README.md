# SQLSpec Benchmark Tool

A comprehensive performance benchmarking framework for SQLSpec.

## Overview

This modular benchmark tool replaces the original monolithic `benchmark_performance.py` script with a well-organized module structure that provides:

- **Performance Testing**: Benchmarks for SQL parsing, compilation, parameter styles, and TypedParameter overhead
- **Historical Analysis**: DuckDB storage for comparing performance over time and detecting regressions
- **Rich CLI Interface**: Beautiful terminal interface with progress bars and formatted results
- **Containerized Testing**: Automatic Docker container management for database adapters

## Quick Start

```bash
# Install benchmark dependencies
uv sync --extra benchmark

# Run all benchmarks in quick mode
uv run python -m tools.benchmark run --quick

# Run specific benchmark suite
uv run python -m tools.benchmark run --suite sql-compilation

# Run parameter styles for specific adapter
uv run python -m tools.benchmark run --suite parameter-styles --adapter sqlite
```

## Module Structure

```
tools/benchmark/
├── __init__.py              # Main module
├── __main__.py              # Entry point (python -m tools.benchmark)
├── cli.py                   # Rich-Click CLI interface
├── config.py                # Configuration management
├── core/                    # Core functionality
│   ├── metrics.py           # Performance metrics and system info
│   ├── runner.py            # Benchmark orchestration
│   └── storage.py           # DuckDB storage backend
├── suites/                  # Benchmark suites
│   ├── base.py              # Abstract base class
│   ├── parameter_styles.py  # Parameter conversion benchmarks
│   ├── sql_compilation.py   # SQL parsing/compilation benchmarks
│   ├── typed_parameters.py  # TypedParameter overhead benchmarks
│   ├── orm_comparison.py    # SQLSpec vs SQLAlchemy benchmarks
│   └── async_operations.py  # Async operations (TODO)
├── infrastructure/          # Infrastructure management
│   └── containers.py        # Docker container management
└── visualization/           # Reporting (TODO)
```

## Features

### Benchmark Suites

1. **SQL Compilation** (`sql-compilation`)
   - SQLGlot parsing performance for queries of varying complexity
   - SQLSpec compilation performance with parameter handling

2. **Parameter Styles** (`parameter-styles`)
   - Parameter style conversion performance across adapters
   - Tests QMARK, NAMED_DOLLAR, NAMED_PYFORMAT, NAMED_COLON styles

3. **TypedParameter** (`typed-parameters`)
   - TypedParameter wrapping overhead
   - Type preservation and access performance

4. **ORM Comparison** (`orm-comparison`)
   - SQLSpec vs SQLAlchemy Core vs SQLAlchemy ORM
   - Performance comparisons for common operations

### Storage & Analysis

- **DuckDB Backend**: All results stored in `.benchmark/results.duckdb`
- **Historical Tracking**: Compare current runs against historical averages
- **Regression Detection**: Automatic alerts for performance regressions (>5% by default)
- **Data Retention**: Configurable cleanup of old benchmark data

### CLI Options

```bash
# Execution control
--suite <name>           # Run specific benchmark suite
--adapter <name>         # Test specific adapter (sqlite, duckdb, etc.)
--iterations <n>         # Override default iteration count
--quick                  # Quick mode with fewer iterations

# Container management
--keep-containers        # Don't cleanup containers after run
--no-containers          # Skip container-based tests

# Storage
--storage <path>         # Override storage location
--verbose               # Enable verbose output
```

### Configuration

Environment variables:

- `SQLSPEC_BENCHMARK_STORAGE`: Override storage path
- `SQLSPEC_BENCHMARK_ITERATIONS`: Default iteration count
- `SQLSPEC_BENCHMARK_QUICK`: Enable quick mode by default

## Implementation Status

✅ **Complete**:

- Core framework (metrics, storage, runner)
- CLI interface with rich formatting
- SQL compilation benchmarks
- Parameter styles benchmarks
- TypedParameter benchmarks
- DuckDB storage with regression detection

🚧 **In Progress**:

- ORM comparison benchmarks (basic structure implemented)
- Docker container management (basic PostgreSQL/Oracle support)

📋 **TODO**:

- Async operations benchmarks
- Advanced visualization and reporting
- Complete ORM comparison implementation
- Statistical analysis and trending
- Export capabilities (beyond DuckDB)

## Dependencies

Required packages (automatically installed with `uv sync --extra benchmark`):

- `rich>=13.0.0` - Terminal formatting and progress bars
- `rich-click>=1.8.0` - CLI interface
- `psutil>=5.9.0` - System information
- `duckdb>=0.9.0` - Results storage and analysis
- `sqlalchemy>=2.0.0` - ORM comparison benchmarks

## Migration from Original Tool

The new modular tool is a complete replacement for `tools/benchmark_performance.py`. Key improvements:

1. **Modular Design**: Organized into logical modules vs single 881-line file
2. **Better Storage**: DuckDB with rich querying vs JSON files
3. **Regression Detection**: Automatic performance monitoring vs manual comparison
4. **Rich Interface**: Beautiful terminal output vs basic print statements
5. **Extensibility**: Easy to add new benchmark suites vs monolithic structure

All existing benchmarks have been preserved and enhanced with the new framework.
