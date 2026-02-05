"""Benchmark script for comparing sqlspec vs raw drivers vs SQLAlchemy.

Originally contributed by euri10 (Benoit Barthelet) in PR #354.
"""

from __future__ import annotations

import asyncio
import inspect
import sqlite3
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
from rich.console import Console
from rich.table import Table

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

if TYPE_CHECKING:
    from collections.abc import Sequence

# Pool leak detection helper
_leaked_pools: list[str] = []

__all__ = (
    "main",
    "print_benchmark_table",
    "raw_asyncpg_initialization",
    "raw_asyncpg_read_heavy",
    "raw_asyncpg_write_heavy",
    "raw_sqlite_initialization",
    "raw_sqlite_iterative_inserts",
    "raw_sqlite_read_heavy",
    "raw_sqlite_repeated_queries",
    "raw_sqlite_write_heavy",
    "run_benchmark",
    "sqlalchemy_asyncpg_initialization",
    "sqlalchemy_asyncpg_read_heavy",
    "sqlalchemy_asyncpg_write_heavy",
    "sqlalchemy_sqlite_initialization",
    "sqlalchemy_sqlite_iterative_inserts",
    "sqlalchemy_sqlite_read_heavy",
    "sqlalchemy_sqlite_repeated_queries",
    "sqlalchemy_sqlite_write_heavy",
    "sqlspec_asyncpg_initialization",
    "sqlspec_asyncpg_read_heavy",
    "sqlspec_asyncpg_write_heavy",
    "sqlspec_sqlite_initialization",
    "sqlspec_sqlite_iterative_inserts",
    "sqlspec_sqlite_read_heavy",
    "sqlspec_sqlite_repeated_queries",
    "sqlspec_sqlite_write_heavy",
)


ROWS_TO_INSERT = 10_000
POOL_SIZE = 5  # Default pool size for async adapters


@click.command()
@click.option(
    "--driver",
    multiple=True,
    default=["sqlite"],
    show_default=True,
    help="List of driver names to benchmark (default: sqlite)",
)
@click.option(
    "--rows", default=ROWS_TO_INSERT, show_default=True, help="Number of rows to insert/read in heavy scenarios"
)
@click.option(
    "--pool-size",
    default=POOL_SIZE,
    show_default=True,
    help="Connection pool size for async adapters (1=single connection, matches sync behavior)",
)
def main(driver: tuple[str, ...], rows: int, pool_size: int) -> None:
    """Run benchmarks for the specified drivers.

    Compares raw driver, sqlspec, and SQLAlchemy performance across
    initialization, write-heavy, and read-heavy scenarios.
    """
    global ROWS_TO_INSERT, POOL_SIZE
    ROWS_TO_INSERT = rows
    POOL_SIZE = pool_size

    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for drv in driver:
        click.echo(f"Running benchmark for driver: {drv} (rows={rows}, pool_size={pool_size})")
        results.extend(run_benchmark(drv, errors))
    if results:
        print_benchmark_table(results)
    else:
        click.echo("No benchmark results to display.")
    if errors:
        for err in errors:
            click.secho(f"Error: {err}", fg="red")
    if _leaked_pools:
        click.secho("Pool leaks detected:", fg="yellow")
        for leak in _leaked_pools:
            click.secho(f"  - {leak}", fg="yellow")
        _leaked_pools.clear()
    click.echo(f"Benchmarks complete for drivers: {', '.join(driver)}")


def run_benchmark(driver: str, errors: list[str]) -> list[dict[str, Any]]:
    """Run all benchmark scenarios for a driver.

    Args:
        driver: The database driver name (e.g., "sqlite", "asyncpg")
        errors: List to append error messages to

    Returns:
        List of benchmark result dictionaries
    """
    libraries = ["raw", "sqlspec", "sqlalchemy"]
    scenarios = ["initialization", "write_heavy", "read_heavy", "iterative_inserts", "repeated_queries"]
    results: list[dict[str, Any]] = []

    for scenario in scenarios:
        for lib in libraries:
            func = SCENARIO_REGISTRY.get((lib, driver, scenario))
            if func is None:
                errors.append(f"No implementation for library={lib}, driver={driver}, scenario={scenario}")
                continue

            try:
                start = time.perf_counter()
                if inspect.iscoroutinefunction(func):
                    asyncio.run(func())
                else:
                    func()
                elapsed = time.perf_counter() - start

                results.append({"driver": driver, "library": lib, "scenario": scenario, "time": elapsed})
            except Exception as exc:
                errors.append(f"{lib}/{driver}/{scenario}: {exc}")

    return results


# --- Scenario helpers and registry ---
# SQLite implementations
# ------------------------------

CREATE_TEST_TABLE = "CREATE TABLE test (value TEXT);"
DROP_TEST_TABLE = "DROP TABLE IF EXISTS test;"
INSERT_TEST_VALUE = "INSERT INTO test (value) VALUES (?);"
INSERT_TEST_VALUE_ASYNCPG = "INSERT INTO test (value) VALUES ($1);"
SELECT_TEST_VALUES = "SELECT * FROM test;"
INSERT_TEST_VALUE_SQLA = "INSERT INTO test (value) VALUES (:value);"


def raw_sqlite_initialization() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute(CREATE_TEST_TABLE)
        conn.close()


def raw_sqlite_write_heavy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute(CREATE_TEST_TABLE)
        # Use executemany for fair comparison
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        conn.commit()
        conn.close()


def raw_sqlite_read_heavy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute(CREATE_TEST_TABLE)
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        conn.commit()
        cursor = conn.execute(SELECT_TEST_VALUES)
        rows = cursor.fetchall()
        assert len(rows) == ROWS_TO_INSERT
        conn.close()


def sqlspec_sqlite_initialization() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)


def sqlspec_sqlite_write_heavy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            # Use execute_many for bulk inserts
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)


def sqlspec_sqlite_read_heavy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)
            rows = session.fetch(SELECT_TEST_VALUES)
            assert len(rows) == ROWS_TO_INSERT


def _get_sqlalchemy() -> tuple[Any, Any]:
    """Import SQLAlchemy lazily."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        return None, None
    else:
        return create_engine, text


def sqlalchemy_sqlite_initialization() -> None:
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            conn.commit()


def sqlalchemy_sqlite_write_heavy() -> None:
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            # Use insert with bindparams for fair bulk comparison
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()


def sqlalchemy_sqlite_read_heavy() -> None:
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()
            result = conn.execute(text(SELECT_TEST_VALUES))
            rows = result.fetchall()
            assert len(rows) == ROWS_TO_INSERT


# Iterative insert scenarios - tests per-call overhead
# This is what euri10's original benchmark measured for sqlspec
# but not for raw/sqlalchemy (which used executemany)


def raw_sqlite_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute(CREATE_TEST_TABLE)
        for i in range(ROWS_TO_INSERT):
            conn.execute(INSERT_TEST_VALUE, (f"value_{i}",))
        conn.commit()
        conn.close()


def sqlspec_sqlite_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            for i in range(ROWS_TO_INSERT):
                session.execute(INSERT_TEST_VALUE, (f"value_{i}",))


def sqlalchemy_sqlite_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            for i in range(ROWS_TO_INSERT):
                conn.execute(text(INSERT_TEST_VALUE_SQLA), {"value": f"value_{i}"})
            conn.commit()


# Query cache scenarios - tests repeated single-row operations
# These stress the query preparation/caching path
SELECT_BY_VALUE = "SELECT * FROM test WHERE value = ?;"
SELECT_BY_VALUE_SQLA = "SELECT * FROM test WHERE value = :value;"


def raw_sqlite_repeated_queries() -> None:
    """Repeated single-row queries - tests query preparation overhead."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute(CREATE_TEST_TABLE)
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        conn.commit()
        # Query same rows repeatedly with different params
        for i in range(ROWS_TO_INSERT):
            cursor = conn.execute(SELECT_BY_VALUE, (f"value_{i % 100}",))
            cursor.fetchone()
        conn.close()


def sqlspec_sqlite_repeated_queries() -> None:
    """Repeated single-row queries - tests query cache effectiveness."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)
            # Query same rows repeatedly with different params
            # This should hit the query cache after the first few iterations
            for i in range(ROWS_TO_INSERT):
                session.fetch_one_or_none(SELECT_BY_VALUE, (f"value_{i % 100}",))


def sqlalchemy_sqlite_repeated_queries() -> None:
    """Repeated single-row queries."""
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()
            # Query same rows repeatedly with different params
            for i in range(ROWS_TO_INSERT):
                result = conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"})
                result.fetchone()


# Aiosqlite implementations
# These test async sqlite performance


def _check_pool_leak(pool: Any, scenario_name: str) -> None:
    """Check for connection leaks in a pool.

    Args:
        pool: Connection pool with size() and checked_out() methods
        scenario_name: Name of the scenario for error reporting
    """
    if pool is None:
        return

    with suppress(AttributeError, TypeError):
        total = pool.size()
        checked_out = pool.checked_out()
        if checked_out > 0:
            _leaked_pools.append(f"{scenario_name}: {checked_out}/{total} connections leaked")


def _get_aiosqlite() -> Any:
    """Import aiosqlite lazily."""
    try:
        import aiosqlite
    except ImportError:
        return None
    else:
        return aiosqlite


def _get_aiosqlite_config() -> Any:
    """Import AiosqliteConfig lazily."""
    try:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
    except ImportError:
        return None
    else:
        return AiosqliteConfig


async def raw_aiosqlite_initialization() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        async with aiosqlite.connect(tmp.name) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            await conn.commit()


async def raw_aiosqlite_write_heavy() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        async with aiosqlite.connect(tmp.name) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await conn.executemany(INSERT_TEST_VALUE, data)
            await conn.commit()


async def raw_aiosqlite_read_heavy() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        async with aiosqlite.connect(tmp.name) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await conn.executemany(INSERT_TEST_VALUE, data)
            await conn.commit()
            cursor = await conn.execute(SELECT_TEST_VALUES)
            rows = await cursor.fetchall()
            assert len(rows) == ROWS_TO_INSERT


async def raw_aiosqlite_iterative_inserts() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        async with aiosqlite.connect(tmp.name) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            for i in range(ROWS_TO_INSERT):
                await conn.execute(INSERT_TEST_VALUE, (f"value_{i}",))
            await conn.commit()


async def raw_aiosqlite_repeated_queries() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        async with aiosqlite.connect(tmp.name) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await conn.executemany(INSERT_TEST_VALUE, data)
            await conn.commit()
            for i in range(ROWS_TO_INSERT):
                cursor = await conn.execute(SELECT_BY_VALUE, (f"value_{i % 100}",))
                await cursor.fetchone()


async def sqlspec_aiosqlite_initialization() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    # Use delete=False so we control when the file is deleted (after pool close)
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
        # Properly close the pool to release all connections
        _check_pool_leak(config.connection_instance, "aiosqlite/initialization")
        await config.close_pool()
    finally:
        with suppress(OSError):
            tmp_path.unlink()  # noqa: ASYNC240


async def sqlspec_aiosqlite_write_heavy() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await session.execute_many(INSERT_TEST_VALUE, data)
        _check_pool_leak(config.connection_instance, "aiosqlite/write_heavy")
        await config.close_pool()
    finally:
        with suppress(OSError):
            tmp_path.unlink()  # noqa: ASYNC240


async def sqlspec_aiosqlite_read_heavy() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await session.execute_many(INSERT_TEST_VALUE, data)
            rows = await session.fetch(SELECT_TEST_VALUES)
            assert len(rows) == ROWS_TO_INSERT
        _check_pool_leak(config.connection_instance, "aiosqlite/read_heavy")
        await config.close_pool()
    finally:
        with suppress(OSError):
            tmp_path.unlink()  # noqa: ASYNC240


async def sqlspec_aiosqlite_iterative_inserts() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            for i in range(ROWS_TO_INSERT):
                await session.execute(INSERT_TEST_VALUE, (f"value_{i}",))
        _check_pool_leak(config.connection_instance, "aiosqlite/iterative_inserts")
        await config.close_pool()
    finally:
        with suppress(OSError):
            tmp_path.unlink()  # noqa: ASYNC240


async def sqlspec_aiosqlite_repeated_queries() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await session.execute_many(INSERT_TEST_VALUE, data)
            for i in range(ROWS_TO_INSERT):
                await session.fetch_one_or_none(SELECT_BY_VALUE, (f"value_{i % 100}",))
        _check_pool_leak(config.connection_instance, "aiosqlite/repeated_queries")
        await config.close_pool()
    finally:
        with suppress(OSError):
            tmp_path.unlink()  # noqa: ASYNC240


async def sqlalchemy_aiosqlite_initialization() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp.name}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            await conn.commit()


async def sqlalchemy_aiosqlite_write_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp.name}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            await conn.commit()


async def sqlalchemy_aiosqlite_read_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp.name}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            await conn.commit()
            result = await conn.execute(text(SELECT_TEST_VALUES))
            rows = result.fetchall()
            assert len(rows) == ROWS_TO_INSERT


async def sqlalchemy_aiosqlite_iterative_inserts() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp.name}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            for i in range(ROWS_TO_INSERT):
                await conn.execute(text(INSERT_TEST_VALUE_SQLA), {"value": f"value_{i}"})
            await conn.commit()


async def sqlalchemy_aiosqlite_repeated_queries() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp.name}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            await conn.commit()
            for i in range(ROWS_TO_INSERT):
                result = await conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"})
                result.fetchone()


# Asyncpg implementations
# These require asyncpg and optionally SQLAlchemy[asyncio] to be installed

ASYNCPG_DSN = "postgresql://postgres:postgres@localhost/postgres"


def _get_asyncpg() -> Any:
    """Import asyncpg lazily."""
    try:
        from asyncpg import connect
    except ImportError:
        return None
    else:
        return connect


def _get_asyncpg_config() -> Any:
    """Import AsyncpgConfig lazily."""
    try:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
    except ImportError:
        return None
    else:
        return AsyncpgConfig


def _get_async_sqlalchemy() -> tuple[Any, Any]:
    """Import async SQLAlchemy lazily."""
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine
    except ImportError:
        return None, None
    else:
        return create_async_engine, text


async def raw_asyncpg_initialization() -> None:
    connect = _get_asyncpg()
    if connect is None:
        return
    conn = await connect(dsn=ASYNCPG_DSN)
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    await conn.close()


async def raw_asyncpg_write_heavy() -> None:
    connect = _get_asyncpg()
    if connect is None:
        return
    conn = await connect(dsn=ASYNCPG_DSN)
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    # Use executemany for fair comparison
    data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
    await conn.executemany(INSERT_TEST_VALUE_ASYNCPG, data)
    await conn.close()


async def raw_asyncpg_read_heavy() -> None:
    connect = _get_asyncpg()
    if connect is None:
        return
    conn = await connect(dsn=ASYNCPG_DSN)
    rows = await conn.fetch(SELECT_TEST_VALUES)
    assert len(rows) == ROWS_TO_INSERT
    await conn.close()


async def sqlspec_asyncpg_initialization() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        return
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": ASYNCPG_DSN})
    async with spec.provide_session(config) as session:
        await session.execute(DROP_TEST_TABLE)
        await session.execute(CREATE_TEST_TABLE)


async def sqlspec_asyncpg_write_heavy() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        return
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": ASYNCPG_DSN})
    async with spec.provide_session(config) as session:
        await session.execute(DROP_TEST_TABLE)
        await session.execute(CREATE_TEST_TABLE)
        data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        await session.execute_many(INSERT_TEST_VALUE_ASYNCPG, data)


async def sqlspec_asyncpg_read_heavy() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        return
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": ASYNCPG_DSN})
    async with spec.provide_session(config) as session:
        rows = await session.fetch(SELECT_TEST_VALUES)
        assert len(rows) == ROWS_TO_INSERT


async def sqlalchemy_asyncpg_initialization() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    engine = create_async_engine(f"postgresql+asyncpg://{ASYNCPG_DSN.split('://')[1]}")
    async with engine.connect() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        await conn.commit()


async def sqlalchemy_asyncpg_write_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    engine = create_async_engine(f"postgresql+asyncpg://{ASYNCPG_DSN.split('://')[1]}")
    async with engine.connect() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
        await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
        await conn.commit()


async def sqlalchemy_asyncpg_read_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    engine = create_async_engine(f"postgresql+asyncpg://{ASYNCPG_DSN.split('://')[1]}")
    async with engine.begin() as conn:
        result = await conn.execute(text(SELECT_TEST_VALUES))
        rows = result.fetchall()
        assert len(rows) == ROWS_TO_INSERT


SCENARIO_REGISTRY: dict[tuple[str, str, str], Any] = {
    # SQLite scenarios
    ("raw", "sqlite", "initialization"): raw_sqlite_initialization,
    ("raw", "sqlite", "write_heavy"): raw_sqlite_write_heavy,
    ("raw", "sqlite", "read_heavy"): raw_sqlite_read_heavy,
    ("raw", "sqlite", "iterative_inserts"): raw_sqlite_iterative_inserts,
    ("raw", "sqlite", "repeated_queries"): raw_sqlite_repeated_queries,
    ("sqlspec", "sqlite", "initialization"): sqlspec_sqlite_initialization,
    ("sqlspec", "sqlite", "write_heavy"): sqlspec_sqlite_write_heavy,
    ("sqlspec", "sqlite", "read_heavy"): sqlspec_sqlite_read_heavy,
    ("sqlspec", "sqlite", "iterative_inserts"): sqlspec_sqlite_iterative_inserts,
    ("sqlspec", "sqlite", "repeated_queries"): sqlspec_sqlite_repeated_queries,
    ("sqlalchemy", "sqlite", "initialization"): sqlalchemy_sqlite_initialization,
    ("sqlalchemy", "sqlite", "write_heavy"): sqlalchemy_sqlite_write_heavy,
    ("sqlalchemy", "sqlite", "read_heavy"): sqlalchemy_sqlite_read_heavy,
    ("sqlalchemy", "sqlite", "iterative_inserts"): sqlalchemy_sqlite_iterative_inserts,
    ("sqlalchemy", "sqlite", "repeated_queries"): sqlalchemy_sqlite_repeated_queries,
    # Aiosqlite scenarios
    ("raw", "aiosqlite", "initialization"): raw_aiosqlite_initialization,
    ("raw", "aiosqlite", "write_heavy"): raw_aiosqlite_write_heavy,
    ("raw", "aiosqlite", "read_heavy"): raw_aiosqlite_read_heavy,
    ("raw", "aiosqlite", "iterative_inserts"): raw_aiosqlite_iterative_inserts,
    ("raw", "aiosqlite", "repeated_queries"): raw_aiosqlite_repeated_queries,
    ("sqlspec", "aiosqlite", "initialization"): sqlspec_aiosqlite_initialization,
    ("sqlspec", "aiosqlite", "write_heavy"): sqlspec_aiosqlite_write_heavy,
    ("sqlspec", "aiosqlite", "read_heavy"): sqlspec_aiosqlite_read_heavy,
    ("sqlspec", "aiosqlite", "iterative_inserts"): sqlspec_aiosqlite_iterative_inserts,
    ("sqlspec", "aiosqlite", "repeated_queries"): sqlspec_aiosqlite_repeated_queries,
    ("sqlalchemy", "aiosqlite", "initialization"): sqlalchemy_aiosqlite_initialization,
    ("sqlalchemy", "aiosqlite", "write_heavy"): sqlalchemy_aiosqlite_write_heavy,
    ("sqlalchemy", "aiosqlite", "read_heavy"): sqlalchemy_aiosqlite_read_heavy,
    ("sqlalchemy", "aiosqlite", "iterative_inserts"): sqlalchemy_aiosqlite_iterative_inserts,
    ("sqlalchemy", "aiosqlite", "repeated_queries"): sqlalchemy_aiosqlite_repeated_queries,
    # Asyncpg scenarios
    ("raw", "asyncpg", "initialization"): raw_asyncpg_initialization,
    ("raw", "asyncpg", "write_heavy"): raw_asyncpg_write_heavy,
    ("raw", "asyncpg", "read_heavy"): raw_asyncpg_read_heavy,
    ("sqlspec", "asyncpg", "initialization"): sqlspec_asyncpg_initialization,
    ("sqlspec", "asyncpg", "write_heavy"): sqlspec_asyncpg_write_heavy,
    ("sqlspec", "asyncpg", "read_heavy"): sqlspec_asyncpg_read_heavy,
    ("sqlalchemy", "asyncpg", "initialization"): sqlalchemy_asyncpg_initialization,
    ("sqlalchemy", "asyncpg", "write_heavy"): sqlalchemy_asyncpg_write_heavy,
    ("sqlalchemy", "asyncpg", "read_heavy"): sqlalchemy_asyncpg_read_heavy,
}


def print_benchmark_table(results: list[dict[str, Any]]) -> None:
    console = Console()
    table = Table(title="Benchmark Results")
    table.add_column("Driver", style="cyan", no_wrap=True)
    table.add_column("Library", style="magenta")
    table.add_column("Scenario", style="green")
    table.add_column("Time (s)", justify="right", style="yellow")
    table.add_column("% Slower vs Raw", justify="right", style="red")

    # Build a lookup for raw times: {(driver, scenario): time}
    raw_times = {}
    for row in results:
        if row["library"] == "raw":
            raw_times[(row["driver"], row["scenario"])] = row["time"]

    for row in results:
        driver = row["driver"]
        scenario = row["scenario"]
        lib = row["library"]
        t = row["time"]
        if lib == "raw":
            percent_slower = "—"
        else:
            raw_time = raw_times.get((driver, scenario))
            percent_slower = f"{100 * (t - raw_time) / raw_time:.1f}%" if raw_time and raw_time > 0 else "n/a"
        table.add_row(driver, lib, scenario, f"{t:.4f}", percent_slower)
    console.print(table)


if __name__ == "__main__":
    main()
