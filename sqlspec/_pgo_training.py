"""PGO training workload for sqlspec.

Exercises hot paths to generate compiler profile data for Profile-Guided Optimization.
This module is excluded from mypyc compilation — it runs against compiled modules.

Run as: python -m sqlspec._pgo_training
"""

from __future__ import annotations

import sys
import tempfile
import time
from pathlib import Path

__all__ = ("main",)


def _train_text_transforms() -> None:
    """Exercise text transformation hot paths."""
    from sqlspec.utils.text import camelize, kebabize, pascalize, slugify, snake_case

    inputs = [
        "hello_world",
        "foo_bar_baz",
        "some_long_name",
        "a_b",
        "test_input_string",
        "already_camel",
        "UPPER_CASE",
        "x",
        "multi_word_column_name_here",
    ]
    for _ in range(25000):
        for s in inputs:
            camelize(s)
            snake_case(s)
            pascalize(s)
            kebabize(s)
            slugify(s)


def _train_schema_transforms() -> None:
    """Exercise schema and dict-key transforms."""
    from sqlspec.utils.schema import transform_dict_keys
    from sqlspec.utils.text import camelize

    small = {f"key_{i}": i for i in range(5)}
    medium = {f"key_{i}": i for i in range(20)}
    large = {f"key_{i}": i for i in range(100)}
    for _ in range(15000):
        transform_dict_keys(small, camelize)
        transform_dict_keys(medium, camelize)
        transform_dict_keys(large, camelize)


def _train_sqlite_sync() -> None:
    """Exercise sync driver via in-memory SQLite.

    Covers: driver initialization, execute, execute_many, fetch, fetch_one_or_none,
    query cache warm/miss/evict paths, parameter processing, and type conversion.
    """
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite.config import SqliteConfig

    create_sql = "CREATE TABLE test (value TEXT);"
    insert_sql = "INSERT INTO test (value) VALUES (?);"
    select_all = "SELECT * FROM test;"
    select_by = "SELECT * FROM test WHERE value = ?;"

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_name = tmp.name

    try:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp_name)
        with spec.provide_session(config) as session:
            session.execute(create_sql)

            # Write heavy: bulk insert via execute_many
            data = [(f"value_{i}",) for i in range(2000)]
            session.execute_many(insert_sql, data)

            # Read heavy: fetch all rows
            session.fetch(select_all)

            # Repeated queries: hammer the query cache
            for i in range(10000):
                session.fetch_one_or_none(select_by, (f"value_{i % 100}",))

        config.close_pool()
    finally:
        Path(tmp_name).unlink()

    # Second pass: focus on query cache warm path with more iterations
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_name = tmp.name

    try:
        config2 = SqliteConfig(database=tmp_name)
        with spec.provide_session(config2) as session:
            session.execute(create_sql)
            for i in range(500):
                session.execute(insert_sql, (f"val_{i}",))
            # 50k queries to thoroughly exercise the query cache fast path
            for i in range(50000):
                session.fetch_one_or_none(select_by, (f"val_{i % 500}",))

        config2.close_pool()
    finally:
        Path(tmp_name).unlink()


def main() -> None:
    """Run all PGO training workloads."""
    start = time.perf_counter()

    workloads: list[tuple[str, object]] = [
        ("text_transforms", _train_text_transforms),
        ("schema_transforms", _train_schema_transforms),
        ("sqlite_sync", _train_sqlite_sync),
    ]

    for name, fn in workloads:
        t0 = time.perf_counter()
        fn()  # type: ignore[operator]
        elapsed = time.perf_counter() - t0
        print(f"  {name}: {elapsed:.2f}s")  # noqa: T201

    total = time.perf_counter() - start
    print(f"PGO training complete in {total:.2f}s")  # noqa: T201


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"PGO training failed: {exc}", file=sys.stderr)  # noqa: T201
        sys.exit(1)
