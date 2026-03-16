"""PGO training workload for sqlspec.

Exercises hot paths to generate compiler profile data for Profile-Guided Optimization.
This script runs against compiled modules and should not be packaged with the library.

Run as: python tools/scripts/pgo_training.py
"""

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
        for value in inputs:
            camelize(value)
            snake_case(value)
            pascalize(value)
            kebabize(value)
            slugify(value)


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


def _train_type_dispatch() -> None:
    """Exercise TypeDispatcher hot paths — O(1) type lookups with MRO caching."""
    from decimal import Decimal

    from sqlspec.utils.dispatch import TypeDispatcher

    dispatcher: TypeDispatcher[str] = TypeDispatcher()
    dispatcher.register_all((
        (int, "integer"),
        (float, "float"),
        (str, "string"),
        (bool, "boolean"),
        (bytes, "bytes"),
        (Decimal, "decimal"),
        (list, "list"),
        (dict, "dict"),
        (tuple, "tuple"),
        (type(None), "null"),
    ))

    # Mixed-type lookups to exercise cache hits and MRO resolution
    objects: list[object] = [42, 3.14, "hello", True, b"data", Decimal("1.5"), [1], {"k": 1}, (1,), None]
    for _ in range(50000):
        for obj in objects:
            dispatcher.get(obj)


def _train_query_cache() -> None:
    """Exercise QueryCache LRU operations — cache hits, misses, and eviction."""
    from sqlspec.driver._query_cache import CachedQuery, QueryCache

    cache = QueryCache(max_size=64)

    # Build dummy cached entries
    from sqlspec.core.parameters._types import ParameterStyle

    dummy_entries = []
    for i in range(100):
        entry = CachedQuery(
            compiled_sql=f"SELECT * FROM t{i} WHERE id = ?",
            parameter_profile=ParameterStyle.QMARK,  # type: ignore[arg-type]
            input_named_parameters=(),
            applied_wrap_types=False,
            parameter_casts={},
            operation_type="SELECT",  # type: ignore[arg-type]
            operation_profile="read",  # type: ignore[arg-type]
            param_count=1,
            processed_state="compiled",  # type: ignore[arg-type]
        )
        dummy_entries.append((f"SELECT * FROM t{i} WHERE id = ?", entry))

    # Exercise cache set (with eviction) and get (hits + misses)
    for _ in range(5000):
        for sql, entry in dummy_entries:
            cache.set(sql, entry)
        # Hit the most recent entries (in cache)
        for sql, _ in dummy_entries[-64:]:
            cache.get(sql)
        # Miss on evicted entries
        for sql, _ in dummy_entries[:36]:
            cache.get(sql)
        cache.clear()


def _train_hashable_keys() -> None:
    """Exercise make_cache_key_hashable — recursive nested structure hashing."""
    from sqlspec.driver._common import make_cache_key_hashable

    simple_dict = {"name": "test", "value": 42, "active": True}
    nested_dict = {"user": {"name": "test", "roles": ["admin", "user"]}, "count": 5}
    list_of_dicts = [{"id": i, "val": f"v{i}"} for i in range(10)]
    mixed = {"a": [1, {"b": [2, 3]}, (4, 5)], "c": None, "d": b"bytes"}

    for _ in range(20000):
        make_cache_key_hashable(simple_dict)
        make_cache_key_hashable(nested_dict)
        make_cache_key_hashable(list_of_dicts)
        make_cache_key_hashable(mixed)
        make_cache_key_hashable(("a", 1, None, True))
        make_cache_key_hashable(42)


def _train_serialization() -> None:
    """Exercise JSON encode/decode paths."""
    from datetime import date, datetime, timezone

    from sqlspec._serialization import convert_date_to_iso, convert_datetime_to_gmt_iso, decode_json, encode_json

    small_obj = {"key": "value", "num": 42}
    medium_obj = {f"field_{i}": i * 1.5 for i in range(20)}
    list_obj = [{"id": i, "name": f"item_{i}", "active": i % 2 == 0} for i in range(50)]
    now = datetime.now(tz=timezone.utc)
    today = date.today()

    for _ in range(10000):
        s1 = encode_json(small_obj)
        decode_json(s1)
        s2 = encode_json(medium_obj)
        decode_json(s2)
        s3 = encode_json(list_obj)
        decode_json(s3)
        convert_datetime_to_gmt_iso(now)
        convert_date_to_iso(today)


def _train_parameter_fingerprinting() -> None:
    """Exercise structural and value fingerprinting — used for query cache keying."""
    from sqlspec.core.parameters import structural_fingerprint, value_fingerprint

    dict_params = {"name": "test", "id": 42, "active": True}
    tuple_params = ("test", 42, True)
    list_params = [("a", 1), ("b", 2), ("c", 3)]

    for _ in range(15000):
        structural_fingerprint(dict_params)
        structural_fingerprint(tuple_params)
        structural_fingerprint(list_params, is_many=True)
        value_fingerprint(dict_params)
        value_fingerprint(tuple_params)


def _train_sqlite_sync() -> None:
    """Exercise sync driver via file-backed SQLite."""
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

            data = [(f"value_{i}",) for i in range(2000)]
            session.execute_many(insert_sql, data)
            session.fetch(select_all)

            for i in range(10000):
                session.fetch_one_or_none(select_by, (f"value_{i % 100}",))

        config.close_pool()
    finally:
        Path(tmp_name).unlink()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_name = tmp.name

    try:
        config2 = SqliteConfig(database=tmp_name)
        with spec.provide_session(config2) as session:
            session.execute(create_sql)
            for i in range(500):
                session.execute(insert_sql, (f"val_{i}",))
            for i in range(50000):
                session.fetch_one_or_none(select_by, (f"val_{i % 500}",))

        config2.close_pool()
    finally:
        Path(tmp_name).unlink()


def main() -> None:
    """Run all PGO training workloads."""
    start = time.perf_counter()

    workloads = [
        ("text_transforms", _train_text_transforms),
        ("schema_transforms", _train_schema_transforms),
        ("type_dispatch", _train_type_dispatch),
        ("query_cache", _train_query_cache),
        ("hashable_keys", _train_hashable_keys),
        ("serialization", _train_serialization),
        ("param_fingerprinting", _train_parameter_fingerprinting),
        ("sqlite_sync", _train_sqlite_sync),
    ]

    for name, fn in workloads:
        t0 = time.perf_counter()
        fn()
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
