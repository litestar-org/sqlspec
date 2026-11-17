"""PGO training workload for sqlspec.

Exercises hot paths to generate compiler profile data for Profile-Guided Optimization.
This script runs against compiled modules and should not be packaged with the library.

Run as: python tools/scripts/pgo_training.py
"""

import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path

__all__ = ("main",)


@dataclass(slots=True)
class _TrainingRow:
    """Dataclass row schema for exercising schema-mapped reads during PGO training."""

    id: int
    value: str


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
    from sqlspec.core.cache import CachedQuery, QueryCache
    from sqlspec.core.compiler import OperationProfile
    from sqlspec.core.parameters._types import ParameterInfo, ParameterProfile, ParameterStyle
    from sqlspec.core.statement import ProcessedState

    cache = QueryCache(max_size=64)

    # Build dummy cached entries
    parameter_profile = ParameterProfile((
        ParameterInfo(name=None, style=ParameterStyle.QMARK, position=1, ordinal=1, placeholder_text="?"),
    ))
    operation_profile = OperationProfile(returns_rows=True, modifies_rows=False)

    dummy_entries = []
    for i in range(100):
        compiled_sql = f"SELECT * FROM t{i} WHERE id = ?"
        processed_state = ProcessedState(
            compiled_sql=compiled_sql,
            execution_parameters=(),
            parsed_expression=None,
            operation_type="SELECT",
            input_named_parameters=(),
            applied_wrap_types=False,
            filter_hash=0,
            parameter_fingerprint=None,
            parameter_casts={},
            validation_errors=[],
            parameter_profile=parameter_profile,
            operation_profile=operation_profile,
            is_many=False,
        )
        entry = CachedQuery(
            compiled_sql=compiled_sql,
            parameter_profile=parameter_profile,
            input_named_parameters=(),
            applied_wrap_types=False,
            parameter_casts={},
            operation_type="SELECT",
            operation_profile=operation_profile,
            param_count=parameter_profile.total_count,
            processed_state=processed_state,
        )
        dummy_entries.append((compiled_sql, entry))

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
    """Exercise _compile_cache_key — cache-key generation for compiled statements."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.core.statement import SQL

    spec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with spec.provide_session(config) as session:
        statements = (
            SQL("SELECT * FROM t WHERE id = ?", (1,), statement_config=session.statement_config),
            SQL("SELECT * FROM t WHERE name = :name", {"name": "test"}, statement_config=session.statement_config),
            SQL("SELECT * FROM t WHERE id IN (?, ?, ?)", (1, 2, 3), statement_config=session.statement_config),
            SQL("SELECT * FROM t", statement_config=session.statement_config),
        )
        for _ in range(20000):
            for stmt in statements:
                session._compile_cache_key(stmt, session.statement_config, False)


def _train_serialization() -> None:
    """Exercise JSON encode/decode paths."""
    from datetime import datetime

    from sqlspec.utils.serializers._json import convert_date_to_iso, convert_datetime_to_gmt_iso, from_json, to_json

    small_obj = {"key": "value", "num": 42}
    medium_obj = {f"field_{i}": i * 1.5 for i in range(20)}
    list_obj = [{"id": i, "name": f"item_{i}", "active": i % 2 == 0} for i in range(50)]
    now = datetime.now(tz=UTC)
    today = datetime.now(tz=UTC).date()

    for _ in range(10000):
        s1 = to_json(small_obj)
        from_json(s1)
        s2 = to_json(medium_obj)
        from_json(s2)
        s3 = to_json(list_obj)
        from_json(s3)
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
    """Exercise sync driver paths via file-backed SQLite."""
    from sqlspec import SQLSpec, StatementStack
    from sqlspec.adapters.sqlite.config import SqliteConfig

    create_sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"
    insert_sql = "INSERT INTO test (id, value) VALUES (?, ?);"
    select_all = "SELECT id, value FROM test;"
    select_page = "SELECT id, value FROM test ORDER BY id LIMIT 50;"
    select_by = "SELECT id, value FROM test WHERE id = ?;"
    select_value_sql = "SELECT value FROM test WHERE id = ?;"
    select_named = "SELECT id, value FROM test WHERE id = :id AND value = :val;"

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_name = tmp.name

    try:
        spec = SQLSpec()
        config = SqliteConfig(connection_config={"database": tmp_name})
        with spec.provide_session(config) as session:
            session.execute(create_sql)

            data = [(i, f"value_{i}") for i in range(2000)]
            session.execute_many(insert_sql, data)
            session.select(select_all)
            session.select(select_page, schema_type=_TrainingRow)
            session.execute_script("CREATE INDEX IF NOT EXISTS idx_test_value ON test (value);")

            for i in range(2000):
                session.select(select_page)
                session.select(select_by, i % 100, schema_type=_TrainingRow)
                session.select_one(select_by, (i % 100,))
                session.select_one(select_by, (i % 100,), schema_type=_TrainingRow)
                session.select_value(select_value_sql, (i % 100,))
                session.select_value_or_none(select_value_sql, (i % 100,))
                session.select_with_total(select_page)
                session.fetch_one_or_none(select_by, i % 100)
                session.execute(select_by, i % 100).scalar()
                session.execute(select_named, {"id": i % 100, "val": f"value_{i % 100}"}).all()

            stack = (
                StatementStack()
                .push_execute(select_by, (1,))
                .push_execute(select_value_sql, (2,))
                .push_execute(select_named, {"id": 3, "val": "value_3"})
            )
            for _ in range(500):
                session.execute_stack(stack)

            for i in range(300):
                with spec.provide_session(config) as pooled_session:
                    pooled_session.select_one(select_by, i % 100)

        config.close_pool()
    finally:
        Path(tmp_name).unlink(missing_ok=True)


def _train_duckdb_sync() -> None:
    """Exercise sync driver via in-memory DuckDB."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb.config import DuckDBConfig

    create_sql = "CREATE TABLE test (id INTEGER, value TEXT);"
    insert_sql = "INSERT INTO test (id, value) VALUES (?, ?);"
    select_all = "SELECT * FROM test;"
    select_by = "SELECT * FROM test WHERE id = ?;"

    spec = SQLSpec()
    config = DuckDBConfig()
    with spec.provide_session(config) as session:
        session.execute(create_sql)
        data = [(i, f"value_{i}") for i in range(2000)]
        session.execute_many(insert_sql, data)
        session.fetch(select_all)
        for i in range(5000):
            session.fetch_one_or_none(select_by, (i % 100,))
        for i in range(1000):
            session.select_one(select_by, (i % 100,), schema_type=_TrainingRow)
            session.select_value("SELECT value FROM test WHERE id = ?", i % 100)
    config.close_pool()


def _train_aiosqlite_async() -> None:
    """Exercise async driver paths via file-backed aiosqlite."""
    import asyncio

    import anyio

    async def _run() -> None:
        from sqlspec import SQLSpec, StatementStack
        from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

        create_sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"
        insert_sql = "INSERT INTO test (id, value) VALUES (?, ?);"
        select_all = "SELECT id, value FROM test;"
        select_page = "SELECT id, value FROM test ORDER BY id LIMIT 50;"
        select_by = "SELECT id, value FROM test WHERE id = ?;"
        select_value_sql = "SELECT value FROM test WHERE id = ?;"
        select_named = "SELECT id, value FROM test WHERE id = :id AND value = :val;"

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            spec = SQLSpec()
            config = AiosqliteConfig(connection_config={"database": tmp_name})
            async with spec.provide_session(config) as session:
                await session.execute(create_sql)
                data = [(i, f"value_{i}") for i in range(1000)]
                await session.execute_many(insert_sql, data)
                await session.select(select_all)
                await session.execute_script("CREATE INDEX IF NOT EXISTS idx_async_value ON test (value);")
                for i in range(1000):
                    await session.select(select_page)
                    await session.select(select_by, i % 100, schema_type=_TrainingRow)
                    await session.select_one(select_by, (i % 100,))
                    await session.select_one(select_by, (i % 100,), schema_type=_TrainingRow)
                    await session.select_value(select_value_sql, (i % 100,))
                    await session.select_value_or_none(select_value_sql, (i % 100,))
                    await session.select_with_total(select_page)
                    await session.fetch_one_or_none(select_by, i % 100)
                    await session.execute(select_named, {"id": i % 100, "val": f"value_{i % 100}"})
                stack = (
                    StatementStack()
                    .push_execute(select_by, (1,))
                    .push_execute(select_value_sql, (2,))
                    .push_execute(select_named, {"id": 3, "val": "value_3"})
                )
                for _ in range(300):
                    await session.execute_stack(stack)
            for i in range(200):
                async with spec.provide_session(config) as session:
                    _ = await session.select_value(select_value_sql, i % 100)
            await config.close_pool()
        finally:
            await anyio.Path(tmp_name).unlink(missing_ok=True)

    asyncio.run(_run())


def _train_routing_and_loader() -> None:
    """Exercise routing sessions, HMAC routing hints, file loader, and numeric Postgres compilation."""
    import asyncio
    import sqlite3

    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
    from sqlspec.core.statement import SQL, StatementConfig
    from sqlspec.driver import AsyncRoutingSession, SyncRoutingSession
    from sqlspec.loader import SQLFileLoader
    from sqlspec.utils.routing import (
        RoutingContext,
        RoutingContextPayload,
        apply_request_routing_hints,
        sign_primary_pin,
    )

    pg_config = StatementConfig(
        dialect="postgres",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC,
            supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON, ParameterStyle.QMARK},
            default_execution_parameter_style=ParameterStyle.NUMERIC,
            supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        ),
    )
    for i in range(5000):
        SQL(
            "SELECT id, value FROM test WHERE id = :id AND value = :val",
            {"id": i, "val": "item"},
            statement_config=pg_config,
        ).compile()

    with tempfile.TemporaryDirectory() as tmp_dir:
        base = Path(tmp_dir)
        sql_file = base / "queries.sql"
        sql_file.write_text(
            "-- name: by_id\n-- slot: table\nSELECT id, value FROM /* slot: table */ WHERE id = :id;\n",
            encoding="utf-8",
        )
        loader = SQLFileLoader()
        loader.add_fragment("common_cols", "id, value")
        loader.add_named_sql("by_frag", "SELECT /* include: common_cols */ FROM test WHERE id = :id")
        loader.load_sql(sql_file)
        for _ in range(1000):
            loader.get_sql("by_id", table="test").compile()
            loader.get_sql("by_frag").compile()

        secret = b"pgo-training-secret-key"
        causal_val = "lsn-1"
        token = sign_primary_pin(secret, ttl_seconds=60.0, bind_key="default", causal_token=causal_val)
        base_payload = RoutingContextPayload()
        for _ in range(1000):
            apply_request_routing_hints(
                base_payload,
                bind_key="default",
                routing_header_value="replica",
                cookie_header=f"sqlspec_pin={token}",
                primary_pin_cookie="sqlspec_pin",
                primary_pin_secret=secret,
            )

        primary_db = str(base / "primary.db")
        replica_db = str(base / "replica.db")
        for db_path, origin in ((primary_db, "primary"), (replica_db, "replica")):
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, origin TEXT)")
                conn.executemany("INSERT INTO notes VALUES (?, ?)", [(i, origin) for i in range(10)])
                conn.commit()

        sync_cfg = SqliteConfig(connection_config={"database": primary_db}, replicas=[{"database": replica_db}])
        try:
            with SyncRoutingSession(sync_cfg) as rs:
                for i in range(500):
                    rs.select("SELECT id, origin FROM notes WHERE id < ?", (5,))
                    rs.select_one("SELECT id, origin FROM notes WHERE id = ?", (i % 10,))
                    rs.select_one_or_none("SELECT id, origin FROM notes WHERE id = ?", (i % 10,))
                    rs.select_value("SELECT origin FROM notes WHERE id = ?", (i % 10,))
                    rs.fetch("SELECT id, origin FROM notes WHERE id < ?", (5,))
                    rs.fetch_one("SELECT id, origin FROM notes WHERE id = ?", (i % 10,))
                with RoutingContext.primary(), rs.transaction():
                    rs.execute("UPDATE notes SET origin = ? WHERE id = ?", ("updated", 1))
                with RoutingContext.replica():
                    rs.select_value("SELECT origin FROM notes WHERE id = ?", (2,))
                with RoutingContext.auto():
                    rs.select_value("SELECT origin FROM notes WHERE id = ?", (3,))
        finally:
            sync_cfg.close_pool()

        async def _run_async_routing() -> None:
            async_cfg = AiosqliteConfig(connection_config={"database": primary_db}, replicas=[{"database": replica_db}])
            try:
                async with AsyncRoutingSession(async_cfg) as rs:
                    for i in range(300):
                        await rs.select("SELECT id, origin FROM notes WHERE id < ?", (5,))
                        await rs.select_one("SELECT id, origin FROM notes WHERE id = ?", (i % 10,))
                        await rs.select_one_or_none("SELECT id, origin FROM notes WHERE id = ?", (i % 10,))
                        await rs.select_value("SELECT origin FROM notes WHERE id = ?", (i % 10,))
                        await rs.fetch("SELECT id, origin FROM notes WHERE id < ?", (5,))
                        await rs.fetch_one("SELECT id, origin FROM notes WHERE id = ?", (i % 10,))
                    with RoutingContext.primary():
                        async with rs.transaction():
                            await rs.execute("UPDATE notes SET origin = ? WHERE id = ?", ("async-upd", 1))
            finally:
                await async_cfg.close_pool()

        asyncio.run(_run_async_routing())


def _train_builder_and_where() -> None:
    """Exercise SQL builder construction, WHERE clauses, and compilation."""
    from sqlspec.builder import Delete, Insert, Select, Update
    from sqlspec.builder._merge import Merge

    # SELECT with various WHERE patterns
    for _ in range(1000):
        q = Select("id", "name", "email").from_("users").where("id = :id", id=42).where("active = :active", active=True)
        q.build()

        q = (
            Select("*")
            .from_("orders")
            .where("status = :status", status="pending")
            .where("total > :total", total=99.99)
            .order_by("created_at")
            .limit(10)
        )
        q.build()

        q = (
            Select("u.id", "u.name", "o.total")
            .from_("users", alias="u")
            .join("orders", alias="o", on="u.id = o.user_id")
            .where("u.active = :active", active=True)
            .order_by("o.total", desc=True)
            .limit(25)
            .offset(50)
        )
        q.build()

        q = Select("category", "COUNT(*) as cnt").from_("products").group_by("category").having("COUNT(*) > 5")
        q.build()

    # INSERT builder
    for _ in range(1000):
        Insert().into("users").columns("name", "email", "active").values("Alice", "alice@test.com", True).build()

    # UPDATE with WHERE
    for _ in range(1000):
        Update().table("users").set(name="Bob", active=False).where("id = :id", id=1).build()

    # DELETE with WHERE
    for _ in range(1000):
        Delete().from_("sessions").where("expired = :expired", expired=True).build()

    # MERGE (exercises TypeDispatcher for type inference)
    for _ in range(500):
        (
            Merge()
            .into("target_table")
            .using("source_table", alias="s")
            .on("target_table.id = s.id")
            .when_matched_then_update(name="s.name", updated_at="NOW()")
            .when_not_matched_then_insert(columns=["id", "name"], values=["s.id", "s.name"])
        ).build()


def _train_adbc_sqlite() -> None:
    """Exercise ADBC driver via SQLite backend."""
    import pyarrow as pa

    from sqlspec import SQLSpec
    from sqlspec.adapters.adbc.config import AdbcConfig

    _ = pa.__version__
    spec = SQLSpec()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_name = tmp.name

    try:
        config = AdbcConfig(connection_config={"uri": f"sqlite://{tmp_name}"})
        with spec.provide_session(config) as session:
            session.execute("CREATE TABLE test (id INTEGER, value TEXT);")
            data = [(i, f"value_{i}") for i in range(1000)]
            session.execute_many("INSERT INTO test (id, value) VALUES (?, ?);", data)
            session.fetch("SELECT * FROM test;")
            for i in range(5000):
                session.fetch_one_or_none("SELECT * FROM test WHERE id = ?;", (i % 100,))
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
        ("builder_and_where", _train_builder_and_where),
        ("sqlite_sync", _train_sqlite_sync),
        ("duckdb_sync", _train_duckdb_sync),
        ("aiosqlite_async", _train_aiosqlite_async),
        ("routing_and_loader", _train_routing_and_loader),
        ("adbc_sqlite", _train_adbc_sqlite),
    ]

    for name, fn in workloads:
        t0 = time.perf_counter()
        try:
            fn()
        except ImportError as exc:
            elapsed = time.perf_counter() - t0
            print(f"  {name}: skipped ({exc.name} not installed)")  # noqa: T201
            continue
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
