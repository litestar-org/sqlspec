"""Unit tests for data dictionary table column profiling and data-driven DDL inference."""

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.data_dictionary import ColumnProfileMetadata, DDLResult, MetadataFidelity, MetadataSource, MetadataSupport


def test_column_profile_metadata_struct_contract() -> None:
    """ColumnProfileMetadata should expose typed profile attributes and serialize cleanly."""
    profile = ColumnProfileMetadata(
        column_name="id",
        declared_type="TEXT",
        inferred_type="BIGINT",
        row_count=3,
        null_count=0,
        distinct_count=3,
        nullable=False,
        max_char_length=4,
        is_unique_candidate=True,
    )
    assert profile.column_name == "id"
    assert profile.declared_type == "TEXT"
    assert profile.inferred_type == "BIGINT"
    assert profile.row_count == 3
    assert profile.null_count == 0
    assert profile.distinct_count == 3
    assert profile.nullable is False
    assert profile.max_char_length == 4
    assert profile.is_unique_candidate is True
    assert profile.to_dict()["column_name"] == "id"


def test_sqlite_table_profile_and_infer_ddl_from_staging_text_table() -> None:
    """SqliteDataDictionary should profile loosely typed TEXT columns and infer refined DDL."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as session:
        session.execute(
            """
            CREATE TABLE staging_orders (
                id TEXT,
                customer_uuid TEXT,
                amount TEXT,
                is_paid TEXT,
                order_date TEXT,
                created_at TEXT,
                payload TEXT,
                notes TEXT
            )
            """
        )
        rows = [
            (
                "1001",
                "550e8400-e29b-41d4-a716-446655440000",
                "19.99",
                "true",
                "2026-10-28",
                "2026-10-28T10:15:30Z",
                '{"source": "web", "items": 2}',
                "express",
            ),
            (
                "1002",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "125.50",
                "false",
                "2026-10-29",
                "2026-10-29T11:20:00Z",
                '{"source": "mobile", "items": 5}',
                None,
            ),
            (
                "1003",
                "7c9e6679-7425-40de-944b-e07fc1f90ae7",
                "0.05",
                "true",
                "2026-10-30",
                "2026-10-30T14:45:10Z",
                '{"source": "api", "items": 1}',
                "gift",
            ),
        ]
        for row in rows:
            session.execute("INSERT INTO staging_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row)

        dictionary = session.data_dictionary
        profiles = dictionary.get_table_profile(session, "staging_orders")
        by_name = {p.column_name: p for p in profiles}

        assert by_name["id"].row_count == 3
        assert by_name["id"].null_count == 0
        assert by_name["id"].distinct_count == 3
        assert by_name["id"].nullable is False
        assert by_name["id"].is_unique_candidate is True
        assert by_name["id"].max_char_length == 4
        assert by_name["id"].inferred_type in {"INTEGER", "BIGINT"}

        assert by_name["notes"].null_count == 1
        assert by_name["notes"].nullable is True
        assert by_name["notes"].is_unique_candidate is False

        pg_ddl = dictionary.infer_table_ddl_from_data(
            session, "staging_orders", target_table="orders_clean", target_dialect="postgres", primary_key="id"
        )
        assert isinstance(pg_ddl, DDLResult)
        assert pg_ddl.status == MetadataSupport.SUPPORTED
        assert pg_ddl.fidelity == MetadataFidelity.GENERATED
        assert pg_ddl.source == MetadataSource.GENERATED
        assert pg_ddl.ddl is not None
        assert 'CREATE TABLE "orders_clean"' in pg_ddl.ddl
        assert '"id" BIGINT NOT NULL PRIMARY KEY' in pg_ddl.ddl
        assert '"customer_uuid" UUID NOT NULL' in pg_ddl.ddl
        assert '"amount" DECIMAL' in pg_ddl.ddl or '"amount" NUMERIC' in pg_ddl.ddl
        assert '"is_paid" BOOLEAN NOT NULL' in pg_ddl.ddl
        assert '"order_date" DATE NOT NULL' in pg_ddl.ddl
        assert '"created_at" TIMESTAMPTZ NOT NULL' in pg_ddl.ddl
        assert '"payload" JSONB NOT NULL' in pg_ddl.ddl
        assert '"notes" VARCHAR' in pg_ddl.ddl or '"notes" TEXT' in pg_ddl.ddl
        assert '"notes" VARCHAR(7) NOT NULL' not in pg_ddl.ddl


def test_duckdb_table_profile_and_infer_ddl_preserves_strong_types() -> None:
    """DuckDBDataDictionary should refine VARCHAR columns while preserving strongly typed columns."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as session:
        session.execute(
            """
            CREATE TABLE raw_metrics (
                metric_id BIGINT NOT NULL,
                metric_code VARCHAR,
                latency_ms VARCHAR,
                recorded_at VARCHAR,
                attributes VARCHAR
            )
            """
        )
        session.execute(
            """
            INSERT INTO raw_metrics VALUES
                (1, 'db.query', '12.5', '2026-10-29T08:00:00Z', '{"host": "n1"}'),
                (2, 'db.connect', '3.25', '2026-10-29T08:01:00Z', '{"host": "n2"}')
            """
        )

        dictionary = session.data_dictionary
        profiles = dictionary.get_table_profile(session, "raw_metrics")
        by_name = {p.column_name: p for p in profiles}

        assert by_name["metric_id"].declared_type == "BIGINT"
        assert by_name["metric_id"].inferred_type == "BIGINT"
        assert by_name["metric_id"].is_unique_candidate is True

        assert by_name["latency_ms"].inferred_type.startswith(("DECIMAL", "NUMERIC", "DOUBLE"))
        assert by_name["recorded_at"].inferred_type in {"TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP"}
        assert by_name["attributes"].inferred_type == "JSON"

        ddl_res = dictionary.infer_table_ddl_from_data(session, "raw_metrics", primary_key="metric_id")
        assert ddl_res.status == MetadataSupport.SUPPORTED
        assert ddl_res.ddl is not None
        assert "BIGINT NOT NULL PRIMARY KEY" in ddl_res.ddl
        assert "JSON NOT NULL" in ddl_res.ddl


@pytest.mark.anyio
async def test_aiosqlite_async_table_profile_and_infer_ddl() -> None:
    """AiosqliteDataDictionary should profile columns and infer DDL asynchronously."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    try:
        async with config.provide_session() as session:
            await session.execute(
                """
                CREATE TABLE raw_users (
                    user_id TEXT,
                    active_flag TEXT,
                    profile_json TEXT
                )
                """
            )
            await session.execute("INSERT INTO raw_users VALUES (?, ?, ?)", ("42", "true", '{"role": "admin"}'))
            await session.execute("INSERT INTO raw_users VALUES (?, ?, ?)", ("43", "false", '{"role": "member"}'))

            dictionary = session.data_dictionary
            profiles = await dictionary.get_table_profile(session, "raw_users", target_dialect="postgres")
            by_name = {p.column_name: p for p in profiles}

            assert by_name["user_id"].inferred_type == "BIGINT"
            assert by_name["active_flag"].inferred_type == "BOOLEAN"
            assert by_name["profile_json"].inferred_type == "JSONB"

            ddl_res = await dictionary.infer_table_ddl_from_data(
                session, "raw_users", target_table="users", target_dialect="postgres", primary_key="user_id"
            )
            assert ddl_res.status == MetadataSupport.SUPPORTED
            assert ddl_res.ddl is not None
            assert 'CREATE TABLE "users"' in ddl_res.ddl
            assert '"user_id" BIGINT NOT NULL PRIMARY KEY' in ddl_res.ddl
            assert '"active_flag" BOOLEAN NOT NULL' in ddl_res.ddl
            assert '"profile_json" JSONB NOT NULL' in ddl_res.ddl
    finally:
        await config.close_pool()
