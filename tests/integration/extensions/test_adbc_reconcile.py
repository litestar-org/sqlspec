"""Integration acceptance tests for ADBC multi-backend extension schema reconciliation."""

import contextlib
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("adbc_driver_manager")

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.litestar.store import AdbcStore
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError, MigrationError, SQLSpecError
from sqlspec.migrations.schema import metadata_has_index
from sqlspec.utils.sync_tools import run_

pytestmark = [pytest.mark.adbc, pytest.mark.integration]


def _extension_tables(prefix: str) -> tuple[dict[str, Any], tuple[str, ...]]:
    litestar_table = f"{prefix}_ls"
    events_table = f"{prefix}_ev"
    adk_session = f"{prefix}_as"
    adk_event = f"{prefix}_ae"
    adk_app_state = f"{prefix}_aa"
    adk_user_state = f"{prefix}_au"
    adk_meta = f"{prefix}_am"
    adk_memory = f"{prefix}_mm"
    ext: dict[str, Any] = {
        "litestar": {"migration_mode": "reconcile", "session_table": litestar_table},
        "events": {"migration_mode": "reconcile", "queue_table": events_table},
        "adk": {
            "migration_mode": "reconcile",
            "enable_memory": True,
            "session_table": adk_session,
            "events_table": adk_event,
            "app_state_table": adk_app_state,
            "user_state_table": adk_user_state,
            "metadata_table": adk_meta,
            "memory_table": adk_memory,
        },
    }
    tables = (adk_memory, adk_meta, adk_user_state, adk_app_state, adk_event, adk_session, events_table, litestar_table)
    return ext, tables


def _resolve_backend_base_config(
    request: pytest.FixtureRequest, backend: str, tmp_path: Path, prefix: str
) -> tuple[dict[str, Any], StatementConfig | None]:
    if backend == "bigquery":
        pytest.importorskip("adbc_driver_bigquery")
        try:
            bigquery_service = request.getfixturevalue("bigquery_service")
        except pytest.skip.Exception:
            raise
        except Exception as exc:
            pytest.skip(f"BigQuery service unavailable: {exc}")
        return (
            {
                "driver_name": "bigquery",
                "project_id": bigquery_service.project,
                "dataset_id": bigquery_service.dataset,
                "db_kwargs": {"adbc.bigquery.sql.endpoint": bigquery_service.endpoint},
            },
            None,
        )

    fixture_map = {
        "postgresql": "adbc_postgresql_config",
        "duckdb": "adbc_duckdb_config",
        "gizmosql": "adbc_gizmosql_config",
        "db2": "adbc_db2_config",
        "oracle": "adbc_oracle_config",
        "snowflake": "adbc_snowflake_config",
    }
    try:
        base_config = request.getfixturevalue(fixture_map[backend])
    except pytest.skip.Exception:
        raise
    except Exception as exc:
        pytest.skip(f"ADBC {backend} fixture unavailable: {exc}")

    assert isinstance(base_config, AdbcConfig)
    conn_cfg = dict(base_config.connection_config)
    if backend == "duckdb":
        conn_cfg["path"] = str(tmp_path / f"{prefix}.duckdb")
    elif backend == "gizmosql":
        conn_cfg["gizmosql_backend"] = "duckdb"

    stmt_cfg = base_config.statement_config if base_config.explicit_statement_dialect is not None else None
    return conn_cfg, stmt_cfg


def _full_error_text(exc: BaseException) -> str:
    parts = [str(exc)]
    current = exc.__cause__ or exc.__context__
    while current is not None:
        parts.append(str(current))
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _skip_if_driver_or_emulator_unsupported(backend: str, exc: BaseException) -> None:
    lowered = _full_error_text(exc).lower()
    if any(
        marker in lowered
        for marker in (
            "cannot open shared object file",
            "no module named",
            "failed to import connect function",
            "could not configure connection",
            "adbc.bigquery.sql.endpoint",
            "sql1042c",
            "57019",
            "connection refused",
        )
    ):
        pytest.skip(f"ADBC {backend} backend or driver unavailable in test environment: {exc}")
    if (
        backend == "snowflake"
        and isinstance(exc, (SQLSpecError, MigrationError))
        and ("not implemented" in lowered or "not currently supported" in lowered)
    ):
        pytest.skip(f"Snowflake emulator lacks required DDL/catalog feature: {exc}")


def _drop_tables_best_effort(config: AdbcConfig, backend: str, tables: tuple[str, ...]) -> None:
    try:
        with config.provide_session() as cleanup:
            with contextlib.suppress(Exception):
                cleanup.rollback()
            for table in tables:
                drop_sql = f"DROP TABLE {table} PURGE" if backend == "oracle" else f"DROP TABLE {table}"
                if backend in {"postgresql", "duckdb", "gizmosql", "snowflake", "bigquery"}:
                    drop_sql = f"DROP TABLE IF EXISTS {table}"
                try:
                    cleanup.execute(drop_sql)
                    with contextlib.suppress(Exception):
                        cleanup.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        cleanup.rollback()
    except Exception:
        return


@pytest.mark.parametrize(
    ("backend", "drop_target"),
    [
        pytest.param("postgresql", "litestar", marks=[pytest.mark.postgres, pytest.mark.xdist_group("postgres")]),
        pytest.param("duckdb", "litestar", marks=[pytest.mark.duckdb, pytest.mark.xdist_group("duckdb")]),
        pytest.param("gizmosql", "litestar", marks=[pytest.mark.xdist_group("gizmosql")]),
        pytest.param("db2", "adk", marks=[pytest.mark.db2, pytest.mark.xdist_group("db2")]),
        pytest.param("oracle", "adk", marks=[pytest.mark.oracle, pytest.mark.xdist_group("oracle")]),
        pytest.param("snowflake", "none", marks=[pytest.mark.snowflake, pytest.mark.xdist_group("snowflake")]),
        pytest.param("bigquery", "none", marks=[pytest.mark.bigquery, pytest.mark.xdist_group("bigquery")]),
    ],
)
def test_adbc_reconcile_converges_across_backends(
    request: pytest.FixtureRequest, tmp_path: Path, backend: str, drop_target: str
) -> None:
    """ADBC migrate_up() reconciles litestar, events, and adk across every fixture-backed dialect."""
    prefix = f"r{backend[:2]}{uuid4().hex[:4]}"
    mig_table = f"{prefix}_mig"
    conn_cfg, stmt_cfg = _resolve_backend_base_config(request, backend, tmp_path, prefix)
    ext, tables = _extension_tables(prefix)
    litestar_table = str(ext["litestar"]["session_table"])
    adk_session = str(ext["adk"]["session_table"])

    config = AdbcConfig(
        connection_config=conn_cfg,
        statement_config=stmt_cfg,
        migration_config={"script_location": str(tmp_path / f"{prefix}_migrations"), "version_table_name": mig_table},
        extension_config=ext,
    )
    expected_tables = tables
    try:
        try:
            config.migrate_up()
        except (ImproperConfigurationError, SQLSpecError, MigrationError, OSError) as exc:
            if (
                backend == "snowflake"
                and "foreign key constraints are not currently supported" in _full_error_text(exc).lower()
            ):
                config.close_pool()
                no_fk_ext = {"litestar": ext["litestar"], "events": ext["events"]}
                expected_tables = (str(ext["events"]["queue_table"]), litestar_table)
                config = AdbcConfig(
                    connection_config=conn_cfg,
                    statement_config=stmt_cfg,
                    migration_config={
                        "script_location": str(tmp_path / f"{prefix}_migrations_nofk"),
                        "version_table_name": mig_table,
                    },
                    extension_config=no_fk_ext,
                )
                try:
                    config.migrate_up()
                except (ImproperConfigurationError, SQLSpecError, MigrationError, OSError) as retry_exc:
                    _skip_if_driver_or_emulator_unsupported(backend, retry_exc)
                    raise
            else:
                _skip_if_driver_or_emulator_unsupported(backend, exc)
                raise

        with config.provide_session() as driver:
            existing_tables = {row["table_name"].casefold() for row in driver.data_dictionary.get_tables(driver)}
            assert {name.casefold() for name in expected_tables}.issubset(existing_tables)
            assert int(driver.select_value(f"SELECT COUNT(*) FROM {mig_table}")) == 0

        session_store = AdbcStore(config)
        run_(session_store.set)("seed-session", b"persisted-payload")
        assert run_(session_store.get)("seed-session") == b"persisted-payload"

        if drop_target == "litestar":
            index_table = litestar_table
            dropped_index = f"idx_{litestar_table}_expires_at"
        elif drop_target == "adk":
            index_table = adk_session
            dropped_index = f"idx_{adk_session}_update_time"
        else:
            index_table = ""
            dropped_index = ""

        if dropped_index:
            with config.provide_session() as driver:
                before_drop = driver.data_dictionary.get_indexes(driver, table=index_table)
                assert metadata_has_index(before_drop, index_table, dropped_index)
                driver.execute(f"DROP INDEX {dropped_index}")
                driver.commit()
                after_drop = driver.data_dictionary.get_indexes(driver, table=index_table)
                assert not metadata_has_index(after_drop, index_table, dropped_index)

        config.migrate_up()

        with config.provide_session() as driver:
            assert int(driver.select_value(f"SELECT COUNT(*) FROM {mig_table}")) == 0
            if dropped_index:
                restored = driver.data_dictionary.get_indexes(driver, table=index_table)
                assert metadata_has_index(restored, index_table, dropped_index)

        assert run_(session_store.get)("seed-session") == b"persisted-payload"
    finally:
        _drop_tables_best_effort(config, backend, (*tables, mig_table))
        config.close_pool()
