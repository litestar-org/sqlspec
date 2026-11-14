"""Smoke-test imports for mypyc-built wheels."""

import argparse
import asyncio
import importlib
import importlib.machinery
import importlib.util
import inspect
import json
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, NamedTuple

__all__ = ("SMOKE_IMPORTS", "SmokeImport", "is_compiled_module", "main", "run_construction_checks", "run_smoke")

COMPILED_SUFFIXES: tuple[str, ...] = tuple(dict.fromkeys((*importlib.machinery.EXTENSION_SUFFIXES, ".so", ".pyd")))


class SmokeImport(NamedTuple):
    """A module and optional attribute required by the compiled wheel smoke."""

    name: str
    module: str
    attribute: str | None = None
    require_compiled: bool = False
    optional_dependency: str | None = None


SMOKE_IMPORTS: tuple[SmokeImport, ...] = (
    SmokeImport("oracle_core", "sqlspec.adapters.oracledb.core", "OracleAcquireOptions", True, "oracledb"),
    SmokeImport("oracle_type_converter", "sqlspec.adapters.oracledb.type_converter", None, True, "oracledb"),
    SmokeImport("observability_common", "sqlspec.observability._common", "get_w3c_trace_context", True),
    SmokeImport("teradata_dialect", "sqlspec.dialects.teradata", "Vantage"),
    SmokeImport("teradata_generators", "sqlspec.dialects.teradata._generators", "teradata_dispatch", True),
    SmokeImport("teradata_dd_config", "sqlspec.data_dictionary.dialects.teradata.config", "TERADATA_CONFIG", True),
    SmokeImport("teradata_core", "sqlspec.adapters.teradata.core", "build_connection_config", True),
    SmokeImport("teradata_pool", "sqlspec.adapters.teradata.pool", "TeradataConnectionPool", True, "teradatasql"),
    SmokeImport("teradata_type_converter", "sqlspec.adapters.teradata.type_converter", None, True),
    SmokeImport("package", "sqlspec"),
    SmokeImport("base_sqlspec", "sqlspec.base", "SQLSpec", True),
    SmokeImport("prometheus_observer", "sqlspec.extensions.prometheus._observer", "PrometheusStatementObserver", True),
    SmokeImport("async_bridge", "sqlspec.utils.sync_tools", "async_", True),
    SmokeImport("core_routing", "sqlspec.core.routing", "ReplicaSpec", True),
    SmokeImport("routing_context", "sqlspec.utils.routing", "RoutingContext"),
    SmokeImport("driver_routing", "sqlspec.driver._routing", "SyncRoutingSession"),
    SmokeImport("deferred_session", "sqlspec.extensions._deferred_session", "DeferredAsyncSession"),
    SmokeImport("core_statement", "sqlspec.core.statement", "SQL", True),
    SmokeImport("builder_select", "sqlspec.builder._select", "Select", True),
    SmokeImport("env_utils", "sqlspec.utils.env", "get_env", True),
    SmokeImport("sync_driver", "sqlspec.driver._sync", "SyncDriverAdapterBase", True),
    SmokeImport("async_driver", "sqlspec.driver._async", "AsyncDriverAdapterBase", True),
    SmokeImport("tenant_lease_state", "sqlspec.core.tenant.base", "TenantLeaseState", True),
    SmokeImport("tenant_lease", "sqlspec.driver._tenant", "AsyncTenantLease", True),
    SmokeImport("tenant_context", "sqlspec.utils.tenant._context", "TenantContext", True),
    SmokeImport("tenant_mssql_batches", "sqlspec.utils.tenant._mssql", "MssqlTenantSettings", True),
    SmokeImport("tenant_postgres_settings", "sqlspec.utils.tenant._postgres", "PostgresTenantSettings", True),
    *(
        SmokeImport(f"{adapter}_tenant", f"sqlspec.adapters.{adapter}.tenant", None, True, dependency)
        for adapter, dependency in (
            ("asyncpg", "asyncpg"),
            ("cockroach_asyncpg", "asyncpg"),
            ("cockroach_psycopg", "psycopg"),
            ("mssql_python", "mssql_python"),
            ("oracledb", "oracledb"),
            ("psqlpy", "psqlpy"),
            ("psycopg", "psycopg"),
            ("pymssql", "pymssql"),
            ("snowflake", "snowflake.connector"),
        )
    ),
    SmokeImport("storage_registry", "sqlspec.storage.registry", "StorageRegistry", True),
    SmokeImport("storage_pipeline", "sqlspec.storage.pipeline", "SyncStoragePipeline", True),
    SmokeImport("storage_staging_sql", "sqlspec.storage._staging_sql", "build_stage_merge_sql", True),
    SmokeImport("storage_ingest", "sqlspec.storage._ingest", "execute_load_from_storage_sync", True),
    SmokeImport("storage_backend_local", "sqlspec.storage.backends.local", "LocalStore", True),
    SmokeImport("storage_backend_fsspec", "sqlspec.storage.backends.fsspec", "FSSpecBackend", True),
    SmokeImport("storage_backend_obstore", "sqlspec.storage.backends.obstore", "ObStoreBackend", True),
    SmokeImport("sqlite_pool", "sqlspec.adapters.sqlite.pool", "SqliteConnectionPool", True),
    SmokeImport("sybase_pool", "sqlspec.adapters.sybase.pool", "SybaseConnectionPool", True, "pymssql"),
    SmokeImport("sqlanywhere_pool", "sqlspec.adapters.sqlanywhere.pool", "SqlanywhereConnectionPool", True, "sqlanydb"),
    SmokeImport(
        "snowflake_pool", "sqlspec.adapters.snowflake.pool", "SnowflakeConnectionPool", True, "snowflake.connector"
    ),
    SmokeImport(
        "snowflake_core", "sqlspec.adapters.snowflake.core", "normalize_connection_params", True, "snowflake.connector"
    ),
    SmokeImport("data_dictionary_registry", "sqlspec.data_dictionary._registry", "get_dialect_config", True),
    SmokeImport("data_dictionary_loader", "sqlspec.data_dictionary._loader", "DataDictionaryLoader", True),
    SmokeImport("data_dictionary_index_ddl", "sqlspec.data_dictionary._index_ddl", "SyncWithoutIndexesContext", True),
    SmokeImport("pgvector_dialect", "sqlspec.dialects.postgres._pgvector", "PGVector"),
    SmokeImport("spanner_dialect", "sqlspec.dialects.spanner._spanner", "Spanner"),
    SmokeImport("db2_dialect", "sqlspec.dialects.db2", "DB2"),
    SmokeImport("sybase_dialect", "sqlspec.dialects.sybase", "Sybase"),
    SmokeImport("sqlanywhere_dialect", "sqlspec.dialects.sybase", "SQLAnywhere"),
    SmokeImport("sybase_generators", "sqlspec.dialects.sybase._generators", "sybase_select_sql", True),
    SmokeImport("fastapi_providers", "sqlspec.extensions.fastapi.providers", "provide_filters", True, "fastapi"),
    SmokeImport(
        "litestar_providers", "sqlspec.extensions.litestar.providers", "create_filter_dependencies", True, "litestar"
    ),
    SmokeImport("event_payload", "sqlspec.extensions.events._payload", "encode_notify_payload", True),
    SmokeImport("event_channel", "sqlspec.extensions.events._channel", "SyncEventChannel", True),
    SmokeImport("event_queue", "sqlspec.extensions.events._queue", "SyncTableEventQueue", True),
    *(
        SmokeImport(f"{adapter}_events_hub", f"sqlspec.adapters.{adapter}.events._hub", None, True, adapter)
        for adapter in ("asyncpg", "oracledb", "psqlpy", "psycopg")
    ),
    SmokeImport("adk_record_types", "sqlspec.extensions.adk._types", "StoredSession", True, "google.adk"),
    SmokeImport("migration_runner", "sqlspec.migrations.runner", "SyncMigrationRunner", True),
    SmokeImport("sqlite_type_converter", "sqlspec.adapters.sqlite.type_converter", "register_type_handlers", True),
)


def is_compiled_module(module: Any) -> bool:
    """Return whether an imported module appears to be a compiled extension."""
    module_file = getattr(module, "__file__", "") or ""
    return module_file.endswith(COMPILED_SUFFIXES)


def run_smoke(*, require_compiled: bool = False) -> list[dict[str, Any]]:
    """Import the compiled-wheel smoke matrix and return per-entry results."""
    results: list[dict[str, Any]] = []
    for entry in SMOKE_IMPORTS:
        result: dict[str, Any] = {
            "name": entry.name,
            "module": entry.module,
            "attribute": entry.attribute,
            "imported": False,
            "compiled": False,
            "compiled_required": require_compiled and entry.require_compiled,
            "error": None,
            "skipped": False,
            "skip_reason": None,
        }
        try:
            module = importlib.import_module(entry.module)
            if entry.attribute is not None:
                getattr(module, entry.attribute)
            result["imported"] = True
            result["compiled"] = is_compiled_module(module)
            if require_compiled and entry.require_compiled and not result["compiled"]:
                result["error"] = "module was imported from Python source, not a compiled extension"
        except ModuleNotFoundError as exc:
            missing_name = exc.name or ""
            optional_dependency = entry.optional_dependency
            if _is_missing_optional_dependency(missing_name, optional_dependency):
                if require_compiled and entry.require_compiled and not _has_compiled_module_file(entry.module):
                    result["error"] = "compiled extension is missing from the installed package"
                else:
                    result["skipped"] = True
                    result["skip_reason"] = f"optional dependency missing: {optional_dependency}"
            else:
                result["error"] = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
        results.append(result)
    return results


def run_construction_checks(*, require_compiled: bool = False) -> list[dict[str, Any]]:
    """Run construction-time smoke checks for provider classes."""
    return [
        *[
            _check_public_import_order(order, require_compiled=require_compiled)
            for order in ("facade_first", "definitions_first", "builder_first", "migration_first")
        ],
        _check_concurrent_public_exports("attributes", require_compiled=require_compiled),
        _check_concurrent_public_exports("direct_imports", require_compiled=require_compiled),
        _check_migration_first_use(require_compiled=require_compiled),
        _check_extension_reconciliation(require_compiled=require_compiled),
        _check_litestar_openapi(require_compiled=require_compiled),
        _check_sqlspec_construction(),
        _check_tenant_lease_boundary(require_compiled=require_compiled),
        _check_session_routing_boundary(require_compiled=require_compiled),
        _check_oracle_26ai_boundary(require_compiled=require_compiled),
        _check_adapter_config_construction(),
        _check_statement_sentinel_identity(require_compiled=require_compiled),
        _check_statement_cache_rebind(require_compiled=require_compiled),
        _check_obstore_arrow_reads(require_compiled=require_compiled),
        _check_pipeline_arrow_streaming(require_compiled=require_compiled),
        _check_service_subclasses(require_compiled=require_compiled),
        _check_aiosqlite_ambient_exception(require_compiled=require_compiled),
        _check_aiosqlite_exception_mapping(require_compiled=require_compiled),
        _check_fastapi_filter_construction(require_compiled=require_compiled),
        _check_litestar_filter_construction(require_compiled=require_compiled),
    ]


def main(argv: Sequence[str] | None = None) -> int:
    """Run the mypyc smoke CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--require-compiled", action="store_true", help="Fail when compiled-surface modules import from Python source."
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output.")
    args = parser.parse_args(argv)

    results = run_smoke(require_compiled=args.require_compiled)
    results.extend(run_construction_checks(require_compiled=args.require_compiled))
    if args.json:
        json.dump({"results": results}, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(_format_text(results))
        sys.stdout.write("\n")
    return 1 if _failed_results(results) else 0


def _new_smoke_result(
    *, name: str, module: str, attribute: str | None, compiled_required: bool = False
) -> dict[str, Any]:
    return {
        "name": name,
        "module": module,
        "attribute": attribute,
        "imported": False,
        "compiled": False,
        "compiled_required": compiled_required,
        "error": None,
        "skipped": False,
        "skip_reason": None,
    }


def _has_compiled_module_file(module_name: str) -> bool:
    spec = importlib.util.find_spec("sqlspec")
    if spec is None or spec.submodule_search_locations is None:
        return False
    relative_module = Path(*module_name.split(".")[1:])
    return any(
        (Path(location) / relative_module).with_suffix(suffix).is_file()
        for location in spec.submodule_search_locations
        for suffix in COMPILED_SUFFIXES
    )


def _is_missing_optional_dependency(missing_name: str, optional_dependency: str | None) -> bool:
    if optional_dependency is None or not missing_name:
        return False
    return (
        missing_name == optional_dependency
        or missing_name.startswith(f"{optional_dependency}.")
        or optional_dependency.startswith(f"{missing_name}.")
    )


def _check_sqlspec_construction() -> dict[str, Any]:
    """Construct SQLSpec surfaces that must work from a compiled wheel."""
    result = _new_smoke_result(name="sqlspec_construction", module="sqlspec.base", attribute="SQLSpec")
    try:
        base_module = importlib.import_module("sqlspec.base")
        loader_module = importlib.import_module("sqlspec.loader")
        sqlite_module = importlib.import_module("sqlspec.adapters.sqlite")
        sqlspec_cls = base_module.SQLSpec
        sql_file_loader_cls = loader_module.SQLFileLoader
        sqlite_config_cls = sqlite_module.SqliteConfig
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(base_module)
    try:
        manager = sqlspec_cls(loader=sql_file_loader_cls())
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_file = Path(tmpdir) / "nested" / "sql" / "smoke.sql"
            sql_file.parent.mkdir(parents=True)
            sql_file.write_text("-- name: mypyc_smoke_query\nSELECT 1;\n")
            manager.load_sql_files(sql_file)
            manager.get_sql("mypyc_smoke_query")
        config = manager.add_config(sqlite_config_cls(connection_config={"database": ":memory:"}))
        manager.event_channel(config)
        manager.telemetry_snapshot()
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _check_statement_sentinel_identity(*, require_compiled: bool = False) -> dict[str, Any]:
    """Check that statement and typing modules share the same Empty sentinel."""
    result = _new_smoke_result(
        name="statement_sentinel_identity",
        module="sqlspec.core.statement",
        attribute="Empty",
        compiled_required=require_compiled,
    )
    try:
        statement_module = importlib.import_module("sqlspec.core.statement")
        private_typing = importlib.import_module("sqlspec._typing")
        public_typing = importlib.import_module("sqlspec.typing")
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(statement_module)
    if require_compiled and not result["compiled"]:
        result["error"] = "module was imported from Python source, not a compiled extension"
        return result
    try:
        assert statement_module.Empty is private_typing.Empty is public_typing.Empty
    except AssertionError as exc:
        result["error"] = f"{type(exc).__name__}: Empty sentinel identity mismatch"
    return result


def _check_statement_cache_rebind(*, require_compiled: bool = False) -> dict[str, Any]:
    """Exercise a cache-hit copy, parameter rebind, and expression snapshot."""
    result = _new_smoke_result(
        name="statement_cache_rebind",
        module="sqlspec.core.statement",
        attribute="SQL",
        compiled_required=require_compiled,
    )
    try:
        statement_module = importlib.import_module("sqlspec.core.statement")
        sqlglot_exp = importlib.import_module("sqlglot.expressions")
        sql_cls = statement_module.SQL
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(statement_module)
    if require_compiled and not result["compiled"]:
        result["error"] = "module was imported from Python source, not a compiled extension"
        return result
    try:
        original = sql_cls("SELECT * FROM t WHERE id = :id", {"id": 1})
        original.compile()
        copy = original.copy(parameters={"id": 2})
        assert copy._compiled_from_cache is True
        assert copy.get_processed_state() is original.get_processed_state()
        copy.compile()
        expression = copy._current_expression()
        assert isinstance(expression, sqlglot_exp.Expr)
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _check_obstore_arrow_reads(*, require_compiled: bool = False) -> dict[str, Any]:
    """Read, stream, and create-only write Parquet through ObStoreBackend with real obstore buffers."""
    result = _new_smoke_result(
        name="obstore_arrow_reads",
        module="sqlspec.storage.backends.obstore",
        attribute="ObStoreBackend",
        compiled_required=require_compiled,
    )
    for dependency in ("obstore", "pyarrow"):
        if importlib.util.find_spec(dependency) is None:
            result["skipped"] = True
            result["skip_reason"] = f"optional dependency missing: {dependency}"
            return result
    try:
        backend_module = importlib.import_module("sqlspec.storage.backends.obstore")
        exceptions_module = importlib.import_module("sqlspec.exceptions")
        pa = importlib.import_module("pyarrow")
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(backend_module)
    if require_compiled and not result["compiled"]:
        result["error"] = "module was imported from Python source, not a compiled extension"
        return result

    async def read_async(backend: Any) -> "tuple[Any, list[Any], bytes]":
        table = await backend.read_arrow_async("smoke.parquet")
        batches = [batch async for batch in backend.stream_arrow_async("smoke.parquet")]
        return table, batches, await backend.read_bytes_async("smoke.bin")

    expected = pa.table({"id": [1, 2, 3]})
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            for uri in ("memory://", Path(tmp_dir).as_uri()):
                backend = backend_module.ObStoreBackend(uri)
                backend.write_arrow_sync("smoke.parquet", expected)
                backend.write_bytes_sync("smoke.bin", b"smoke", overwrite=False)
                try:
                    backend.write_bytes_sync("smoke.bin", b"again", overwrite=False)
                except exceptions_module.FileExistsInStorageError:
                    pass
                else:
                    raise AssertionError(f"{uri}: overwrite=False replaced an existing object")
                assert backend.read_arrow_sync("smoke.parquet").equals(expected), f"{uri}: read_arrow_sync"
                streamed = pa.Table.from_batches(list(backend.stream_arrow_sync("smoke.parquet")))
                assert streamed.equals(expected), f"{uri}: stream_arrow_sync"
                assert backend.read_bytes_sync("smoke.bin") == b"smoke", f"{uri}: read_bytes_sync"
                table, batches, data = asyncio.run(read_async(backend))
                assert table.equals(expected), f"{uri}: read_arrow_async"
                assert pa.Table.from_batches(batches).equals(expected), f"{uri}: stream_arrow_async"
                assert data == b"smoke", f"{uri}: read_bytes_async"
    except Exception as exc:
        underlying = exc.__cause__ or exc.__context__
        detail = f" (from {type(underlying).__name__}: {underlying})" if underlying is not None else ""
        result["error"] = f"{type(exc).__name__}: {exc}{detail}"
    return result


def _generate_failing_arrow_batches(pa: Any, target_schema: Any, /) -> Iterator[Any]:
    """Yield a single batch and raise RuntimeError to simulate midstream failure."""
    yield pa.RecordBatch.from_pydict({"id": [10]}, schema=target_schema)
    raise RuntimeError("failing batch")


def _generate_slow_arrow_batches(
    pa: Any, target_schema: Any, started_event: asyncio.Event, event_loop: asyncio.AbstractEventLoop, /
) -> Iterator[Any]:
    """Yield a batch, notify caller, pause to allow cancellation, and yield again."""
    yield pa.RecordBatch.from_pydict({"id": [20]}, schema=target_schema)
    event_loop.call_soon_threadsafe(started_event.set)
    time.sleep(0.3)
    yield pa.RecordBatch.from_pydict({"id": [21]}, schema=target_schema)


async def _cancel_in_flight_write(pipeline: Any, destination: str, pa: Any, target_schema: Any, /) -> None:
    """Launch async batch write, await start, cancel, and expect CancelledError."""
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    batches = _generate_slow_arrow_batches(pa, target_schema, started, loop)
    task = asyncio.create_task(pipeline.write_arrow_batches_async(destination, batches))
    await started.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("task was not cancelled")


async def _cancel_stream_aenter(backend: Any, path: str, /) -> None:
    """Cancel open_write_stream_async().__aenter__() and verify CancelledError propagates."""
    loop = asyncio.get_running_loop()
    stream = backend.open_write_stream_async(path)
    task = asyncio.create_task(stream.__aenter__())
    loop.call_soon(task.cancel)
    try:
        sink = await task
    except asyncio.CancelledError:
        return
    await stream.__aexit__(None, None, None)
    _ = sink
    raise AssertionError("open_write_stream_async.__aenter__ was not cancelled")


async def _verify_preflight_async_batch_cleanup(
    async_p: Any, destination: str, arrow_helpers_module: Any, exceptions_module: Any, /
) -> None:
    """Verify write_arrow_batches_async closes opened async batch iterators on pre-iteration errors."""
    events: list[str] = []

    class _SmokeAsyncRows:
        column_types: dict[str, str] = {"id": "int64"}

        def __init__(self) -> None:
            self._items = [{"id": 1}, {"id": 2}]
            self._idx = 0

        def __aiter__(self) -> "_SmokeAsyncRows":
            return self

        async def __anext__(self) -> dict[str, int]:
            if self._idx >= len(self._items):
                raise StopAsyncIteration
            item = self._items[self._idx]
            self._idx += 1
            return item

        async def aclose(self) -> None:
            events.append("aclosed")

    batches = await arrow_helpers_module.arrow_batches_from_async_rows(_SmokeAsyncRows(), batch_size=1)
    assert batches is not None
    assert events == []
    try:
        await async_p.write_arrow_batches_async(destination, batches, file_format="json")
    except exceptions_module.StorageCapabilityError:
        pass
    else:
        raise AssertionError("write_arrow_batches_async(file_format='json') did not raise StorageCapabilityError")
    assert events == ["aclosed"], f"expected ['aclosed'], got {events!r}"


def _verify_pipeline_preflight_and_cancel_smoke(
    *,
    uri: str,
    alias: str,
    expected_backend: str,
    backend: Any,
    local_dir: Path,
    sample_table: Any,
    sync_p: Any,
    async_p: Any,
    pa: Any,
    arrow_helpers_module: Any,
    exceptions_module: Any,
) -> None:
    """Verify pre-iteration batch cleanup and staged stream __aenter__ cancellation."""
    sync_closed: list[str] = []
    deferred_reader = arrow_helpers_module.arrow_reader_with_deferred_close(
        pa.RecordBatchReader.from_batches(sample_table.schema, sample_table.to_batches()),
        lambda closed=sync_closed: closed.append("closed"),
    )
    try:
        sync_p.write_arrow_batches(f"alias://{alias}/preflight_bad.json", deferred_reader, file_format="json")
    except exceptions_module.StorageCapabilityError:
        pass
    else:
        raise AssertionError(f"{uri}: sync json format did not raise StorageCapabilityError")
    assert sync_closed == ["closed"], f"{uri}: deferred reader not closed on preflight error"

    asyncio.run(
        _verify_preflight_async_batch_cleanup(
            async_p, f"alias://{alias}/preflight_async_bad.json", arrow_helpers_module, exceptions_module
        )
    )

    if expected_backend == "local":
        asyncio.run(_cancel_stream_aenter(backend, "cancelled_aenter.parquet"))
        assert [p.name for p in local_dir.iterdir() if p.name.startswith(".sqlspec-stage-")] == []
        assert not (local_dir / "cancelled_aenter.parquet").exists()


def _verify_fsspec_aenter_cancel_smoke(*, tmp_dir: str, require_compiled: bool) -> str | None:
    """Verify _AsyncFsspecWriteStream.__aenter__ cancellation when fsspec is installed."""
    if importlib.util.find_spec("fsspec") is None:
        return None
    fsspec_module = importlib.import_module("sqlspec.storage.backends.fsspec")
    if require_compiled and not is_compiled_module(fsspec_module):
        return "sqlspec.storage.backends.fsspec was imported from source, not a compiled extension"
    fsspec_dir = Path(tmp_dir) / "fsspec_root"
    fsspec_dir.mkdir(parents=True, exist_ok=True)
    fsspec_backend = fsspec_module.FSSpecBackend("file://", base_path=str(fsspec_dir))
    asyncio.run(_cancel_stream_aenter(fsspec_backend, "cancelled_fsspec_aenter.parquet"))
    assert [p.name for p in fsspec_dir.iterdir() if p.name.startswith(".sqlspec-stage-")] == []
    assert not (fsspec_dir / "cancelled_fsspec_aenter.parquet").exists()
    return None


def _check_pipeline_arrow_streaming(*, require_compiled: bool = False) -> dict[str, Any]:
    """Stream Arrow tables and batches through compiled pipelines over ObStoreBackend and LocalStore."""
    result = _new_smoke_result(
        name="pipeline_arrow_streaming",
        module="sqlspec.storage.pipeline",
        attribute="SyncStoragePipeline",
        compiled_required=require_compiled,
    )
    for dependency in ("obstore", "pyarrow"):
        if importlib.util.find_spec(dependency) is None:
            result["skipped"] = True
            result["skip_reason"] = f"optional dependency missing: {dependency}"
            return result
    try:
        pipeline_module = importlib.import_module("sqlspec.storage.pipeline")
        backend_module = importlib.import_module("sqlspec.storage.backends.obstore")
        local_module = importlib.import_module("sqlspec.storage.backends.local")
        type_guards_module = importlib.import_module("sqlspec.utils.type_guards")
        sync_driver_module = importlib.import_module("sqlspec.driver._sync")
        base_module = importlib.import_module("sqlspec.base")
        sqlite_module = importlib.import_module("sqlspec.adapters.sqlite")
        arrow_payload_module = importlib.import_module("sqlspec.storage._arrow_payload")
        arrow_helpers_module = importlib.import_module("sqlspec.utils.arrow_helpers")
        registry_module = importlib.import_module("sqlspec.storage.registry")
        exceptions_module = importlib.import_module("sqlspec.exceptions")
        pa = importlib.import_module("pyarrow")
        decode_arrow = arrow_payload_module.decode_arrow_payload
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    compiled_pipeline = is_compiled_module(pipeline_module)
    compiled_backend = is_compiled_module(backend_module)
    compiled_local = is_compiled_module(local_module)
    compiled_type_guards = is_compiled_module(type_guards_module)
    compiled_sync_driver = is_compiled_module(sync_driver_module)
    arrow_payload_interpreted = not is_compiled_module(arrow_payload_module)
    result["compiled"] = (
        compiled_pipeline and compiled_backend and compiled_local and compiled_type_guards and compiled_sync_driver
    )

    if require_compiled:
        if not compiled_pipeline:
            result["error"] = "sqlspec.storage.pipeline was imported from source, not a compiled extension"
            return result
        if not compiled_backend:
            result["error"] = "sqlspec.storage.backends.obstore was imported from source, not a compiled extension"
            return result
        if not compiled_local:
            result["error"] = "sqlspec.storage.backends.local was imported from source, not a compiled extension"
            return result
        if not compiled_type_guards:
            result["error"] = "sqlspec.utils.type_guards was imported from source, not a compiled extension"
            return result
        if not compiled_sync_driver:
            result["error"] = "sqlspec.driver._sync was imported from source, not a compiled extension"
            return result
        if not arrow_payload_interpreted:
            result["error"] = "sqlspec.storage._arrow_payload should remain interpreted"
            return result

    expected_rows = 3
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            obstore_dir = Path(tmp_dir) / "obstore_root"
            obstore_dir.mkdir(parents=True, exist_ok=True)
            local_dir = Path(tmp_dir) / "local_root"
            local_dir.mkdir(parents=True, exist_ok=True)
            targets = [
                ("memory://", "mem", "obstore"),
                (obstore_dir.as_uri(), "file", "obstore"),
                (local_dir.as_uri(), "local", "local"),
            ]
            for uri, prefix, expected_backend in targets:
                registry = registry_module.StorageRegistry()
                alias = f"{prefix}_alias"
                registry.register_alias(alias, uri, backend=expected_backend)
                backend = registry.get(alias)
                sync_p = pipeline_module.SyncStoragePipeline(registry=registry)
                async_p = pipeline_module.AsyncStoragePipeline(registry=registry)

                sample_table = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})

                for fmt in ("parquet", "arrow-ipc", "csv"):
                    dest_sync = f"alias://{alias}/sync_table_{fmt}.{fmt}"
                    t_sync = sync_p.write_arrow(sample_table, dest_sync, format_hint=fmt)
                    assert t_sync["backend"] == expected_backend, f"{uri}: {fmt} sync backend"
                    assert t_sync["rows_processed"] == expected_rows, f"{uri}: {fmt} sync rows"
                    assert t_sync["bytes_processed"] > 0, f"{uri}: {fmt} sync bytes"
                    assert t_sync["format"] == fmt, f"{uri}: {fmt} sync format"

                    dest_sync_b = f"alias://{alias}/sync_batches_{fmt}.{fmt}"
                    t_sync_b = sync_p.write_arrow_batches(dest_sync_b, sample_table.to_batches(), file_format=fmt)
                    assert t_sync_b["backend"] == expected_backend, f"{uri}: {fmt} sync batches backend"
                    assert t_sync_b["rows_processed"] == expected_rows, f"{uri}: {fmt} sync batches rows"

                    dest_async = f"alias://{alias}/async_table_{fmt}.{fmt}"
                    t_async = asyncio.run(async_p.write_arrow(sample_table, dest_async, format_hint=fmt))
                    assert t_async["backend"] == expected_backend, f"{uri}: {fmt} async backend"
                    assert t_async["rows_processed"] == expected_rows, f"{uri}: {fmt} async rows"

                    dest_async_b = f"alias://{alias}/async_batches_{fmt}.{fmt}"
                    t_async_b = asyncio.run(
                        async_p.write_arrow_batches_async(dest_async_b, sample_table.to_batches(), file_format=fmt)
                    )
                    assert t_async_b["backend"] == expected_backend, f"{uri}: {fmt} async batches backend"
                    assert t_async_b["rows_processed"] == expected_rows, f"{uri}: {fmt} async batches rows"

                    raw = backend.read_bytes_sync(f"sync_table_{fmt}.{fmt}")
                    decoded = decode_arrow(raw, fmt)
                    assert decoded.num_rows == expected_rows, f"{uri}: {fmt} decoded rows"

                existing_table = pa.table({"id": [1]})
                sync_p.write_arrow(existing_table, f"alias://{alias}/existing.parquet")

                try:
                    sync_p.write_arrow_batches(
                        f"alias://{alias}/existing.parquet", _generate_failing_arrow_batches(pa, existing_table.schema)
                    )
                except RuntimeError:
                    pass
                else:
                    raise AssertionError(f"{uri}: midstream failure did not raise RuntimeError")

                assert backend.read_arrow_sync("existing.parquet").equals(existing_table), f"{uri}: existing preserved"

                asyncio.run(
                    _cancel_in_flight_write(async_p, f"alias://{alias}/existing.parquet", pa, existing_table.schema)
                )
                assert backend.read_arrow_sync("existing.parquet").equals(existing_table), f"{uri}: cancel preserved"

                once_dest = f"alias://{alias}/once.parquet"
                sync_p.write_arrow(sample_table, once_dest, overwrite=False)
                try:
                    sync_p.write_arrow(sample_table, once_dest, overwrite=False)
                except exceptions_module.FileExistsInStorageError:
                    pass
                else:
                    raise AssertionError(f"{uri}: Sync overwrite=False did not raise FileExistsInStorageError")

                try:
                    asyncio.run(async_p.write_arrow(sample_table, once_dest, overwrite=False))
                except exceptions_module.FileExistsInStorageError:
                    pass
                else:
                    raise AssertionError(f"{uri}: Async overwrite=False did not raise FileExistsInStorageError")

                if uri == "memory://":
                    assert backend.supports_direct_arrow_write("parquet", overwrite=False) is True
                    once_batches_dest = f"alias://{alias}/once_batches.parquet"
                    t_once_b = sync_p.write_arrow_batches(
                        once_batches_dest, sample_table.to_batches(), file_format="parquet", overwrite=False
                    )
                    assert t_once_b["rows_processed"] == expected_rows
                    assert [k for k in backend.list_objects_sync() if ".sqlspec-stage-" in k] == []
                    try:
                        sync_p.write_arrow_batches(
                            once_batches_dest, sample_table.to_batches(), file_format="parquet", overwrite=False
                        )
                    except exceptions_module.FileExistsInStorageError:
                        pass
                    else:
                        raise AssertionError("memory:// staged overwrite=False batches did not raise conflict")
                    assert [k for k in backend.list_objects_sync() if ".sqlspec-stage-" in k] == []

                _verify_pipeline_preflight_and_cancel_smoke(
                    uri=uri,
                    alias=alias,
                    expected_backend=expected_backend,
                    backend=backend,
                    local_dir=local_dir,
                    sample_table=sample_table,
                    sync_p=sync_p,
                    async_p=async_p,
                    pa=pa,
                    arrow_helpers_module=arrow_helpers_module,
                    exceptions_module=exceptions_module,
                )

            fsspec_err = _verify_fsspec_aenter_cancel_smoke(tmp_dir=tmp_dir, require_compiled=require_compiled)
            if fsspec_err is not None:
                result["error"] = fsspec_err
                return result

            spec = base_module.SQLSpec()
            sqlite_cfg = sqlite_module.SqliteConfig()
            spec.add_config(sqlite_cfg)
            try:
                with spec.provide_session(sqlite_cfg) as sqlite_session:
                    sqlite_session.execute("CREATE TABLE smoke_export (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
                    sqlite_rows = [(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five")]
                    sqlite_session.execute_many("INSERT INTO smoke_export (id, label) VALUES (?, ?)", sqlite_rows)
                    sqlite_session.commit()

                    export_dest = local_dir / "sqlite_export.parquet"
                    export_job = sqlite_session.select_to_storage(
                        "SELECT id, label FROM smoke_export WHERE id >= ? ORDER BY id",
                        export_dest,
                        (1,),
                        format_hint="parquet",
                        batch_size=2,
                    )
                    assert export_job.telemetry["rows_processed"] == len(sqlite_rows)
                    assert export_job.telemetry["bytes_processed"] > 0
                    decoded_export = decode_arrow(export_dest.read_bytes(), "parquet")
                    assert decoded_export.column_names == ["id", "label"]
                    assert decoded_export.to_pylist() == [
                        {"id": 1, "label": "one"},
                        {"id": 2, "label": "two"},
                        {"id": 3, "label": "three"},
                        {"id": 4, "label": "four"},
                        {"id": 5, "label": "five"},
                    ]

                    empty_dest = local_dir / "sqlite_empty.parquet"
                    empty_job = sqlite_session.select_to_storage(
                        "SELECT id, label FROM smoke_export WHERE id < ?",
                        empty_dest,
                        (0,),
                        format_hint="parquet",
                        batch_size=2,
                    )
                    assert empty_job.telemetry["rows_processed"] == 0
                    decoded_empty = decode_arrow(empty_dest.read_bytes(), "parquet")
                    assert decoded_empty.num_rows == 0
                    assert decoded_empty.column_names == ["id", "label"]

                    shard_a = local_dir / "shard_a.parquet"
                    shard_b = local_dir / "shard_b.parquet"
                    sync_p.write_arrow(pa.table({"id": [1, 2], "label": ["a", "b"]}), shard_a, format_hint="parquet")
                    sync_p.write_arrow(
                        pa.table({"id": [2, 3], "label": ["b_updated", "c"]}), shard_b, format_hint="parquet"
                    )
                    sqlite_session.execute("CREATE TABLE smoke_ingest (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
                    sqlite_session.execute("CREATE INDEX idx_smoke_ingest_label ON smoke_ingest (label)")
                    sqlite_session.commit()
                    shards = [shard_a, shard_b]
                    ingest_job = sqlite_session.load_from_storage(
                        "smoke_ingest",
                        shards,
                        file_format="parquet",
                        stage_table=True,
                        conflict_keys=["id"],
                        drop_indexes=True,
                        checkpoint_table="_smoke_ckpt",
                        max_workers=len(shards),
                        executor="process",
                    )
                    assert ingest_job.telemetry["shards_processed"] == len(shards)
                    ingested_rows = sqlite_session.select("SELECT id, label FROM smoke_ingest ORDER BY id")
                    assert ingested_rows == [
                        {"id": 1, "label": "a"},
                        {"id": 2, "label": "b_updated"},
                        {"id": 3, "label": "c"},
                    ]
                    restored_indexes = {
                        idx["index_name"]
                        for idx in sqlite_session.data_dictionary.get_indexes(sqlite_session, "smoke_ingest")
                    }
                    assert "idx_smoke_ingest_label" in restored_indexes
            finally:
                sqlite_cfg.close_pool()
    except Exception as exc:
        underlying = exc.__cause__ or exc.__context__
        detail = f" (from {type(underlying).__name__}: {underlying})" if underlying is not None else ""
        result["error"] = f"{type(exc).__name__}: {exc}{detail}"
    return result


def _check_aiosqlite_exception_mapping(*, require_compiled: bool = False) -> dict[str, Any]:
    """Exercise mapped aiosqlite statement errors in isolated child processes."""
    result = _new_smoke_result(
        name="aiosqlite_exception_mapping",
        module="sqlspec.driver._async",
        attribute="AsyncDriverAdapterBase",
        compiled_required=require_compiled,
    )
    try:
        async_driver_module = importlib.import_module("sqlspec.driver._async")
        importlib.import_module("aiosqlite")
    except ModuleNotFoundError as exc:
        if _is_missing_optional_dependency(exc.name or "", "aiosqlite"):
            result["skipped"] = True
            result["skip_reason"] = "optional dependency missing: aiosqlite"
            return result
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(async_driver_module)
    if require_compiled and not result["compiled"]:
        result["error"] = "module was imported from Python source, not a compiled extension"
        return result

    child_script = """
import asyncio
import sys

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import SQLSpecError


async def main() -> None:
    config = AiosqliteConfig()
    spec = SQLSpec()
    spec.add_config(config)
    try:
        async with spec.provide_session(config) as driver:
            await driver.execute("CREATE TABLE smoke_items (id INTEGER PRIMARY KEY)")
            operation = getattr(driver, sys.argv[1])
            try:
                await operation("SELECT missing_column FROM smoke_items")
            except SQLSpecError as exc:
                if "missing_column" not in str(exc):
                    raise
            else:
                raise AssertionError("invalid statement did not raise SQLSpecError")
    finally:
        await config.close_pool()


asyncio.run(main())
print(f"{sys.argv[1]}:SQLSpecError")
"""
    for operation_name in ("select", "execute"):
        try:
            completed = subprocess.run(
                [sys.executable, "-I", "-c", child_script, operation_name],
                capture_output=True,
                check=False,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            result["error"] = f"{operation_name} exception subprocess timed out"
            return result
        expected_marker = f"{operation_name}:SQLSpecError"
        if completed.returncode != 0 or expected_marker not in completed.stdout:
            result["error"] = (
                f"{operation_name} exception subprocess failed with return code {completed.returncode}; "
                f"stdout={completed.stdout!r}; stderr={completed.stderr!r}"
            )
            return result
    return result


def _check_aiosqlite_ambient_exception(*, require_compiled: bool = False) -> dict[str, Any]:
    """Exercise successful compiled async operations while handling another exception."""
    result = _new_smoke_result(
        name="aiosqlite_ambient_exception",
        module="sqlspec.driver._async",
        attribute="AsyncDriverAdapterBase",
        compiled_required=require_compiled,
    )
    try:
        async_driver_module = importlib.import_module("sqlspec.driver._async")
        importlib.import_module("aiosqlite")
    except ModuleNotFoundError as exc:
        if _is_missing_optional_dependency(exc.name or "", "aiosqlite"):
            result["skipped"] = True
            result["skip_reason"] = "optional dependency missing: aiosqlite"
            return result
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(async_driver_module)
    if require_compiled and not result["compiled"]:
        result["error"] = "module was imported from Python source, not a compiled extension"
        return result

    child_script = """
import asyncio

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.observability import ObservabilityConfig


async def main() -> None:
    observed = []
    config = AiosqliteConfig(observability_config=ObservabilityConfig(statement_observers=(observed.append,)))
    spec = SQLSpec()
    spec.add_config(config)
    try:
        async with spec.provide_session(config) as driver:
            await driver.execute("CREATE TABLE smoke_items (id INTEGER PRIMARY KEY, value TEXT)")

            try:
                raise ValueError("ambient-select")
            except ValueError:
                rows = await driver.select("SELECT 1 AS value")
            assert rows == [{"value": 1}]
            assert observed

            await driver.execute("SELECT ? AS value", [1])
            try:
                raise ValueError("ambient-direct-cache")
            except ValueError:
                direct_rows = await driver.execute("SELECT ? AS value", [2])
            assert direct_rows.get_data() == [{"value": 2}]

            await driver.execute("SELECT :value AS value", {"value": 3})
            try:
                raise ValueError("ambient-rebound-cache")
            except ValueError:
                rebound_rows = await driver.execute("SELECT :value AS value", {"value": 4})
            assert rebound_rows.get_data() == [{"value": 4}]

            try:
                raise ValueError("ambient-many")
            except ValueError:
                await driver.execute_many(
                    "INSERT INTO smoke_items (id, value) VALUES (?, ?)", [(1, "one"), (2, "two")]
                )

            try:
                raise ValueError("ambient-script")
            except ValueError:
                await driver.execute_script(
                    "INSERT INTO smoke_items (id, value) VALUES (3, 'three');"
                    "INSERT INTO smoke_items (id, value) VALUES (4, 'four');"
                )

            try:
                raise ValueError("ambient-stream")
            except ValueError:
                async with driver.select_stream(
                    "SELECT id, value FROM smoke_items ORDER BY id", chunk_size=1
                ) as stream:
                    streamed = [row async for row in stream]
            assert streamed == [
                {"id": 1, "value": "one"},
                {"id": 2, "value": "two"},
                {"id": 3, "value": "three"},
                {"id": 4, "value": "four"},
            ]
    finally:
        await config.close_pool()


asyncio.run(main())
print("ambient:ok")
"""
    try:
        completed = subprocess.run(
            [sys.executable, "-I", "-c", child_script], capture_output=True, check=False, text=True, timeout=30
        )
    except subprocess.TimeoutExpired:
        result["error"] = "ambient exception subprocess timed out"
        return result
    if completed.returncode != 0 or "ambient:ok" not in completed.stdout:
        result["error"] = (
            f"ambient exception subprocess failed with return code {completed.returncode}; "
            f"stdout={completed.stdout!r}; stderr={completed.stderr!r}"
        )
    return result


def _check_litestar_filter_construction(*, require_compiled: bool = False) -> dict[str, Any]:
    """Construct every paired-annotation Litestar filter provider (issue #475).

    This invokes ``create_filter_dependencies`` with a config that drives every
    provider class with two ``Annotated`` locals: ``_BeforeAfterFilterProvider``
    (twice, via ``created_at`` and ``updated_at``),
    ``_LimitOffsetFilterProvider``, ``_SearchFilterProvider``, and
    ``_OrderByProvider``. The provider module intentionally remains interpreted.
    """
    result = _new_smoke_result(
        name="litestar_filter_construction",
        module="sqlspec.extensions.litestar.providers",
        attribute="create_filter_dependencies",
        compiled_required=require_compiled,
    )
    try:
        providers = importlib.import_module("sqlspec.extensions.litestar.providers")
    except ModuleNotFoundError as exc:
        if _is_missing_optional_dependency(exc.name or "", "litestar"):
            result["skipped"] = True
            result["skip_reason"] = "optional dependency missing: litestar"
            return result
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(providers)
    if require_compiled and not result["compiled"]:
        result["error"] = "module was imported from Python source, not a compiled extension"
        return result
    config = providers.FilterConfig(
        created_at=True,
        updated_at=True,
        pagination_type="limit_offset",
        pagination_size=25,
        search=["name", "email"],
        search_ignore_case=True,
        sort_field=["created_at", "name"],
        sort_order="asc",
    )
    try:
        deps = providers.create_filter_dependencies(config)
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    expected = {"created_filter", "updated_filter", "limit_offset_filter", "search_filter", "order_by_filter"}
    missing = sorted(expected - deps.keys())
    if missing:
        result["error"] = f"create_filter_dependencies returned without expected keys: {missing}"
    return result


def _check_fastapi_filter_construction(*, require_compiled: bool = False) -> dict[str, Any]:
    """Construct every generated FastAPI filter provider."""
    result = _new_smoke_result(
        name="fastapi_filter_construction",
        module="sqlspec.extensions.fastapi.providers",
        attribute="provide_filters",
        compiled_required=require_compiled,
    )
    try:
        providers = importlib.import_module("sqlspec.extensions.fastapi.providers")
    except ModuleNotFoundError as exc:
        if _is_missing_optional_dependency(exc.name or "", "fastapi"):
            result["skipped"] = True
            result["skip_reason"] = "optional dependency missing: fastapi"
            return result
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(providers)
    if require_compiled and not result["compiled"]:
        result["error"] = "module was imported from Python source, not a compiled extension"
        return result
    config = providers.FilterConfig(
        id_filter=int,
        created_at=True,
        updated_at=True,
        pagination_type="limit_offset",
        pagination_size=25,
        search=["name", "email"],
        search_ignore_case=True,
        sort_field=["created_at", "name"],
        sort_order="asc",
        not_in_fields=[providers.FieldNameType("status")],
        in_fields=[providers.FieldNameType("role")],
        null_fields=["deleted_at"],
        not_null_fields=["published_at"],
        boolean_fields=["active"],
        choice_fields=[providers.ChoiceField("kind", ["a", "b"])],
    )
    try:
        dependency = providers.provide_filters(config)
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    signature = getattr(dependency, "__signature__", None)
    if signature is None:
        result["error"] = "provide_filters returned dependency without __signature__"
        return result

    expected = {
        "active_boolean_filter",
        "created_filter",
        "deleted_at_null_filter",
        "id_filter",
        "kind_choices_filter",
        "limit_offset_filter",
        "order_by_filter",
        "published_at_not_null_filter",
        "role_in_filter",
        "search_filter",
        "status_not_in_filter",
        "updated_filter",
    }
    missing = sorted(expected - signature.parameters.keys())
    if missing:
        result["error"] = f"provide_filters returned without expected parameters: {missing}"
    return result


def _discover_adapter_config_classes(*, skipped: "list[str] | None" = None) -> "list[tuple[str, type[Any]]]":
    """Return every database config class defined by an adapter ``config`` module.

    A class qualifies when it is defined in ``sqlspec.adapters.<name>.config`` and
    carries ``migration_tracker_type``, which excludes the pool, connection and
    extension helper classes that share those modules.
    """
    adapters_package = importlib.import_module("sqlspec.adapters")
    package_root = Path(next(iter(adapters_package.__path__)))
    discovered: list[tuple[str, type[Any]]] = []
    for config_path in sorted(package_root.glob("*/config.py")):
        module_name = f"sqlspec.adapters.{config_path.parent.name}.config"
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if skipped is None or (exc.name or "").split(".")[0] not in {
                "adbc_driver_manager",
                "aiomysql",
                "aiosqlite",
                "arrow_odbc",
                "asyncmy",
                "asyncpg",
                "duckdb",
                "google",
                "ibm_db",
                "ibm_db_dbi",
                "mssql_python",
                "mysql",
                "oracledb",
                "psqlpy",
                "psycopg",
                "psycopg_pool",
                "pymssql",
                "pymysql",
                "sqlanydb",
                "teradatasql",
            }:
                raise
            skipped.append(f"{module_name}: optional dependency missing: {exc.name}")
            continue
        discovered.extend(
            (f"{module_name}.{candidate.__name__}", candidate)
            for candidate in vars(module).values()
            if inspect.isclass(candidate)
            and candidate.__module__ == module_name
            and hasattr(candidate, "migration_tracker_type")
        )
    return discovered


def _adapter_config_construction_failure(qualified_name: str, config_cls: "type[Any]") -> "str | None":
    """Return a failure description when an adapter config cannot be constructed."""
    try:
        config_cls()
    except Exception as exc:
        return f"{qualified_name}: {exc!r}"
    return None


def _check_adapter_config_construction() -> dict[str, Any]:
    """Construct every adapter database config with no arguments."""
    result = _new_smoke_result(
        name="adapter_config_construction", module="sqlspec.adapters", attribute="migration_tracker_type"
    )
    try:
        skipped: list[str] = []
        discovered = _discover_adapter_config_classes(skipped=skipped)
        result["skipped_adapters"] = skipped
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    failures = [
        failure
        for failure in (
            _adapter_config_construction_failure(qualified_name, config_cls)
            for qualified_name, config_cls in discovered
        )
        if failure is not None
    ]
    if failures:
        result["error"] = "; ".join(failures)
    return result


def _check_service_subclasses(*, require_compiled: bool = False) -> dict[str, Any]:
    """Exercise Python service subclasses against the compiled query and transaction runtime."""
    result = _new_smoke_result(
        name="service_subclasses", module="sqlspec.service._core", attribute=None, compiled_required=require_compiled
    )
    try:
        service_module = importlib.import_module("sqlspec.service._core")
        importlib.import_module("aiosqlite")
    except ModuleNotFoundError as exc:
        if _is_missing_optional_dependency(exc.name or "", "aiosqlite"):
            result["skipped"] = True
            result["skip_reason"] = "optional dependency missing: aiosqlite"
            return result
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["imported"] = True
    result["compiled"] = is_compiled_module(service_module)
    if require_compiled and not result["compiled"]:
        result["error"] = "module was imported from Python source, not a compiled extension"
        return result

    child_script = """
import asyncio
import sys

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

if sys.argv[1] == "compiled":
    from importlib.machinery import EXTENSION_SUFFIXES
    import sqlspec.service._core

    assert sqlspec.service._core.__file__.endswith(tuple(EXTENSION_SUFFIXES))


class AppSyncService(SQLSpecSyncService[SqliteDriver]):
    __slots__ = ("calls",)

    def __init__(self, config):
        super().__init__(config=config)
        self.calls = 0

    def provide_session(self, session=None, *, read_only=False):
        self.calls += 1
        return super().provide_session(session, read_only=read_only)


class SyncService(AppSyncService):
    __slots__ = ()


class AppAsyncService(SQLSpecAsyncService[AiosqliteDriver]):
    __slots__ = ("calls",)

    def __init__(self, config):
        super().__init__(config=config)
        self.calls = 0

    def provide_session(self, session=None, *, read_only=False):
        self.calls += 1
        return super().provide_session(session, read_only=read_only)


class AsyncService(AppAsyncService):
    __slots__ = ()


for base in (SQLSpecAsyncService, SQLSpecSyncService, SyncService, AsyncService):
    assert base.__dictoffset__ == 0, base
    assert "__slots__" in vars(base), base


config = SqliteConfig()
try:
    service = SyncService(config)
    assert service.get_one("SELECT 1 AS value") == {"value": 1}
    assert service.calls == 1
    with service.begin_transaction() as session:
        assert service.session is session
        assert service.get_one("SELECT 2 AS value") == {"value": 2}
    assert service.calls == 3
    try:
        raise ValueError("caller error")
    except ValueError:
        assert service.exists("SELECT 1")
        assert service.paginate("SELECT value FROM (SELECT 1 AS value) AS source").items == [{"value": 1}]
        assert service.get_one("SELECT 1 AS value") == {"value": 1}
finally:
    config.close_pool()


async def main():
    config = AiosqliteConfig()
    try:
        service = AsyncService(config)
        assert await service.get_one("SELECT 1 AS value") == {"value": 1}
        assert service.calls == 1
        async with service.begin_transaction() as session:
            assert service.session is session
            assert await service.get_one("SELECT 2 AS value") == {"value": 2}
        assert service.calls == 3
        try:
            raise ValueError("caller error")
        except ValueError:
            assert await service.exists("SELECT 1")
            assert (await service.paginate("SELECT value FROM (SELECT 1 AS value) AS source")).items == [{"value": 1}]
            assert await service.get_one("SELECT 1 AS value") == {"value": 1}
    finally:
        await config.close_pool()


asyncio.run(main())
print("service-subclasses:ok")
"""
    if not require_compiled:
        package = importlib.import_module("sqlspec")
        source_file = package.__file__
        if source_file is None:
            result["error"] = "SQLSpec package origin is unavailable"
            return result
        source_root = str(Path(source_file).resolve().parent.parent)
        child_script = f"import sys; sys.path.insert(0, {source_root!r})\n" + child_script
    try:
        completed = subprocess.run(
            [sys.executable, "-I", "-c", child_script, "compiled" if require_compiled else "source"],
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        result["error"] = "service subclass subprocess timed out"
        return result
    if completed.returncode != 0 or "service-subclasses:ok" not in completed.stdout:
        result["error"] = (
            f"service subclass subprocess failed with return code {completed.returncode}; "
            f"stdout={completed.stdout!r}; stderr={completed.stderr!r}"
        )
    return result


def _run_startup_check(
    name: str, script: str, *, require_compiled: bool, optional_dependency: str | None = None
) -> dict[str, Any]:
    """Run first-use checks in a fresh interpreter, retaining installed origins."""
    result = _new_smoke_result(name=name, module="sqlspec.base", attribute=None, compiled_required=require_compiled)
    if optional_dependency is not None and importlib.util.find_spec(optional_dependency) is None:
        result["skipped"] = True
        result["skip_reason"] = f"optional dependency missing: {optional_dependency}"
        return result
    footer = """
import json
import sys
from importlib.machinery import EXTENSION_SUFFIXES

modules = {name: sys.modules[name] for name in ("sqlspec.base", "sqlspec.builder._select")}
print(json.dumps({
    "imported": True,
    "compiled": all(module.__file__.endswith(tuple(EXTENSION_SUFFIXES)) for module in modules.values()),
    "origins": {name: module.__file__ for name, module in modules.items()},
}))
"""
    try:
        completed = subprocess.run(
            [sys.executable, "-I", "-c", script + footer], capture_output=True, check=False, text=True, timeout=30
        )
        if completed.returncode != 0:
            result["error"] = (
                f"subprocess failed with return code {completed.returncode}; "
                f"stdout={completed.stdout!r}; stderr={completed.stderr!r}"
            )
            return result
        result.update(json.loads(completed.stdout.splitlines()[-1]))
        if require_compiled and not result["compiled"]:
            result["error"] = "module was imported from Python source, not a compiled extension"
    except (subprocess.TimeoutExpired, ValueError, IndexError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _check_public_import_order(order: str, *, require_compiled: bool) -> dict[str, Any]:
    """Resolve package exports in either order without proxying the defining objects."""
    imports = {
        "facade_first": "from sqlspec import SQLSpec, SQL, QueryBuilder, Select, sql\n",
        "definitions_first": (
            "from sqlspec.base import SQLSpec\n"
            "from sqlspec.core.statement import SQL\n"
            "from sqlspec.builder._base import QueryBuilder\n"
            "from sqlspec.builder._select import Select\n"
            "from sqlspec.builder._factory import sql\n"
        ),
        "builder_first": "from sqlspec.builder import sql, QueryBuilder, Select\nfrom sqlspec import SQLSpec, SQL\n",
        "migration_first": (
            "from sqlspec.migrations import SyncMigrationTracker, SchemaTarget\n"
            "from sqlspec import SQLSpec, SQL, QueryBuilder, Select, sql\n"
        ),
    }
    script = (
        imports[order]
        + """
import importlib
import sqlspec
import sqlspec.builder as builder

for facade in (sqlspec, builder):
    namespace = {}
    exec("from " + facade.__name__ + " import *", namespace)
    for name in facade.__all__:
        assert namespace[name] is getattr(facade, name), name
    assert set(facade.__all__).issubset(dir(facade))
assert SQLSpec is sqlspec.SQLSpec is importlib.import_module("sqlspec.base").SQLSpec
assert SQL is sqlspec.SQL is importlib.import_module("sqlspec.core.statement").SQL
assert QueryBuilder is builder.QueryBuilder is importlib.import_module("sqlspec.builder._base").QueryBuilder
assert Select is sqlspec.Select is builder.Select is importlib.import_module("sqlspec.builder._select").Select
assert sql is sqlspec.sql is builder.sql is importlib.import_module("sqlspec.builder._factory").sql
assert isinstance(sql.select("value"), QueryBuilder)
assert isinstance(sql.select("value"), Select)

from typing import get_type_hints
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.migrations import SchemaEnsureResult

for method in (BaseEventQueueStore.reconcile_schema_sync, BaseEventQueueStore.reconcile_schema_async):
    assert get_type_hints(method)["return"] is SchemaEnsureResult
"""
    )
    return _run_startup_check(f"public_import_order_{order}", script, require_compiled=require_compiled)


def _check_concurrent_public_exports(mode: str, *, require_compiled: bool) -> dict[str, Any]:
    """Resolve mixed facades and direct modules concurrently in a fresh process."""
    script = """
import importlib
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

exports = [
    ("sqlspec", "SQLSpec", "sqlspec.base"),
    ("sqlspec", "SQL", "sqlspec.core.statement"),
    ("sqlspec", "sql", "sqlspec.builder._factory"),
    ("sqlspec", "SyncDatabaseConfig", "sqlspec.config"),
    ("sqlspec", "EventMessage", "sqlspec.extensions.events._models"),
    ("sqlspec", "Select", "sqlspec.builder._select"),
    ("sqlspec", "StatementConfig", "sqlspec.core.statement"),
    ("sqlspec", "SQLFileLoader", "sqlspec.loader"),
    ("sqlspec.builder", "sql", "sqlspec.builder._factory"),
    ("sqlspec.builder", "QueryBuilder", "sqlspec.builder._base"),
    ("sqlspec.builder", "Select", "sqlspec.builder._select"),
    ("sqlspec.migrations", "SyncMigrationTracker", "sqlspec.migrations.tracker"),
    ("sqlspec.migrations", "SyncMigrationCommands", "sqlspec.migrations.commands"),
    ("sqlspec.migrations", "SchemaTarget", "sqlspec.migrations.schema"),
    ("sqlspec.extensions.events", "EventMessage", "sqlspec.extensions.events._models"),
    ("sqlspec.extensions.events", "EventRuntimeHints", "sqlspec.extensions.events._hints"),
    ("sqlspec.extensions.events", "BaseEventQueueStore", "sqlspec.extensions.events._store"),
]
"""
    if mode == "attributes":
        script += """
packages = {package: importlib.import_module(package) for package, _, _ in exports}
barrier = Barrier(len(exports))

def resolve(item):
    package, name, _ = item
    barrier.wait()
    return getattr(packages[package], name)

with ThreadPoolExecutor(max_workers=len(exports)) as pool:
    values = list(pool.map(resolve, exports))
"""
    else:
        script += """
# Initialize every actual parent, including nested packages such as sqlspec.core.
# Racing parent initialization against its child already fails in baseline native wheels.
for package, _, defining_module in exports:
    importlib.import_module(package)
    importlib.import_module(defining_module.rpartition(".")[0])
barrier = Barrier(len(exports))

def resolve(index):
    package, name, defining_module = exports[index]
    barrier.wait()
    module = importlib.import_module(defining_module if index % 2 else package)
    return getattr(module, name)

with ThreadPoolExecutor(max_workers=len(exports)) as pool:
    values = list(pool.map(resolve, range(len(exports))))
"""
    script += """
for (package, name, defining_module), value in zip(exports, values):
    assert value is getattr(importlib.import_module(package), name), (package, name)
    assert value is getattr(importlib.import_module(defining_module), name), (defining_module, name)

from sqlspec import sql
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(connection_config={"database": ":memory:"})
try:
    with config.provide_session() as session:
        assert session.select_value(sql.select("1 AS value")) == 1
finally:
    config.close_pool()
"""
    return _run_startup_check(f"concurrent_public_exports_{mode}", script, require_compiled=require_compiled)


def _check_migration_first_use(*, require_compiled: bool) -> dict[str, Any]:
    """Exercise supported subclasses and real migrations through helper invalidation."""
    script = """
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations import SyncMigrationTracker

tracker_calls = []

class AppTracker(SyncMigrationTracker):
    __slots__ = ()

    def __init__(self, version_table_name="ddl_migrations", version_table_schema=None):
        tracker_calls.append(version_table_name)
        super().__init__(version_table_name, version_table_schema)

class AppConfig(SqliteConfig):
    __slots__ = ()
    migration_tracker_type = AppTracker

class AppSQLSpec(SQLSpec):
    __slots__ = ()

with TemporaryDirectory() as root:
    root = Path(root)
    migrations = root / "migrations"
    migrations.mkdir()
    (migrations / "0001_items.sql").write_text(
        "-- name: migrate-0001-up\\nCREATE TABLE smoke_items (id INTEGER PRIMARY KEY);\\n"
        "-- name: migrate-0001-down\\nDROP TABLE smoke_items;\\n"
    )
    config = AppConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(migrations), "echo": False},
    )
    try:
        assert issubclass(AppSQLSpec, SQLSpec)
        manager = SQLSpec()
        assert manager.add_config(config) is config
        loader = config.get_migration_loader()
        commands = config.get_migration_commands()
        assert isinstance(commands.tracker, AppTracker)
        count = len(tracker_calls)
        assert count == 1
        assert config.get_migration_commands() is commands
        assert len(tracker_calls) == count
        commands.upgrade(echo=False)
        with manager.provide_session(config) as session:
            assert session.select_value("SELECT COUNT(*) FROM smoke_items") == 0
            assert session.select_value("SELECT version_num FROM ddl_migrations") == "0001"
        extra = root / "extra.sql"
        extra.write_text("-- name: extra_probe\\nSELECT 7;\\n")
        config.load_migration_sql_files(extra)
        extension = root / "extension"
        extension.mkdir()
        config.add_extension_migrations("smoke", extension)
        assert config.get_migration_loader() is loader
        refreshed = config.get_migration_commands()
        assert refreshed is not commands
        with config.provide_session() as session:
            assert session.select_value(loader.get_sql("extra_probe")) == 7
        assert config.remove_extension_migrations("smoke")
        assert config.get_migration_loader() is loader
        assert config.get_migration_commands() is not refreshed
        config.set_migration_config({"script_location": str(migrations), "echo": False})
        assert config.get_migration_loader() is not loader
        commands = config.get_migration_commands()
        assert commands.tracker.version_table_name == "ddl_migrations"
        commands.downgrade("base", echo=False)
        with config.provide_session() as session:
            assert session.select_value(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = :name",
                {"name": "smoke_items"},
            ) == 0
    finally:
        config.close_pool()
"""
    return _run_startup_check("migration_first_use", script, require_compiled=require_compiled)


def _check_litestar_openapi(*, require_compiled: bool) -> dict[str, Any]:
    """Resolve public pagination annotations through Litestar's schema generation."""
    script = """
import msgspec
from litestar import Litestar, get

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import OffsetPagination
from sqlspec.extensions.litestar import SQLSpecPlugin

class Item(msgspec.Struct):
    name: str

@get("/items")
async def items() -> OffsetPagination[Item]:
    return OffsetPagination(items=[Item(name="smoke")], limit=10, offset=0, total=1)

config = SqliteConfig(connection_config={"database": ":memory:"})
try:
    manager = SQLSpec()
    manager.add_config(config)
    app = Litestar(route_handlers=[items], plugins=[SQLSpecPlugin(sqlspec=manager)])
    schema = app.openapi_schema
    assert "/items" in schema.paths
    components = schema.components.schemas
    assert "Item" in components
    pagination = next(component for name, component in components.items() if name.startswith("OffsetPagination"))
    assert set(pagination.required) == {"items", "limit", "offset", "total"}
    assert set(pagination.properties) == {"items", "limit", "offset", "total"}
finally:
    config.close_pool()
"""
    return _run_startup_check(
        "litestar_openapi", script, require_compiled=require_compiled, optional_dependency="litestar"
    )


def _check_extension_reconciliation(*, require_compiled: bool) -> dict[str, Any]:
    """Exercise compiled runner reconciliation across built-in Litestar and external callbacks."""
    script = """
import asyncio
import importlib.util
import sys
from importlib.machinery import EXTENSION_SUFFIXES
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlspec import SQLSpec, Select
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations import create_migration_commands

with TemporaryDirectory() as root_dir:
    root = Path(root_dir)
    migrations = root / "migrations"
    migrations.mkdir()
    ext_dir = root / "smoke_ext"
    ext_dir.mkdir()
    (ext_dir / "0001_shipped.sql").write_text(
        "-- name: migrate-0001-up\\nCREATE TABLE smoke_numbered (id INTEGER PRIMARY KEY);\\n"
        "-- name: migrate-0001-down\\nDROP TABLE smoke_numbered;\\n",
        encoding="utf-8",
    )
    (ext_dir / "reconcile.py").write_text(
        "def reconcile(context):\\n"
        "    context.driver.execute_script(\\n"
        "        'CREATE TABLE IF NOT EXISTS smoke_calls (id INTEGER PRIMARY KEY); '\\n"
        "        'INSERT INTO smoke_calls DEFAULT VALUES;'\\n"
        "    )\\n",
        encoding="utf-8",
    )
    config = SqliteConfig(
        connection_config={"database": str(root / "smoke.db")},
        migration_config={"script_location": str(migrations), "echo": False},
        extension_config={"litestar": {"session_table": "smoke_sessions", "migration_mode": "reconcile"}},
    )
    manager = SQLSpec()
    assert manager.add_config(config) is config
    assert config.add_extension_migrations("smoke_ext", ext_dir, migration_mode="reconcile") is None
    try:
        create_migration_commands(config).upgrade(echo=False)
        config.migrate_up(echo=False)
        with manager.provide_session(config) as session:
            assert session.select_value(Select("COUNT(*)").from_("smoke_calls")) == 2
            assert (
                session.select_value(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'smoke_sessions'"
                )
                == 1
            )
            assert (
                session.select_value(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'smoke_numbered'"
                )
                == 0
            )
            if session.select_value(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'ddl_migrations'"
            ):
                assert session.select_value("SELECT COUNT(*) FROM ddl_migrations WHERE version_num LIKE 'ext_%'") == 0
    finally:
        config.close_pool()

    if importlib.util.find_spec("aiosqlite") is not None:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig

        async_migrations = root / "async_migrations"
        async_migrations.mkdir()
        async_ext = root / "smoke_async"
        async_ext.mkdir()
        (async_ext / "reconcile.py").write_text(
            "async def reconcile(context):\\n"
            "    assert context.is_async_driver\\n"
            "    await context.driver.execute_script(\\n"
            "        'CREATE TABLE IF NOT EXISTS smoke_async_calls (id INTEGER PRIMARY KEY)'\\n"
            "    )\\n",
            encoding="utf-8",
        )

        async def run_async_reconcile() -> None:
            async_config = AiosqliteConfig(
                connection_config={"database": str(root / "smoke_async.db")},
                migration_config={"script_location": str(async_migrations), "echo": False},
            )
            assert async_config.add_extension_migrations("smoke_async", async_ext, migration_mode="reconcile") is None
            try:
                await create_migration_commands(async_config).upgrade(echo=False)
                async with async_config.provide_session() as session:
                    assert (
                        await session.select_value(
                            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'smoke_async_calls'"
                        )
                        == 1
                    )
            finally:
                await async_config.close_pool()

        asyncio.run(run_async_reconcile())

    fail_migrations = root / "fail_migrations"
    fail_migrations.mkdir()
    fail_ext = root / "smoke_fail"
    fail_ext.mkdir()
    (fail_ext / "reconcile.py").write_text(
        "def reconcile(context):\\n    raise ValueError('boom')\\n",
        encoding="utf-8",
    )
    fail_config = SqliteConfig(
        connection_config={"database": str(root / "smoke_fail.db")},
        migration_config={"script_location": str(fail_migrations), "echo": False},
    )
    assert fail_config.add_extension_migrations("smoke_fail", fail_ext, migration_mode="reconcile") is None
    try:
        try:
            create_migration_commands(fail_config).upgrade(echo=False)
        except MigrationError as error:
            assert isinstance(error.__cause__, ValueError)
        else:
            raise AssertionError("failing reconciler did not raise MigrationError")
    finally:
        fail_config.close_pool()

base_compiled = sys.modules["sqlspec.base"].__file__.endswith(tuple(EXTENSION_SUFFIXES))
runner_compiled = sys.modules["sqlspec.migrations.runner"].__file__.endswith(tuple(EXTENSION_SUFFIXES))
assert runner_compiled == base_compiled
"""
    return _run_startup_check(
        "extension_reconciliation", script, require_compiled=require_compiled, optional_dependency="litestar"
    )


def _smoke_tenant_lease(leases: Any, strategy: Any, hooks: tuple[Any, Any, Any, Any], *, asynchronous: bool) -> Any:
    """Build an interpreted subclass of a compiled lease that routes its hooks to callables."""
    acquire, release, invalidate, finish = hooks

    class SyncLease(leases.SyncTenantLease):  # type: ignore[misc]
        __slots__ = ()

        def _acquire(self) -> object:
            return acquire()

        def _release(self, conn: object) -> None:
            release(conn)

        def _invalidate(self, conn: object) -> None:
            invalidate(conn)

        def _finish_transaction(self, conn: object, exc_type: type[BaseException] | None) -> None:
            finish(conn, exc_type)

    class AsyncLease(leases.AsyncTenantLease):  # type: ignore[misc]
        __slots__ = ()

        async def _acquire(self) -> object:
            return acquire()

        async def _release(self, conn: object) -> None:
            release(conn)

        async def _invalidate(self, conn: object) -> None:
            invalidate(conn)

        async def _finish_transaction(self, conn: object, exc_type: type[BaseException] | None) -> None:
            finish(conn, exc_type)

    return AsyncLease(strategy) if asynchronous else SyncLease(strategy)


async def _run_tenant_lease_case(
    leases: Any, contracts: Any, context: Any, errors: Any, *, asynchronous: bool, scenario: str
) -> None:
    """Check one strategy and lease-hook ordering scenario."""
    events: list[str] = []
    connection = object()
    setup_error = ValueError("injected checkout failure")
    cleanup_error = RuntimeError("injected reset failure")

    def checkout(conn: object, payload: Any) -> Any:
        assert conn is connection
        events.append("checkout")
        if scenario == "checkout_failure":
            raise setup_error
        return (
            {"owned_key": "baseline"}
            if scenario == "mapping"
            else contracts.TenantLeaseState(payload=payload, baseline={})
        )

    def checkin(conn: object, state: Any) -> None:
        assert conn is connection
        assert state.payload.tenant_id == "a"
        events.append("checkin")
        assert dict(state.baseline) == ({"owned_key": "baseline"} if scenario == "mapping" else {})
        if scenario == "checkin_failure":
            raise cleanup_error

    class SyncStrategy:
        __slots__ = ()

        def checkout(self, conn: object, payload: Any) -> Any:
            return checkout(conn, payload)

        def checkin(self, conn: object, state: Any) -> None:
            checkin(conn, state)

    class AsyncStrategy:
        __slots__ = ()

        async def checkout(self, conn: object, payload: Any) -> Any:
            return checkout(conn, payload)

        async def checkin(self, conn: object, state: Any) -> None:
            checkin(conn, state)

    def acquire() -> object:
        events.append("acquire")
        return connection

    def release(conn: object) -> None:
        assert conn is connection
        events.append("release")

    def invalidate(conn: object) -> None:
        assert conn is connection
        events.append("invalidate")
        if scenario == "checkout_failure":
            raise RuntimeError("injected invalidation failure")

    def finish(conn: object, exc_type: type[BaseException] | None) -> None:
        assert conn is connection
        assert exc_type is None
        events.append("finish")

    lease = _smoke_tenant_lease(
        leases,
        AsyncStrategy() if asynchronous else SyncStrategy(),
        (acquire, release, invalidate, finish),
        asynchronous=asynchronous,
    )
    scope = context.TenantContext.bypass() if scenario == "bypass" else context.TenantContext.scope("a")
    with scope:
        try:
            acquired = lease.acquire()
            if asynchronous:
                acquired = await acquired
        except ValueError as exc:
            assert scenario == "checkout_failure"
            if exc is not setup_error:
                raise AssertionError("Checkout must preserve its original exception") from exc
            if not any("invalidation failed" in note for note in exc.__notes__):
                raise AssertionError("Failed invalidation must add a note to the original exception") from exc
            assert events == ["acquire", "checkout", "invalidate"]
            return
        assert scenario != "checkout_failure", "checkout failure must propagate"
        assert acquired is connection
        try:
            cleanup = lease.release(connection)
            if asynchronous:
                await cleanup
        except errors.TenantCleanupError as exc:
            if scenario != "checkin_failure" or exc.__cause__ is not cleanup_error:
                raise AssertionError("Cleanup must propagate with its original cause") from exc
        else:
            assert scenario != "checkin_failure", "reset failure must propagate"
    expected = (
        ["acquire", "finish", "release"]
        if scenario == "bypass"
        else ["acquire", "checkout", "finish", "checkin", "invalidate" if scenario == "checkin_failure" else "release"]
    )
    assert events == expected, (asynchronous, scenario, events)


def _check_tenant_lease_boundary(*, require_compiled: bool = False) -> dict[str, Any]:
    """Exercise compiled leases through interpreted subclasses and structural Python strategies."""
    result = _new_smoke_result(
        name="tenant_lease_boundary",
        module="sqlspec.driver._tenant",
        attribute="AsyncTenantLease",
        compiled_required=require_compiled,
    )
    try:
        leases = importlib.import_module("sqlspec.driver._tenant")
        contracts = importlib.import_module("sqlspec.core.tenant.base")
        context = importlib.import_module("sqlspec.utils.tenant._context")
        errors = importlib.import_module("sqlspec.exceptions")
        result["imported"] = True
        result["compiled"] = is_compiled_module(leases) and is_compiled_module(context)
        if require_compiled and not result["compiled"]:
            raise AssertionError("tenant leases and tenant context must import from compiled extensions")

        async def run_cases() -> None:
            for asynchronous in (False, True):
                for scenario in ("normal", "mapping", "bypass", "checkin_failure", "checkout_failure"):
                    await _run_tenant_lease_case(
                        leases, contracts, context, errors, asynchronous=asynchronous, scenario=scenario
                    )

        asyncio.run(run_cases())
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _check_oracle_26ai_boundary(*, require_compiled: bool = False) -> dict[str, Any]:
    """Exercise Oracle native helpers through interpreted config and SDK boundaries."""
    result = _new_smoke_result(
        name="oracle_26ai_boundary",
        module="sqlspec.adapters.oracledb.core",
        attribute="OracleAcquireOptions",
        compiled_required=require_compiled,
    )
    try:
        sdk = importlib.import_module("oracledb")
    except ImportError:
        result.update(skipped=True, skip_reason="oracledb not installed")
        return result
    try:
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, MagicMock, Mock

        from sqlspec.adapters.oracledb import OracleAsyncClientContextStrategy, OracleSyncClientContextStrategy

        oracle = importlib.import_module("sqlspec.adapters.oracledb")
        core = importlib.import_module("sqlspec.adapters.oracledb.core")
        tenant = importlib.import_module("sqlspec.adapters.oracledb.tenant")
        common = importlib.import_module("sqlspec.observability._common")
        contracts = importlib.import_module("sqlspec.utils.tenant")
        errors = importlib.import_module("sqlspec.exceptions")
        result["imported"] = True
        result["compiled"] = all(is_compiled_module(module) for module in (core, tenant, common))
        if require_compiled and not result["compiled"]:
            raise AssertionError("Oracle core, tenant strategies and observability helpers must be native")
        for config_type in (oracle.OracleSyncConfig, oracle.OracleAsyncConfig):
            namespace = config_type().get_signature_namespace()
            assert (
                namespace["OracleSyncSessionlessTransactionContext"] is oracle.OracleSyncSessionlessTransactionContext
            )
            assert (
                namespace["OracleAsyncSessionlessTransactionContext"] is oracle.OracleAsyncSessionlessTransactionContext
            )
        options = oracle.OracleAcquireOptions(sharding_key=("tenant",), purity="SELF")
        assert options.sharding_key == ["tenant"]
        assert options == core.OracleAcquireOptions(sharding_key=["tenant"], purity="SELF")
        assert core.synthesize_session_state_tag(core.OracleSessionContextConfig(time_zone="UTC")) == "TIME_ZONE=UTC"
        assert common.get_w3c_trace_context() == (None, "")
        if importlib.util.find_spec("opentelemetry") is not None:
            from opentelemetry import trace
            from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

            propagated = SpanContext(trace_id=1, span_id=2, is_remote=True, trace_flags=TraceFlags(0))
            with trace.use_span(NonRecordingSpan(propagated)):
                traceparent, _ = common.get_w3c_trace_context()
                assert traceparent is not None
                assert traceparent.endswith("-00")

        class FailedSyncStrategy(OracleSyncClientContextStrategy):
            __slots__ = ()

            def checkin(self, connection: Any, state: Any) -> None:
                raise RuntimeError("cleanup failed")

        class FailedAsyncStrategy(OracleAsyncClientContextStrategy):
            __slots__ = ()

            async def checkin(self, connection: Any, state: Any) -> None:
                raise RuntimeError("cleanup failed")

        async def cases() -> None:
            for asynchronous in (False, True):
                native = AsyncMock if asynchronous else Mock
                for failure in (False, True):
                    connection = SimpleNamespace(
                        thin=True,
                        version="23.26.3",
                        transaction_priority=sdk.TransactionPriority.HIGH,
                        set_app_context=Mock(),
                        clear_app_context=Mock(),
                        rollback=native(),
                        begin_sessionless_transaction=native(return_value=b"txn"),
                        suspend_sessionless_transaction=native(),
                        transaction_in_progress=False,
                        run_pipeline=None,
                    )
                    pool = MagicMock()
                    pool.thin = True
                    pool.acquire = native(return_value=connection)
                    pool.release, pool.drop = native(), native()
                    if asynchronous:
                        strategy = FailedAsyncStrategy() if failure else oracle.OracleAsyncClientContextStrategy()
                        config = oracle.OracleAsyncConfig(
                            connection_instance=pool, driver_features={"tenant_strategy": strategy}
                        )
                    else:
                        strategy = FailedSyncStrategy() if failure else oracle.OracleSyncClientContextStrategy()
                        config = oracle.OracleSyncConfig(
                            connection_instance=pool, driver_features={"tenant_strategy": strategy}
                        )
                    failed = False
                    with contracts.TenantContext.scope("tenant"):
                        try:
                            if asynchronous:
                                async with (
                                    config.provide_session() as session,
                                    session.sessionless_transaction() as transaction_id,
                                ):
                                    assert transaction_id == b"txn"
                            else:
                                with (
                                    config.provide_session() as session,
                                    session.sessionless_transaction() as transaction_id,
                                ):
                                    assert transaction_id == b"txn"
                        except errors.TenantCleanupError:
                            failed = True
                    assert failed is failure
                    if failure:
                        pool.drop.assert_called_once_with(connection)
                        pool.release.assert_not_called()
                    else:
                        pool.release.assert_called_once_with(connection)
                        connection.clear_app_context.assert_called_once_with("CLIENTCONTEXT")
                    connection.suspend_sessionless_transaction.assert_called_once_with()

        asyncio.run(cases())
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _check_session_routing_boundary(*, require_compiled: bool = False) -> dict[str, Any]:
    """Exercise compiled classification with interpreted routing facades and native WAL readers."""
    result = _new_smoke_result(
        name="session_routing_boundary",
        module="sqlspec.core.routing",
        attribute="ReplicaSpec",
        compiled_required=require_compiled,
    )
    expected_count = 2
    try:
        core = importlib.import_module("sqlspec.core.routing")
        errors = importlib.import_module("sqlspec.exceptions")
        registry_module = importlib.import_module("sqlspec.base")
        context = importlib.import_module("sqlspec.utils.routing")
        statements = importlib.import_module("sqlspec.core.statement")
        services = importlib.import_module("sqlspec.service")
        sqlite = importlib.import_module("sqlspec.adapters.sqlite")
        result["imported"] = True
        result["compiled"] = is_compiled_module(core)
        if require_compiled:
            assert is_compiled_module(core)
            assert is_compiled_module(errors)
            for module_name in (
                "sqlspec.utils.routing",
                "sqlspec.driver._routing",
                "sqlspec.extensions._deferred_session",
                "sqlspec.service._base",
            ):
                assert not is_compiled_module(importlib.import_module(module_name)), module_name
        assert statements.SQL("SELECT 1").is_read_only()
        assert not statements.SQL("SELECT nextval('sequence')", dialect="postgres").is_read_only()
        with tempfile.TemporaryDirectory() as directory:
            config = sqlite.SqliteConfig(
                connection_config={"database": str(Path(directory) / "routing.db")}, replicas=True
            )
            registry = registry_module.SQLSpec()
            registry.add_config(config)
            try:
                with registry.provide_session(config) as writer:
                    writer.execute("CREATE TABLE entries (id INTEGER PRIMARY KEY)")
                    writer.execute("INSERT INTO entries VALUES (1)")
                    writer.commit()
                with registry.provide_read_session(config) as reader:
                    assert reader.select_value("SELECT COUNT(*) FROM entries") == 1
                    try:
                        reader.execute("INSERT INTO entries VALUES (2)")
                    except errors.ReadOnlyTransactionError:
                        pass
                    else:
                        raise AssertionError("Read provider accepted a write")
                with context.RoutingContext.scope(), registry.provide_routing_session(config) as routing:
                    assert routing.select_value("SELECT COUNT(*) FROM entries") == 1
                    routing.execute("INSERT INTO entries VALUES (2)")
                    routing.commit()
                    assert routing.select_value("SELECT COUNT(*) FROM entries") == expected_count
                service = services.SQLSpecSyncService(config=config)
                with service.provide_session(read_only=True) as reader:
                    assert reader.select_value("SELECT COUNT(*) FROM entries") == expected_count
            finally:
                config.close_pool()
            if importlib.util.find_spec("aiosqlite") is not None:
                asynchronous = importlib.import_module("sqlspec.adapters.aiosqlite")

                async def run_async() -> None:
                    config = asynchronous.AiosqliteConfig(
                        connection_config={"database": str(Path(directory) / "async.db")}, replicas=True
                    )
                    registry = registry_module.SQLSpec()
                    registry.add_config(config)
                    try:
                        async with registry.provide_session(config) as writer:
                            await writer.execute("CREATE TABLE entries (id INTEGER PRIMARY KEY)")
                            await writer.execute("INSERT INTO entries VALUES (1)")
                            await writer.commit()
                        async with context.RoutingContext.scope(), registry.provide_routing_session(config) as routing:
                            assert await routing.select_value("SELECT COUNT(*) FROM entries") == 1
                            async with routing.select_stream("SELECT id FROM entries") as stream:
                                assert [row async for row in stream] == [{"id": 1}]
                            await routing.execute("INSERT INTO entries VALUES (2)")
                            await routing.commit()
                            assert await routing.select_value("SELECT COUNT(*) FROM entries") == expected_count
                        service = services.SQLSpecAsyncService(config=config)
                        async with service.provide_session(read_only=True) as reader:
                            assert await reader.select_value("SELECT COUNT(*) FROM entries") == expected_count
                    finally:
                        await config.close_pool()

                asyncio.run(run_async())
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _failed_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        result
        for result in results
        if not result["skipped"] and (result["error"] is not None or not result["imported"])
    ]


def _format_text(results: list[dict[str, Any]]) -> str:
    lines = ["mypyc wheel smoke results:"]
    for result in results:
        if result["skipped"]:
            lines.append(f"- SKIP {result['module']} ({result['skip_reason']})")
            continue
        status = "OK" if result["error"] is None and result["imported"] else "FAIL"
        compiled = "compiled" if result["compiled"] else "interpreted"
        required = " required" if result["compiled_required"] else ""
        lines.append(f"- {status} {result['module']} ({compiled}{required})")
        lines.extend(f"- SKIP {adapter}" for adapter in result.get("skipped_adapters", []))
        if result["error"] is not None:
            lines.append(f"  {result['error']}")
    return "\n".join(lines)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
