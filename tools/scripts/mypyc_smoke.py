"""Smoke-test imports for mypyc-built wheels."""

import argparse
import importlib
import importlib.machinery
import importlib.util
import inspect
import json
import subprocess
import sys
import tempfile
from collections.abc import Sequence
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
    SmokeImport("package", "sqlspec"),
    SmokeImport("base_sqlspec", "sqlspec.base", "SQLSpec", True),
    SmokeImport("prometheus_observer", "sqlspec.extensions.prometheus._observer", "PrometheusStatementObserver", True),
    SmokeImport("async_bridge", "sqlspec.utils.sync_tools", "async_", True),
    SmokeImport("core_statement", "sqlspec.core.statement", "SQL", True),
    SmokeImport("builder_select", "sqlspec.builder._select", "Select", True),
    SmokeImport("env_utils", "sqlspec.utils.env", "get_env", True),
    SmokeImport("sync_driver", "sqlspec.driver._sync", "SyncDriverAdapterBase", True),
    SmokeImport("async_driver", "sqlspec.driver._async", "AsyncDriverAdapterBase", True),
    SmokeImport("storage_registry", "sqlspec.storage.registry", "StorageRegistry", True),
    SmokeImport("storage_pipeline", "sqlspec.storage.pipeline", "SyncStoragePipeline", True),
    SmokeImport("storage_backend_local", "sqlspec.storage.backends.local", "LocalStore", True),
    SmokeImport("storage_backend_fsspec", "sqlspec.storage.backends.fsspec", "FSSpecBackend", True),
    SmokeImport("storage_backend_obstore", "sqlspec.storage.backends.obstore", "ObStoreBackend", True),
    SmokeImport("sqlite_pool", "sqlspec.adapters.sqlite.pool", "SqliteConnectionPool", True),
    SmokeImport("data_dictionary_registry", "sqlspec.data_dictionary._registry", "get_dialect_config", True),
    SmokeImport("data_dictionary_loader", "sqlspec.data_dictionary._loader", "DataDictionaryLoader", True),
    SmokeImport("pgvector_dialect", "sqlspec.dialects.postgres._pgvector", "PGVector"),
    SmokeImport("spanner_dialect", "sqlspec.dialects.spanner._spanner", "Spanner"),
    SmokeImport("fastapi_providers", "sqlspec.extensions.fastapi.providers", "provide_filters", True, "fastapi"),
    SmokeImport(
        "litestar_providers", "sqlspec.extensions.litestar.providers", "create_filter_dependencies", True, "litestar"
    ),
    SmokeImport("event_payload", "sqlspec.extensions.events._payload", "encode_notify_payload", True),
    SmokeImport("event_channel", "sqlspec.extensions.events._channel", "SyncEventChannel", True),
    SmokeImport("event_queue", "sqlspec.extensions.events._queue", "SyncTableEventQueue", True),
    SmokeImport("adk_record_types", "sqlspec.extensions.adk._types", "StoredSession", True, "google.adk"),
    SmokeImport("migration_runner", "sqlspec.migrations.runner", "SyncMigrationRunner", True),
    SmokeImport("sqlite_type_converter", "sqlspec.adapters.sqlite.type_converter", "register_type_handlers", True),
)


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


def is_compiled_module(module: Any) -> bool:
    """Return whether an imported module appears to be a compiled extension."""
    module_file = getattr(module, "__file__", "") or ""
    return module_file.endswith(COMPILED_SUFFIXES)


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
                result["skipped"] = True
                result["skip_reason"] = f"optional dependency missing: {optional_dependency}"
            else:
                result["error"] = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
        results.append(result)
    return results


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
                "mssql_python",
                "mysql",
                "oracledb",
                "psqlpy",
                "psycopg",
                "psycopg_pool",
                "pymssql",
                "pymysql",
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

    def provide_session(self, session=None):
        self.calls += 1
        return super().provide_session(session)


class SyncService(AppSyncService):
    __slots__ = ()


class AppAsyncService(SQLSpecAsyncService[AiosqliteDriver]):
    __slots__ = ("calls",)

    def __init__(self, config):
        super().__init__(config=config)
        self.calls = 0

    def provide_session(self, session=None):
        self.calls += 1
        return super().provide_session(session)


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
        _check_litestar_openapi(require_compiled=require_compiled),
        _check_sqlspec_construction(),
        _check_adapter_config_construction(),
        _check_statement_sentinel_identity(require_compiled=require_compiled),
        _check_statement_cache_rebind(require_compiled=require_compiled),
        _check_service_subclasses(require_compiled=require_compiled),
        _check_aiosqlite_ambient_exception(require_compiled=require_compiled),
        _check_aiosqlite_exception_mapping(require_compiled=require_compiled),
        _check_fastapi_filter_construction(require_compiled=require_compiled),
        _check_litestar_filter_construction(require_compiled=require_compiled),
    ]


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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
