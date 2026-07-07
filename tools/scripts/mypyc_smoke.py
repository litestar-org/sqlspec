"""Smoke-test imports for mypyc-built wheels."""

import argparse
import importlib
import json
import sys
from collections.abc import Sequence
from typing import Any, NamedTuple

__all__ = ("SMOKE_IMPORTS", "SmokeImport", "is_compiled_module", "main", "run_construction_checks", "run_smoke")

COMPILED_SUFFIXES = (".so", ".pyd")


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
    SmokeImport("adk_record_types", "sqlspec.extensions.adk._types", "SessionRecord", True, "google.adk"),
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


def run_construction_checks(*, require_compiled: bool = False) -> list[dict[str, Any]]:
    """Run construction-time smoke checks for provider classes."""
    return [
        _check_sqlspec_construction(),
        _check_statement_sentinel_identity(require_compiled=require_compiled),
        _check_statement_cache_rebind(require_compiled=require_compiled),
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
