"""Tests for the compiled-wheel smoke matrix."""

import importlib.util
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

from pytest import MonkeyPatch

try:
    import tomllib  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _load_mypyc_smoke_module() -> ModuleType:
    module_path = PROJECT_ROOT / "tools" / "scripts" / "mypyc_smoke.py"
    spec = importlib.util.spec_from_file_location("mypyc_smoke_for_tests", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_mypyc_include_paths() -> set[str]:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    include_patterns: Sequence[str] = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]["mypyc"][
        "include"
    ]
    paths: set[str] = set()
    for pattern in include_patterns:
        if any(marker in pattern for marker in "*?["):
            paths.update(path.relative_to(PROJECT_ROOT).as_posix() for path in PROJECT_ROOT.glob(pattern))
        else:
            paths.add(pattern)
    return paths


def test_smoke_matrix_covers_compiled_wheel_import_surfaces() -> None:
    module = _load_mypyc_smoke_module()

    smoke_names = {entry.name for entry in module.SMOKE_IMPORTS}

    assert {
        "package",
        "async_bridge",
        "core_statement",
        "builder_select",
        "env_utils",
        "sync_driver",
        "async_driver",
        "storage_registry",
        "data_dictionary_registry",
        "sqlite_type_converter",
    }.issubset(smoke_names)

    compiled_required = {entry.name for entry in module.SMOKE_IMPORTS if entry.require_compiled}
    assert compiled_required == {
        "async_driver",
        "adk_record_types",
        "async_bridge",
        "builder_select",
        "core_statement",
        "data_dictionary_loader",
        "data_dictionary_registry",
        "env_utils",
        "event_payload",
        "event_queue",
        "migration_runner",
        "sqlite_pool",
        "sqlite_type_converter",
        "storage_registry",
        "storage_pipeline",
        "sync_driver",
    }


def test_compiled_smoke_requirements_are_in_mypyc_include_list() -> None:
    module = _load_mypyc_smoke_module()
    included_paths = _load_mypyc_include_paths()

    missing = [entry.module.replace(".", "/") + ".py" for entry in module.SMOKE_IMPORTS if entry.require_compiled]
    missing = [path for path in missing if path not in included_paths]

    assert missing == []


def test_smoke_runner_imports_matrix_without_requiring_compilation() -> None:
    module = _load_mypyc_smoke_module()

    results = module.run_smoke(require_compiled=False)

    assert all(result["imported"] or result["skipped"] for result in results)
    assert any(result["module"] == "sqlspec.driver._sync" for result in results)
    assert any(result["module"] == "sqlspec.adapters.sqlite.type_converter" for result in results)
    assert any(result["module"] == "sqlspec.storage.pipeline" for result in results)
    assert any(result["module"] == "sqlspec.migrations.runner" for result in results)
    assert any(result["module"] == "sqlspec.utils.env" for result in results)
    assert any(result["module"] == "sqlspec.utils.sync_tools" for result in results)


def test_construction_checks_build_provider_signatures_without_requiring_compilation() -> None:
    module = _load_mypyc_smoke_module()

    results = module.run_construction_checks(require_compiled=False)

    assert all(result["imported"] or result["skipped"] for result in results)
    assert {result["name"] for result in results} == {"fastapi_filter_construction", "litestar_filter_construction"}


def test_smoke_runner_skips_optional_adk_dependency(monkeypatch: MonkeyPatch) -> None:
    module = _load_mypyc_smoke_module()
    monkeypatch.setattr(
        module,
        "SMOKE_IMPORTS",
        (module.SmokeImport("adk_record_types", "sqlspec.extensions.adk._types", "SessionRecord", True, "google.adk"),),
    )

    def import_missing_optional_dependency(name: str) -> ModuleType:
        raise ModuleNotFoundError("No module named 'google.adk'", name="google.adk")

    monkeypatch.setattr(module.importlib, "import_module", import_missing_optional_dependency)

    results = module.run_smoke(require_compiled=True)

    assert results == [
        {
            "name": "adk_record_types",
            "module": "sqlspec.extensions.adk._types",
            "attribute": "SessionRecord",
            "imported": False,
            "compiled": False,
            "compiled_required": True,
            "error": None,
            "skipped": True,
            "skip_reason": "optional dependency missing: google.adk",
        }
    ]
    assert module._failed_results(results) == []


def test_smoke_runner_skips_missing_optional_parent_package(monkeypatch: MonkeyPatch) -> None:
    module = _load_mypyc_smoke_module()
    monkeypatch.setattr(
        module,
        "SMOKE_IMPORTS",
        (module.SmokeImport("adk_record_types", "sqlspec.extensions.adk._types", "SessionRecord", True, "google.adk"),),
    )

    def import_missing_optional_parent(name: str) -> ModuleType:
        raise ModuleNotFoundError("No module named 'google'", name="google")

    monkeypatch.setattr(module.importlib, "import_module", import_missing_optional_parent)

    results = module.run_smoke(require_compiled=True)

    assert results[0]["skipped"] is True
    assert results[0]["error"] is None
    assert results[0]["skip_reason"] == "optional dependency missing: google.adk"
    assert module._failed_results(results) == []
