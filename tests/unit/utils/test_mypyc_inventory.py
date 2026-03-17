"""Tests for mypyc inventory reporting."""

import importlib.util
from pathlib import Path
from types import ModuleType


def _load_mypyc_inventory_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[3] / "tools" / "scripts" / "mypyc_inventory.py"
    spec = importlib.util.spec_from_file_location("mypyc_inventory_for_tests", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_inventory_reports_current_compiled_surface() -> None:
    module = _load_mypyc_inventory_module()

    inventory = module.build_inventory()
    project_root = Path(__file__).resolve().parents[3]
    include_patterns, exclude_patterns = module.load_mypyc_patterns(project_root)
    modules = module.list_sqlspec_modules(project_root)
    compiled = [
        path for path in modules if module.classify_module(path, include_patterns, exclude_patterns) == "compiled"
    ]
    interpreted = [
        path for path in modules if module.classify_module(path, include_patterns, exclude_patterns) == "interpreted"
    ]

    assert inventory["summary"] == {
        "compiled_count": len(compiled),
        "interpreted_count": len(interpreted),
        "total_modules": len(modules),
    }
    assert inventory["compiled_modules"] == compiled
    assert inventory["interpreted_modules"] == interpreted

    hot_surfaces = inventory["hot_surfaces"]
    assert hot_surfaces["sqlspec/config.py"]["status"] == "interpreted"
    assert hot_surfaces["sqlspec/config.py"]["classification"] == "helper_split_first"
    assert hot_surfaces["sqlspec/base.py"]["status"] == "interpreted"
    assert hot_surfaces["sqlspec/base.py"]["classification"] == "helper_split_first"
    assert hot_surfaces["sqlspec/_serialization.py"]["status"] == "interpreted"
    assert hot_surfaces["sqlspec/_serialization.py"]["classification"] == "prove_separately"
    assert hot_surfaces["sqlspec/storage/pipeline.py"]["status"] == "interpreted"
    assert hot_surfaces["sqlspec/storage/pipeline.py"]["classification"] == "helper_split_first"
    assert hot_surfaces["sqlspec/storage/registry.py"]["status"] == "compiled"
    assert hot_surfaces["sqlspec/storage/errors.py"]["status"] == "compiled"
    assert hot_surfaces["sqlspec/storage/_utils.py"]["status"] == "interpreted"
    assert hot_surfaces["sqlspec/utils/module_loader.py"]["status"] == "interpreted"
    assert hot_surfaces["sqlspec/utils/arrow_helpers.py"]["status"] == "interpreted"


def test_build_inventory_preserves_known_exclusions() -> None:
    module = _load_mypyc_inventory_module()

    inventory = module.build_inventory()

    assert inventory["preserved_exclusions"] == [
        "sqlspec/adapters/**/data_dictionary.py",
        "sqlspec/config.py",
        "sqlspec/data_dictionary/_loader.py",
        "sqlspec/dialects/postgres/_paradedb.py",
        "sqlspec/dialects/postgres/_pgvector.py",
        "sqlspec/dialects/spanner/_spangres.py",
        "sqlspec/dialects/spanner/_spanner.py",
        "sqlspec/migrations/commands.py",
        "sqlspec/observability/_formatting.py",
        "sqlspec/utils/arrow_helpers.py",
    ]


def test_build_inventory_summarizes_adapter_shells() -> None:
    module = _load_mypyc_inventory_module()

    inventory = module.build_inventory()

    assert inventory["adapter_config_shells"]["count"] > 0
    assert inventory["adapter_config_shells"]["status"] == "interpreted"
    assert inventory["adapter_config_shells"]["classification"] == "helper_split_first"
    assert inventory["adapter_core_helpers"]["count"] > 0
    assert inventory["adapter_core_helpers"]["status"] == "compiled"
    assert inventory["adapter_core_helpers"]["classification"] == "compile_now"
