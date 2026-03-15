"""Inventory the current mypyc compiled vs interpreted module surface."""

from fnmatch import fnmatch
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

__all__ = (
    "HOT_SURFACE_CLASSIFICATIONS",
    "build_inventory",
    "classify_module",
    "list_sqlspec_modules",
    "load_mypyc_patterns",
)


HOT_SURFACE_CLASSIFICATIONS: dict[str, dict[str, str]] = {
    "sqlspec/config.py": {
        "classification": "helper_split_first",
        "reason": "Owns runtime hooks, migration setup, and observability/bootstrap orchestration.",
    },
    "sqlspec/base.py": {
        "classification": "helper_split_first",
        "reason": "Registry/session wrappers still manage runtime pool and telemetry orchestration.",
    },
    "sqlspec/_serialization.py": {
        "classification": "prove_separately",
        "reason": "Serializer selection remains dynamic and historically broke same-unit coercion paths.",
    },
    "sqlspec/storage/pipeline.py": {
        "classification": "helper_split_first",
        "reason": "Most orchestration is safe, but Arrow encode/decode helpers still cross interpreted boundaries.",
    },
    "sqlspec/storage/registry.py": {
        "classification": "compile_now",
        "reason": "Pure routing/cache logic with backend selection only.",
    },
    "sqlspec/storage/errors.py": {
        "classification": "compile_now",
        "reason": "Storage error normalization is typed runtime logic with no Arrow dependence.",
    },
    "sqlspec/storage/_utils.py": {
        "classification": "helper_split_first",
        "reason": "Path resolution is safe, but the same module owns dynamic PyArrow import shims.",
    },
    "sqlspec/utils/module_loader.py": {
        "classification": "keep_interpreted",
        "reason": "Heavy dynamic import and optional dependency probing surface.",
    },
    "sqlspec/utils/serializers.py": {
        "classification": "compile_now",
        "reason": "Already part of the compiled utility path and performance sensitive.",
    },
    "sqlspec/utils/sync_tools.py": {
        "classification": "compile_now",
        "reason": "Hot async bridge helpers are already in the include set.",
    },
    "sqlspec/utils/schema.py": {
        "classification": "compile_now",
        "reason": "Core schema conversion path is already compiled and actively optimized.",
    },
    "sqlspec/utils/type_converters.py": {
        "classification": "compile_now",
        "reason": "Compiled adapter coercion helpers are on the hot path.",
    },
    "sqlspec/storage/backends/base.py": {
        "classification": "compile_now",
        "reason": "Mypyc-safe runtime base classes and iterator wrappers.",
    },
    "sqlspec/utils/arrow_helpers.py": {
        "classification": "keep_interpreted",
        "reason": "Direct PyArrow boundary with historical mypyc segfault risk.",
    },
}


def load_mypyc_patterns(root: Path) -> tuple[list[str], list[str]]:
    """Load mypyc include/exclude glob patterns from pyproject.toml."""

    config = tomllib.loads((root / "pyproject.toml").read_text())
    mypyc_config = config["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]["mypyc"]
    return list(mypyc_config["include"]), list(mypyc_config["exclude"])


def list_sqlspec_modules(root: Path) -> list[str]:
    """Return all Python module paths under sqlspec/."""

    return sorted(str(path.relative_to(root)).replace("\\", "/") for path in (root / "sqlspec").rglob("*.py"))


def classify_module(module_path: str, include_patterns: list[str], exclude_patterns: list[str]) -> str:
    """Return whether a module is currently compiled or interpreted."""

    included = any(fnmatch(module_path, pattern) for pattern in include_patterns)
    excluded = any(fnmatch(module_path, pattern) for pattern in exclude_patterns)
    return "compiled" if included and not excluded else "interpreted"


def build_inventory(root: Path | None = None) -> dict[str, Any]:
    """Build the current module inventory and hot-surface classification."""

    project_root = root or Path(__file__).resolve().parents[2]
    include_patterns, exclude_patterns = load_mypyc_patterns(project_root)
    modules = list_sqlspec_modules(project_root)

    compiled: list[str] = []
    interpreted: list[str] = []
    for module in modules:
        if classify_module(module, include_patterns, exclude_patterns) == "compiled":
            compiled.append(module)
        else:
            interpreted.append(module)

    hot_surfaces: dict[str, dict[str, str]] = {}
    for module_path, details in HOT_SURFACE_CLASSIFICATIONS.items():
        hot_surfaces[module_path] = {
            "status": classify_module(module_path, include_patterns, exclude_patterns),
            "classification": details["classification"],
            "reason": details["reason"],
        }

    adapter_configs = sorted(
        module for module in modules if module.startswith("sqlspec/adapters/") and module.endswith("/config.py")
    )
    adapter_cores = sorted(
        module for module in modules if module.startswith("sqlspec/adapters/") and module.endswith("/core.py")
    )

    return {
        "summary": {
            "compiled_count": len(compiled),
            "interpreted_count": len(interpreted),
            "total_modules": len(modules),
        },
        "compiled_modules": compiled,
        "interpreted_modules": interpreted,
        "adapter_config_shells": {
            "count": len(adapter_configs),
            "modules": adapter_configs,
            "status": "interpreted",
            "classification": "helper_split_first",
        },
        "adapter_core_helpers": {
            "count": len(adapter_cores),
            "modules": adapter_cores,
            "status": "compiled",
            "classification": "compile_now",
        },
        "preserved_exclusions": sorted(
            pattern
            for pattern in exclude_patterns
            if pattern
            in {
                "sqlspec/dialects/**",
                "sqlspec/utils/arrow_helpers.py",
                "sqlspec/builder/_vector_expressions.py",
                "sqlspec/data_dictionary/_loader.py",
                "sqlspec/adapters/**/data_dictionary.py",
                "sqlspec/observability/_formatting.py",
                "sqlspec/migrations/commands.py",
                "sqlspec/config.py",
            }
        ),
        "hot_surfaces": hot_surfaces,
    }


if __name__ == "__main__":  # pragma: no cover
    pass
