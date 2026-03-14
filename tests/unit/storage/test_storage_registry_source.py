"""Source-level regressions for storage registry hot paths.

These tests load the Python source module directly so they remain stable even
when a stale compiled extension exists in the workspace.
"""

import importlib.util
from pathlib import Path


def _load_registry_source_module():
    module_path = Path(__file__).resolve().parents[3] / "sqlspec" / "storage" / "registry.py"
    spec = importlib.util.spec_from_file_location("storage_registry_source_tests", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_source_registry_clear_cache_removes_parameterized_uri_entries(tmp_path: Path) -> None:
    """Source implementation should evict backend-override cache entries by base URI."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()

    backend1 = registry.get(f"file://{tmp_path}", backend="local")
    registry.clear_cache(f"file://{tmp_path}")
    backend2 = registry.get(f"file://{tmp_path}", backend="local")

    assert backend1 is not backend2
