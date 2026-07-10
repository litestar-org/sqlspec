"""Regression tests for import-time optional dependency leakage."""

from __future__ import annotations

import subprocess
import sys

import pytest

FORBIDDEN = ("pandas", "polars", "pyarrow", "litestar", "pydantic", "prometheus_client")


def test_import_sqlspec_does_not_import_heavy_optional_deps() -> None:
    """Importing ``sqlspec`` in a fresh subprocess should not pull heavy optional deps."""
    script = (
        f"import sys, sqlspec; leaked=[name for name in {FORBIDDEN!r} if name in sys.modules]; print(','.join(leaked))"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    leaked = [name for name in result.stdout.strip().split(",") if name]
    assert not leaked, f"import sqlspec eagerly imported: {leaked}"


@pytest.mark.skipif(any(name in sys.modules for name in FORBIDDEN), reason="forbidden deps already preloaded")
def test_import_sqlspec_does_not_import_heavy_optional_deps_in_process() -> None:
    """Same-process import should also stay clean when the environment is pristine."""
    import sqlspec  # noqa: F401

    leaked = [name for name in FORBIDDEN if name in sys.modules]
    assert not leaked, f"import sqlspec eagerly imported: {leaked}"
