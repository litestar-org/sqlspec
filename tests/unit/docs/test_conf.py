"""Regression tests for Sphinx docs configuration."""

import importlib.util
from pathlib import Path


def _load_docs_conf() -> object:
    docs_conf_path = Path(__file__).resolve().parents[3] / "docs" / "conf.py"
    spec = importlib.util.spec_from_file_location("sqlspec_docs_conf", docs_conf_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_docs_conf_disables_smartquotes() -> None:
    """Rendered examples should preserve straight ASCII quotes."""
    conf = _load_docs_conf()

    assert getattr(conf, "smartquotes", None) is False
