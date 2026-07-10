"""Import-time boundaries for opt-in framework and observability extensions."""

import subprocess
import sys

import pytest


def _run_import_probe(script: str) -> "subprocess.CompletedProcess[str]":
    return subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


def test_public_trace_export_resolves_real_opentelemetry_module() -> None:
    """Direct public access should return the installed OTel trace module, not its shim."""
    script = """
import importlib.util

if importlib.util.find_spec("opentelemetry.trace") is None:
    print("missing")
else:
    import sqlspec.typing as sqlspec_typing
    resolved = sqlspec_typing.trace
    from opentelemetry import trace
    assert resolved is trace
    print("ok")
"""
    result = _run_import_probe(script)
    if result.stdout.strip() == "missing":
        pytest.skip("opentelemetry is not installed")
    assert result.stdout.strip() == "ok"


@pytest.mark.parametrize(
    ("extension_module", "dependency_module", "symbols"),
    [
        ("sqlspec.extensions.otel", "opentelemetry", ("trace",)),
        ("sqlspec.extensions.prometheus._observer", "prometheus_client", ("Counter", "Histogram")),
    ],
)
def test_observability_extensions_resolve_real_symbols_once(
    extension_module: str, dependency_module: str, symbols: "tuple[str, ...]"
) -> None:
    """Opt-in observability imports should resolve and cache real dependency symbols."""
    script = f"""
import importlib
import importlib.util
import sys

if importlib.util.find_spec({dependency_module!r}) is None:
    print("missing")
else:
    import sqlspec
    assert {dependency_module!r} not in sys.modules
    extension = importlib.import_module({extension_module!r})
    dependency = importlib.import_module({dependency_module!r})
    dependency_identity = sys.modules[{dependency_module!r}]
    for symbol in {symbols!r}:
        assert getattr(extension, symbol) is getattr(dependency, symbol)
    assert importlib.import_module({extension_module!r}) is extension
    assert sys.modules[{dependency_module!r}] is dependency_identity
    print("ok")
"""
    result = _run_import_probe(script)
    if result.stdout.strip() == "missing":
        pytest.skip(f"{dependency_module} is not installed")
    assert result.stdout.strip() == "ok"


def test_litestar_extension_import_is_outside_bare_import_contract() -> None:
    """Framework extensions may import their dependency after explicit opt-in."""
    script = """
import importlib
import importlib.util
import sys

if importlib.util.find_spec("litestar") is None:
    print("missing")
else:
    import sqlspec
    assert "litestar" not in sys.modules
    importlib.import_module("sqlspec.extensions.litestar")
    assert "litestar" in sys.modules
    print("ok")
"""
    result = _run_import_probe(script)
    if result.stdout.strip() == "missing":
        pytest.skip("litestar is not installed")
    assert result.stdout.strip() == "ok"
