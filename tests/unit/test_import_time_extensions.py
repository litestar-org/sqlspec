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


@pytest.mark.parametrize(
    "helper_name",
    [
        "resolve_adk_config",
        "resolve_adk_store_class",
        "resolve_adk_memory_store_class",
        "resolve_adk_artifact_store_class",
        "adk_sessions_migration_enabled",
        "adk_memory_migration_enabled",
    ],
)
def test_adk_migration_helper_lazy_import_isolates_optional_dependencies(helper_name: str) -> None:
    """ADK migration helpers must load lazily without importing google.adk or pydantic."""
    forbidden = (
        "google.adk",
        "pydantic",
        "sqlspec.extensions.adk.service",
        "sqlspec.extensions.adk.store",
        "sqlspec.extensions.adk.memory",
        "sqlspec.extensions.adk.artifact",
        "sqlspec.extensions.adk.maintenance",
    )
    script = f"""
import importlib.abc
import sys


class _BlockedImportFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if any(fullname == prefix or fullname.startswith(prefix + ".") for prefix in ("google.adk", "pydantic")):
            raise ModuleNotFoundError(f"blocked optional dependency: {{fullname}}", name=fullname)
        return None


sys.meta_path.insert(0, _BlockedImportFinder())
from sqlspec.extensions.adk import {helper_name} as exported_helper
import sqlspec.extensions.adk as adk_pkg
import sqlspec.extensions.adk._config_utils as config_utils

assert exported_helper is getattr(config_utils, {helper_name!r})
assert {helper_name!r} in adk_pkg.__all__
assert {helper_name!r} in dir(adk_pkg)
forbidden = {forbidden!r}
loaded = [
    name for name in sys.modules if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
]
assert not loaded, f"Unexpected modules loaded: {{loaded!r}}"
print("ok")
"""
    result = _run_import_probe(script)
    assert result.stdout.strip() == "ok"


def test_events_runtime_hints_import_leaves_channel_and_migrations_unloaded() -> None:
    """Bare EventRuntimeHints import should list resolve_backend_name without loading _channel or migrations."""
    forbidden = ("sqlspec.extensions.events._channel", "sqlspec.migrations")
    script = f"""
import sys
from sqlspec.extensions.events import EventRuntimeHints
import sqlspec.extensions.events as events_pkg

assert EventRuntimeHints is not None
assert "resolve_backend_name" in events_pkg.__all__
assert "resolve_backend_name" in dir(events_pkg)
forbidden = {forbidden!r}
loaded = [
    name for name in sys.modules if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
]
assert not loaded, f"Unexpected modules loaded: {{loaded!r}}"
print("ok")
"""
    result = _run_import_probe(script)
    assert result.stdout.strip() == "ok"


def test_events_resolve_backend_name_import_isolates_cli_and_migrations() -> None:
    """Importing resolve_backend_name should return _channel.resolve_backend_name without CLI or migration loaders."""
    forbidden = ("sqlspec.migrations.commands", "sqlspec.extensions.events.migrations", "rich")
    script = f"""
import sys
from sqlspec.extensions.events import resolve_backend_name
import sqlspec.extensions.events as events_pkg
import sqlspec.extensions.events._channel as channel_mod

assert resolve_backend_name is channel_mod.resolve_backend_name
assert "resolve_backend_name" in events_pkg.__all__
assert "resolve_backend_name" in dir(events_pkg)
forbidden = {forbidden!r}
loaded = [
    name for name in sys.modules if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
]
assert not loaded, f"Unexpected modules loaded: {{loaded!r}}"
print("ok")
"""
    result = _run_import_probe(script)
    assert result.stdout.strip() == "ok"
