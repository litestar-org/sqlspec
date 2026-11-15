"""Tests for names promoted to the public package surfaces."""

import importlib
import pickle
import subprocess
import sys

import pytest

import sqlspec
from sqlspec.utils import uuids


def test_top_level_uuid_exports() -> None:
    """The top-level package re-exports the identifier generators from ``sqlspec.utils.uuids``."""
    assert sqlspec.uuid4 is uuids.uuid4
    assert sqlspec.uuid6 is uuids.uuid6
    assert sqlspec.uuid7 is uuids.uuid7
    assert sqlspec.nanoid is uuids.nanoid


def test_top_level_exports_resolve() -> None:
    """Every name in ``sqlspec.__all__`` resolves, including lazily loaded names."""
    for name in sqlspec.__all__:
        getattr(sqlspec, name)


@pytest.mark.parametrize("defining_first", [False, True])
def test_tenant_exports_retain_defining_objects(defining_first: bool) -> None:
    exports = {
        "TenantContext": "sqlspec.utils.tenant",
        "TenantContextPayload": "sqlspec.utils.tenant",
        "TenantContextError": "sqlspec.exceptions",
        "TenantCleanupError": "sqlspec.exceptions",
    }
    script = f"""
import importlib
exports = {exports!r}
if {defining_first!r}:
    for module in set(exports.values()):
        importlib.import_module(module)
import sqlspec
namespace = {{}}
exec('from sqlspec import *', namespace)
for name, module in exports.items():
    defining_object = getattr(importlib.import_module(module), name)
    assert getattr(sqlspec, name) is defining_object, name
    assert namespace[name] is defining_object, name
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


def test_root_tenant_scope_and_validation() -> None:
    from sqlspec import TenantContext, TenantContextError, TenantContextPayload

    with TenantContext.scope("tenant-a") as payload:
        assert payload.tenant_id == "tenant-a"
        assert TenantContext.get() is payload
    with pytest.raises(TenantContextError) as exc_info:
        TenantContextPayload("")
    assert isinstance(exc_info.value, ValueError)


def test_builder_exports_retain_defining_objects() -> None:
    """All builder exports remain real objects through normal and star imports."""
    from sqlspec import builder
    from sqlspec.builder._factory import sql

    namespace: dict[str, object] = {}
    exec("from sqlspec.builder import *", namespace)
    for name in builder.__all__:
        value = getattr(builder, name)
        assert namespace[name] is value
        if name == "sql":
            assert value is sql
        else:
            assert value is getattr(importlib.import_module(value.__module__), value.__name__)
    assert set(builder.__all__).issubset(dir(builder))
    with pytest.raises(AttributeError, match="has no attribute 'unknown_builder'"):
        getattr(builder, "unknown_builder")


def test_exported_builder_class_supports_subclasses_and_pickle() -> None:
    from sqlspec.builder import Select

    class AppSelect(Select):
        pass

    assert AppSelect("1").build().sql == Select("1").build().sql
    assert pickle.loads(pickle.dumps(Select)) is Select


@pytest.mark.parametrize("defining_first", [False, True])
def test_builder_import_order_and_concurrent_access(defining_first: bool) -> None:
    script = f"""
import importlib
from concurrent.futures import ThreadPoolExecutor

if {defining_first!r}:
    importlib.import_module('sqlspec.builder._factory')
builder = importlib.import_module('sqlspec.builder')
with ThreadPoolExecutor(max_workers=4) as pool:
    factories = list(pool.map(lambda _: builder.sql, range(8)))
from sqlspec.builder._factory import sql
assert all(factory is sql for factory in factories)
assert builder.Select('1').build().sql == sql.select('1').build().sql
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


@pytest.mark.parametrize(
    ("package_name", "name", "defining_module"),
    [
        ("sqlspec", "SQLSpec", "sqlspec.base"),
        ("sqlspec.core", "SQL", "sqlspec.core.statement"),
        ("sqlspec.driver", "SyncDriverAdapterBase", "sqlspec.driver._sync"),
        ("sqlspec.builder", "Merge", "sqlspec.builder._merge"),
        ("sqlspec.migrations", "MigrationSquasher", "sqlspec.migrations.squash"),
        ("sqlspec.extensions.events", "SyncEventChannel", "sqlspec.extensions.events._channel"),
    ],
)
def test_public_export_import_failure_is_retryable(package_name: str, name: str, defining_module: str) -> None:
    """Inspection stays lazy, failed imports retain their error, and later access retries."""
    script = f"""
import importlib
import importlib.abc
import sys
package = importlib.import_module({package_name!r})
assert {name!r} in dir(package)
assert {defining_module!r} not in sys.modules
class BlockImport(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == {defining_module!r}:
            raise ModuleNotFoundError('temporarily unavailable', name=fullname)
blocker = BlockImport()
sys.meta_path.insert(0, blocker)
try:
    getattr(package, {name!r})
except ModuleNotFoundError as error:
    assert error.name == {defining_module!r}
else:
    raise AssertionError('import error was hidden')
sys.meta_path.remove(blocker)
value = getattr(package, {name!r})
assert value is getattr(importlib.import_module({defining_module!r}), {name!r})
assert getattr(package, {name!r}) is value
try:
    getattr(package, 'unknown_export')
except AttributeError:
    pass
else:
    raise AssertionError('unknown export resolved')
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


def test_root_reload_preserves_exports_and_import_side_effects() -> None:
    script = """
import importlib
import logging
import pickle
import warnings
import sqlspec
from sqlspec.base import SQLSpec
from sqlspec.builder._factory import sql
from sqlspec.utils.logging import SqlglotCommandFallbackFilter
assert sqlspec.SQLSpec is SQLSpec
assert sqlspec.sql is sql
assert pickle.loads(pickle.dumps(sqlspec.SQLSpec)) is SQLSpec
version = sqlspec.__version__
importlib.reload(sqlspec)
assert sqlspec.__version__ == version
assert sqlspec.SQLSpec is SQLSpec
assert sqlspec.sql is sql
assert sqlspec.config is importlib.import_module('sqlspec.config')
assert sqlspec.observability is importlib.import_module('sqlspec.observability')
assert sum(isinstance(f, SqlglotCommandFallbackFilter) for f in logging.getLogger('sqlglot').filters) == 1
assert any(action == 'ignore' and category is FutureWarning and pattern.pattern.startswith('You are using a Python version') for action, pattern, category, module, lineno in warnings.filters if pattern is not None)
namespace = {}
exec('from sqlspec import *', namespace)
assert all(namespace[name] is getattr(sqlspec, name) for name in sqlspec.__all__)
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


def test_concurrent_root_exports_preserve_identity() -> None:
    """Resolve top-level exports concurrently without deadlock or identity drift."""
    script = """
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
sys.setswitchinterval(0.000001)
import sqlspec
names = ['SQLSpec', 'SQL', 'sql', 'SyncDatabaseConfig', 'EventMessage', 'Select', 'StatementConfig', 'SQLFileLoader']
barrier = Barrier(len(names))
def resolve(name):
    barrier.wait()
    return getattr(sqlspec, name)
with ThreadPoolExecutor(max_workers=len(names)) as pool:
    values = list(pool.map(resolve, names))
assert all(value is getattr(sqlspec, name) for name, value in zip(names, values))
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False, timeout=20)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("package_name", "package_attr", "submodule_name", "submodule_attr"),
    [
        ("sqlspec.core", "SQL", "sqlspec.core.splitter", "contains_statement_terminator"),
        ("sqlspec.core.parameters", "ParameterValidator", "sqlspec.core.parameters._validator", "ParameterValidator"),
        ("sqlspec.driver", "SyncDriverAdapterBase", "sqlspec.driver._common", "ExecutionResult"),
        ("sqlspec.observability", "ObservabilityConfig", "sqlspec.observability._common", "resolve_db_system"),
        ("sqlspec.data_dictionary", "get_dialect_config", "sqlspec.data_dictionary._registry", "get_dialect_config"),
        ("sqlspec.utils.tenant", "TenantContext", "sqlspec.utils.tenant._context", "TenantContext"),
        ("sqlspec.utils.serializers", "to_json", "sqlspec.utils.serializers._json", "encode_json"),
    ],
)
def test_concurrent_package_and_submodule_imports_preserve_identity(
    package_name: str, package_attr: str, submodule_name: str, submodule_attr: str
) -> None:
    """Race parent package and direct child submodule imports without deadlock."""
    script = f"""
import importlib
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
sys.setswitchinterval(0.000001)
targets = [
    ({package_name!r}, {package_attr!r}),
    ({submodule_name!r}, {submodule_attr!r}),
] * 4
barrier = Barrier(len(targets))
def load(item):
    module_name, attr_name = item
    barrier.wait()
    return getattr(importlib.import_module(module_name), attr_name)
with ThreadPoolExecutor(max_workers=len(targets)) as pool:
    values = list(pool.map(load, targets))
assert len(set(id(v) for v in values[::2])) == 1
assert len(set(id(v) for v in values[1::2])) == 1
assert values[0] is getattr(importlib.import_module({package_name!r}), {package_attr!r})
assert values[1] is getattr(importlib.import_module({submodule_name!r}), {submodule_attr!r})
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False, timeout=20)
    assert result.returncode == 0, result.stderr
