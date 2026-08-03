"""Guard test keeping ``MIGRATION_CONFIG_KEYS`` a complete inventory of consumers.

``MIGRATION_CONFIG_KEYS`` is a closed allowlist derived from the ``MigrationConfig``
``TypedDict``: a key absent from the declaration is rejected at config construction.
That is only correct while the declaration covers every key SQLSpec reads, and a
hand-maintained restatement drifts. This module scans the package for reads and
asserts the allowlist covers them.

The scanner resolves one level of local aliasing because that is what a name-based
search misses: ``build_template_settings`` rebinds ``config = migration_config or {}``
before reading, which is how three template keys were consumed without being declared.
"""

import ast
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import sqlspec
from sqlspec.config import MIGRATION_CONFIG_KEYS

if TYPE_CHECKING:
    from collections.abc import Iterator

CONFIG_NAME = "migration_config"
PACKAGE_ROOT = Path(sqlspec.__file__).parent


def _unwrap(node: ast.expr) -> ast.expr:
    """Strip ``or`` fallbacks and ``cast()`` wrappers from an expression."""
    while True:
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or) and node.values:
            node = node.values[0]
        elif isinstance(node, ast.Call) and _is_cast(node.func) and node.args:
            node = node.args[-1]
        else:
            return node


def _is_cast(func: ast.expr) -> bool:
    """Check whether a call target is ``cast`` or ``typing.cast``."""
    if isinstance(func, ast.Name):
        return func.id == "cast"
    return isinstance(func, ast.Attribute) and func.attr == "cast"


def _is_config_expr(node: ast.expr, tracked: "set[str]") -> bool:
    """Check whether an expression evaluates to a migration configuration mapping."""
    root = _unwrap(node)
    if isinstance(root, ast.Name):
        return root.id in tracked
    return isinstance(root, ast.Attribute) and root.attr == CONFIG_NAME


def _tracked_names(scope: ast.AST) -> "set[str]":
    """Collect local names bound to a migration configuration within one scope.

    Only direct rebindings are tracked. A name bound to the *result* of a lookup,
    such as ``templates_config = config.get("templates")``, is deliberately not
    tracked: its keys belong to a nested scope, not to ``migration_config``.
    """
    tracked = {CONFIG_NAME}
    for _ in range(3):
        before = len(tracked)
        for node in ast.walk(scope):
            targets: list[ast.expr] = []
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                targets = [node.target]
            else:
                continue
            value = node.value
            if value is None or not _is_config_expr(value, tracked):
                continue
            tracked.update(target.id for target in targets if isinstance(target, ast.Name))
        if len(tracked) == before:
            break
    return tracked


def _literal(node: ast.expr) -> "str | None":
    """Return the value of a string literal node, or None."""
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _reads_in_scope(scope: ast.AST, tracked: "set[str]") -> "Iterator[str]":
    """Yield string-literal keys read from a tracked configuration mapping."""
    for node in ast.walk(scope):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.args:
            if node.func.attr in {"get", "pop"} and _is_config_expr(node.func.value, tracked):
                key = _literal(node.args[0])
                if key is not None:
                    yield key
        elif isinstance(node, ast.Subscript) and _is_config_expr(node.value, tracked):
            key = _literal(node.slice)
            if key is not None:
                yield key
        elif isinstance(node, ast.Compare) and len(node.ops) == 1 and isinstance(node.ops[0], ast.In):
            if _is_config_expr(node.comparators[0], tracked):
                key = _literal(node.left)
                if key is not None:
                    yield key


def collect_consumed_keys(source: str) -> "set[str]":
    """Collect every migration_config key a module reads by string literal."""
    tree = ast.parse(source)
    scopes: list[ast.AST] = [tree]
    scopes.extend(
        node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda))
    )
    consumed: set[str] = set()
    for scope in scopes:
        consumed.update(_reads_in_scope(scope, _tracked_names(scope)))
    return consumed


def _package_sources() -> "Iterator[tuple[Path, str]]":
    """Yield every Python source file shipped in the package."""
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        yield path, path.read_text(encoding="utf-8")


def test_every_consumed_key_is_declared() -> None:
    """No module may read a migration_config key the allowlist would reject."""
    undeclared: dict[str, set[str]] = {}
    for path, source in _package_sources():
        consumed = collect_consumed_keys(source)
        missing = consumed - MIGRATION_CONFIG_KEYS
        if missing:
            undeclared[str(path.relative_to(PACKAGE_ROOT))] = missing

    assert not undeclared, (
        f"migration_config keys read but not declared on MigrationConfig: {undeclared}. "
        "Declare them on the TypedDict; MIGRATION_CONFIG_KEYS rejects everything else at construction."
    )


def test_scanner_finds_keys_read_through_a_local_alias() -> None:
    """The scanner resolves the rebinding pattern that hid three keys during #670."""
    source = """
def build_template_settings(migration_config):
    config = migration_config or {}
    return config.get("templates"), config.get("default_format"), config.get("title")
"""

    assert collect_consumed_keys(source) == {"templates", "default_format", "title"}


@pytest.mark.parametrize(
    "source",
    [
        pytest.param('def f(migration_config):\n    return migration_config.get("undeclared")', id="direct-get"),
        pytest.param('def f(migration_config):\n    return migration_config["undeclared"]', id="subscript"),
        pytest.param('def f(migration_config):\n    return "undeclared" in migration_config', id="containment"),
        pytest.param(
            'def f(config):\n    mc = cast("dict[str, Any]", config.migration_config) or {}\n'
            '    return mc.get("undeclared")',
            id="cast-attribute-alias",
        ),
    ],
)
def test_scanner_detects_an_undeclared_key(source: str) -> None:
    """A guard that cannot fail is worse than no guard, so prove each read shape is caught."""
    consumed = collect_consumed_keys(source)

    assert "undeclared" in consumed
    assert consumed - MIGRATION_CONFIG_KEYS == {"undeclared"}


def test_scanner_ignores_keys_of_nested_mappings() -> None:
    """Keys of a mapping returned by a lookup belong to a nested scope, not the top level."""
    source = """
def build_template_settings(migration_config):
    templates_config = migration_config.get("templates") or {}
    return templates_config.get("sql")
"""

    assert collect_consumed_keys(source) == {"templates"}


def test_scanner_reads_the_real_template_module() -> None:
    """The template reader is scanned as a live consumer, not just as a fixture."""
    source = (PACKAGE_ROOT / "migrations" / "templates.py").read_text(encoding="utf-8")

    assert {"templates", "default_format", "title"} <= collect_consumed_keys(source)
