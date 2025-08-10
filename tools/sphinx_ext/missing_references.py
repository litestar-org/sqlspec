"""Sphinx extension for handling missing references in SQLSpec documentation."""

from __future__ import annotations

import ast
import importlib
import inspect
import re
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Optional, cast

from docutils.utils import get_source_line

if TYPE_CHECKING:
    from collections.abc import Generator

    from docutils.nodes import Element, Node
    from sphinx.addnodes import pending_xref
    from sphinx.application import Sphinx
    from sphinx.environment import BuildEnvironment


@cache
def _get_module_ast(source_file: str) -> ast.AST | ast.Module:
    return ast.parse(Path(source_file).read_text(encoding="utf-8"))


def _get_import_nodes(nodes: list[ast.stmt]) -> Generator[ast.Import | ast.ImportFrom, None, None]:
    for node in nodes:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            yield node
        elif isinstance(node, ast.If) and getattr(node.test, "id", None) == "TYPE_CHECKING":
            yield from _get_import_nodes(node.body)


@cache
def get_module_global_imports(module_import_path: str, reference_target_source_obj: str) -> set[str]:
    """Return a set of names that are imported globally within the containing module of ``reference_target_source_obj``,
    including imports in ``if TYPE_CHECKING`` blocks.
    """
    module = importlib.import_module(module_import_path)
    obj = getattr(module, reference_target_source_obj)
    tree = _get_module_ast(inspect.getsourcefile(obj))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    import_nodes = _get_import_nodes(tree.body)  # type: ignore[attr-defined]
    return {path.asname or path.name for import_node in import_nodes for path in import_node.names}


def _resolve_local_reference(module_path: str, target: str) -> bool:
    """Attempt to resolve a reference within the local codebase.

    Args:
        module_path: The module path (e.g., 'sqlspec.base')
        target: The target class/attribute name

    Returns:
        bool: True if reference exists, False otherwise
    """
    try:
        module = importlib.import_module(module_path)
        if "." in target:
            # Handle fully qualified names (e.g., sqlspec.base.SQLConfig)
            parts = target.split(".")
            current = module
            for part in parts:
                current = getattr(current, part)
            return True
        return hasattr(module, target)
    except (ImportError, AttributeError):
        return False


def _resolve_sqlalchemy_reference(target: str) -> bool:
    """Attempt to resolve SQLAlchemy references.

    Args:
        target: The target class/attribute name

    Returns:
        bool: True if reference exists, False otherwise
    """
    try:
        import sqlalchemy

        if "." in target:
            # Handle nested attributes (e.g., Connection.in_transaction)
            obj_name, attr_name = target.rsplit(".", 1)
            obj = getattr(sqlalchemy, obj_name)
            return hasattr(obj, attr_name)
        return hasattr(sqlalchemy, target)
    except (ImportError, AttributeError):
        return False


def _resolve_litestar_reference(target: str) -> bool:
    """Attempt to resolve Litestar references.

    Args:
        target: The target class/attribute name

    Returns:
        bool: True if reference exists, False otherwise
    """
    try:
        import litestar
        from litestar import datastructures

        # Handle common Litestar classes
        if target in {"Litestar", "State", "Scope", "Message", "AppConfig", "BeforeMessageSendHookHandler"}:
            return True
        if target.startswith("datastructures."):
            _, attr = target.split(".")
            return hasattr(datastructures, attr)
        if target.startswith("config.app."):
            return True  # These are valid Litestar config references
        return hasattr(litestar, target)
    except ImportError:
        return False


def _resolve_sqlalchemy_type_reference(target: str) -> bool:
    """Attempt to resolve SQLAlchemy type references.

    Args:
        target: The target class/attribute name

    Returns:
        bool: True if reference exists, False otherwise
    """
    try:
        from sqlalchemy import types as sa_types

        type_classes = {
            "TypeEngine",
            "TypeDecorator",
            "UserDefinedType",
            "ExternalType",
            "Dialect",
            "_types.TypeDecorator",
        }

        if target in type_classes:
            return True
        if target.startswith("_types."):
            _, attr = target.split(".")
            return hasattr(sa_types, attr)
        return hasattr(sa_types, target)
    except ImportError:
        return False


def _resolve_sqlspec_reference(target: str, module: str) -> bool:
    """Attempt to resolve SQLSpec references.

    Args:
        target: The target class/attribute name
        module: The current module context

    Returns:
        bool: True if reference exists, False otherwise
    """
    # Handle base module references
    base_classes = {"SQLConfig", "SQLSpec", "SessionProtocol", "DriverProtocol", "StatementProtocol", "ResultProtocol"}

    # Handle config module references
    config_classes = {"AsyncDriverConfig", "SyncDriverConfig", "ConnectionConfig", "GenericSessionConfig"}

    # Handle core module references
    core_classes = {"Statement", "Result", "Parameters", "Compiler", "SQLCache"}

    func_references = {"driver.AsyncDriver.execute", "driver.SyncDriver.execute"}

    # Handle type module references
    type_classes = {"ModelT", "FilterTypeT", "StatementTypeT"}

    if target in base_classes or target in config_classes or target in core_classes or target in type_classes:
        return True

    # Handle fully qualified references
    if target.startswith("sqlspec."):
        parts = target.split(".")
        if parts[-1] in base_classes | config_classes | core_classes | type_classes | func_references:
            return True

    # Handle module-relative references
    return bool(module.startswith("sqlspec."))


def _resolve_serialization_reference(target: str) -> bool:
    """Attempt to resolve serialization-related references.

    Args:
        target: The target class/attribute name

    Returns:
        bool: True if reference exists, False otherwise
    """
    serialization_attrs = {"decode_json", "encode_json", "serialization.decode_json", "serialization.encode_json"}
    return target in serialization_attrs


def _resolve_click_reference(target: str) -> bool:
    """Attempt to resolve Click references.

    Args:
        target: The target class/attribute name

    Returns:
        bool: True if reference exists, False otherwise
    """
    try:
        import click

        if target == "Group":
            return True
        return hasattr(click, target)
    except ImportError:
        return False


def on_warn_missing_reference(app: Sphinx, domain: str, node: Node) -> bool | None:
    ignore_refs: dict[str | re.Pattern[str], set[str] | re.Pattern[str]] = app.config["ignore_missing_refs"]

    if node.tagname != "pending_xref":  # type: ignore[attr-defined]
        return None

    if not hasattr(node, "attributes"):
        return None

    # Wrap the main logic in a try-except to catch potential AttributeErrors (e.g., startswith on None)
    try:
        attributes = node.attributes  # type: ignore[attr-defined,unused-ignore]
        target = cast("str", attributes["reftarget"])  # pyright: ignore
        ref_type = attributes.get("reftype")  # pyright: ignore
        module = attributes.get("py:module", "")  # pyright: ignore

        # Check the original ignore logic first
        if reference_target_source_obj := cast(
            "Optional[str]",
            attributes.get(  # pyright: ignore[reportUnknownMemberType]
                "py:class",
                attributes.get("py:meth", attributes.get("py:func")),  # pyright: ignore[reportUnknownMemberType]
            ),
        ):
            global_names = get_module_global_imports(attributes["py:module"], reference_target_source_obj)  # pyright: ignore[reportUnknownArgumentType]

            if target in global_names:
                # autodoc has issues with if TYPE_CHECKING imports, and randomly with type aliases in annotations,
                # so we ignore those errors if we can validate that such a name exists in the containing modules global
                # scope or an if TYPE_CHECKING block. see: https://github.com/sphinx-doc/sphinx/issues/11225 and
                # https://github.com/sphinx-doc/sphinx/issues/9813 for reference
                return True

        # Handle TypeVar references
        if hasattr(target, "__class__") and target.__class__.__name__ == "TypeVar":  # pyright: ignore
            return True

        # Handle SQLSpec references
        if _resolve_sqlspec_reference(target, module):  # pyright: ignore
            return True

        # Handle SQLAlchemy type system references
        if ref_type in {"class", "meth", "attr"} and any(
            x in target for x in ["TypeDecorator", "TypeEngine", "Dialect", "ExternalType", "UserDefinedType"]
        ):
            return _resolve_sqlalchemy_type_reference(target)  # pyright: ignore

        # Handle SQLAlchemy core references
        if (isinstance(target, str) and target.startswith("sqlalchemy.")) or (
            ref_type in {"class", "attr", "obj", "meth"}
            and target
            in {
                "Engine",
                "Session",
                "Connection",
                "MetaData",
                "AsyncSession",
                "AsyncEngine",
                "AsyncConnection",
                "sessionmaker",
                "async_sessionmaker",
            }
        ):
            # Ensure target is string before replace
            clean_target = target.replace("sqlalchemy.", "") if isinstance(target, str) else ""
            if clean_target and _resolve_sqlalchemy_reference(clean_target):
                return True

        # Handle Litestar references
        if ref_type in {"class", "obj"} and (
            (isinstance(target, str) and target.startswith(("datastructures.", "config.app.")))
            or target
            in {
                "Litestar",
                "State",
                "Scope",
                "Message",
                "AppConfig",
                "BeforeMessageSendHookHandler",
                "FieldDefinition",
                "ImproperConfigurationError",
            }
        ):
            return _resolve_litestar_reference(target)  # pyright: ignore

        # Handle serialization references
        if ref_type in {"attr", "meth"} and _resolve_serialization_reference(target):  # pyright: ignore
            return True

        # Handle Click references
        if ref_type == "class" and _resolve_click_reference(target):  # pyright: ignore
            return True

        # for various other autodoc issues that can't be resolved automatically, we check the exact path to be able
        # to suppress specific warnings
        source_line = get_source_line(node)[0]
        source = source_line.split(" ")[-1]
        if target in ignore_refs.get(source, []):  # type: ignore[operator]
            return True
        ignore_ref_rgs = {rg: targets for rg, targets in ignore_refs.items() if isinstance(rg, re.Pattern)}
        for pattern, targets in ignore_ref_rgs.items():
            if not pattern.match(source):
                continue
            if isinstance(targets, set) and target in targets:
                return True
            if targets.match(target):  # type: ignore[union-attr]  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]
                return True

    except AttributeError:
        # Catch the specific error (likely startswith on None) and allow Sphinx to handle the warning normally
        return None

    return None


def on_missing_reference(app: Sphinx, env: BuildEnvironment, node: pending_xref, contnode: Element) -> Element | None:
    """Handle missing references by attempting to resolve them through different methods.

    Args:
        app: The Sphinx application instance
        env: The Sphinx build environment
        node: The pending cross-reference node
        contnode: The content node

    Returns:
        Element | None: The resolved reference node if found, None otherwise
    """
    if not hasattr(node, "attributes"):
        return None

    attributes = node.attributes  # type: ignore[attr-defined,unused-ignore]
    target = attributes["reftarget"]

    # Remove this check since it's causing issues
    if not isinstance(target, str):
        return None

    py_domain = env.domains["py"]

    # autodoc sometimes incorrectly resolves these types, so we try to resolve them as py:data first and fall back to any
    new_node = py_domain.resolve_xref(env, node["refdoc"], app.builder, "data", target, node, contnode)
    if new_node is None:
        resolved_xrefs = py_domain.resolve_any_xref(env, node["refdoc"], app.builder, target, node, contnode)
        for ref in resolved_xrefs:
            if ref:
                return ref[1]
    return new_node


def on_env_before_read_docs(app: Sphinx, env: BuildEnvironment, docnames: set[str]) -> None:
    tmp_examples_path = Path.cwd() / "docs/_build/_tmp_examples"
    tmp_examples_path.mkdir(exist_ok=True, parents=True)
    env.tmp_examples_path = tmp_examples_path  # type: ignore[attr-defined] # pyright: ignore[reportAttributeAccessIssue]


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("env-before-read-docs", on_env_before_read_docs)
    app.connect("missing-reference", on_missing_reference)
    app.connect("warn-missing-reference", on_warn_missing_reference)
    app.add_config_value("ignore_missing_refs", default={}, rebuild="env")
    return {"parallel_read_safe": True, "parallel_write_safe": True}
