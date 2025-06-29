# pyright: ignore=reportGeneralTypeIssues,reportMissingTypeArgument
"""Sphinx extension for changelog and change directives."""

import ast
import importlib
import inspect
import re
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union, cast

from docutils.nodes import Node
from docutils.utils import get_source_line

if TYPE_CHECKING:
    from collections.abc import Generator

    from docutils.nodes import Element
    from sphinx.addnodes import pending_xref
    from sphinx.application import Sphinx
    from sphinx.environment import BuildEnvironment


@cache
def _get_module_ast(source_file: str) -> "Union[ast.AST, ast.Module]":
    return ast.parse(Path(source_file).read_text(encoding="utf-8"))


def _get_import_nodes(nodes: "list[ast.stmt]") -> "Generator[Union[ast.Import, ast.ImportFrom], None, None]":
    for node in nodes:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            yield node
        elif isinstance(node, ast.If) and getattr(node.test, "id", None) == "TYPE_CHECKING":
            yield from _get_import_nodes(node.body)


@cache
def get_module_global_imports(module_import_path: str, reference_target_source_obj: str) -> "set[str]":
    """Return a set of names that are imported globally within the containing module of ``reference_target_source_obj``,
    including imports in ``if TYPE_CHECKING`` blocks.
    """
    module = importlib.import_module(module_import_path)
    obj = getattr(module, reference_target_source_obj)
    tree = _get_module_ast(inspect.getsourcefile(obj))  # pyright: ignore[reportArgumentType]

    import_nodes = _get_import_nodes(tree.body)  # type: ignore[attr-defined]
    return {path.asname or path.name for import_node in import_nodes for path in import_node.names}


def on_warn_missing_reference(app: "Sphinx", domain: str, node: Node) -> "Optional[bool]":
    ignore_refs: dict[Union[str, re.Pattern[str]], Union[set[str], re.Pattern[str]]] = app.config["ignore_missing_refs"]
    if node.tagname != "pending_xref":  # type: ignore[attr-defined]
        return None

    if not hasattr(node, "attributes"):
        return None

    attributes = node.attributes
    target = cast("str", attributes["reftarget"])

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

    return None


def on_missing_reference(
    app: "Sphinx", env: "BuildEnvironment", node: "pending_xref", contnode: "Element"
) -> "Optional[Element]":
    if not hasattr(node, "attributes"):
        return None

    attributes = node.attributes
    target = attributes["reftarget"]
    py_domain = env.domains["py"]

    # autodoc sometimes incorrectly resolves these types, so we try to resolve them as py:data first and fall back to any
    new_node = py_domain.resolve_xref(env, node["refdoc"], app.builder, "data", target, node, contnode)
    if new_node is None:
        resolved_xrefs = py_domain.resolve_any_xref(env, node["refdoc"], app.builder, target, node, contnode)
        for ref in resolved_xrefs:
            if ref:
                return ref[1]
    return new_node


def on_env_before_read_docs(app: "Sphinx", env: "BuildEnvironment", docnames: "set[str]") -> None:
    tmp_examples_path = Path.cwd() / "docs/_build/_tmp_examples"
    tmp_examples_path.mkdir(exist_ok=True, parents=True)
    env.tmp_examples_path = tmp_examples_path  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]


def setup(app: "Sphinx") -> "dict[str, bool]":
    app.connect("env-before-read-docs", on_env_before_read_docs)
    app.connect("missing-reference", on_missing_reference)
    app.connect("warn-missing-reference", on_warn_missing_reference)
    app.add_config_value("ignore_missing_refs", default={}, rebuild="env")

    return {"parallel_read_safe": True, "parallel_write_safe": True}
