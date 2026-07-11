"""Static contract between sqlspec and sqlglot expression argument names.

sqlglot's ``Expression.set()`` and constructor kwargs accept any key, but the
generator only visits keys present in the expression class's ``arg_types`` —
an unknown key is silently dropped from rendered SQL. This suite scans the
package source so that every argument sqlspec passes to a sqlglot expression
is provably visible to the generator.

Constructor kwargs are validated automatically. ``.set()`` receivers assigned
from an ``exp.<Class>(...)`` constructor in the same function are validated
against that class; every other ``.set()`` site must be registered in
``KNOWN_SET_SITES`` after manually verifying the key against the receiver's
runtime class. Adding an unregistered ``.set()`` call fails this suite by
design.
"""

import ast
from collections.abc import Iterator
from pathlib import Path

import sqlglot.expressions as sge

import sqlspec

PACKAGE_ROOT = Path(sqlspec.__file__).parent

EXP_MODULE_ALIASES = frozenset({"exp", "expressions", "sge"})

KNOWN_SET_SITES: frozenset[tuple[str, str, str]] = frozenset({
    ("sqlspec/adapters/bigquery/core.py", "statement_values", "expressions"),
    ("sqlspec/builder/_base.py", "final_expression", "with_"),
    ("sqlspec/builder/_base.py", "node", "quoted"),
    ("sqlspec/builder/_dml.py", "current_expr", "expression"),
    ("sqlspec/builder/_dml.py", "current_expr", "expressions"),
    ("sqlspec/builder/_dml.py", "current_expr", "from_"),
    ("sqlspec/builder/_dml.py", "current_expr", "this"),
    ("sqlspec/builder/_insert.py", "insert_expr", "conflict"),
    ("sqlspec/builder/_join.py", "inner_table", "alias"),
    ("sqlspec/builder/_join.py", "inner_table", "version"),
    ("sqlspec/builder/_join.py", "join_expr", "kind"),
    ("sqlspec/builder/_join.py", "join_expr", "side"),
    ("sqlspec/builder/_join.py", "join_expr", "this"),
    ("sqlspec/builder/_merge.py", "current_expr", "on"),
    ("sqlspec/builder/_merge.py", "current_expr", "this"),
    ("sqlspec/builder/_merge.py", "current_expr", "using"),
    ("sqlspec/builder/_merge.py", "current_expr", "whens"),
    ("sqlspec/builder/_merge.py", "source", "alias"),
    ("sqlspec/builder/_merge.py", "table_expr", "this"),
    ("sqlspec/builder/_merge.py", "then_expr", "where"),
    ("sqlspec/builder/_merge.py", "when_expr", "condition"),
    ("sqlspec/builder/_select.py", "modified_expr", "hint"),
    ("sqlspec/builder/_select.py", "select_expr", "distinct"),
    ("sqlspec/builder/_select.py", "select_expr", "expressions"),
    ("sqlspec/builder/_select.py", "select_expr", "locks"),
    ("sqlspec/builder/_select.py", "self._expression", "returning"),
    ("sqlspec/builder/_select.py", "table", "pivots"),
    ("sqlspec/builder/_select.py", "where_clause", "this"),
    ("sqlspec/builder/_temporal.py", "table_expr", "version"),
    ("sqlspec/core/query_modifiers.py", "existing_where", "this"),
    ("sqlspec/core/query_modifiers.py", "expression", "expressions"),
    ("sqlspec/core/query_modifiers.py", "result", "with_"),
    ("sqlspec/core/query_modifiers.py", "working_expr", "with_"),
    ("sqlspec/dialects/spanner/_generators.py", "properties", "expressions"),
    ("sqlspec/dialects/spanner/_parsers.py", "create", "properties"),
    ("sqlspec/dialects/spanner/_parsers.py", "properties", "expressions"),
    ("sqlspec/driver/_common.py", "count_expr", "from_"),
    ("sqlspec/driver/_common.py", "count_expr", "joins"),
    ("sqlspec/driver/_common.py", "count_expr", "with_"),
    ("sqlspec/driver/_common.py", "count_source", "limit"),
    ("sqlspec/driver/_common.py", "count_source", "offset"),
    ("sqlspec/driver/_common.py", "count_source", "order"),
    ("sqlspec/driver/_common.py", "expr", "with_"),
    ("sqlspec/driver/_common.py", "expr_copy", "with_"),
    ("sqlspec/driver/_common.py", "modified_expr", "with_"),
    ("sqlspec/driver/_common.py", "subquery_expr", "limit"),
    ("sqlspec/driver/_common.py", "subquery_expr", "offset"),
    ("sqlspec/driver/_common.py", "subquery_expr", "order"),
})


def _iter_module_trees() -> "Iterator[tuple[str, ast.Module]]":
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        yield path.relative_to(PACKAGE_ROOT.parent).as_posix(), ast.parse(path.read_text(), filename=str(path))


def _constructor_class(node: ast.Call) -> "type | None":
    func = node.func
    if (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id in EXP_MODULE_ALIASES
        and func.attr[:1].isupper()
    ):
        cls = getattr(sge, func.attr, None)
        if isinstance(cls, type) and issubclass(cls, sge.Expression):
            return cls
    return None


def test_constructor_kwargs_exist_in_arg_types() -> None:
    violations: list[str] = []
    for rel, tree in _iter_module_trees():
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            cls = _constructor_class(node)
            if cls is None:
                continue
            violations.extend(
                f"{rel}:{node.lineno} exp.{cls.__name__}({kw.arg}=...) — valid: {sorted(cls.arg_types)}"
                for kw in node.keywords
                if kw.arg is not None and kw.arg not in cls.arg_types
            )
    assert not violations, "constructor kwargs unknown to sqlglot (silently dropped):\n" + "\n".join(violations)


def test_set_calls_use_known_arg_keys() -> None:
    hard_violations: list[str] = []
    unregistered: list[str] = []
    for rel, tree in _iter_module_trees():
        for func_node in ast.walk(tree):
            if not isinstance(func_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            local_classes: dict[str, type] = {}
            for stmt in ast.walk(func_node):
                if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
                    cls = _constructor_class(stmt.value)
                    if cls is not None:
                        for tgt in stmt.targets:
                            if isinstance(tgt, ast.Name):
                                local_classes[tgt.id] = cls
                if not (
                    isinstance(stmt, ast.Call)
                    and isinstance(stmt.func, ast.Attribute)
                    and stmt.func.attr == "set"
                    and stmt.args
                    and isinstance(stmt.args[0], ast.Constant)
                    and isinstance(stmt.args[0].value, str)
                ):
                    continue
                key = stmt.args[0].value
                recv_src = ast.unparse(stmt.func.value)
                if recv_src.startswith("sql."):
                    continue
                recv_cls = local_classes.get(recv_src)
                if recv_cls is not None:
                    if key not in recv_cls.arg_types:
                        hard_violations.append(
                            f"{rel}:{stmt.lineno} {recv_src}.set({key!r}) on exp.{recv_cls.__name__} — "
                            f"valid: {sorted(recv_cls.arg_types)}"
                        )
                elif _looks_like_sqlglot_receiver(key) and (rel, recv_src, key) not in KNOWN_SET_SITES:
                    unregistered.append(f'("{rel}", "{recv_src}", "{key}"),')
    assert not hard_violations, "set() keys unknown to the receiver's arg_types:\n" + "\n".join(hard_violations)
    assert not unregistered, (
        "unregistered .set() sites — verify each key against the receiver's runtime sqlglot class "
        "(the generator silently drops unknown keys), then add the tuple to KNOWN_SET_SITES:\n"
        + "\n".join(sorted(unregistered))
    )


def _looks_like_sqlglot_receiver(key: str) -> bool:
    return any(key in cls.arg_types for cls in _all_expression_classes()) or not key.startswith("_")


def _all_expression_classes() -> "list[type]":
    return [cls for cls in vars(sge).values() if isinstance(cls, type) and issubclass(cls, sge.Expression)]


def test_no_direct_args_subscript_writes() -> None:
    violations: list[str] = []
    for rel, tree in _iter_module_trees():
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            violations.extend(
                f"{rel}:{node.lineno} {ast.unparse(tgt)} = ..."
                for tgt in node.targets
                if (
                    isinstance(tgt, ast.Subscript)
                    and isinstance(tgt.value, ast.Attribute)
                    and tgt.value.attr == "args"
                    and isinstance(tgt.slice, ast.Constant)
                )
            )
    assert not violations, "direct .args[...] writes bypass arg_types validation:\n" + "\n".join(violations)
