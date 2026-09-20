"""Preserve explicit JSONB operator spelling through SQLGlot round trips."""

from sqlglot import exp
from sqlglot.generator import Generator


def _jsonb_top_key(generator: Generator, expression: exp.JSONBContainsTopKey) -> str:
    return generator.binary(expression, "??")


def _register_jsonb_operator() -> None:
    default = Generator.TRANSFORMS[exp.JSONBContainsTopKey]
    pending = [Generator]
    seen: set[type[Generator]] = set()
    while pending:
        cls = pending.pop()
        if cls in seen:
            continue
        seen.add(cls)
        pending.extend(cls.__subclasses__())
        if cls.TRANSFORMS.get(exp.JSONBContainsTopKey) is default:
            cls.TRANSFORMS[exp.JSONBContainsTopKey] = _jsonb_top_key
            try:
                from sqlglot.generator import _DISPATCH_CACHE

                _DISPATCH_CACHE.pop(cls, None)
            except ImportError:
                pass


_register_jsonb_operator()


_SET_OPERATION_TYPES = (exp.Union, exp.Intersect, exp.Except)


def _trailing_branch_order(expression: exp.SetOperation) -> exp.Expr | None:
    right = expression.expression
    if isinstance(right, exp.Select) and right.args.get("limit") is None and right.args.get("offset") is None:
        return right.args.get("order")
    return None


def _set_operation_sql(generator: Generator, expression: exp.SetOperation) -> str:
    """Render T-SQL pagination outside a set operation.

    Unordered pagination uses SQL Server's no-op ordering to produce valid
    syntax; callers must supply an ordering for deterministic pages.
    """
    if not any(cls.__name__ == "TSQL" for cls in type(generator.dialect).__mro__) or (
        expression.args.get("limit") is None and expression.args.get("offset") is None
    ):
        return generator.set_operations(expression)
    working = expression.copy()
    limit = working.args.get("limit")
    offset = working.args.get("offset")
    order = working.args.get("order") or _trailing_branch_order(working)
    with_ = working.args.get("with_")
    for node in (limit, offset, order, with_):
        if node is not None:
            node.pop()
    outer = (
        exp.Select().select("*").from_(exp.Subquery(this=working, alias=exp.TableAlias(this=exp.to_identifier("_l_0"))))
    )
    for key, value in (("with_", with_), ("order", order), ("offset", offset), ("limit", limit)):
        if value is not None:
            outer.set(key, value)
    return generator.sql(outer)


def _register_set_operation_transform() -> None:
    defaults = {kind: Generator.TRANSFORMS[kind] for kind in _SET_OPERATION_TYPES}
    pending = [Generator]
    seen: set[type[Generator]] = set()
    while pending:
        cls = pending.pop()
        if cls in seen:
            continue
        seen.add(cls)
        pending.extend(cls.__subclasses__())
        changed = False
        for kind in _SET_OPERATION_TYPES:
            if cls.TRANSFORMS.get(kind) is defaults[kind]:
                cls.TRANSFORMS[kind] = _set_operation_sql
                changed = True
        if changed:
            try:
                from sqlglot.generator import _DISPATCH_CACHE

                _DISPATCH_CACHE.pop(cls, None)
            except ImportError:
                pass


_register_set_operation_transform()
