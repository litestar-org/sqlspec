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
