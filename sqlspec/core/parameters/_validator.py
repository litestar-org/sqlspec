"""Parameter extraction utilities."""

import re
from collections import OrderedDict
from typing import Final

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters._types import ParameterInfo, ParameterStyle

__all__ = ("PARAMETER_REGEX", "ParameterValidator", "unescape_qmark_operator")

_PARAM_CHARS: Final[frozenset[str]] = frozenset("?%:@$")

PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    r"""
    (?P<dquote>"(?:[^"\\]|\\.)*") |
    (?P<squote>'(?:[^'\\]|\\.)*') |
    (?P<dollar_quoted_string>\$(?P<dollar_quote_tag_inner>\w*)?\$[\s\S]*?\$\4\$) |
    (?P<line_comment>--[^\r\n]*) |
    (?P<block_comment>/\*(?:[^*]|\*(?!/))*\*/) |
    (?P<pg_q_operator>\?\?|\?\||\?&) |
    (?P<pg_cast>::(?P<cast_type>\w+)) |
    (?P<sql_server_global>@@(?P<global_var_name>\w+)) |
    (?P<pyformat_named>%\((?P<pyformat_name>\w+)\)s) |
    (?P<pyformat_pos>%s) |
    (?P<positional_colon>(?<![A-Za-z0-9_]):(?P<colon_num>\d+)) |
    (?P<named_colon>(?<![A-Za-z0-9_]):(?P<colon_name>\w+)) |
    (?P<named_at>(?<![A-Za-z0-9_])@(?!sqlspec_)(?P<at_name>\w+)) |
    (?P<numeric>(?<![A-Za-z0-9_])\$(?P<numeric_num>\d+)) |
    (?P<named_dollar_param>(?<![A-Za-z0-9_])\$(?P<dollar_param_name>\w+)) |
    (?P<qmark>\?)
    """,
    re.VERBOSE | re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

_SKIP_GROUPS: Final[tuple[str, ...]] = (
    "dquote",
    "squote",
    "dollar_quoted_string",
    "line_comment",
    "block_comment",
    "pg_q_operator",
    "pg_cast",
    "sql_server_global",
)


_OPERAND_START: Final = re.compile(r"\s*(?:'|\$\d|:[A-Za-z_]|%s|%\(\w+\)s|@\w|\?)")
_PRECEDING_WORD: Final = re.compile(r"([A-Za-z_][A-Za-z0-9_$]*)\s*$")
_NON_OPERAND_KEYWORDS: Final[frozenset[str]] = frozenset([
    "select",
    "distinct",
    "all",
    "where",
    "and",
    "or",
    "not",
    "on",
    "when",
    "then",
    "else",
    "in",
    "like",
    "ilike",
    "between",
    "values",
    "set",
    "by",
    "limit",
    "offset",
    "having",
    "returning",
    "case",
    "is",
    "from",
    "using",
    "interval",
    "escape",
    "top",
    "fetch",
    "first",
    "next",
    "as",
    "any",
    "some",
    "exists",
    "join",
    "with",
    "union",
    "except",
    "intersect",
    "return",
    "call",
    "exec",
    "execute",
    "into",
    "over",
    "filter",
    "within",
    "end",
    "if",
])


def _follows_operand(sql: str, start: int) -> bool:
    head = sql[:start].rstrip()
    if not head:
        return False
    if head[-1] in ")]\"'":
        return True
    match = _PRECEDING_WORD.search(head)
    return bool(match and match.group(1).lower() not in _NON_OPERAND_KEYWORDS)


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterValidator:
    """Extracts placeholder metadata and dialect compatibility information."""

    __slots__ = ("_cache_hits", "_cache_max_size", "_cache_misses", "_parameter_cache")

    def __init__(self, cache_max_size: int = 5000) -> None:
        self._parameter_cache: OrderedDict[str, list[ParameterInfo]] = OrderedDict()
        self._cache_max_size = max(cache_max_size, 0)
        self._cache_hits = 0
        self._cache_misses = 0

    def set_cache_max_size(self, cache_max_size: int) -> None:
        """Update the maximum cache size for parameter metadata."""
        self._cache_max_size = max(cache_max_size, 0)
        while len(self._parameter_cache) > self._cache_max_size:
            self._parameter_cache.popitem(last=False)

    def clear_cache(self) -> None:
        """Clear cached parameter metadata and reset stats."""
        self._parameter_cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0

    def cache_stats(self) -> "dict[str, int]":
        """Return cache statistics."""
        return {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "size": len(self._parameter_cache),
            "max_size": self._cache_max_size,
        }

    @staticmethod
    def _extract_parameter_style(match: re.Match[str]) -> "tuple[ParameterStyle | None, str | None]":
        """Map a regex match to a placeholder style and optional name."""
        if match.group("qmark"):
            return ParameterStyle.QMARK, None
        if match.group("named_colon"):
            return ParameterStyle.NAMED_COLON, match.group("colon_name")
        if match.group("numeric"):
            return ParameterStyle.NUMERIC, match.group("numeric_num")
        if match.group("named_at"):
            return ParameterStyle.NAMED_AT, match.group("at_name")
        if match.group("pyformat_named"):
            return ParameterStyle.NAMED_PYFORMAT, match.group("pyformat_name")
        if match.group("pyformat_pos"):
            return ParameterStyle.POSITIONAL_PYFORMAT, None
        if match.group("positional_colon"):
            return ParameterStyle.POSITIONAL_COLON, match.group("colon_num")
        if match.group("named_dollar_param"):
            return ParameterStyle.NAMED_DOLLAR, match.group("dollar_param_name")
        return None, None

    def extract_parameters(self, sql: str) -> "list[ParameterInfo]":
        """Extract ordered parameter metadata from SQL text."""
        if self._cache_max_size <= 0:
            return self._extract_parameters_uncached(sql)

        cache_key = sql
        cached_result = self._parameter_cache.get(cache_key)
        if cached_result is not None:
            self._parameter_cache.move_to_end(cache_key)
            self._cache_hits += 1
            return cached_result
        self._cache_misses += 1

        parameters = self._extract_parameters_uncached(sql)
        if len(self._parameter_cache) >= self._cache_max_size:
            self._parameter_cache.popitem(last=False)
        self._parameter_cache[cache_key] = parameters
        return parameters

    def _extract_parameters_uncached(self, sql: str) -> "list[ParameterInfo]":
        parameters: list[ParameterInfo] = []
        ordinal = 0

        if not _PARAM_CHARS.intersection(sql):
            return []

        for match in PARAMETER_REGEX.finditer(sql):
            if any(match.group(*_SKIP_GROUPS)):
                continue
            style, name = self._extract_parameter_style(match)
            if style is None:
                continue
            if match.group("qmark") and _OPERAND_START.match(sql, match.end()) and _follows_operand(sql, match.start()):
                continue
            placeholder_text = match.group(0)
            parameters.append(ParameterInfo(name, style, match.start(), ordinal, placeholder_text))
            ordinal += 1
        return parameters


_QMARK_ESCAPE: Final[str] = "??"


def unescape_qmark_operator(
    sql: str, param_info: "list[ParameterInfo] | None" = None
) -> "tuple[str, list[ParameterInfo]]":
    """Replace each ``??`` escape outside literals and comments with a single ``?``.

    Returns the rewritten SQL and ``param_info`` with positions shifted to match it.
    """
    infos = param_info if param_info is not None else []
    if _QMARK_ESCAPE not in sql:
        return sql, infos
    segments: list[str] = []
    escape_positions: list[int] = []
    last_end = 0
    for match in PARAMETER_REGEX.finditer(sql):
        if match.group("pg_q_operator") != _QMARK_ESCAPE:
            continue
        segments.extend((sql[last_end : match.start()], "?"))
        escape_positions.append(match.start())
        last_end = match.end()
    if not escape_positions:
        return sql, infos
    segments.append(sql[last_end:])
    shifted: list[ParameterInfo] = []
    for info in infos:
        removed = 0
        for position in escape_positions:
            if position < info.position:
                removed += 1
        shifted.append(
            ParameterInfo(
                name=info.name,
                style=info.style,
                position=info.position - removed,
                ordinal=info.ordinal,
                placeholder_text=info.placeholder_text,
            )
        )
    return "".join(segments), shifted
