"""Parameter extraction utilities."""

import re
from bisect import bisect_left
from collections import OrderedDict
from typing import Final

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters._types import ParameterInfo, ParameterStyle

__all__ = (
    "ANSI_PARAMETER_REGEX",
    "MYSQL_PARAMETER_REGEX",
    "PARAMETER_REGEX",
    "SNOWFLAKE_PARAMETER_REGEX",
    "ParameterValidator",
    "unescape_qmark_operator",
)

_PARAM_CHARS: Final[frozenset[str]] = frozenset("?%:@$")

_REGEX_FLAGS: Final = re.VERBOSE | re.IGNORECASE | re.MULTILINE | re.DOTALL
_BLOCK_COMMENT_PATTERN: Final = r"(?P<block_comment>/\*(?:[^*]|\*(?!/))*(?:\*/|\Z))"
_DOLLAR_QUOTED_PATTERN: Final = (
    r"(?P<dollar_quoted_string>(?<![A-Za-z0-9_$])\$(?P<dollar_quote_tag_inner>(?:[A-Za-z_]\w*)?)\$"
    r"[\s\S]*?(?:\$(?P=dollar_quote_tag_inner)\$|\Z))"
)
_PLACEHOLDER_PATTERNS: Final = r"""
    (?P<pg_q_operator>\?\?|\?\||\?&) |
    (?P<pg_cast>::(?P<cast_type>\w+)) |
    (?P<sql_server_global>@@(?P<global_var_name>\w+)) |
    (?P<pyformat_named>%\((?P<pyformat_name>\w+)\)s) |
    (?P<pyformat_pos>%s(?!\w)) |
    (?P<positional_colon>(?<![A-Za-z0-9_]):(?P<colon_num>\d+)) |
    (?P<named_colon>(?<![A-Za-z0-9_]):(?P<colon_name>\w+)) |
    (?P<named_at>(?<![A-Za-z0-9_])@(?!sqlspec_)(?P<at_name>\w+)) |
    (?P<numeric>(?<![A-Za-z0-9_$])\$(?P<numeric_num>\d+)) |
    (?P<named_dollar_param>(?<![A-Za-z0-9_$])\$(?P<dollar_param_name>\w+)) |
    (?P<qmark>\?)
"""

PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<dquote>"(?:""|\\.|[^"\\])*") |
    (?P<squote>'(?:''|\\.|[^'\\])*') |
    {_DOLLAR_QUOTED_PATTERN} |
    (?P<line_comment>--[^\r\n]*) |
    {_BLOCK_COMMENT_PATTERN} |
    {_PLACEHOLDER_PATTERNS}
    """,
    _REGEX_FLAGS,
)

ANSI_PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<dquote>"(?:""|[^"])*") |
    (?P<escape_squote>(?<![A-Za-z0-9_])E'(?:''|\\.|[^'\\])*') |
    (?P<q_squote>(?<![A-Za-z0-9_])N?Q'(?:
        \[[\s\S]*?(?:\]'|\Z) |
        \{{[\s\S]*?(?:\}}'|\Z) |
        \([\s\S]*?(?:\)'|\Z) |
        <[\s\S]*?(?:>'|\Z) |
        (?P<q_delimiter>[^\s\[{{(<'])[\s\S]*?(?:(?P=q_delimiter)'|\Z)
    )) |
    (?P<squote>'(?:''|[^'])*') |
    {_DOLLAR_QUOTED_PATTERN} |
    (?P<line_comment>--[^\r\n]*) |
    {_BLOCK_COMMENT_PATTERN} |
    {_PLACEHOLDER_PATTERNS}
    """,
    _REGEX_FLAGS,
)

MYSQL_PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<dquote>"(?:""|\\.|[^"\\])*") |
    (?P<squote>'(?:''|\\.|[^'\\])*') |
    (?P<bquote>`(?:``|[^`])*`) |
    (?P<line_comment>(?:--|\#)[^\r\n]*) |
    {_BLOCK_COMMENT_PATTERN} |
    {_PLACEHOLDER_PATTERNS}
    """,
    _REGEX_FLAGS,
)

SNOWFLAKE_PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<dquote>"(?:""|\\.|[^"\\])*") |
    (?P<squote>'(?:''|\\.|[^'\\])*') |
    (?P<dollar_quoted_string>\$\$[\s\S]*?(?:\$\$|\Z)) |
    (?P<line_comment>--[^\r\n]*) |
    {_BLOCK_COMMENT_PATTERN} |
    (?P<pg_q_operator>\?\?) |
    (?P<pg_cast>::(?P<cast_type>\w+)) |
    (?P<sql_server_global>@@(?P<global_var_name>\w+)) |
    (?P<sf_slash_comment>(?<!:)//[^\r\n]*) |
    (?P<sf_user_stage>@~) |
    (?P<sf_table_stage>@(?:[A-Za-z_][A-Za-z0-9_$]*\.)*%[A-Za-z0-9_$]+) |
    (?P<sf_named_stage>@[A-Za-z_][A-Za-z0-9_$.]*) |
    (?P<sf_numeric_dollar>\$\d+) |
    (?P<sf_session_var>\$[A-Za-z_]\w*) |
    (?P<pyformat_named>%\((?P<pyformat_name>\w+)\)s) |
    (?P<pyformat_pos>%s(?!\w)) |
    (?P<positional_colon>(?<![A-Za-z0-9_$"\]\)]):(?P<colon_num>\d+)) |
    (?P<named_colon>(?<![A-Za-z0-9_$"\]\)]):(?P<colon_name>\w+)) |
    (?P<qmark>\?) |
    (?P<sf_word>[A-Za-z_][A-Za-z0-9_$]*) |
    (?P<sf_other>[^\s])
    """,
    _REGEX_FLAGS,
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
_ANSI_SKIP_GROUPS: Final[tuple[str, ...]] = (*_SKIP_GROUPS, "escape_squote", "q_squote")
_MYSQL_SKIP_GROUPS: Final[tuple[str, ...]] = (
    "dquote",
    "squote",
    "bquote",
    "line_comment",
    "block_comment",
    "pg_q_operator",
    "pg_cast",
    "sql_server_global",
)
_SCANNER_ANSI: Final = "ansi"
_SCANNER_MYSQL: Final = "mysql"
_SCANNER_SNOWFLAKE: Final = "snowflake"
_ANSI_STRING_DIALECTS: Final[frozenset[str]] = frozenset({
    "postgres",
    "postgresql",
    "pgvector",
    "paradedb",
    "pg_textsearch",
    "cockroach",
    "cockroachdb",
    "spangres",
    "sqlite",
    "duckdb",
    "oracle",
    "tsql",
    "mssql",
    "sqlserver",
    "sybase",
    "ase",
    "sqlanywhere",
    "db2",
    "teradata",
    "vantage",
})
_BACKSLASH_STRING_DIALECTS: Final[frozenset[str]] = frozenset({"mysql", "mariadb", "bigquery", "spanner"})
_OPERAND_LOOKBEHIND_CHARS: Final = 128


_SNOWFLAKE_PARAM_CHARS: Final[frozenset[str]] = frozenset("?%:")
_SNOWFLAKE_COLON_STYLES: Final[frozenset[ParameterStyle]] = frozenset({
    ParameterStyle.NAMED_COLON,
    ParameterStyle.POSITIONAL_COLON,
})
_SNOWFLAKE_SKIP_GROUPS: Final[tuple[str, ...]] = (
    *_SKIP_GROUPS,
    "sf_slash_comment",
    "sf_user_stage",
    "sf_table_stage",
    "sf_named_stage",
    "sf_numeric_dollar",
    "sf_session_var",
)


_OPERAND_START: Final = re.compile(r"\s*(?:'|\$\d|:[A-Za-z_]|%s(?!\w)|%\(\w+\)s|@\w|\?)")
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
_SNOWFLAKE_NON_OPERAND_KEYWORDS: Final[frozenset[str]] = _NON_OPERAND_KEYWORDS | frozenset({
    "default",
    "immediate",
    "qualify",
    "regexp",
    "rlike",
})


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterValidator:
    """Extracts placeholder metadata and dialect compatibility information."""

    __slots__ = ("_cache_hits", "_cache_max_size", "_cache_misses", "_parameter_cache")

    def __init__(self, cache_max_size: int = 5000) -> None:
        self._parameter_cache: OrderedDict[tuple[str, str | None], list[ParameterInfo]] = OrderedDict()
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

    def extract_parameters(self, sql: str, dialect: str | None = None) -> "list[ParameterInfo]":
        """Extract ordered parameter metadata from SQL text."""
        normalized_dialect = _resolve_scanner(dialect)
        if self._cache_max_size <= 0:
            return self._extract_parameters_uncached(sql, normalized_dialect)

        cache_key = (sql, normalized_dialect)
        cached_result = self._parameter_cache.get(cache_key)
        if cached_result is not None:
            self._parameter_cache.move_to_end(cache_key)
            self._cache_hits += 1
            return cached_result
        self._cache_misses += 1

        parameters = self._extract_parameters_uncached(sql, normalized_dialect)
        if len(self._parameter_cache) >= self._cache_max_size:
            self._parameter_cache.popitem(last=False)
        self._parameter_cache[cache_key] = parameters
        return parameters

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

    @staticmethod
    def _extract_snowflake_parameter_style(match: re.Match[str]) -> "tuple[ParameterStyle | None, str | None]":
        """Map the binding styles supported by the Snowflake lexical grammar."""
        if match.group("qmark"):
            return ParameterStyle.QMARK, None
        if match.group("named_colon"):
            return ParameterStyle.NAMED_COLON, match.group("colon_name")
        if match.group("pyformat_named"):
            return ParameterStyle.NAMED_PYFORMAT, match.group("pyformat_name")
        if match.group("pyformat_pos"):
            return ParameterStyle.POSITIONAL_PYFORMAT, None
        if match.group("positional_colon"):
            return ParameterStyle.POSITIONAL_COLON, match.group("colon_num")
        return None, None

    def _extract_snowflake_parameters(self, sql: str) -> "list[ParameterInfo]":
        """Distinguish colon binds from path access using preceding meaningful tokens.

        Comments and whitespace preserve the preceding expression. Operators and SQL
        expression-introducing keywords make the next colon a binding candidate.
        """
        if not _SNOWFLAKE_PARAM_CHARS.intersection(sql):
            return []
        parameters: list[ParameterInfo] = []
        preceding_operand = False
        for match in SNOWFLAKE_PARAMETER_REGEX.finditer(sql):
            if match.group("line_comment") or match.group("block_comment") or match.group("sf_slash_comment"):
                continue
            word = match.group("sf_word")
            if word is not None:
                preceding_operand = word.lower() not in _SNOWFLAKE_NON_OPERAND_KEYWORDS
                continue
            other = match.group("sf_other")
            if other is not None:
                preceding_operand = other in ")]" or other.isdigit()
                continue
            if any(match.group(*_SNOWFLAKE_SKIP_GROUPS)):
                preceding_operand = match.group("pg_q_operator") is None
                continue
            style, name = self._extract_snowflake_parameter_style(match)
            if style is None:
                continue
            if preceding_operand and style in _SNOWFLAKE_COLON_STYLES:
                continue
            preceding_operand = True
            parameters.append(ParameterInfo(name, style, match.start(), len(parameters), match.group(0)))
        return parameters

    def _extract_parameters_uncached(self, sql: str, normalized_dialect: str | None = None) -> "list[ParameterInfo]":
        if normalized_dialect == _SCANNER_SNOWFLAKE:
            return self._extract_snowflake_parameters(sql)
        if not _PARAM_CHARS.intersection(sql):
            return []
        if normalized_dialect == _SCANNER_ANSI:
            regex = ANSI_PARAMETER_REGEX
            skip_groups = _ANSI_SKIP_GROUPS
        elif normalized_dialect == _SCANNER_MYSQL:
            regex = MYSQL_PARAMETER_REGEX
            skip_groups = _MYSQL_SKIP_GROUPS
        else:
            regex = PARAMETER_REGEX
            skip_groups = _SKIP_GROUPS
        parameters: list[ParameterInfo] = []
        for match in regex.finditer(sql):
            if any(match.group(*skip_groups)):
                continue
            style, name = self._extract_parameter_style(match)
            if style is None:
                continue
            if match.group("qmark") and _OPERAND_START.match(sql, match.end()) and _follows_operand(sql, match.start()):
                continue
            parameters.append(ParameterInfo(name, style, match.start(), len(parameters), match.group(0)))
        return parameters


_QMARK_ESCAPE: Final[str] = "??"


def unescape_qmark_operator(
    sql: str, param_info: "list[ParameterInfo] | None" = None, dialect: str | None = None
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
    scanner = _resolve_scanner(dialect)
    if scanner == _SCANNER_SNOWFLAKE:
        regex = SNOWFLAKE_PARAMETER_REGEX
    elif scanner == _SCANNER_ANSI:
        regex = ANSI_PARAMETER_REGEX
    elif scanner == _SCANNER_MYSQL:
        regex = MYSQL_PARAMETER_REGEX
    else:
        regex = PARAMETER_REGEX
    for match in regex.finditer(sql):
        if match.group("pg_q_operator") != _QMARK_ESCAPE:
            continue
        segments.extend((sql[last_end : match.start()], "?"))
        escape_positions.append(match.start())
        last_end = match.end()
    if not escape_positions:
        return sql, infos
    segments.append(sql[last_end:])
    shifted: list[ParameterInfo] = [
        ParameterInfo(
            name=info.name,
            style=info.style,
            position=info.position - bisect_left(escape_positions, info.position),
            ordinal=info.ordinal,
            placeholder_text=info.placeholder_text,
        )
        for info in infos
    ]
    return "".join(segments), shifted


def _follows_operand(sql: str, start: int) -> bool:
    end = start
    while end > 0 and sql[end - 1].isspace():
        end -= 1
    if end == 0:
        return False
    if sql[end - 1] in ")]\"'":
        return True
    match = _PRECEDING_WORD.search(sql, max(0, end - _OPERAND_LOOKBEHIND_CHARS), end)
    return bool(match and match.group(1).lower() not in _NON_OPERAND_KEYWORDS)


def _normalize_dialect(dialect: str | None) -> str | None:
    """Normalize an optional dialect identifier for lexical selection and caching."""
    if dialect is None:
        return None
    return dialect.strip().lower() or None


def _resolve_scanner(dialect: str | None) -> str | None:
    """Select the lexical grammar for a dialect.

    Dialects whose string literals treat backslashes literally use the ANSI
    grammar. MySQL-family dialects add ``#`` comments and backtick identifiers.
    Unknown dialects keep the permissive default grammar.
    """
    normalized = _normalize_dialect(dialect)
    if normalized is None:
        return None
    if normalized == _SCANNER_SNOWFLAKE:
        return _SCANNER_SNOWFLAKE
    if normalized in _ANSI_STRING_DIALECTS:
        return _SCANNER_ANSI
    if normalized in _BACKSLASH_STRING_DIALECTS:
        return _SCANNER_MYSQL
    return None
