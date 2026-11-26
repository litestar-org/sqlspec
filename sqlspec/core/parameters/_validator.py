"""Parameter extraction utilities."""

import re
from bisect import bisect_left
from collections import OrderedDict
from contextlib import suppress
from typing import TYPE_CHECKING, Final

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters._types import ParameterInfo, ParameterStyle

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = (
    "ANSI_PARAMETER_REGEX",
    "BRACKET_PARAMETER_REGEX",
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
    (?P<dquote>"(?:""|\\.|[^"\\])*(?:"|\\?\Z)) |
    (?P<squote>'(?:''|\\.|[^'\\])*(?:'|\\?\Z)) |
    {_DOLLAR_QUOTED_PATTERN} |
    (?P<line_comment>--[^\r\n]*) |
    {_BLOCK_COMMENT_PATTERN} |
    {_PLACEHOLDER_PATTERNS}
    """,
    _REGEX_FLAGS,
)

ANSI_PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<dquote>"(?:""|[^"])*(?:"|\Z)) |
    (?P<escape_squote>(?<![A-Za-z0-9_])E'(?:''|\\.|[^'\\])*(?:'|\\?\Z)) |
    (?P<q_squote>(?<![A-Za-z0-9_])N?Q'(?:
        \[[\s\S]*?(?:\]'|\Z) |
        \{{[\s\S]*?(?:\}}'|\Z) |
        \([\s\S]*?(?:\)'|\Z) |
        <[\s\S]*?(?:>'|\Z) |
        (?P<q_delimiter>[^\s\[{{(<'])[\s\S]*?(?:(?P=q_delimiter)'|\Z)
    )) |
    (?P<squote>'(?:''|[^'])*(?:'|\Z)) |
    {_DOLLAR_QUOTED_PATTERN} |
    (?P<line_comment>--[^\r\n]*) |
    {_BLOCK_COMMENT_PATTERN} |
    {_PLACEHOLDER_PATTERNS}
    """,
    _REGEX_FLAGS,
)

BRACKET_PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<dquote>"(?:""|[^"])*(?:"|\Z)) |
    (?P<escape_squote>(?<![A-Za-z0-9_])E'(?:''|\\.|[^'\\])*(?:'|\\?\Z)) |
    (?P<q_squote>(?<![A-Za-z0-9_])N?Q'(?:
        \[[\s\S]*?(?:\]'|\Z) |
        \{{[\s\S]*?(?:\}}'|\Z) |
        \([\s\S]*?(?:\)'|\Z) |
        <[\s\S]*?(?:>'|\Z) |
        (?P<q_delimiter>[^\s\[{{(<'])[\s\S]*?(?:(?P=q_delimiter)'|\Z)
    )) |
    (?P<squote>'(?:''|[^'])*(?:'|\Z)) |
    (?P<bracket_identifier>\[(?:\]\]|[^\]])*(?:\]|\Z)) |
    {_DOLLAR_QUOTED_PATTERN} |
    (?P<line_comment>--[^\r\n]*) |
    {_BLOCK_COMMENT_PATTERN} |
    {_PLACEHOLDER_PATTERNS}
    """,
    _REGEX_FLAGS,
)

MYSQL_PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<dquote>"(?:""|\\.|[^"\\])*(?:"|\\?\Z)) |
    (?P<squote>'(?:''|\\.|[^'\\])*(?:'|\\?\Z)) |
    (?P<bquote>`(?:``|[^`])*(?:`|\Z)) |
    (?P<line_comment>(?:--|\#)[^\r\n]*) |
    {_BLOCK_COMMENT_PATTERN} |
    {_PLACEHOLDER_PATTERNS}
    """,
    _REGEX_FLAGS,
)

SNOWFLAKE_PARAMETER_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<dquote>"(?:""|\\.|[^"\\])*(?:"|\\?\Z)) |
    (?P<squote>'(?:''|\\.|[^'\\])*(?:'|\\?\Z)) |
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

_SKIP_GROUPS: Final[frozenset[str]] = frozenset({
    "dquote",
    "squote",
    "dollar_quoted_string",
    "line_comment",
    "block_comment",
    "bracket_identifier",
    "pg_q_operator",
    "pg_cast",
    "sql_server_global",
})
_PARAMETER_GROUP_DISPATCH: Final[dict[str, tuple[ParameterStyle, str | None]]] = {
    "qmark": (ParameterStyle.QMARK, None),
    "named_colon": (ParameterStyle.NAMED_COLON, "colon_name"),
    "numeric": (ParameterStyle.NUMERIC, "numeric_num"),
    "named_at": (ParameterStyle.NAMED_AT, "at_name"),
    "pyformat_named": (ParameterStyle.NAMED_PYFORMAT, "pyformat_name"),
    "pyformat_pos": (ParameterStyle.POSITIONAL_PYFORMAT, None),
    "positional_colon": (ParameterStyle.POSITIONAL_COLON, "colon_num"),
    "named_dollar_param": (ParameterStyle.NAMED_DOLLAR, "dollar_param_name"),
}
_SNOWFLAKE_GROUP_DISPATCH: Final[dict[str, tuple[ParameterStyle, str | None]]] = {
    "qmark": (ParameterStyle.QMARK, None),
    "named_colon": (ParameterStyle.NAMED_COLON, "colon_name"),
    "pyformat_named": (ParameterStyle.NAMED_PYFORMAT, "pyformat_name"),
    "pyformat_pos": (ParameterStyle.POSITIONAL_PYFORMAT, None),
    "positional_colon": (ParameterStyle.POSITIONAL_COLON, "colon_num"),
}
_SCANNER_ANSI: Final = "ansi"
_SCANNER_BRACKET: Final = "bracket"
_SCANNER_NESTED: Final = "ansi_nested"
_SCANNER_MYSQL: Final = "mysql"
_SCANNER_SNOWFLAKE: Final = "snowflake"
_SCANNER_TSQL: Final = "tsql"
_ANSI_STRING_DIALECTS: Final[frozenset[str]] = frozenset({
    "postgres",
    "postgresql",
    "pgvector",
    "paradedb",
    "pg_textsearch",
    "cockroach",
    "cockroachdb",
    "spangres",
    "duckdb",
    "oracle",
    "db2",
    "teradata",
    "vantage",
})
_BRACKET_STRING_DIALECTS: Final[frozenset[str]] = frozenset({"sqlite"})
_TSQL_STRING_DIALECTS: Final[frozenset[str]] = frozenset({"tsql", "mssql", "sqlserver", "sybase", "ase", "sqlanywhere"})
_NESTED_COMMENT_DIALECTS: Final[frozenset[str]] = frozenset({
    "postgres",
    "postgresql",
    "pgvector",
    "paradedb",
    "pg_textsearch",
    "cockroach",
    "cockroachdb",
    "spangres",
    "duckdb",
})
_BACKSLASH_STRING_DIALECTS: Final[frozenset[str]] = frozenset({"mysql", "mariadb", "bigquery", "spanner"})
_OPERAND_LOOKBEHIND_CHARS: Final = 128
_ARRAY_SLICE_STOP_CHARS: Final[frozenset[str]] = frozenset(",;]=<>!|'\"`")
_EMPTY_INT_FROZENSET: Final[frozenset[int]] = frozenset()
_MYSQL_SET_BLOCK_OPENERS: Final[frozenset[str]] = frozenset({"BEGIN", "THEN", "ELSE", "DO"})
_MYSQL_ASSIGN_SCAN_REGEX: Final[re.Pattern[str]] = re.compile(
    r"""
    "(?:""|\\.|[^"\\])*(?:"|\\?\Z) |
    '(?:''|\\.|[^'\\])*(?:'|\\?\Z) |
    `(?:``|[^`])*(?:`|\Z) |
    --[^\r\n]* |
    \#[^\r\n]* |
    /\*(?:[^*]|\*(?!/))*(?:\*/|\Z) |
    @@[A-Za-z_]\w* |
    := |
    (?P<at_var>(?<![A-Za-z0-9_@])@(?!sqlspec_)\w+) |
    (?P<word>[A-Za-z_][A-Za-z0-9_$]*) |
    (?P<punct>[(),;=])
    """,
    re.VERBOSE,
)
_TSQL_STATEMENT_BOUNDARY_KEYWORDS: Final[frozenset[str]] = frozenset({
    "ALTER",
    "BEGIN",
    "BREAK",
    "CLOSE",
    "COMMIT",
    "CONTINUE",
    "CREATE",
    "DEALLOCATE",
    "DELETE",
    "DROP",
    "ELSE",
    "END",
    "EXEC",
    "EXECUTE",
    "FETCH",
    "GO",
    "IF",
    "INSERT",
    "MERGE",
    "OPEN",
    "PRINT",
    "RAISERROR",
    "RETURN",
    "ROLLBACK",
    "SAVE",
    "SELECT",
    "SET",
    "THROW",
    "TRUNCATE",
    "UPDATE",
    "USE",
    "WAITFOR",
    "WHILE",
    "WITH",
})
_TSQL_DECLARE_SCAN_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"""
    (?P<comment>--[^\r\n]* | /\*(?:[^*]|\*(?!/))*(?:\*/|\Z)) |
    "(?:""|[^"])*(?:"|\Z) |
    (?<![A-Za-z0-9_])E'(?:''|\\.|[^'\\])*(?:'|\\?\Z) |
    (?<![A-Za-z0-9_])N?Q'(?:
        \[[\s\S]*?(?:\]'|\Z) |
        \{{[\s\S]*?(?:\}}'|\Z) |
        \([\s\S]*?(?:\)'|\Z) |
        <[\s\S]*?(?:>'|\Z) |
        (?P<q_delimiter>[^\s\[{{(<'])[\s\S]*?(?:(?P=q_delimiter)'|\Z)
    ) |
    '(?:''|[^'])*(?:'|\Z) |
    \[(?:\]\]|[^\]])*(?:\]|\Z) |
    {_DOLLAR_QUOTED_PATTERN} |
    @@\w+ |
    \.[A-Za-z_][A-Za-z0-9_$]* |
    (?P<at_var>(?<![A-Za-z0-9_@])@(?!sqlspec_)\w+) |
    (?P<word>[A-Za-z_][A-Za-z0-9_$]*) |
    (?P<punct>[(),;]) |
    [^\s]
    """,
    _REGEX_FLAGS,
)


_SNOWFLAKE_PARAM_CHARS: Final[frozenset[str]] = frozenset("?%:")
_SNOWFLAKE_COLON_STYLES: Final[frozenset[ParameterStyle]] = frozenset({
    ParameterStyle.NAMED_COLON,
    ParameterStyle.POSITIONAL_COLON,
})
_SNOWFLAKE_SKIP_GROUPS: Final[frozenset[str]] = _SKIP_GROUPS | frozenset({
    "sf_slash_comment",
    "sf_user_stage",
    "sf_table_stage",
    "sf_named_stage",
    "sf_numeric_dollar",
    "sf_session_var",
})
_SNOWFLAKE_COMMENT_GROUPS: Final[frozenset[str]] = frozenset({"line_comment", "block_comment", "sf_slash_comment"})


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
            self._cache_hits += 1
            try:
                self._parameter_cache.move_to_end(cache_key)
            except KeyError:
                return cached_result
            return cached_result
        self._cache_misses += 1

        parameters = self._extract_parameters_uncached(sql, normalized_dialect)
        if len(self._parameter_cache) >= self._cache_max_size:
            with suppress(KeyError):
                self._parameter_cache.popitem(last=False)
        self._parameter_cache[cache_key] = parameters
        return parameters

    @staticmethod
    def _extract_parameter_style(match: re.Match[str]) -> "tuple[ParameterStyle | None, str | None]":
        """Map a regex match to a placeholder style and optional name."""
        lastgroup = match.lastgroup
        if lastgroup is None:
            return None, None
        entry = _PARAMETER_GROUP_DISPATCH.get(lastgroup)
        if entry is None:
            return None, None
        style, name_group = entry
        return style, (match.group(name_group) if name_group is not None else None)

    @staticmethod
    def _extract_snowflake_parameter_style(match: re.Match[str]) -> "tuple[ParameterStyle | None, str | None]":
        """Map the binding styles supported by the Snowflake lexical grammar."""
        lastgroup = match.lastgroup
        if lastgroup is None:
            return None, None
        entry = _SNOWFLAKE_GROUP_DISPATCH.get(lastgroup)
        if entry is None:
            return None, None
        style, name_group = entry
        return style, (match.group(name_group) if name_group is not None else None)

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
            lastgroup = match.lastgroup
            if lastgroup is None or lastgroup in _SNOWFLAKE_COMMENT_GROUPS:
                continue
            if lastgroup == "sf_word":
                preceding_operand = match.group(0).lower() not in _SNOWFLAKE_NON_OPERAND_KEYWORDS
                continue
            if lastgroup == "sf_other":
                other = match.group(0)
                preceding_operand = other in ")]" or other.isdigit()
                continue
            if lastgroup in _SNOWFLAKE_SKIP_GROUPS:
                preceding_operand = lastgroup != "pg_q_operator"
                continue
            entry = _SNOWFLAKE_GROUP_DISPATCH.get(lastgroup)
            if entry is None:
                continue
            style, name_group = entry
            if preceding_operand and style in _SNOWFLAKE_COLON_STYLES:
                continue
            preceding_operand = True
            name = match.group(name_group) if name_group is not None else None
            parameters.append(ParameterInfo(name, style, match.start(), len(parameters), match.group(0)))
        return parameters

    def _extract_parameters_uncached(self, sql: str, normalized_dialect: str | None = None) -> "list[ParameterInfo]":
        if normalized_dialect == _SCANNER_SNOWFLAKE:
            return self._extract_snowflake_parameters(sql)
        if not _PARAM_CHARS.intersection(sql):
            return []
        regex = _scanner_regex(normalized_dialect)
        skipped_at_positions: frozenset[int] | set[int] = _EMPTY_INT_FROZENSET
        if normalized_dialect == _SCANNER_MYSQL:
            skipped_at_positions = _mysql_variable_assignment_positions(sql)
        elif normalized_dialect == _SCANNER_TSQL:
            skipped_at_positions = _tsql_declared_variable_positions(sql)
        parameters: list[ParameterInfo] = []
        for match in _iter_matches(regex, sql, nested_comments=normalized_dialect == _SCANNER_NESTED):
            lastgroup = match.lastgroup
            if lastgroup is None:
                continue
            entry = _PARAMETER_GROUP_DISPATCH.get(lastgroup)
            if entry is None:
                continue
            style, name_group = entry
            if (
                style is ParameterStyle.QMARK
                and _OPERAND_START.match(sql, match.end())
                and _follows_operand(sql, match.start())
            ):
                continue
            if style is ParameterStyle.POSITIONAL_COLON and _is_positional_array_slice(sql, match.start()):
                continue
            if style is ParameterStyle.NAMED_AT and match.start() in skipped_at_positions:
                continue
            name = match.group(name_group) if name_group is not None else None
            parameters.append(ParameterInfo(name, style, match.start(), len(parameters), match.group(0)))
        return parameters


_QMARK_ESCAPE: Final[str] = "??"


def _scanner_regex(scanner: str | None) -> "re.Pattern[str]":
    """Return the compiled grammar for a resolved scanner key."""
    if scanner == _SCANNER_SNOWFLAKE:
        return SNOWFLAKE_PARAMETER_REGEX
    if scanner in {_SCANNER_ANSI, _SCANNER_NESTED}:
        return ANSI_PARAMETER_REGEX
    if scanner in {_SCANNER_BRACKET, _SCANNER_TSQL}:
        return BRACKET_PARAMETER_REGEX
    if scanner == _SCANNER_MYSQL:
        return MYSQL_PARAMETER_REGEX
    return PARAMETER_REGEX


def _nested_comment_end(sql: str, start: int) -> int:
    """Return the index after the block comment that opens at ``start``, honoring nesting.

    Args:
        sql: SQL text.
        start: Index of the opening ``/*``.

    Returns:
        Index just past the matching ``*/`` at depth zero, or ``len(sql)`` when unterminated.
    """
    depth = 0
    index = start
    length = len(sql)
    while index < length - 1:
        pair = sql[index : index + 2]
        if pair == "/*":
            depth += 1
            index += 2
        elif pair == "*/":
            depth -= 1
            index += 2
            if depth == 0:
                return index
        else:
            index += 1
    return length


def _iter_matches(regex: "re.Pattern[str]", sql: str, *, nested_comments: bool) -> "Iterator[re.Match[str]]":
    """Yield grammar matches, resuming after the full extent of nested block comments."""
    if not nested_comments:
        yield from regex.finditer(sql)
        return
    position = 0
    while True:
        match = regex.search(sql, position)
        if match is None:
            return
        yield match
        if match.lastgroup == "block_comment":
            position = max(match.end(), _nested_comment_end(sql, match.start()))
        else:
            position = match.end()


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
    regex = _scanner_regex(scanner)
    for match in _iter_matches(regex, sql, nested_comments=scanner == _SCANNER_NESTED):
        if match.lastgroup != "pg_q_operator" or match.group(0) != _QMARK_ESCAPE:
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


def _preceding_bracket_is_subscript(sql: str, bracket_pos: int) -> bool:
    idx = bracket_pos - 1
    while idx >= 0 and sql[idx].isspace():
        idx -= 1
    if idx < 0:
        return False
    ch = sql[idx]
    if ch in ")]":
        return True
    if ch.isalnum() or ch == "_":
        word_end = idx + 1
        while idx >= 0 and (sql[idx].isalnum() or sql[idx] == "_"):
            idx -= 1
        word = sql[idx + 1 : word_end].lower()
        if word == "array":
            while idx >= 0 and sql[idx].isspace():
                idx -= 1
            return idx >= 0 and sql[idx] == "."
        return word not in _NON_OPERAND_KEYWORDS
    return False


def _is_positional_array_slice(sql: str, colon_pos: int) -> bool:
    idx = colon_pos - 1
    paren_depth = 0
    while idx >= 0:
        ch = sql[idx]
        if ch == ")":
            paren_depth += 1
        elif ch == "(":
            if paren_depth == 0:
                return False
            paren_depth -= 1
        elif paren_depth == 0:
            if ch == "[":
                return _preceding_bracket_is_subscript(sql, idx)
            if ch in _ARRAY_SLICE_STOP_CHARS:
                return False
        idx -= 1
    return False


def _mysql_variable_assignment_positions(sql: str) -> "frozenset[int] | set[int]":
    if "@" not in sql or "=" not in sql:
        return _EMPTY_INT_FROZENSET
    lhs_positions: set[int] = set()
    stmt_keyword: str | None = None
    paren_depth = 0
    expecting_set_lhs = False
    prev_at_start: int | None = None
    is_candidate_set_lhs = False
    for match in _MYSQL_ASSIGN_SCAN_REGEX.finditer(sql):
        group_name = match.lastgroup
        lexeme = match.group(0)
        if lexeme == ":=":
            if prev_at_start is not None:
                lhs_positions.add(prev_at_start)
            prev_at_start = None
            is_candidate_set_lhs = False
            expecting_set_lhs = False
            continue
        if group_name == "at_var":
            prev_at_start = match.start()
            is_candidate_set_lhs = expecting_set_lhs and paren_depth == 0
            expecting_set_lhs = False
            continue
        if group_name == "punct":
            if lexeme == ";":
                stmt_keyword = None
                paren_depth = 0
                expecting_set_lhs = False
            elif lexeme == "(":
                paren_depth += 1
                expecting_set_lhs = False
            elif lexeme == ")":
                if paren_depth > 0:
                    paren_depth -= 1
                expecting_set_lhs = False
            elif lexeme == ",":
                expecting_set_lhs = paren_depth == 0 and stmt_keyword == "SET"
            elif lexeme == "=":
                if prev_at_start is not None and is_candidate_set_lhs:
                    lhs_positions.add(prev_at_start)
                expecting_set_lhs = False
            prev_at_start = None
            is_candidate_set_lhs = False
            continue
        if group_name == "word":
            upper = lexeme.upper()
            if upper in _MYSQL_SET_BLOCK_OPENERS and paren_depth == 0:
                stmt_keyword = None
                expecting_set_lhs = False
            elif stmt_keyword is None:
                stmt_keyword = upper
                expecting_set_lhs = upper == "SET"
            else:
                expecting_set_lhs = False
            prev_at_start = None
            is_candidate_set_lhs = False
    return lhs_positions


def _tsql_declared_variable_positions(sql: str) -> "frozenset[int] | set[int]":
    """Return character offsets of ``@var`` tokens bound by T-SQL ``DECLARE`` statements.

    Scans outside string literals, comments, and quoted or bracketed identifiers for
    ``DECLARE @var ...`` declarations and collects the start positions of each declared
    variable and its subsequent references so they are preserved rather than rewritten
    as bind placeholders.

    Args:
        sql: SQL text to scan.

    Returns:
        Offsets of ``@var`` occurrences belonging to declared local variables.
    """
    if "@" not in sql or "declare" not in sql.lower():
        return _EMPTY_INT_FROZENSET
    declared_vars: set[str] = set()
    skip_positions: set[int] = set()
    in_declare = False
    expecting_decl_var = False
    paren_depth = 0
    case_depth = 0
    for match in _TSQL_DECLARE_SCAN_REGEX.finditer(sql):
        group_name = match.lastgroup
        if group_name == "comment":
            continue
        if group_name == "at_var":
            var_name = match.group(0)[1:].lower()
            if in_declare and expecting_decl_var:
                declared_vars.add(var_name)
                skip_positions.add(match.start())
                expecting_decl_var = False
            elif var_name in declared_vars:
                skip_positions.add(match.start())
            continue
        if group_name == "word":
            upper = match.group(0).upper()
            if upper == "DECLARE" and paren_depth == 0:
                in_declare = True
                expecting_decl_var = True
                case_depth = 0
            elif expecting_decl_var:
                in_declare = False
                expecting_decl_var = False
                case_depth = 0
            elif in_declare and paren_depth == 0:
                if upper == "CASE":
                    case_depth += 1
                elif upper == "END" and case_depth > 0:
                    case_depth -= 1
                elif upper in _TSQL_STATEMENT_BOUNDARY_KEYWORDS and not (upper == "ELSE" and case_depth > 0):
                    in_declare = False
                    case_depth = 0
            continue
        if expecting_decl_var:
            in_declare = False
            expecting_decl_var = False
            case_depth = 0
        if group_name == "punct":
            lexeme = match.group(0)
            if lexeme == ";":
                in_declare = False
                paren_depth = 0
                case_depth = 0
            elif lexeme == "(":
                paren_depth += 1
            elif lexeme == ")":
                if paren_depth > 0:
                    paren_depth -= 1
            elif lexeme == "," and in_declare and paren_depth == 0 and case_depth == 0:
                expecting_decl_var = True
    return skip_positions


def _normalize_dialect(dialect: str | None) -> str | None:
    """Normalize an optional dialect identifier for lexical selection and caching."""
    if dialect is None:
        return None
    return dialect.strip().lower() or None


def _resolve_scanner(dialect: str | None) -> str | None:
    """Select the lexical grammar for a dialect.

    Dialects whose string literals treat backslashes literally use the ANSI
    grammar; SQLite and T-SQL-family dialects add bracket identifiers (with
    T-SQL also preserving declared local variables). MySQL-family dialects add
    ``#`` comments and backtick identifiers. Unknown dialects keep the
    permissive default grammar.
    """
    normalized = _normalize_dialect(dialect)
    if normalized is None:
        return None
    if normalized == _SCANNER_SNOWFLAKE:
        return _SCANNER_SNOWFLAKE
    if normalized in _NESTED_COMMENT_DIALECTS:
        return _SCANNER_NESTED
    if normalized in _TSQL_STRING_DIALECTS:
        return _SCANNER_TSQL
    if normalized in _BRACKET_STRING_DIALECTS:
        return _SCANNER_BRACKET
    if normalized in _ANSI_STRING_DIALECTS:
        return _SCANNER_ANSI
    if normalized in _BACKSLASH_STRING_DIALECTS:
        return _SCANNER_MYSQL
    return None
