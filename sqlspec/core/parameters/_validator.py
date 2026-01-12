"""Parameter extraction utilities."""

import re
from collections import OrderedDict

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters._types import ParameterInfo, ParameterStyle

__all__ = ("PARAMETER_REGEX", "ParameterValidator")

PARAMETER_REGEX = re.compile(
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

    def _extract_parameter_style(self, match: re.Match[str]) -> "tuple[ParameterStyle | None, str | None]":
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

        cached_result = self._parameter_cache.get(sql)
        if cached_result is not None:
            self._parameter_cache.move_to_end(sql)
            self._cache_hits += 1
            return cached_result
        self._cache_misses += 1

        if not any(c in sql for c in ("?", "%", ":", "@", "$")):
            if len(self._parameter_cache) >= self._cache_max_size:
                self._parameter_cache.popitem(last=False)
            self._parameter_cache[sql] = []
            return []

        parameters: list[ParameterInfo] = []
        ordinal = 0

        skip_groups = (
            "dquote",
            "squote",
            "dollar_quoted_string",
            "line_comment",
            "block_comment",
            "pg_q_operator",
            "pg_cast",
            "sql_server_global",
        )

        for match in PARAMETER_REGEX.finditer(sql):
            if any(match.group(group) for group in skip_groups):
                continue
            style, name = self._extract_parameter_style(match)
            if style is None:
                continue
            placeholder_text = match.group(0)
            parameters.append(ParameterInfo(name, style, match.start(), ordinal, placeholder_text))
            ordinal += 1

        if len(self._parameter_cache) >= self._cache_max_size:
            self._parameter_cache.popitem(last=False)
        self._parameter_cache[sql] = parameters
        return parameters

    def _extract_parameters_uncached(self, sql: str) -> "list[ParameterInfo]":
        parameters: list[ParameterInfo] = []
        ordinal = 0

        skip_groups = (
            "dquote",
            "squote",
            "dollar_quoted_string",
            "line_comment",
            "block_comment",
            "pg_q_operator",
            "pg_cast",
            "sql_server_global",
        )

        if not any(c in sql for c in ("?", "%", ":", "@", "$")):
            return []

        for match in PARAMETER_REGEX.finditer(sql):
            if any(match.group(group) for group in skip_groups):
                continue
            style, name = self._extract_parameter_style(match)
            if style is None:
                continue
            placeholder_text = match.group(0)
            parameters.append(ParameterInfo(name, style, match.start(), ordinal, placeholder_text))
            ordinal += 1
        return parameters

    def get_sqlglot_incompatible_styles(self, dialect: str | None = None) -> "set[ParameterStyle]":
        """Return placeholder styles incompatible with SQLGlot for the dialect."""
        base_incompatible = {
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.POSITIONAL_COLON,
        }

        if dialect and dialect.lower() in {"mysql", "mariadb"}:
            return base_incompatible
        if dialect and dialect.lower() in {"postgres", "postgresql"}:
            return {ParameterStyle.POSITIONAL_COLON}
        if dialect and dialect.lower() == "sqlite":
            return {ParameterStyle.POSITIONAL_COLON}
        if dialect and dialect.lower() in {"oracle", "bigquery"}:
            return base_incompatible
        return base_incompatible
