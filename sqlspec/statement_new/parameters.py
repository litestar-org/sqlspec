"""High-performance SQL parameter conversion system."""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, Optional, Union

from typing_extensions import TypedDict

from sqlspec.typing import SQLParameterType

if TYPE_CHECKING:
    from sqlglot import exp

__all__ = (
    "ConvertedParameters",
    "ParameterHandler",
    "ParameterInfo",
    "ParameterStyle",
    "ParameterStyleTransformation",
    "SQLParameterType",
    "TypedParameter",
)

logger = logging.getLogger("sqlspec.sql.parameters")

_PARAMETER_REGEX: Final = re.compile(
    r"""
    (?P<dquote>"(?:[^"\\]|\\.)*") |
    (?P<squote>'(?:[^'\\]|\\.)*') |
    (?P<dollar_quoted_string>\$(?P<dollar_quote_tag_inner>\w*)?\$(?:.|\n)*?\$\4\$) |
    (?P<line_comment>--[^\r\n]*) |
    (?P<block_comment>/\*(?:[^\*]|\*(?!/))*\*/) |
    (?P<pg_cast>::(?P<cast_type>\w+)) |
    (?P<pyformat_named>%\((?P<pyformat_name>\w+)\)s) |
    (?P<pyformat_pos>%s) |
    (?P<positional_colon>:(?P<colon_num>\d+)) |
    (?P<named_colon>:(?P<colon_name>\w+)) |
    (?P<named_at>@(?P<at_name>\w+)) |
    (?P<named_dollar_param>\$(?P<dollar_param_name>\w+)) |
    (?P<pg_q_operator>\?\?|\?&) |
    (?P<qmark>\?)
    """,
    re.VERBOSE | re.IGNORECASE | re.MULTILINE | re.DOTALL,
)


class ParameterStyle(str, Enum):
    NONE = "none"
    STATIC = "static"
    QMARK = "qmark"
    NUMERIC = "numeric"
    NAMED_COLON = "named_colon"
    POSITIONAL_COLON = "positional_colon"
    NAMED_AT = "named_at"
    NAMED_DOLLAR = "named_dollar"
    NAMED_PYFORMAT = "pyformat_named"
    POSITIONAL_PYFORMAT = "pyformat_positional"

    def __str__(self) -> str:
        return self.value


SQLGLOT_INCOMPATIBLE_STYLES: Final = {
    ParameterStyle.POSITIONAL_PYFORMAT,
    ParameterStyle.NAMED_PYFORMAT,
    ParameterStyle.POSITIONAL_COLON,
}


@dataclass
class ParameterInfo:
    name: "Optional[str]"
    style: "ParameterStyle"
    position: int
    ordinal: int = field(compare=False)
    placeholder_text: str = field(compare=False)


@dataclass
class TypedParameter:
    value: Any
    sqlglot_type: "exp.DataType"
    type_hint: str
    semantic_name: "Optional[str]" = None


class TransformationInfo(TypedDict, total=False):
    was_transformed: bool
    placeholder_map: dict[str, Union[str, int]]
    original_styles: list[ParameterStyle]


@dataclass
class ParameterStyleTransformation:
    was_transformed: bool = False
    original_styles: list[ParameterStyle] = field(default_factory=list)
    transformed_style: Optional[ParameterStyle] = None
    placeholder_map: dict[str, Union[str, int]] = field(default_factory=dict)
    reverse_map: dict[Union[str, int], str] = field(default_factory=dict)
    original_param_info: list["ParameterInfo"] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.placeholder_map and not self.reverse_map:
            self.reverse_map = {v: k for k, v in self.placeholder_map.items()}


@dataclass
class ConvertedParameters:
    transformed_sql: str
    parameter_info: list["ParameterInfo"]
    merged_parameters: "SQLParameterType"
    transformation_state: ParameterStyleTransformation


@dataclass
class ParameterHandler:
    def __post_init__(self) -> None:
        self._parameter_cache: dict[str, list[ParameterInfo]] = {}

    def transform_sql_for_parsing(
        self, sql: str, parameters_info: list[ParameterInfo]
    ) -> tuple[str, ParameterStyleTransformation]:
        """Transforms SQL with incompatible parameter styles for SQLGlot parsing."""
        if not any(p.style in SQLGLOT_INCOMPATIBLE_STYLES for p in parameters_info):
            return sql, ParameterStyleTransformation()

        transformed_sql_parts = []
        placeholder_map: dict[str, Union[str, int]] = {}
        current_pos = 0
        for i, p_info in enumerate(parameters_info):
            transformed_sql_parts.append(sql[current_pos : p_info.position])
            unique_placeholder_name = f":param_{i}"
            map_key = f"param_{i}"
            if p_info.name:
                placeholder_map[map_key] = p_info.name
            else:
                placeholder_map[map_key] = p_info.ordinal
            transformed_sql_parts.append(unique_placeholder_name)
            current_pos = p_info.position + len(p_info.placeholder_text)
        transformed_sql_parts.append(sql[current_pos:])

        transformation = ParameterStyleTransformation(
            was_transformed=True,
            original_styles=list({p.style for p in parameters_info}),
            transformed_style=ParameterStyle.NAMED_COLON,
            placeholder_map=placeholder_map,
            original_param_info=parameters_info,
        )
        return "".join(transformed_sql_parts), transformation

    def transform_placeholder_style(
        self, sql: str, target_style: ParameterStyle, transformation_state: ParameterStyleTransformation
    ) -> str:
        """Transforms SQL placeholders to the target parameter style."""
        if not transformation_state.was_transformed:
            return sql

        result_parts = []
        current_pos = 0
        for p_info in transformation_state.original_param_info:
            result_parts.append(sql[current_pos : p_info.position])
            new_placeholder = self._get_placeholder_for_style(target_style, p_info)
            result_parts.append(new_placeholder)
            current_pos = p_info.position + len(p_info.placeholder_text)
        result_parts.append(sql[current_pos:])
        return "".join(result_parts)

    def extract_parameters(self, sql: str) -> "list[ParameterInfo]":
        if sql in self._parameter_cache:
            return self._parameter_cache[sql]

        parameters: list[ParameterInfo] = []
        ordinal = 0
        for match in _PARAMETER_REGEX.finditer(sql):
            param_info = self._create_parameter_info_from_match(match, ordinal)
            if param_info:
                parameters.append(param_info)
                ordinal += 1

        self._parameter_cache[sql] = parameters
        return parameters

    def _create_parameter_info_from_match(self, match: "re.Match[str]", ordinal: int) -> "Optional[ParameterInfo]":
        if (
            match.group("dquote")
            or match.group("squote")
            or match.group("dollar_quoted_string")
            or match.group("line_comment")
            or match.group("block_comment")
            or match.group("pg_cast")
        ):
            return None

        # Special handling for question marks - check if it's a PostgreSQL operator
        if match.group("pg_q_operator"):
            return None

        position = match.start()
        name: Optional[str] = None
        style: ParameterStyle

        if match.group("pyformat_named"):
            name = match.group("pyformat_name")
            style = ParameterStyle.NAMED_PYFORMAT
        elif match.group("pyformat_pos"):
            style = ParameterStyle.POSITIONAL_PYFORMAT
        elif match.group("positional_colon"):
            name = match.group("colon_num")
            style = ParameterStyle.POSITIONAL_COLON
        elif match.group("named_colon"):
            name = match.group("colon_name")
            style = ParameterStyle.NAMED_COLON
        elif match.group("named_at"):
            name = match.group("at_name")
            style = ParameterStyle.NAMED_AT
        elif match.group("named_dollar_param"):
            name_candidate = match.group("dollar_param_name")
            if not name_candidate.isdigit():
                name = name_candidate
                style = ParameterStyle.NAMED_DOLLAR
            else:
                name = name_candidate
                style = ParameterStyle.NUMERIC
        elif match.group("qmark"):
            style = ParameterStyle.QMARK
        else:
            logger.warning(
                "Unhandled SQL token pattern found by regex. Matched group: %s. Token: '%s'",
                match.lastgroup,
                match.group(0),
            )
            return None

        return ParameterInfo(name, style, position, ordinal, match.group(0))

    def _get_placeholder_for_style(self, target_style: ParameterStyle, param_info: ParameterInfo) -> str:
        """Generates a placeholder using a dispatch table for cleaner code."""
        style_map = {
            ParameterStyle.QMARK: "?",
            ParameterStyle.NUMERIC: f"${param_info.ordinal + 1}",
            ParameterStyle.NAMED_COLON: f":{param_info.name or f'param_{param_info.ordinal}'}",
            ParameterStyle.POSITIONAL_COLON: f":{param_info.ordinal + 1}",
            ParameterStyle.NAMED_AT: f"@{param_info.name or f'param_{param_info.ordinal}'}",
            ParameterStyle.NAMED_DOLLAR: f"${param_info.name or f'param_{param_info.ordinal}'}",
            ParameterStyle.NAMED_PYFORMAT: f"%({param_info.name or f'param_{param_info.ordinal}'})s",
            ParameterStyle.POSITIONAL_PYFORMAT: "%s",
        }
        return style_map.get(target_style, param_info.placeholder_text)

    def convert_parameters_direct(
        self,
        sql: str,
        from_style: ParameterStyle,
        to_style: ParameterStyle,
        use_sqlglot: bool = True,
    ) -> ConvertedParameters:
        """Converts parameters with an optional SQLGlot bypass."""
        parameters_info = self.extract_parameters(sql)

        if not use_sqlglot or not parameters_info:
            return self._convert_direct_regex(sql, parameters_info, to_style)

        # SQLGlot doesn't support parameter style conversion directly
        # So we always use regex-based conversion
        return self._convert_direct_regex(sql, parameters_info, to_style)

    def _convert_direct_regex(
        self,
        sql: str,
        parameters_info: list[ParameterInfo],
        target_style: ParameterStyle,
    ) -> ConvertedParameters:
        """Direct parameter conversion without SQLGlot."""
        transformed_sql_parts = []
        current_pos = 0
        for p_info in parameters_info:
            transformed_sql_parts.append(sql[current_pos : p_info.position])
            new_placeholder = self._get_placeholder_for_style(target_style, p_info)
            transformed_sql_parts.append(new_placeholder)
            current_pos = p_info.position + len(p_info.placeholder_text)
        transformed_sql_parts.append(sql[current_pos:])

        return ConvertedParameters(
            transformed_sql="".join(transformed_sql_parts),
            parameter_info=parameters_info,
            merged_parameters=None,  # Direct conversion doesn't handle merging
            transformation_state=ParameterStyleTransformation(was_transformed=True)
        )
