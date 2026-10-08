"""Shared property parsing for the Spanner GoogleSQL and PostgreSQL dialects.

Spanner DDL extensions are wired into sqlglot through ``PROPERTY_PARSERS``
dict entries: sqlglot invokes those with the parser as an explicit argument
(``PROPERTY_PARSERS[key](self)``), so the callables work identically under
pure-Python sqlglot and sqlglot[c]. Monkeypatching parser methods is not an
option: compiled parser internals dispatch through the native vtable and
never see a Python-level method override.

All clauses normalize to the canonical property nodes defined alongside the
generators, so either dialect can re-render them.
"""

from typing import Any, Final, cast

from sqlglot import exp
from sqlglot.parsers.bigquery import BigQueryParser
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokenizer_core import Token, TokenType

from sqlspec.dialects.spanner._expressions import (
    CosineDistance,
    DotProduct,
    EuclideanDistance,
    Search,
    SpannerGraphTable,
    SpannerPropertyGraph,
    get_next_sequence_value,
)
from sqlspec.dialects.spanner._generators import (
    _INTERLEAVE_IN_NAME,
    _INTERLEAVE_NAME,
    _ROW_DELETION_NAME,
    _normalize_interval_expression,
)

__all__ = (
    "SpangresParser",
    "SpannerParser",
    "attach_hints",
    "build_interleave_property",
    "normalize_spanner_tokens",
    "parse_hint_expression",
    "register_spanner_property_parsers",
)

_PROPERTY_PARSERS_REGISTERED_ATTR: Final[str] = "_sqlspec_spanner_property_parsers"
_WITH_ACTION_SENTINEL: Final[str] = "__sqlspec_then_return_with_action__"
_DML_STATEMENT_TOKENS: Final[frozenset[TokenType]] = frozenset({TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE})
_SPANNER_DIALECT_NAMES: Final[frozenset[str]] = frozenset({"Spangres", "Spanner"})


def build_interleave_property(parent: exp.Expr, on_delete: "str | None" = None, in_parent: bool = True) -> exp.Property:
    """Build the canonical interleave property node."""
    if not in_parent:
        return exp.Property(this=exp.Literal.string(_INTERLEAVE_IN_NAME), value=exp.Tuple(expressions=[parent]))
    values: list[exp.Expr] = [parent]
    if on_delete is not None:
        values.append(exp.Literal.string(_normalize_on_delete_value(on_delete)))
    return exp.Property(this=exp.Literal.string(_INTERLEAVE_NAME), value=exp.Tuple(expressions=values))


def _normalize_on_delete_value(on_delete: str) -> str:
    return " ".join(on_delete.upper().split())


def _build_row_deletion_property(column: exp.Expr, interval: exp.Expr) -> exp.Property:
    return exp.Property(this=exp.Literal.string(_ROW_DELETION_NAME), value=exp.Tuple(expressions=[column, interval]))


def _is_spanner_parser(parser: Any) -> bool:
    dialect = getattr(parser, "dialect", None)
    return dialect is not None and type(dialect).__name__ in _SPANNER_DIALECT_NAMES


def _parse_interleave(parser: Any) -> "exp.Property | None":
    """Parse ``INTERLEAVE IN [PARENT] table [ON DELETE {CASCADE | NO ACTION}]``.

    The INTERLEAVE token is already consumed by sqlglot's property dispatch.
    """
    if not parser._match_text_seq("IN"):
        parser._retreat(parser._index - 1)
        return None

    in_parent = bool(parser._match_text_seq("PARENT"))
    parent = cast("exp.Expr", parser._parse_table(schema=True, is_db_reference=True))
    on_delete: str | None = None

    if in_parent and parser._match_text_seq("ON", "DELETE"):
        if parser._match_text_seq("CASCADE"):
            on_delete = "CASCADE"
        elif parser._match_text_seq("NO", "ACTION"):
            on_delete = "NO ACTION"

    return build_interleave_property(parent, on_delete, in_parent=in_parent)


def _parse_row_deletion_policy(parser: Any) -> "exp.Property | None":
    """Parse ``ROW DELETION POLICY (OLDER_THAN(column, INTERVAL n DAY))``.

    The ROW token is already consumed by sqlglot's property dispatch.
    """
    if not parser._match_text_seq("DELETION", "POLICY"):
        parser._retreat(parser._index - 1)
        return None

    parser._match(TokenType.L_PAREN)
    parser._match_text_seq("OLDER_THAN")
    parser._match(TokenType.L_PAREN)
    column = cast("exp.Expr", parser._parse_id_var())
    parser._match(TokenType.COMMA)
    parser._match_text_seq("INTERVAL")
    interval = _normalize_interval_expression(cast("exp.Expr", parser._parse_expression()))
    parser._match(TokenType.R_PAREN)
    parser._match(TokenType.R_PAREN)

    return _build_row_deletion_property(column, interval)


def _parse_ttl(parser: Any) -> "exp.Property | None":
    """Parse ``TTL INTERVAL interval_spec ON column`` into the canonical policy node.

    The TTL token is already consumed by sqlglot's property dispatch.
    """
    parser._match_text_seq("INTERVAL")
    interval = _normalize_interval_expression(cast("exp.Expr", parser._parse_expression()))
    parser._match_text_seq("ON")
    column = cast("exp.Expr", parser._parse_id_var())

    return _build_row_deletion_property(column, interval)


def _parse_primary_key_property(parser: Any) -> Any:
    """Parse table-level ``PRIMARY KEY (col [ASC|DESC], ...)`` property in Spanner DDL."""
    if not parser._match(TokenType.L_PAREN, advance=False):
        return parser._parse_primary_key(in_props=True)
    expressions = parser._parse_wrapped_csv(lambda: parser._parse_ordered(parser._parse_field))
    return parser.expression(
        exp.PrimaryKey(
            expressions=expressions,
            options=parser._parse_key_constraint_options(),
            include=parser._parse_index_params(),
        )
    )


def _build_property_entry(handler: Any, original: Any) -> Any:
    def _entry(parser: Any, **kwargs: Any) -> Any:
        if _is_spanner_parser(parser):
            return handler(parser)
        if original is not None:
            return original(parser, **kwargs)
        parser._retreat(parser._index - 1)
        return None

    return _entry


def _parse_get_next_sequence_value(parser: Any) -> exp.Anonymous:
    """Parse GET_NEXT_SEQUENCE_VALUE(SEQUENCE sequence_name)."""
    if _is_spanner_parser(parser):
        parser._match_text_seq("SEQUENCE")
        seq_name = cast("exp.Expr", parser._parse_table_parts(schema=True))
        return get_next_sequence_value(seq_name)
    return exp.Anonymous(this="GET_NEXT_SEQUENCE_VALUE", expressions=parser._parse_csv(parser._parse_lambda))


def _build_search(args: list[Any], dialect: Any) -> exp.Expr:
    """Build a Spanner Search node or fall back to Anonymous for BigQuery."""
    if dialect is not None and type(dialect).__name__ == "Spanner":
        return Search.from_arg_list(args)
    return exp.Anonymous(this="SEARCH", expressions=args)


def _build_cosine_distance(args: list[Any], dialect: Any) -> exp.Expr:
    """Build a Spanner/Spangres CosineDistance node or fall back to Anonymous."""
    if dialect is not None and type(dialect).__name__ in _SPANNER_DIALECT_NAMES:
        return CosineDistance.from_arg_list(args)
    return exp.Anonymous(this="COSINE_DISTANCE", expressions=args)


def _build_euclidean_distance(args: list[Any], dialect: Any) -> exp.Expr:
    """Build a Spanner/Spangres EuclideanDistance node or fall back to Anonymous."""
    if dialect is not None and type(dialect).__name__ in _SPANNER_DIALECT_NAMES:
        return EuclideanDistance.from_arg_list(args)
    return exp.Anonymous(this="EUCLIDEAN_DISTANCE", expressions=args)


def _build_dot_product(args: list[Any], dialect: Any) -> exp.Expr:
    """Build a Spanner/Spangres DotProduct node or fall back to Anonymous."""
    if dialect is not None and type(dialect).__name__ in _SPANNER_DIALECT_NAMES:
        return DotProduct.from_arg_list(args)
    return exp.Anonymous(this="DOT_PRODUCT", expressions=args)


def _convert_userdefined_type(data_type: exp.DataType) -> exp.DataType:
    """Assign explicit TOKENLIST kind to unparameterized USERDEFINED data types in Spanner."""
    if not data_type.args.get("kind"):
        data_type.set("kind", "TOKENLIST")
    return data_type


def _parse_options_properties(parser: Any) -> exp.Properties | None:
    """Parse an OPTIONS (key = value, ...) property block."""
    if not parser._match_text_seq("OPTIONS"):
        return None
    parser._match(TokenType.L_PAREN)
    props = parser._parse_csv(parser._parse_property)
    parser._match(TokenType.R_PAREN)
    return exp.Properties(expressions=props)


def _parse_create_vector_index(parser: Any) -> exp.Index:
    """Parse CREATE VECTOR INDEX [IF NOT EXISTS] name ON table (cols) [STORING (cols)] [WHERE ...] [OPTIONS (...)]."""
    exists = parser._parse_exists(not_=True)
    name = parser._parse_id_var()
    parser._match_text_seq("ON")
    table = exp.Table(this=parser._parse_id_var())
    parser._match(TokenType.L_PAREN)
    cols = parser._parse_csv(parser._parse_column)
    parser._match(TokenType.R_PAREN)
    storing: list[exp.Expr] | None = None
    if parser._match_text_seq("STORING"):
        parser._match(TokenType.L_PAREN)
        storing = parser._parse_csv(parser._parse_column)
        parser._match(TokenType.R_PAREN)
    where = parser._parse_where()
    options = _parse_options_properties(parser)
    params = exp.IndexParameters(columns=cols)
    idx = exp.Index(this=name, table=table, params=params, where=where, kind="VECTOR")
    if exists:
        idx.set("exists", True)
    if storing:
        idx.set("storing", storing)
    if options:
        idx.set("options", options)
    return idx


def _parse_create_search_index(parser: Any) -> exp.Index:
    """Parse CREATE SEARCH INDEX [IF NOT EXISTS] name ON table (cols) [STORING (cols)] [PARTITION BY cols] [ORDER BY order] [OPTIONS (...)]."""
    exists = parser._parse_exists(not_=True)
    name = parser._parse_id_var()
    parser._match_text_seq("ON")
    table = exp.Table(this=parser._parse_id_var())
    parser._match(TokenType.L_PAREN)
    cols = parser._parse_csv(parser._parse_column)
    parser._match(TokenType.R_PAREN)
    storing: list[exp.Expr] | None = None
    if parser._match_text_seq("STORING"):
        parser._match(TokenType.L_PAREN)
        storing = parser._parse_csv(parser._parse_column)
        parser._match(TokenType.R_PAREN)
    partition_by: list[exp.Expr] | None = None
    if parser._match(TokenType.PARTITION_BY) or parser._match_text_seq("PARTITION", "BY"):
        partition_by = parser._parse_csv(parser._parse_column)
    order = parser._parse_order()
    options = _parse_options_properties(parser)
    params = exp.IndexParameters(columns=cols)
    idx = exp.Index(this=name, table=table, params=params, kind="SEARCH")
    if exists:
        idx.set("exists", True)
    if storing:
        idx.set("storing", storing)
    if partition_by:
        idx.set("partition_by", partition_by)
    if order:
        idx.set("order", order)
    if options:
        idx.set("options", options)
    return idx


def _parse_create_sequence(parser: Any) -> exp.Create:
    """Parse CREATE SEQUENCE name [OPTIONS (...)]."""
    exists = parser._parse_exists(not_=True)
    name = parser._parse_table_parts(schema=True)
    options = _parse_options_properties(parser)
    return exp.Create(this=name, kind="SEQUENCE", exists=exists, properties=options)


def _parse_alter_sequence(parser: Any) -> exp.Alter:
    """Parse ALTER SEQUENCE name SET OPTIONS (...)."""
    name = parser._parse_table_parts(schema=True)
    options: exp.Properties | None = None
    if parser._match_text_seq("SET", "OPTIONS") or parser._match_text_seq("OPTIONS"):
        parser._retreat(parser._index - 1)
        options = _parse_options_properties(parser)
    return exp.Alter(this=name, kind="SEQUENCE", options=options)


def _parse_create_change_stream(parser: Any) -> exp.Create:
    """Parse CREATE CHANGE STREAM name [FOR ...] [OPTIONS (...)]."""
    name = parser._parse_id_var()
    for_expressions: list[exp.Expr] = []
    if parser._match_text_seq("FOR"):
        if parser._match_text_seq("ALL"):
            for_expressions.append(exp.var("ALL"))
        else:

            def _parse_stream_target() -> exp.Expr:
                target_table = cast("exp.Expr", parser._parse_id_var())
                if parser._match(TokenType.L_PAREN):
                    cols = parser._parse_csv(parser._parse_column)
                    parser._match(TokenType.R_PAREN)
                    return exp.Anonymous(this=target_table.name, expressions=cols)
                return target_table

            targets = parser._parse_csv(_parse_stream_target)
            for_expressions.extend(targets)
    options = _parse_options_properties(parser)
    return exp.Create(this=name, kind="CHANGE STREAM", expressions=for_expressions, properties=options)


def _parse_alter_change_stream(parser: Any) -> exp.Alter:
    """Parse ALTER CHANGE STREAM name [SET ...] [OPTIONS (...)]."""
    name = parser._parse_id_var()
    options: exp.Properties | None = None
    if parser._match_text_seq("SET", "OPTIONS") or parser._match_text_seq("OPTIONS"):
        parser._retreat(parser._index - 1)
        options = _parse_options_properties(parser)
    return exp.Alter(this=name, kind="CHANGE STREAM", options=options)


def _parse_drop_spanner_object(parser: Any, kind: str) -> exp.Drop:
    """Parse DROP <kind> [IF EXISTS] name."""
    exists = parser._parse_exists()
    name = parser._parse_table_parts(schema=True)
    return exp.Drop(this=name, kind=kind, exists=exists)


def _parse_graph_element_list(parser: Any) -> list[exp.Expr]:
    """Parse comma-separated graph node/edge table definitions inside NODE/EDGE TABLES (...)."""
    parser._match(TokenType.L_PAREN)
    elements: list[exp.Expr] = []
    while parser._curr is not None and parser._curr.token_type != TokenType.R_PAREN:
        start_token = parser._curr
        end_token = start_token
        depth = 0
        while parser._curr is not None:
            curr = parser._curr
            tt = curr.token_type
            if depth == 0 and tt in {TokenType.COMMA, TokenType.R_PAREN}:
                break
            if tt in {TokenType.L_PAREN, TokenType.L_BRACKET, TokenType.L_BRACE}:
                depth += 1
            elif tt in {TokenType.R_PAREN, TokenType.R_BRACKET, TokenType.R_BRACE}:
                depth = max(0, depth - 1)
            end_token = curr
            parser._advance()
        raw_elem = parser._find_sql(start_token, end_token).strip()
        if raw_elem:
            elements.append(exp.var(" ".join(raw_elem.split())))
        if not parser._match(TokenType.COMMA):
            break
    parser._match(TokenType.R_PAREN)
    return elements


def _parse_create_property_graph(parser: Any, *, replace: bool = False) -> exp.Create:
    """Parse CREATE [OR REPLACE] PROPERTY GRAPH [IF NOT EXISTS] name NODE TABLES (...) [EDGE TABLES (...)]."""
    exists = parser._parse_exists(not_=True)
    name = parser._parse_id_var()
    node_tables: list[exp.Expr] = []
    if parser._match_text_seq("NODE", "TABLES"):
        node_tables = _parse_graph_element_list(parser)
    edge_tables: list[exp.Expr] | None = None
    if parser._match_text_seq("EDGE", "TABLES"):
        edge_tables = _parse_graph_element_list(parser)
    graph = SpannerPropertyGraph(this=name, node_tables=node_tables, edge_tables=edge_tables)
    return exp.Create(this=graph, kind="PROPERTY GRAPH", exists=exists, replace=replace)


_original_bq_function_graph_table: Any = BigQueryParser.FUNCTION_PARSERS.get("GRAPH_TABLE")


def _parse_graph_table(parser: Any) -> SpannerGraphTable | exp.Expr:
    """Parse GRAPH_TABLE(graph MATCH pattern [WHERE expr] COLUMNS (col_expr [AS alias], ...))."""
    if not _is_spanner_parser(parser):
        if _original_bq_function_graph_table is not None:
            return cast("exp.Expr", _original_bq_function_graph_table(parser))
        return exp.Anonymous(this="GRAPH_TABLE", expressions=parser._parse_csv(parser._parse_lambda))
    graph = cast("exp.Expr", parser._parse_id_var())
    parser._match_text_seq("MATCH")
    start_token = parser._curr
    end_token = start_token
    depth = 0
    while parser._curr is not None:
        curr = parser._curr
        tt = curr.token_type
        if depth == 0 and (tt in {TokenType.R_PAREN, TokenType.WHERE} or curr.text.upper() in {"WHERE", "COLUMNS"}):
            break
        if tt in {TokenType.L_PAREN, TokenType.L_BRACKET, TokenType.L_BRACE}:
            depth += 1
        elif tt in {TokenType.R_PAREN, TokenType.R_BRACKET, TokenType.R_BRACE}:
            depth = max(0, depth - 1)
        end_token = curr
        parser._advance()
    match_sql = parser._find_sql(start_token, end_token).strip() if start_token and end_token else ""
    match_expr = exp.var(" ".join(match_sql.split()))
    where_expr: exp.Expr | None = None
    if parser._match(TokenType.WHERE) or parser._match_text_seq("WHERE"):
        where_expr = cast("exp.Expr", parser._parse_conjunction())
    columns: list[exp.Expr] = []
    if parser._match_text_seq("COLUMNS"):
        columns = parser._parse_wrapped_csv(lambda: parser._parse_alias(parser._parse_conjunction()))
    return SpannerGraphTable(this=graph, match=match_expr, where=where_expr, columns=columns)


def _closing_brace_index(tokens: "list[Token]", start: int) -> int:
    """Return the index of the matching closing brace token, or -1 if unclosed."""
    depth = 1
    for position in range(start, len(tokens)):
        token_type = tokens[position].token_type
        if token_type == TokenType.L_BRACE:
            depth += 1
        elif token_type == TokenType.R_BRACE:
            depth -= 1
            if depth == 0:
                return position
    return -1


def normalize_spanner_tokens(tokens: "list[Token]", sql: str = "") -> "list[Token]":
    """Convert Spanner GoogleSQL ``@{...}`` hint token sequences into token comments.

    Token-level normalization ensures ``@{...}`` inside string literals or SQL
    comments is left untouched while statement, table, and join hints attach to
    the appropriate token's ``comments`` list for ``attach_hints`` to consume.

    Args:
        tokens: Tokens produced by the base BigQuery tokenizer.
        sql: Original SQL text used to extract raw hint body substrings.

    Returns:
        Normalized token list with ``@{...}`` sequences folded into comments.
    """
    result: list[Token] = []
    pending_comments: list[str] = []
    index = 0
    total = len(tokens)
    while index < total:
        token = tokens[index]
        if (
            token.token_type == TokenType.PARAMETER
            and token.text == "@"
            and index + 1 < total
            and tokens[index + 1].token_type == TokenType.L_BRACE
            and (not sql or token.end + 1 == tokens[index + 1].start)
        ):
            l_brace = tokens[index + 1]
            end_idx = _closing_brace_index(tokens, index + 2)
            if end_idx >= 0:
                r_brace = tokens[end_idx]
                if sql and r_brace.start > l_brace.end:
                    hint_body = sql[l_brace.end + 1 : r_brace.start].strip()
                else:
                    hint_body = "".join(
                        ", " if part.token_type == TokenType.COMMA else part.text
                        for part in tokens[index + 2 : end_idx]
                    ).strip()
                collected_comments = [comment for part in tokens[index : end_idx + 1] for comment in part.comments]
                if hint_body:
                    collected_comments.append(f"@ {hint_body}")
                if result and result[-1].token_type not in {TokenType.SEMICOLON, TokenType.L_PAREN}:
                    result[-1].comments.extend(collected_comments)
                else:
                    pending_comments.extend(collected_comments)
                index = end_idx + 1
                continue
        if pending_comments:
            token.comments = [*pending_comments, *token.comments]
            pending_comments = []
        result.append(token)
        index += 1
    return _collapse_then_return(result)


def _is_word(token: "Token", word: str) -> bool:
    return token.token_type in {TokenType.VAR, TokenType.IDENTIFIER} and token.text.upper() == word


def _collapse_then_return(tokens: "list[Token]") -> "list[Token]":
    """Fold top-level DML ``THEN RETURN [WITH ACTION [AS alias]]`` into a ``RETURNING`` clause.

    ``WITH ACTION`` becomes a leading sentinel column, followed by a comma, that
    ``attach_hints`` moves onto the parsed ``Returning`` node.

    Args:
        tokens: Tokens with Spanner hints already normalized.

    Returns:
        Tokens where each DML ``THEN RETURN`` is a single ``RETURNING`` token.
    """
    result: list[Token] = []
    statement_kind: TokenType | None = None
    depth = 0
    index = 0
    total = len(tokens)
    while index < total:
        token = tokens[index]
        token_type = token.token_type
        if token_type == TokenType.SEMICOLON:
            statement_kind = None
            depth = 0
        elif statement_kind is None:
            statement_kind = token_type
        if token_type == TokenType.L_PAREN:
            depth += 1
        elif token_type == TokenType.R_PAREN:
            depth -= 1
        if (
            token_type == TokenType.THEN
            and depth == 0
            and statement_kind in _DML_STATEMENT_TOKENS
            and index + 1 < total
            and _is_word(tokens[index + 1], "RETURN")
        ):
            return_token = tokens[index + 1]
            result.append(
                Token(
                    TokenType.RETURNING,
                    "THEN RETURN",
                    token.line,
                    token.col,
                    token.start,
                    return_token.end,
                    [*token.comments, *return_token.comments],
                )
            )
            index += 2
            if (
                index + 1 < total
                and tokens[index].token_type == TokenType.WITH
                and _is_word(tokens[index + 1], "ACTION")
            ):
                action = tokens[index + 1]
                result.append(
                    Token(TokenType.VAR, _WITH_ACTION_SENTINEL, action.line, action.col, action.start, action.end)
                )
                index += 2
                if index + 1 < total and tokens[index].token_type == TokenType.ALIAS:
                    result.extend(tokens[index : index + 2])
                    index += 2
                result.append(Token(TokenType.COMMA, ",", action.line, action.col, action.end, action.end))
            continue
        result.append(token)
        index += 1
    return result


def _attach_then_return_action(returning: exp.Returning) -> None:
    expressions = list(returning.expressions)
    if not expressions:
        return
    first = expressions[0]
    column = first.this if isinstance(first, exp.Alias) else first
    if not isinstance(column, exp.Column) or column.name != _WITH_ACTION_SENTINEL:
        return
    returning.set("expressions", expressions[1:])
    returning.set("with_action", first.args["alias"] if isinstance(first, exp.Alias) else exp.true())


def _attach_hidden_column_constraint(column_def: exp.ColumnDef) -> None:
    constraints = list(column_def.args.get("constraints") or [])
    if not constraints:
        return
    computed: exp.ComputedColumnConstraint | None = None
    remaining: list[exp.Expr] = []
    has_hidden = False
    for constraint in constraints:
        kind = constraint.kind if isinstance(constraint, exp.ColumnConstraint) else None
        if isinstance(kind, exp.ComputedColumnConstraint):
            computed = kind
            remaining.append(constraint)
        elif isinstance(kind, exp.Var) and kind.name.upper() == "HIDDEN":
            has_hidden = True
        else:
            remaining.append(constraint)
    if has_hidden and computed is not None:
        computed.set("hidden", True)
        column_def.set("constraints", remaining)


def parse_hint_expression(raw_hint: str) -> exp.Hint:
    """Parse hint string into canonical exp.Hint."""
    raw = raw_hint.strip()
    if raw.startswith("@"):
        raw = raw[1:].strip()
    pairs = [p.strip() for p in raw.split(",") if p.strip()]
    exprs: list[exp.Expr] = []
    for pair in pairs:
        if "=" in pair:
            k, v = pair.split("=", 1)
            v_str = v.strip()
            val_expr: exp.Expr = exp.Literal.number(int(v_str)) if v_str.isdigit() else exp.var(v_str)
            exprs.append(exp.EQ(this=exp.to_identifier(k.strip()), expression=val_expr))
        else:
            exprs.append(exp.to_identifier(pair.strip()))
    return exp.Hint(expressions=exprs)


def attach_hints(expression: exp.Expr) -> None:
    """Attach parsed hints from node comments to AST nodes and resolve ``THEN RETURN WITH ACTION``."""
    for node in expression.walk():
        if isinstance(node, exp.Returning):
            _attach_then_return_action(node)
        elif isinstance(node, exp.ColumnDef):
            _attach_hidden_column_constraint(node)
        comments = getattr(node, "comments", None)
        if not comments:
            continue
        hint_comments = [c for c in comments if c.strip().startswith("@") and "=" in c.strip()[1:]]
        for hc in hint_comments:
            hint = parse_hint_expression(hc)
            target_table: exp.Table | None = None
            if isinstance(node, exp.Table):
                target_table = node
            elif isinstance(node, exp.TableAlias) and isinstance(node.parent, exp.Table):
                target_table = node.parent
            if target_table is not None:
                comments.remove(hc)
                existing_hints = list(target_table.args.get("hints") or [])
                existing_hints.append(hint)
                target_table.set("hints", existing_hints)
            elif isinstance(node, (exp.Select, exp.Query)):
                comments.remove(hc)
                node.set("hint", hint)
            elif isinstance(node, exp.Join):
                comments.remove(hc)
                node.set("spanner_hint", hint)


_original_bq_statement_create: Any = BigQueryParser.STATEMENT_PARSERS.get(TokenType.CREATE)
_original_bq_statement_alter: Any = BigQueryParser.STATEMENT_PARSERS.get(TokenType.ALTER)
_original_bq_statement_drop: Any = BigQueryParser.STATEMENT_PARSERS.get(TokenType.DROP)
_original_bq_constraint_hidden: Any = BigQueryParser.CONSTRAINT_PARSERS.get("HIDDEN")


def _parse_hidden_column_constraint(parser: Any) -> exp.Expr | None:
    """Parse trailing HIDDEN modifier on Spanner generated/tokenlist columns."""
    if _is_spanner_parser(parser):
        return exp.var("HIDDEN")
    if _original_bq_constraint_hidden is not None:
        return cast("exp.Expr | None", _original_bq_constraint_hidden(parser))
    return None


def _bq_parse_create(self: Any) -> exp.Create | exp.Index | exp.Command:
    """Parse Spanner CREATE statements including VECTOR INDEX, SEARCH INDEX, SEQUENCE, CHANGE STREAM, and PROPERTY GRAPH."""
    dialect = getattr(self, "dialect", None)
    if dialect is not None and type(dialect).__name__ == "Spanner":
        if self._match_text_seq("VECTOR", "INDEX"):
            return _parse_create_vector_index(self)
        if self._match_text_seq("SEARCH", "INDEX"):
            return _parse_create_search_index(self)
        if self._match_text_seq("SEQUENCE"):
            return _parse_create_sequence(self)
        if self._match_text_seq("CHANGE", "STREAM"):
            return _parse_create_change_stream(self)
        if self._match_text_seq("PROPERTY", "GRAPH"):
            return _parse_create_property_graph(self, replace=False)
        if self._match_text_seq("OR", "REPLACE", "PROPERTY", "GRAPH"):
            return _parse_create_property_graph(self, replace=True)
    if _original_bq_statement_create is not None:
        return cast("exp.Create | exp.Index | exp.Command", _original_bq_statement_create(self))
    return cast("exp.Create | exp.Index | exp.Command", self._parse_create())


def _bq_parse_alter(self: Any) -> exp.Alter | exp.Command:
    """Parse Spanner ALTER statements including SEQUENCE and CHANGE STREAM."""
    dialect = getattr(self, "dialect", None)
    if dialect is not None and type(dialect).__name__ == "Spanner":
        if self._match_text_seq("SEQUENCE"):
            return _parse_alter_sequence(self)
        if self._match_text_seq("CHANGE", "STREAM"):
            return _parse_alter_change_stream(self)
    if _original_bq_statement_alter is not None:
        return cast("exp.Alter | exp.Command", _original_bq_statement_alter(self))
    return cast("exp.Alter | exp.Command", self._parse_alter())


def _bq_parse_drop(self: Any) -> exp.Drop | exp.Command:
    """Parse Spanner DROP statements including CHANGE STREAM, PROPERTY GRAPH, VECTOR INDEX, and SEARCH INDEX."""
    dialect = getattr(self, "dialect", None)
    if dialect is not None and type(dialect).__name__ == "Spanner":
        if self._match_text_seq("CHANGE", "STREAM"):
            return _parse_drop_spanner_object(self, "CHANGE STREAM")
        if self._match_text_seq("PROPERTY", "GRAPH"):
            return _parse_drop_spanner_object(self, "PROPERTY GRAPH")
        if self._match_text_seq("VECTOR", "INDEX"):
            return _parse_drop_spanner_object(self, "VECTOR INDEX")
        if self._match_text_seq("SEARCH", "INDEX"):
            return _parse_drop_spanner_object(self, "SEARCH INDEX")
    if _original_bq_statement_drop is not None:
        return cast("exp.Drop | exp.Command", _original_bq_statement_drop(self))
    return cast("exp.Drop | exp.Command", self._parse_drop())


def register_spanner_property_parsers() -> None:
    """Install Spanner parser extensions on the BigQuery and Postgres parser classes."""
    for parser_class in (BigQueryParser, PostgresParser):
        if getattr(parser_class, _PROPERTY_PARSERS_REGISTERED_ATTR, False):
            continue
        property_parsers: dict[str, Any] = dict(parser_class.PROPERTY_PARSERS)
        for key, handler in (
            ("INTERLEAVE", _parse_interleave),
            ("PRIMARY KEY", _parse_primary_key_property),
            ("ROW", _parse_row_deletion_policy),
            ("TTL", _parse_ttl),
        ):
            property_parsers[key] = _build_property_entry(handler, property_parsers.get(key))
        setattr(parser_class, "PROPERTY_PARSERS", property_parsers)
        if parser_class is BigQueryParser:
            BigQueryParser.FUNCTIONS["COSINE_DISTANCE"] = _build_cosine_distance
            BigQueryParser.FUNCTIONS["EUCLIDEAN_DISTANCE"] = _build_euclidean_distance
            BigQueryParser.FUNCTIONS["DOT_PRODUCT"] = _build_dot_product
            BigQueryParser.FUNCTIONS["SEARCH"] = _build_search
            BigQueryParser.FUNCTION_PARSERS["GET_NEXT_SEQUENCE_VALUE"] = _parse_get_next_sequence_value
            BigQueryParser.FUNCTION_PARSERS["GRAPH_TABLE"] = _parse_graph_table
            BigQueryParser.CONSTRAINT_PARSERS["HIDDEN"] = _parse_hidden_column_constraint
            BigQueryParser.TYPE_CONVERTERS = {
                **BigQueryParser.TYPE_CONVERTERS,
                exp.DataType.Type.USERDEFINED: _convert_userdefined_type,
            }
            BigQueryParser.STATEMENT_PARSERS[TokenType.CREATE] = _bq_parse_create
            BigQueryParser.STATEMENT_PARSERS[TokenType.ALTER] = _bq_parse_alter
            BigQueryParser.STATEMENT_PARSERS[TokenType.DROP] = _bq_parse_drop
        elif parser_class is PostgresParser:
            PostgresParser.FUNCTIONS["COSINE_DISTANCE"] = _build_cosine_distance
            PostgresParser.FUNCTIONS["EUCLIDEAN_DISTANCE"] = _build_euclidean_distance
            PostgresParser.FUNCTIONS["DOT_PRODUCT"] = _build_dot_product
            PostgresParser.FUNCTION_PARSERS["GET_NEXT_SEQUENCE_VALUE"] = _parse_get_next_sequence_value
        setattr(parser_class, _PROPERTY_PARSERS_REGISTERED_ATTR, True)


register_spanner_property_parsers()

SpannerParser = BigQueryParser
SpangresParser = PostgresParser
