"""Migration squash engine for combining multiple migrations into a single file.

This module provides utilities to consolidate multiple sequential migrations
into a single "release" migration file, following the Django-style squash workflow.

Three strategies are available:

- ``concat`` (default) keeps every original statement verbatim, in order, and
  concatenates the original down migrations in reverse order.
- ``ddl`` folds the chronological DDL into net ``CREATE`` statements. It refuses
  with :class:`~sqlspec.exceptions.SquashValidationError` whenever the folded
  output could not be faithful to sequential application.
- ``schema`` emits DDL reflected from the live database data dictionary.
"""

import re
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import ErrorLevel, ParseError, TokenError, UnsupportedError
from sqlglot.tokenizer_core import Token, TokenType

from sqlspec.data_dictionary._types import DDLResult
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SquashValidationError
from sqlspec.migrations._backup import create_backup, remove_backup, restore_backup
from sqlspec.migrations.loaders import _load_migration_sql
from sqlspec.migrations.validation import (
    validate_extension_consistency,
    validate_squash_idempotency,
    validate_squash_range,
)
from sqlspec.migrations.version import _format_sequential_version, parse_extension_stem, parse_version
from sqlspec.utils.logging import get_logger
from sqlspec.utils.text import slugify

if TYPE_CHECKING:
    from sqlspec.migrations.runner import SyncMigrationRunner
    from sqlspec.migrations.templates import MigrationTemplateSettings

__all__ = (
    "DDLStateFolder",
    "MigrationSquasher",
    "SquashPlan",
    "fold_migration_ddl",
    "group_migrations_by_type",
    "parse_version_range",
    "schema_ddl_statements",
)

logger = get_logger("sqlspec.migrations.squash")

_TRANSACTIONAL_DIRECTIVE_RE = re.compile(r"^--\s*transactional:\s*(true|false)\s*$", re.MULTILINE | re.IGNORECASE)
_SCHEMA_DIRECTIVE_RE = re.compile(r"^--\s*schema:\s*(\S+)\s*$", re.MULTILINE | re.IGNORECASE)
_RENAME_TABLE_COMMAND_RE = re.compile(r"^\s*TABLE\s+([^\s;,]+)\s+TO\s+([^\s;,]+)\s*$", re.IGNORECASE)
_SQUASH_STRATEGIES = frozenset({"concat", "ddl", "schema"})
_CONCAT_HINT = "Use --strategy concat to keep the original statements verbatim."
_SNIPPET_LENGTH = 160
_TEXT_TOKEN_TYPES = frozenset({
    TokenType.VAR,
    TokenType.IDENTIFIER,
    TokenType.NUMBER,
    TokenType.STRING,
    TokenType.NATIONAL_STRING,
    TokenType.HEREDOC_STRING,
    TokenType.RAW_STRING,
    TokenType.BIT_STRING,
    TokenType.HEX_STRING,
    TokenType.BYTE_STRING,
    TokenType.UNICODE_STRING,
})
_CASE_SENSITIVE_TOKEN_TYPES = frozenset({
    TokenType.IDENTIFIER,
    TokenType.STRING,
    TokenType.NATIONAL_STRING,
    TokenType.HEREDOC_STRING,
    TokenType.RAW_STRING,
    TokenType.UNICODE_STRING,
})
_EQUIVALENT_TOKEN_TYPES: dict[TokenType, TokenType] = {
    TokenType.KEY: TokenType.INDEX,
    TokenType.CHARACTER_SET: TokenType.SET,
}
_DML_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.TruncateTable)
_TABLE_CONSTRAINT_TYPES = (exp.Constraint, exp.PrimaryKey, exp.ForeignKey, exp.UniqueColumnConstraint)
_GENERATED_COLUMN_TYPES = (exp.ComputedColumnConstraint, exp.GeneratedAsIdentityColumnConstraint)
_SUPPORTED_ALTER_COLUMN_ARGS = frozenset({"this", "dtype", "using", "default", "drop", "allow_null"})
_MAX_TYPE_TOKENS = 12
_TYPE_PLACEHOLDER = "__sqlspec_column_type__"
_ALTER_TYPE_KEYWORDS = frozenset({"SET", "DATA", "TYPE"})
_MYSQL_FAMILY = frozenset({"mysql", "singlestore", "doris", "starrocks", "tidb"})
_SEQUENCE_FUNCTIONS = frozenset({"NEXTVAL", "CURRVAL", "SETVAL", "LASTVAL"})


class DDLStateFolder:
    """Fold a chronological sequence of SQL DDL statements into net schema DDL.

    The folder keeps a catalog of every object created within the squash range and
    applies later ``ALTER``/``DROP``/``RENAME``/``COMMENT`` statements to it. Objects
    that are never rewritten are emitted with their original statement text; only
    rewritten objects are re-rendered through SQLGlot.

    The folder refuses (raises :class:`~sqlspec.exceptions.SquashValidationError`)
    whenever the folded output could differ from applying the statements in order:
    statements against objects created before the range, DML that a later structural
    change would invalidate, unsupported statement kinds or ``ALTER`` actions,
    procedural or unparsable statements, and rewrites of statements SQLGlot cannot
    reproduce faithfully. The synthesized down statements drop exactly the objects the
    folded up statements create, which restores the pre-range schema because nothing
    outside the range may be touched.
    """

    __slots__ = (
        "_comments",
        "_ctas_sources",
        "_current_faithful",
        "_current_origin",
        "_current_text",
        "_dialect",
        "_dialect_impl",
        "_dml",
        "_dml_touched",
        "_dml_written",
        "_indexes",
        "_order",
        "_schemas",
        "_sequences",
        "_tables",
        "_views",
    )

    def __init__(self, dialect: str | None = None) -> None:
        self._dialect = dialect
        self._dialect_impl: Dialect = Dialect.get_or_raise(dialect)
        self._schemas: dict[str, _FoldedStatement] = {}
        self._sequences: dict[str, _FoldedStatement] = {}
        self._tables: dict[str, _FoldedStatement] = {}
        self._views: dict[str, _FoldedStatement] = {}
        self._indexes: dict[str, _FoldedStatement] = {}
        self._comments: dict[tuple[str, str], _FoldedStatement] = {}
        self._dml: list[_FoldedStatement] = []
        self._dml_touched: dict[str, _FoldedStatement] = {}
        self._dml_written: dict[str, _FoldedStatement] = {}
        self._ctas_sources: dict[str, set[str]] = {}
        self._order = 0
        self._current_text = ""
        self._current_origin: str | None = None
        self._current_faithful = True

    def feed(self, statement: str, *, origin: str | None = None) -> None:
        """Parse and fold SQL text into the accumulated schema catalog.

        Args:
            statement: Raw SQL text that may contain one or more semicolon-delimited statements.
            origin: Optional label (usually the migration version) used in refusal messages.

        Raises:
            SquashValidationError: If any statement cannot be folded faithfully.
        """
        self._current_origin = origin
        for text, expr, tokens in self._split_statements(statement):
            self._current_text = text
            spans = self._column_type_spans(expr, tokens, statement)
            self._current_faithful = self._is_faithful(expr, tokens, spans)
            for node, arg_key, first, last in spans:
                original_type = statement[tokens[first].start : tokens[last].end + 1]
                node.set(arg_key, exp.DataType(this=exp.DataType.Type.USERDEFINED, kind=original_type))
            self._apply_expression(expr)

    def to_statements(self) -> tuple[list[str], list[str]]:
        """Render the folded UP statements and the DOWN statements that reverse them.

        Returns:
            Tuple of ``(up_statements, down_statements)``.

        Raises:
            SquashValidationError: If the folded objects cannot be ordered or rendered faithfully.
        """
        relation_keys = self._ordered_relation_keys()
        up_statements: list[str] = [self._render(entry) for entry in self._schemas.values()]
        up_statements.extend(self._render(entry) for entry in self._sequences.values())
        up_statements.extend(self._render(self._relation(key)) for key in relation_keys)
        up_statements.extend(self._render(entry) for entry in self._indexes.values())
        up_statements.extend(self._render(entry) for entry in self._comments.values())
        up_statements.extend(self._render(entry) for entry in self._dml)

        down_statements: list[str] = []
        for key in reversed(relation_keys):
            entry = self._relation(key)
            if key in self._views:
                down_statements.append(self._render_drop(entry, "VIEW", materialized=self._is_materialized(entry)))
            else:
                down_statements.append(self._render_drop(entry, "TABLE"))
        down_statements.extend(self._render_drop(entry, "SEQUENCE") for entry in reversed(self._sequences.values()))
        down_statements.extend(self._render_drop(entry, "SCHEMA") for entry in reversed(self._schemas.values()))
        return up_statements, down_statements

    def _split_statements(self, sql: str) -> list[tuple[str, exp.Expr, list[Token]]]:
        self._current_text = sql.strip()
        try:
            tokens = self._dialect_impl.tokenize(sql)
        except TokenError as error:
            msg = f"it could not be tokenized ({error})"
            raise self._refusal(msg) from error

        chunks: list[list[Token]] = [[]]
        for token in tokens:
            if token.token_type == TokenType.SEMICOLON:
                chunks.append([])
            else:
                chunks[-1].append(token)

        statements: list[tuple[str, exp.Expr, list[Token]]] = []
        for chunk in chunks:
            if not chunk:
                continue
            text = sql[chunk[0].start : chunk[-1].end + 1]
            self._current_text = text
            try:
                parsed = self._dialect_impl.parser().parse(chunk, sql)
            except ParseError as error:
                msg = "SQLGlot cannot parse it"
                raise self._refusal(msg) from error
            statements.extend((text, expr, chunk) for expr in parsed if expr is not None)
        return statements

    def _column_type_spans(self, expr: exp.Expr, tokens: list[Token], sql: str) -> list[tuple[exp.Expr, str, int, int]]:
        """Locate the original token span of each declared column data type.

        SQLGlot canonicalizes type synonyms (for example SQLite renders ``VARCHAR(20)`` as
        ``TEXT(20)``). The span is the longest run of tokens after the column name (or after
        ``ALTER COLUMN ... [SET DATA] TYPE``) whose parsed type renders identically to the
        parsed column type. The spans let the fidelity check compare column types semantically
        while keeping expression casts strict, and let rewritten tables keep the declared type text.

        Args:
            expr: Parsed statement.
            tokens: Tokens of the statement, with offsets into ``sql``.
            sql: Full SQL text the tokens were produced from.

        Returns:
            Tuples of ``(node, arg_key, first_token_index, last_token_index)``.
        """
        token_positions = {token.start: position for position, token in enumerate(tokens)}
        spans: list[tuple[exp.Expr, str, int, int]] = []
        for column in expr.find_all(exp.ColumnDef):
            kind = column.args.get("kind")
            ident = column.this
            if not isinstance(kind, exp.DataType) or not isinstance(ident, exp.Identifier):
                continue
            start = ident.meta.get("start")
            position = token_positions.get(start) if isinstance(start, int) else None
            if position is None:
                continue
            last = self._type_span_end(kind, tokens, position + 1, sql)
            if last is not None:
                spans.append((column, "kind", position + 1, last))
        for alter_col in expr.find_all(exp.AlterColumn):
            dtype = alter_col.args.get("dtype")
            ident = alter_col.this.this if isinstance(alter_col.this, exp.Column) else alter_col.this
            if not isinstance(dtype, exp.DataType) or not isinstance(ident, exp.Identifier):
                continue
            start = ident.meta.get("start")
            position = token_positions.get(start) if isinstance(start, int) else None
            if position is None:
                continue
            first = position + 1
            while first < len(tokens) and tokens[first].text.upper() in _ALTER_TYPE_KEYWORDS:
                first += 1
            last = self._type_span_end(dtype, tokens, first, sql)
            if last is not None:
                spans.append((alter_col, "dtype", first, last))
        return spans

    def _type_span_end(self, kind: exp.DataType, tokens: list[Token], first: int, sql: str) -> int | None:
        expected = kind.sql(dialect=self._dialect)
        matched: int | None = None
        for last in range(first, min(first + _MAX_TYPE_TOKENS, len(tokens))):
            try:
                parsed = self._dialect_impl.parser(error_level=ErrorLevel.IMMEDIATE).parse_into(
                    exp.DataType, tokens[first : last + 1], sql
                )
            except ParseError:
                continue
            if len(parsed) == 1 and parsed[0] is not None and parsed[0].sql(dialect=self._dialect) == expected:
                matched = last
        return matched

    def _apply_expression(self, expr: exp.Expr) -> None:
        if isinstance(expr, exp.Command):
            self._apply_command(expr)
        elif expr.find(exp.Command) is not None:
            msg = "it contains syntax SQLGlot can only read as an opaque command"
            raise self._refusal(msg)
        elif isinstance(expr, exp.Create):
            self._apply_create(expr)
        elif isinstance(expr, exp.Alter):
            self._apply_alter(expr)
        elif isinstance(expr, exp.Drop):
            self._apply_drop(expr)
        elif isinstance(expr, exp.Comment):
            self._apply_comment(expr)
        elif isinstance(expr, _DML_TYPES):
            self._apply_dml(expr)
        else:
            msg = f"{type(expr).__name__} statements are not supported"
            raise self._refusal(msg)

    def _apply_command(self, expr: exp.Command) -> None:
        keyword = str(expr.this).upper()
        literal = expr.expression.name if isinstance(expr.expression, exp.Expr) else str(expr.expression or "")
        match = _RENAME_TABLE_COMMAND_RE.match(literal) if keyword == "RENAME" else None
        if match is None:
            msg = "SQLGlot can only read it as an opaque command, so its effect on the schema is unknown"
            raise self._refusal(msg)
        old_table = exp.to_table(match.group(1), dialect=self._dialect)
        new_table = exp.to_table(match.group(2), dialect=self._dialect)
        old_key = self._object_key(old_table)
        self._require_table(old_key, "RENAME TABLE")
        self._assert_restructurable(old_key, "RENAME TABLE")
        self._rename_table(old_key, new_table)

    def _apply_create(self, expr: exp.Create) -> None:
        kind = str(expr.args.get("kind") or "").upper()
        if expr.find(exp.TemporaryProperty) is not None:
            msg = "temporary objects are not part of the persistent schema"
            raise self._refusal(msg)
        if kind == "TABLE":
            self._create_table(expr)
        elif kind == "VIEW":
            self._create_view(expr)
        elif kind == "INDEX":
            self._create_index(expr)
        elif kind == "SCHEMA":
            self._create_simple(expr, self._schemas, kind)
        elif kind == "SEQUENCE":
            self._create_simple(expr, self._sequences, kind)
        else:
            msg = f"CREATE {kind or 'statements of this kind'} cannot be folded"
            raise self._refusal(msg)

    def _create_simple(self, expr: exp.Create, catalog: "dict[str, _FoldedStatement]", kind: str) -> None:
        key = self._object_key(expr.this)
        if self._create_is_noop(expr, key, key in catalog, kind):
            return
        catalog[key] = self._new_entry(expr)

    def _create_table(self, expr: exp.Create) -> None:
        key = self._object_key(expr.this)
        if key in self._views:
            msg = f"{key} is already a view created earlier in the squash range"
            raise self._refusal(msg)
        if self._create_is_noop(expr, key, key in self._tables, "TABLE"):
            return
        query = expr.expression
        if isinstance(query, exp.Query):
            sources = self._expand_dependencies(self._query_sources(query))
            for source in sources:
                writer = self._dml_written.get(source)
                if writer is not None:
                    msg = (
                        f"CREATE TABLE AS SELECT reads {source}, which DML in migration "
                        f"{writer.origin or '<unknown>'} modifies first; the folded migration would run it before that DML"
                    )
                    raise self._refusal(msg)
            for source in sources:
                self._ctas_sources.setdefault(source, set()).add(key)
        elif not isinstance(expr.this, exp.Schema):
            msg = "CREATE TABLE without a column list cannot be folded"
            raise self._refusal(msg)
        self._tables[key] = self._new_entry(expr)

    def _create_view(self, expr: exp.Create) -> None:
        key = self._object_key(expr.this)
        if key in self._tables:
            msg = f"{key} is already a table created earlier in the squash range"
            raise self._refusal(msg)
        if key in self._views and expr.args.get("replace"):
            self._assert_restructurable(key, "CREATE OR REPLACE VIEW")
            self._views[key] = self._new_entry(expr)
            return
        if self._create_is_noop(expr, key, key in self._views, "VIEW"):
            return
        self._views[key] = self._new_entry(expr)

    def _create_index(self, expr: exp.Create) -> None:
        index = expr.this
        table = index.args.get("table") if isinstance(index, exp.Index) else None
        if not isinstance(index, exp.Index) or not isinstance(table, exp.Table):
            msg = "the index definition could not be read"
            raise self._refusal(msg)
        table_key = self._object_key(table)
        view_entry = self._views.get(table_key)
        if table_key not in self._tables and (view_entry is None or not self._is_materialized(view_entry)):
            msg = f"CREATE INDEX targets {table_key}, which is not created within the squash range"
            raise self._refusal(msg)
        name = index.this
        base_name = self._name_key(name) if isinstance(name, exp.Expr) else ""
        key = self._index_catalog_key(base_name, table_key) if base_name else f"<unnamed index {self._order}>"
        if key in self._indexes:
            if expr.args.get("exists"):
                return
            msg = f"index {base_name or key} is already created earlier in the squash range"
            raise self._refusal(msg)
        self._indexes[key] = self._new_entry(expr, owner=table_key)

    def _create_is_noop(self, expr: exp.Create, key: str, exists_in_range: bool, kind: str) -> bool:
        if exists_in_range:
            if expr.args.get("exists"):
                return True
            msg = f"{kind} {key} is already created earlier in the squash range"
            raise self._refusal(msg)
        if expr.args.get("exists"):
            msg = (
                f"CREATE {kind} IF NOT EXISTS cannot tell whether {key} existed before the squash range, "
                "so the folded down migration could drop a pre-existing object"
            )
            raise self._refusal(msg)
        if expr.args.get("replace"):
            msg = f"CREATE OR REPLACE {kind} may replace {key}, which could exist before the squash range"
            raise self._refusal(msg)
        return False

    def _apply_alter(self, expr: exp.Alter) -> None:
        kind = str(expr.args.get("kind") or "TABLE").upper()
        target = expr.this
        if not isinstance(target, exp.Table):
            msg = "the ALTER target could not be read"
            raise self._refusal(msg)
        if not self._current_faithful:
            msg = "SQLGlot does not reproduce it faithfully, so it cannot be merged into a folded definition"
            raise self._refusal(msg)
        if expr.args.get("not_valid"):
            msg = "NOT VALID constraints cannot be folded into CREATE TABLE"
            raise self._refusal(msg)
        actions = [action for action in expr.args.get("actions") or [] if isinstance(action, exp.Expr)]
        if kind == "INDEX":
            self._alter_index(target, actions)
            return
        if kind != "TABLE":
            msg = f"ALTER {kind} cannot be folded"
            raise self._refusal(msg)
        table_key = self._object_key(target)
        entry = self._require_table(table_key, "ALTER TABLE")
        if isinstance(entry.expr.expression, exp.Query):
            msg = f"{table_key} is created with CREATE TABLE AS SELECT, which cannot absorb ALTER TABLE"
            raise self._refusal(msg)
        for action in actions:
            table_key = self._apply_alter_action(table_key, target, action)

    def _apply_alter_action(self, table_key: str, target: exp.Table, action: exp.Expr) -> str:
        label = f"ALTER TABLE {type(action).__name__}"
        self._assert_restructurable(table_key, label)
        entry = self._tables[table_key]
        if isinstance(action, exp.AlterRename):
            new_table = action.this
            if not isinstance(new_table, exp.Table):
                msg = "the RENAME target could not be read"
                raise self._refusal(msg)
            target_db = target.args.get("db")
            if isinstance(target_db, exp.Identifier) and not new_table.db:
                new_table = new_table.copy()
                new_table.set("db", target_db.copy())
            return self._rename_table(table_key, new_table)
        schema = self._table_schema(entry)
        if isinstance(action, exp.ColumnDef):
            self._add_column(table_key, entry, schema, action)
        elif isinstance(action, exp.ModifyColumn):
            self._modify_column(table_key, entry, schema, action)
        elif isinstance(action, exp.Drop):
            self._drop_from_table(table_key, entry, schema, action)
        elif isinstance(action, exp.DropPrimaryKey):
            self._drop_primary_key(table_key, entry, schema)
        elif isinstance(action, exp.RenameColumn):
            self._rename_column(table_key, entry, schema, action)
        elif isinstance(action, exp.AlterColumn):
            self._alter_column(table_key, entry, schema, action)
        elif isinstance(action, exp.AddConstraint):
            constraints = [item for item in action.expressions if isinstance(item, exp.Expr)]
            self._add_constraints(entry, schema, constraints)
        elif isinstance(action, _TABLE_CONSTRAINT_TYPES):
            self._add_constraints(entry, schema, [action])
        else:
            msg = f"ALTER TABLE action {type(action).__name__} cannot be folded"
            raise self._refusal(msg)
        return table_key

    def _alter_index(self, target: exp.Table, actions: list[exp.Expr]) -> None:
        if len(actions) != 1 or not isinstance(actions[0], exp.AlterRename):
            msg = "only ALTER INDEX ... RENAME TO can be folded"
            raise self._refusal(msg)
        raw_old_key = self._object_key(target)
        old_key = self._resolve_index_key(raw_old_key)
        entry = self._indexes.get(old_key)
        if entry is None or not isinstance(entry.expr.this, exp.Index):
            msg = f"ALTER INDEX targets {raw_old_key}, which is not created within the squash range"
            raise self._refusal(msg)
        new_node = actions[0].this
        new_ident = new_node.this if isinstance(new_node, exp.Table) else None
        if not isinstance(new_ident, exp.Identifier):
            msg = "the RENAME target could not be read"
            raise self._refusal(msg)
        new_base = self._ident_key(new_ident)
        new_key = self._index_catalog_key(new_base, entry.owner)
        if new_key in self._indexes:
            msg = f"index {new_base} is already created earlier in the squash range"
            raise self._refusal(msg)
        self._mark_modified(entry)
        entry.expr.this.set("this", new_ident.copy())
        self._indexes = {(new_key if key == old_key else key): value for key, value in self._indexes.items()}
        comment = self._comments.pop(("INDEX", old_key), None)
        if comment is not None:
            self._mark_modified(comment)
            comment_target = comment.expr.this
            if isinstance(comment_target, exp.Table):
                comment_target.set("this", new_ident.copy())
            self._comments["INDEX", new_key] = comment

    def _add_column(self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema, action: exp.ColumnDef) -> None:
        column_key = self._name_key(action)
        if self._find_column(schema, column_key) is not None:
            if action.args.get("exists"):
                return
            msg = f"column {column_key} already exists on {table_key}"
            raise self._refusal(msg)
        if action.args.get("position") is not None:
            msg = "column positioning (FIRST/AFTER) cannot be folded"
            raise self._refusal(msg)
        self._assert_views_allow(table_key, set(), "ADD COLUMN")
        self._mark_modified(entry)
        column = action.copy()
        column.set("exists", None)
        columns = [item for item in schema.expressions if isinstance(item, exp.ColumnDef)]
        constraints = [item for item in schema.expressions if not isinstance(item, exp.ColumnDef)]
        schema.set("expressions", [*columns, column, *constraints])

    def _modify_column(
        self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema, action: exp.ModifyColumn
    ) -> None:
        new_definition = action.this
        if not isinstance(new_definition, exp.ColumnDef):
            msg = "the MODIFY COLUMN definition could not be read"
            raise self._refusal(msg)
        if new_definition.args.get("position") is not None:
            msg = "column positioning (FIRST/AFTER) cannot be folded"
            raise self._refusal(msg)
        rename_from = action.args.get("rename_from")
        new_key = self._name_key(new_definition)
        old_key = self._name_key(rename_from) if isinstance(rename_from, exp.Expr) else new_key
        existing = self._find_column(schema, old_key)
        if existing is None:
            msg = f"column {old_key} does not exist on {table_key}"
            raise self._refusal(msg)
        if old_key != new_key:
            if self._find_column(schema, new_key) is not None:
                msg = f"column {new_key} already exists on {table_key}"
                raise self._refusal(msg)
            self._assert_views_allow(table_key, None, "CHANGE COLUMN")
            self._mark_modified(entry)
            new_ident = new_definition.this
            if isinstance(new_ident, exp.Identifier):
                self._rename_column_references(table_key, old_key, new_ident)
        else:
            self._assert_views_allow(table_key, {old_key}, "MODIFY COLUMN")
            self._mark_modified(entry)
        replacement = new_definition.copy()
        replacement_constraints = list(replacement.args.get("constraints") or [])
        existing_key_kinds = {
            type(c.args.get("kind"))
            for c in replacement_constraints
            if isinstance(c.args.get("kind"), (exp.PrimaryKeyColumnConstraint, exp.UniqueColumnConstraint))
        }
        for constraint in existing.args.get("constraints") or []:
            kind = constraint.args.get("kind")
            if isinstance(kind, (exp.PrimaryKeyColumnConstraint, exp.UniqueColumnConstraint)) and (
                type(kind) not in existing_key_kinds
            ):
                replacement_constraints.append(constraint.copy())
                existing_key_kinds.add(type(kind))
        if replacement_constraints:
            replacement.set("constraints", replacement_constraints)
        schema.set(
            "expressions",
            [
                replacement if isinstance(item, exp.ColumnDef) and self._name_key(item) == new_key else item
                for item in schema.expressions
            ],
        )

    def _drop_from_table(self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema, action: exp.Drop) -> None:
        drop_kind = str(action.args.get("kind") or "").upper()
        if_exists = bool(action.args.get("exists"))
        names = [self._name_key(node) for node in self._drop_targets(action)]
        for name in names:
            if drop_kind == "COLUMN":
                self._drop_column(table_key, entry, schema, name, if_exists=if_exists)
            elif drop_kind == "FOREIGN KEY":
                msg = (
                    "ALTER TABLE DROP FOREIGN KEY leaves behind the index MySQL created for the foreign key, "
                    "which the folded CREATE TABLE cannot reproduce exactly"
                )
                raise self._refusal(msg)
            elif drop_kind == "CONSTRAINT":
                self._drop_constraint(table_key, entry, schema, name, if_exists=if_exists)
            elif drop_kind == "INDEX":
                self._drop_table_index(table_key, entry, schema, name)
            else:
                msg = f"ALTER TABLE DROP {drop_kind or 'action'} cannot be folded"
                raise self._refusal(msg)

    def _drop_column(
        self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema, column_key: str, *, if_exists: bool
    ) -> None:
        if self._find_column(schema, column_key) is None:
            if if_exists:
                return
            msg = f"column {column_key} does not exist on {table_key}"
            raise self._refusal(msg)
        pk_columns = self._primary_key_columns(table_key)
        if column_key in pk_columns and (len(pk_columns) > 1 or (self._dialect or "") in {"sqlite", "duckdb"}):
            msg = (
                f"dropping primary key column {column_key} on {table_key} leaves dialect-specific primary key and "
                "NOT NULL state, which the folded CREATE TABLE cannot reproduce"
            )
            raise self._refusal(msg)
        self._assert_views_allow(table_key, {column_key}, "DROP COLUMN")
        for other_key, other in self._tables.items():
            if other_key != table_key and column_key in self._referenced_columns(other.expr, table_key):
                msg = f"column {column_key} on {table_key} is referenced by a foreign key on {other_key}"
                raise self._refusal(msg)
        self._mark_modified(entry)
        kept: list[exp.Expr] = []
        for item in schema.expressions:
            if isinstance(item, exp.ColumnDef):
                if self._name_key(item) == column_key:
                    continue
                self._strip_column_constraints(item, column_key, table_key)
            elif column_key in self._local_column_keys(item, table_key):
                continue
            kept.append(item)
        schema.set("expressions", kept)
        stale_indexes = [
            index_key
            for index_key, index_entry in self._indexes.items()
            if index_entry.owner == table_key and column_key in self._index_column_keys(index_entry)
        ]
        for index_key in stale_indexes:
            self._indexes.pop(index_key)
            self._comments.pop(("INDEX", index_key), None)
        self._comments.pop(("COLUMN", f"{table_key}.{column_key}"), None)

    def _strip_column_constraints(self, column: exp.ColumnDef, column_key: str, table_key: str) -> None:
        kept: list[exp.Expr] = []
        for constraint in column.args.get("constraints") or []:
            if column_key in self._local_column_keys(constraint, table_key):
                if isinstance(constraint.args.get("kind"), _GENERATED_COLUMN_TYPES):
                    msg = f"generated column {self._name_key(column)} depends on dropped column {column_key}"
                    raise self._refusal(msg)
                continue
            kept.append(constraint)
        column.set("constraints", kept)

    def _drop_constraint(
        self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema, name: str, *, if_exists: bool
    ) -> None:
        found = any(
            isinstance(item, exp.Constraint) and self._name_key(item) == name for item in schema.expressions
        ) or any(
            isinstance(constraint.this, exp.Identifier) and self._ident_key(constraint.this) == name
            for column in schema.expressions
            if isinstance(column, exp.ColumnDef)
            for constraint in column.args.get("constraints") or []
        )
        if not found:
            if if_exists:
                return
            msg = f"constraint {name} is not defined by name on {table_key} within the squash range"
            raise self._refusal(msg)
        named = [
            node
            for item in schema.expressions
            for node in ([item] if isinstance(item, exp.Constraint) else item.args.get("constraints") or [])
            if isinstance(node, (exp.Constraint, exp.ColumnConstraint))
            and isinstance(node.this, exp.Identifier)
            and self._ident_key(node.this) == name
        ]
        if any(node.find(exp.PrimaryKey, exp.PrimaryKeyColumnConstraint) is not None for node in named):
            msg = (
                f"dropping primary key constraint {name} leaves dialect-specific NOT NULL state on its columns, "
                "which the folded CREATE TABLE cannot reproduce exactly"
            )
            raise self._refusal(msg)
        if (self._dialect or "") in _MYSQL_FAMILY and any(node.find(exp.Reference) is not None for node in named):
            msg = (
                f"dropping foreign key constraint {name} leaves behind the index MySQL created for it, "
                "which the folded CREATE TABLE cannot reproduce exactly"
            )
            raise self._refusal(msg)
        self._mark_modified(entry)
        kept: list[exp.Expr] = []
        for item in schema.expressions:
            if isinstance(item, exp.Constraint) and self._name_key(item) == name:
                continue
            if isinstance(item, exp.ColumnDef):
                item.set(
                    "constraints",
                    [
                        constraint
                        for constraint in item.args.get("constraints") or []
                        if not (
                            isinstance(constraint.this, exp.Identifier) and self._ident_key(constraint.this) == name
                        )
                    ],
                )
            kept.append(item)
        schema.set("expressions", kept)

    def _drop_table_index(self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema, name: str) -> None:
        resolved_key = self._resolve_index_key(name, owner=table_key)
        index_entry = self._indexes.get(resolved_key)
        if index_entry is not None and index_entry.owner == table_key:
            self._indexes.pop(resolved_key)
            self._comments.pop(("INDEX", resolved_key), None)
            return
        inline = [
            item
            for item in schema.expressions
            if isinstance(item, exp.IndexColumnConstraint)
            and isinstance(item.this, exp.Identifier)
            and self._ident_key(item.this) == name
        ]
        if not inline:
            msg = f"index {name} is not defined on {table_key} within the squash range"
            raise self._refusal(msg)
        self._mark_modified(entry)
        schema.set("expressions", [item for item in schema.expressions if item not in inline])

    def _drop_primary_key(self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema) -> None:
        """Drop the primary key while keeping its columns NOT NULL, as MySQL does."""
        primary_key_columns = self._primary_key_columns(table_key)
        kept: list[exp.Expr] = []
        found = False
        for item in schema.expressions:
            if isinstance(item, exp.PrimaryKey) or (
                isinstance(item, exp.Constraint) and any(isinstance(sub, exp.PrimaryKey) for sub in item.expressions)
            ):
                found = True
                continue
            if isinstance(item, exp.ColumnDef):
                constraints = list(item.args.get("constraints") or [])
                filtered = [
                    c for c in constraints if not isinstance(c.args.get("kind"), exp.PrimaryKeyColumnConstraint)
                ]
                if len(filtered) != len(constraints):
                    found = True
                    item.set("constraints", filtered)
            kept.append(item)
        if not found:
            msg = f"{table_key} has no primary key defined within the squash range"
            raise self._refusal(msg)
        self._mark_modified(entry)
        for item in kept:
            if not isinstance(item, exp.ColumnDef) or self._name_key(item) not in primary_key_columns:
                continue
            constraints = list(item.args.get("constraints") or [])
            if not any(
                isinstance(c.args.get("kind"), exp.NotNullColumnConstraint)
                and not c.args["kind"].args.get("allow_null")
                for c in constraints
            ):
                constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))
                item.set("constraints", constraints)
        schema.set("expressions", kept)

    def _rename_column(
        self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema, action: exp.RenameColumn
    ) -> None:
        old_key = self._name_key(action.this)
        new_node = action.args.get("to")
        new_ident = new_node.this if isinstance(new_node, exp.Column) else new_node
        if not isinstance(new_ident, exp.Identifier):
            msg = "the RENAME COLUMN target could not be read"
            raise self._refusal(msg)
        if self._find_column(schema, old_key) is None:
            if action.args.get("exists"):
                return
            msg = f"column {old_key} does not exist on {table_key}"
            raise self._refusal(msg)
        if self._find_column(schema, self._ident_key(new_ident)) is not None:
            msg = f"column {self._ident_key(new_ident)} already exists on {table_key}"
            raise self._refusal(msg)
        self._assert_views_allow(table_key, None, "RENAME COLUMN")
        self._mark_modified(entry)
        self._rename_column_references(table_key, old_key, new_ident)

    def _rename_column_references(self, table_key: str, old_key: str, new_ident: exp.Identifier) -> None:
        schema = self._table_schema(self._tables[table_key])
        for item in schema.expressions:
            if isinstance(item, exp.ColumnDef) and self._name_key(item) == old_key:
                item.set("this", new_ident.copy())
            for ident in self._local_column_refs(item, table_key):
                if self._ident_key(ident) == old_key:
                    ident.replace(new_ident.copy())
        for index_entry in self._indexes.values():
            if index_entry.owner != table_key:
                continue
            index_refs = [ident for ident in self._index_column_refs(index_entry) if self._ident_key(ident) == old_key]
            if index_refs:
                self._mark_modified(index_entry)
                for ident in index_refs:
                    ident.replace(new_ident.copy())
        for other_key, other in self._tables.items():
            if other_key == table_key:
                continue
            foreign_refs = [
                ident
                for ident in self._foreign_reference_idents(other.expr, table_key)
                if self._ident_key(ident) == old_key
            ]
            if foreign_refs:
                self._mark_modified(other)
                for ident in foreign_refs:
                    ident.replace(new_ident.copy())
        new_key = self._ident_key(new_ident)
        comment = self._comments.get(("COLUMN", f"{table_key}.{old_key}"))
        if comment is not None:
            self._mark_modified(comment)
            if isinstance(comment.expr.this, exp.Column):
                comment.expr.this.set("this", new_ident.copy())
            self._comments = {
                (("COLUMN", f"{table_key}.{new_key}") if key == ("COLUMN", f"{table_key}.{old_key}") else key): value
                for key, value in self._comments.items()
            }

    def _alter_column(
        self, table_key: str, entry: "_FoldedStatement", schema: exp.Schema, action: exp.AlterColumn
    ) -> None:
        unsupported = sorted(
            arg for arg, value in action.args.items() if value and arg not in _SUPPORTED_ALTER_COLUMN_ARGS
        )
        if unsupported:
            msg = f"ALTER COLUMN with {', '.join(unsupported)} cannot be folded"
            raise self._refusal(msg)
        column_key = self._name_key(action)
        column = self._find_column(schema, column_key)
        if column is None:
            msg = f"column {column_key} does not exist on {table_key}"
            raise self._refusal(msg)
        dtype = action.args.get("dtype")
        if isinstance(dtype, exp.DataType):
            self._assert_views_allow(table_key, {column_key}, "ALTER COLUMN TYPE")
        self._mark_modified(entry)
        if isinstance(dtype, exp.DataType):
            column.set("kind", dtype.copy())
        constraints: list[exp.ColumnConstraint] = list(column.args.get("constraints") or [])
        default_expr = action.args.get("default")
        allow_null = action.args.get("allow_null")
        if isinstance(default_expr, exp.Expr):
            constraints = [c for c in constraints if not isinstance(c.kind, exp.DefaultColumnConstraint)]
            constraints.append(exp.ColumnConstraint(kind=exp.DefaultColumnConstraint(this=default_expr.copy())))
        elif action.args.get("drop") and allow_null is None:
            constraints = [c for c in constraints if not isinstance(c.kind, exp.DefaultColumnConstraint)]
        if allow_null is False:
            constraints = [c for c in constraints if not isinstance(c.kind, exp.NotNullColumnConstraint)]
            constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))
        elif allow_null is True:
            constraints = [
                c
                for c in constraints
                if not (isinstance(c.kind, exp.NotNullColumnConstraint) and not c.kind.args.get("allow_null"))
            ]
        column.set("constraints", constraints)

    def _add_constraints(self, entry: "_FoldedStatement", schema: exp.Schema, constraints: list[exp.Expr]) -> None:
        if not constraints:
            msg = "the added constraint could not be read"
            raise self._refusal(msg)
        self._mark_modified(entry)
        schema.set("expressions", [*schema.expressions, *(constraint.copy() for constraint in constraints)])

    def _rename_table(self, old_key: str, new_table: exp.Table) -> str:
        new_key = self._object_key(new_table)
        if new_key == old_key:
            return old_key
        if new_key in self._tables or new_key in self._views:
            msg = f"{new_key} already exists within the squash range"
            raise self._refusal(msg)
        self._assert_views_allow(old_key, None, "RENAME TABLE")
        entry = self._tables[old_key]
        referencing: list[tuple[_FoldedStatement, exp.Table]] = []
        for other in self._tables.values():
            for reference in other.expr.find_all(exp.Reference):
                ref_table = self._reference_table(reference)
                if ref_table is not None and self._resolve_reference_key(ref_table) == old_key:
                    referencing.append((other, ref_table))
        self._mark_modified(entry)
        self._set_create_table_node(entry.expr, new_table.copy())
        self._tables = {(new_key if key == old_key else key): value for key, value in self._tables.items()}
        for other, ref_table in referencing:
            self._mark_modified(other)
            ref_table.replace(new_table.copy())
        rebuilt_indexes: dict[str, _FoldedStatement] = {}
        renamed_index_keys: dict[str, str] = {}
        for index_key, index_entry in self._indexes.items():
            if index_entry.owner == old_key and isinstance(index_entry.expr.this, exp.Index):
                self._mark_modified(index_entry)
                index_entry.expr.this.set("table", new_table.copy())
                index_entry.owner = new_key
                base_name = index_key.rsplit(".", 1)[-1]
                new_index_key = (
                    self._index_catalog_key(base_name, new_key) if not base_name.startswith("<unnamed ") else base_name
                )
                rebuilt_indexes[new_index_key] = index_entry
                renamed_index_keys[index_key] = new_index_key
            else:
                rebuilt_indexes[index_key] = index_entry
        self._indexes = rebuilt_indexes
        rebuilt_comments: dict[tuple[str, str], _FoldedStatement] = {}
        for (comment_kind, comment_key), comment in self._comments.items():
            if comment_kind == "TABLE" and comment_key == old_key:
                self._mark_modified(comment)
                comment.expr.set("this", new_table.copy())
                rebuilt_comments[comment_kind, new_key] = comment
            elif comment_kind == "COLUMN" and comment_key.startswith(f"{old_key}."):
                self._mark_modified(comment)
                column = comment.expr.this
                if isinstance(column, exp.Column):
                    column.set("table", new_table.this.copy() if isinstance(new_table.this, exp.Expr) else None)
                    new_db = new_table.args.get("db")
                    column.set("db", new_db.copy() if isinstance(new_db, exp.Expr) else None)
                rebuilt_comments[comment_kind, f"{new_key}.{comment_key[len(old_key) + 1 :]}"] = comment
            elif comment_kind == "INDEX" and comment_key in renamed_index_keys:
                rebuilt_comments[comment_kind, renamed_index_keys[comment_key]] = comment
            else:
                rebuilt_comments[comment_kind, comment_key] = comment
        self._comments = rebuilt_comments
        return new_key

    def _apply_drop(self, expr: exp.Drop) -> None:
        kind = str(expr.args.get("kind") or "").upper()
        cascade = bool(expr.args.get("cascade"))
        for target in self._drop_targets(expr):
            if kind == "TABLE":
                self._drop_table(self._object_key(target), cascade=cascade)
            elif kind == "VIEW":
                self._drop_view(self._object_key(target), cascade=cascade)
            elif kind == "INDEX":
                self._drop_index(self._object_key(target))
            elif kind == "SEQUENCE":
                self._drop_sequence(self._object_key(target))
            elif kind == "SCHEMA":
                self._drop_schema(self._object_key(target))
            else:
                msg = f"DROP {kind or 'statements of this kind'} cannot be folded"
                raise self._refusal(msg)

    def _drop_table(self, key: str, *, cascade: bool) -> None:
        self._require_table(key, "DROP TABLE")
        self._assert_restructurable(key, "DROP TABLE")
        dependent_views = self._dependent_views(key)
        referencing = [
            other_key
            for other_key, other in self._tables.items()
            if other_key != key and self._references_table(other.expr, key)
        ]
        if (dependent_views or referencing) and not cascade:
            msg = f"DROP TABLE {key} without CASCADE while {', '.join([*dependent_views, *referencing])} depend on it"
            raise self._refusal(msg)
        for view_key in dependent_views:
            if view_key in self._views:
                self._drop_view(view_key, cascade=True)
        for other_key in referencing:
            self._strip_foreign_keys(other_key, key)
        self._tables.pop(key)
        self._forget_object(key)
        for sources in self._ctas_sources.values():
            sources.discard(key)

    def _drop_view(self, key: str, *, cascade: bool) -> None:
        if key not in self._views:
            msg = f"DROP VIEW targets {key}, which is not created within the squash range"
            raise self._refusal(msg)
        self._assert_restructurable(key, "DROP VIEW")
        dependent_views = self._dependent_views(key)
        if dependent_views and not cascade:
            msg = f"DROP VIEW {key} without CASCADE while {', '.join(dependent_views)} depend on it"
            raise self._refusal(msg)
        for view_key in dependent_views:
            if view_key in self._views:
                self._drop_view(view_key, cascade=True)
        self._views.pop(key)
        self._forget_object(key)

    def _drop_index(self, key: str) -> None:
        resolved_key = self._resolve_index_key(key)
        if resolved_key not in self._indexes:
            msg = f"DROP INDEX targets {key}, which is not created within the squash range"
            raise self._refusal(msg)
        self._indexes.pop(resolved_key)
        self._comments.pop(("INDEX", resolved_key), None)

    def _drop_sequence(self, key: str) -> None:
        if key not in self._sequences:
            msg = f"DROP SEQUENCE targets {key}, which is not created within the squash range"
            raise self._refusal(msg)
        users = self._sequence_users(key)
        if users:
            msg = (
                f"DROP SEQUENCE {key} while {', '.join(users)} still reference it; the folded definitions "
                "would keep the reference to a dropped sequence"
            )
            raise self._refusal(msg)
        self._sequences.pop(key)
        self._comments.pop(("SEQUENCE", key), None)

    def _sequence_users(self, key: str) -> list[str]:
        """Return in-range tables and views whose definitions reference sequence ``key``.

        Covers ``nextval('seq')``-style calls (including ``'seq'::regclass`` casts),
        ``NEXT VALUE FOR seq``, and Oracle ``seq.NEXTVAL`` references.

        Args:
            key: Catalog key of the sequence.

        Returns:
            Keys of the tables and views that reference the sequence.
        """
        users: list[str] = []
        for relation_key, entry in [*self._tables.items(), *self._views.items()]:
            if any(self._references_sequence(node, key) for node in entry.expr.walk()):
                users.append(relation_key)
        return users

    def _references_sequence(self, node: exp.Expr, key: str) -> bool:
        if isinstance(node, exp.NextValueFor):
            return self._sequence_name_matches(node.this, key)
        if isinstance(node, exp.Column) and node.name.upper() in _SEQUENCE_FUNCTIONS:
            owner = node.args.get("table")
            return isinstance(owner, exp.Identifier) and self._sequence_name_matches(
                exp.Table(this=owner.copy(), db=node.args.get("db")), key
            )
        if isinstance(node, exp.Literal) and node.is_string and node.find_ancestor(exp.Func) is not None:
            try:
                table = exp.to_table(node.this, dialect=self._dialect)
            except (ParseError, TokenError):
                return False
            return self._sequence_name_matches(table, key)
        return False

    def _sequence_name_matches(self, node: exp.Expr | None, key: str) -> bool:
        if node is None:
            return False
        candidate = self._object_key(node)
        if not candidate:
            return False
        if "." in candidate and "." in key:
            return candidate == key
        return candidate.rsplit(".", 1)[-1] == key.rsplit(".", 1)[-1]

    def _drop_schema(self, key: str) -> None:
        if key not in self._schemas:
            msg = f"DROP SCHEMA targets {key}, which is not created within the squash range"
            raise self._refusal(msg)
        prefix = f"{key}."
        contained = [
            object_key
            for object_key in [*self._tables, *self._views, *self._sequences]
            if object_key.startswith(prefix)
        ]
        if contained:
            msg = f"DROP SCHEMA {key} while {', '.join(contained)} still exist in it"
            raise self._refusal(msg)
        self._schemas.pop(key)
        self._comments.pop(("SCHEMA", key), None)

    def _strip_foreign_keys(self, table_key: str, target_key: str) -> None:
        entry = self._tables[table_key]
        schema = self._table_schema(entry)
        self._mark_modified(entry)
        kept: list[exp.Expr] = []
        for item in schema.expressions:
            if isinstance(item, exp.ColumnDef):
                item.set(
                    "constraints",
                    [
                        constraint
                        for constraint in item.args.get("constraints") or []
                        if not self._references_table(constraint, target_key)
                    ],
                )
            elif self._references_table(item, target_key):
                continue
            kept.append(item)
        schema.set("expressions", kept)

    def _forget_object(self, key: str) -> None:
        for index_key in [index_key for index_key, entry in self._indexes.items() if entry.owner == key]:
            self._indexes.pop(index_key)
            self._comments.pop(("INDEX", index_key), None)
        prefix = f"{key}."
        stale_comments = [
            comment_key
            for comment_key in self._comments
            if (comment_key[0] != "COLUMN" and comment_key[1] == key)
            or (comment_key[0] == "COLUMN" and comment_key[1].startswith(prefix))
        ]
        for comment_key in stale_comments:
            self._comments.pop(comment_key)

    def _apply_comment(self, expr: exp.Comment) -> None:
        kind = str(expr.args.get("kind") or "").upper()
        target = expr.this
        if not isinstance(target, exp.Expr):
            msg = "the COMMENT target could not be read"
            raise self._refusal(msg)
        if kind == "COLUMN" and isinstance(target, exp.Column):
            table_key = self._object_key(
                exp.Table(this=target.args.get("table"), db=target.args.get("db"), catalog=target.args.get("catalog"))
            )
            column_key = self._name_key(target)
            table_entry = self._require_table(table_key, "COMMENT ON COLUMN")
            if self._find_column(self._table_schema(table_entry), column_key) is None:
                msg = f"column {column_key} does not exist on {table_key}"
                raise self._refusal(msg)
            key = f"{table_key}.{column_key}"
        elif kind == "INDEX":
            raw_key = self._object_key(target)
            key = self._resolve_index_key(raw_key)
            if key not in self._indexes:
                msg = f"COMMENT ON INDEX targets {raw_key}, which is not created within the squash range"
                raise self._refusal(msg)
        else:
            key = self._object_key(target)
            catalogs = {
                "TABLE": self._tables,
                "VIEW": self._views,
                "MATERIALIZED VIEW": self._views,
                "SEQUENCE": self._sequences,
                "SCHEMA": self._schemas,
            }
            catalog = catalogs.get(kind)
            if catalog is None:
                msg = f"COMMENT ON {kind or 'objects of this kind'} cannot be folded"
                raise self._refusal(msg)
            if key not in catalog:
                msg = f"COMMENT ON {kind} targets {key}, which is not created within the squash range"
                raise self._refusal(msg)
        self._comments[kind, key] = self._new_entry(expr)

    def _apply_dml(self, expr: exp.Expr) -> None:
        targets = self._dml_targets(expr)
        if not targets:
            msg = "the DML target table could not be read"
            raise self._refusal(msg)
        target_keys: list[str] = []
        for target in targets:
            key = self._object_key(target)
            if key not in self._tables:
                msg = f"DML targets {key}, which is not a table created within the squash range"
                raise self._refusal(msg)
            target_keys.append(key)
        target_ids = {id(target) for target in targets}
        cte_names = self._cte_names(expr)
        sources = [
            self._object_key(table)
            for table in expr.find_all(exp.Table)
            if id(table) not in target_ids and self._is_relation_reference(table, cte_names)
        ]
        entry = self._new_entry(expr)
        for key in self._expand_dependencies([*target_keys, *sources]):
            if key in self._tables or key in self._views:
                self._dml_touched.setdefault(key, entry)
        for key in target_keys:
            self._dml_written.setdefault(key, entry)
        self._dml.append(entry)

    def _assert_restructurable(self, key: str, label: str) -> None:
        touched = self._dml_touched.get(key)
        if touched is not None:
            msg = (
                f"{label} changes {key} after DML in migration {touched.origin or '<unknown>'} "
                f"('{self._snippet(touched.text)}') uses it; the folded migration would replay that DML "
                "against the final shape"
            )
            raise self._refusal(msg)
        readers = self._ctas_sources.get(key)
        if readers:
            msg = f"{label} changes {key} after CREATE TABLE AS SELECT for {', '.join(sorted(readers))} reads it"
            raise self._refusal(msg)

    def _assert_views_allow(self, table_key: str, columns: set[str] | None, label: str) -> None:
        for view_key in self._dependent_views(table_key):
            query = self._views[view_key].expr.expression
            if columns is None:
                msg = f"{label} on {table_key} would have to rewrite view {view_key}, which depends on it"
                raise self._refusal(msg)
            if not isinstance(query, exp.Expr):
                continue
            uses_star = any(not isinstance(star.parent, exp.Count) for star in query.find_all(exp.Star))
            if uses_star or self._column_names(query) & columns:
                msg = f"{label} on {table_key} changes columns that view {view_key} selects"
                raise self._refusal(msg)

    def _require_table(self, key: str, label: str) -> "_FoldedStatement":
        entry = self._tables.get(key)
        if entry is None:
            msg = f"{label} targets {key}, which is not a table created within the squash range"
            raise self._refusal(msg)
        return entry

    def _new_entry(self, expr: exp.Expr, owner: str = "") -> "_FoldedStatement":
        self._order += 1
        return _FoldedStatement(
            expr.copy(), self._current_text, self._current_origin, self._order, self._current_faithful, owner
        )

    def _mark_modified(self, entry: "_FoldedStatement") -> None:
        if not entry.faithful:
            msg = (
                f"it rewrites the statement from migration {entry.origin or '<unknown>'} "
                f"('{self._snippet(entry.text)}'), which SQLGlot does not reproduce faithfully"
            )
            raise self._refusal(msg)
        entry.modified = True

    def _ordered_relation_keys(self) -> list[str]:
        entries: dict[str, _FoldedStatement] = {**self._tables, **self._views}
        keys = sorted(entries, key=lambda key: entries[key].order)
        dependencies = {key: self._relation_dependencies(key, entries) for key in keys}
        ordered: list[str] = []
        state: dict[str, int] = {}
        for root in keys:
            if root in state:
                continue
            state[root] = 1
            stack: list[tuple[str, int]] = [(root, 0)]
            while stack:
                node, position = stack[-1]
                node_dependencies = dependencies[node]
                if position < len(node_dependencies):
                    stack[-1] = (node, position + 1)
                    dependency = node_dependencies[position]
                    dependency_state = state.get(dependency, 0)
                    if dependency_state == 1:
                        msg = (
                            f"{node} and {dependency} depend on each other, which CREATE statements cannot express "
                            "without separate ALTER statements"
                        )
                        raise self._refusal(msg, entries[node])
                    if dependency_state == 0:
                        state[dependency] = 1
                        stack.append((dependency, 0))
                else:
                    stack.pop()
                    state[node] = 2
                    ordered.append(node)
        return ordered

    def _relation_dependencies(self, key: str, entries: "dict[str, _FoldedStatement]") -> list[str]:
        entry = entries[key]
        found: list[str] = []
        query = entry.expr.expression
        if isinstance(query, exp.Query):
            found.extend(self._query_sources(query))
        if key in self._tables:
            for reference in entry.expr.find_all(exp.Reference):
                ref_table = self._reference_table(reference)
                if ref_table is not None:
                    found.append(self._resolve_reference_key(ref_table))
        unique = {dependency for dependency in found if dependency != key and dependency in entries}
        return sorted(unique, key=lambda dependency: entries[dependency].order)

    def _dependent_views(self, key: str) -> list[str]:
        return [
            view_key
            for view_key, view in self._views.items()
            if view_key != key
            and isinstance(view.expr.expression, exp.Query)
            and key in self._query_sources(view.expr.expression)
        ]

    def _expand_dependencies(self, keys: Iterable[str]) -> list[str]:
        expanded: list[str] = []
        pending = list(keys)
        while pending:
            key = pending.pop()
            if key in expanded:
                continue
            expanded.append(key)
            view = self._views.get(key)
            if view is not None and isinstance(view.expr.expression, exp.Query):
                pending.extend(self._query_sources(view.expr.expression))
        return expanded

    def _query_sources(self, query: exp.Expr) -> list[str]:
        cte_names = self._cte_names(query)
        sources: list[str] = []
        for table in query.find_all(exp.Table):
            if self._is_relation_reference(table, cte_names):
                key = self._object_key(table)
                if key not in sources:
                    sources.append(key)
        return sources

    def _cte_names(self, node: exp.Expr) -> set[str]:
        names: set[str] = set()
        for cte in node.find_all(exp.CTE):
            alias = cte.args.get("alias")
            if isinstance(alias, exp.TableAlias) and isinstance(alias.this, exp.Identifier):
                names.add(self._ident_key(alias.this))
        return names

    def _is_relation_reference(self, table: exp.Table, cte_names: set[str]) -> bool:
        if not isinstance(table.this, exp.Identifier):
            return False
        return bool(table.args.get("db")) or self._ident_key(table.this) not in cte_names

    def _references_table(self, node: exp.Expr, target_key: str) -> bool:
        for reference in node.find_all(exp.Reference):
            ref_table = self._reference_table(reference)
            if ref_table is not None and self._resolve_reference_key(ref_table) == target_key:
                return True
        return False

    def _referenced_columns(self, expr: exp.Expr, target_key: str) -> set[str]:
        columns: set[str] = set()
        for reference in expr.find_all(exp.Reference):
            ref_table = self._reference_table(reference)
            if ref_table is None or self._resolve_reference_key(ref_table) != target_key:
                continue
            ref_schema = reference.this
            if isinstance(ref_schema, exp.Schema) and ref_schema.expressions:
                columns.update(self._name_key(ident) for ident in ref_schema.expressions)
            else:
                columns.update(self._primary_key_columns(target_key))
        return columns

    def _foreign_reference_idents(self, expr: exp.Expr, target_key: str) -> list[exp.Identifier]:
        idents: list[exp.Identifier] = []
        for reference in expr.find_all(exp.Reference):
            ref_table = self._reference_table(reference)
            ref_schema = reference.this
            if (
                ref_table is not None
                and isinstance(ref_schema, exp.Schema)
                and self._resolve_reference_key(ref_table) == target_key
            ):
                idents.extend(ident for ident in ref_schema.expressions if isinstance(ident, exp.Identifier))
        return idents

    def _primary_key_columns(self, table_key: str) -> set[str]:
        entry = self._tables.get(table_key)
        if entry is None:
            return set()
        columns: set[str] = set()
        for item in self._table_schema(entry).expressions:
            if isinstance(item, exp.ColumnDef):
                if any(
                    isinstance(c.args.get("kind"), exp.PrimaryKeyColumnConstraint)
                    for c in item.args.get("constraints") or []
                ):
                    columns.add(self._name_key(item))
            elif isinstance(item, exp.PrimaryKey) or (
                isinstance(item, exp.Constraint) and any(isinstance(sub, exp.PrimaryKey) for sub in item.expressions)
            ):
                columns.update(self._local_column_keys(item, table_key))
        return columns

    def _local_column_keys(self, node: exp.Expr, owner_key: str) -> set[str]:
        return {self._ident_key(ident) for ident in self._local_column_refs(node, owner_key)}

    def _local_column_refs(self, node: exp.Expr, owner_key: str) -> list[exp.Identifier]:
        """Return identifiers inside a table item that name columns of the owning table.

        Args:
            node: A column definition, column constraint, or table-level constraint.
            owner_key: Catalog key of the table that owns ``node``.

        Returns:
            Identifier nodes that refer to columns of ``owner_key``, including self-referencing
            foreign key targets but excluding columns of other referenced tables.
        """
        refs: list[exp.Identifier] = []
        for ident in node.find_all(exp.Identifier):
            parent = ident.parent
            if isinstance(parent, exp.Column):
                if parent.this is ident and parent.find_ancestor(exp.Reference) is None:
                    refs.append(ident)
            elif isinstance(parent, exp.Schema):
                if parent.this is None or (
                    isinstance(parent.parent, exp.Reference)
                    and isinstance(parent.this, exp.Table)
                    and self._resolve_reference_key(parent.this) == owner_key
                ):
                    refs.append(ident)
            elif isinstance(parent, (exp.ForeignKey, exp.PrimaryKey)) and ident.arg_key == "expressions":
                refs.append(ident)
        return refs

    def _index_column_refs(self, entry: "_FoldedStatement") -> list[exp.Identifier]:
        index = entry.expr.this
        params = index.args.get("params") if isinstance(index, exp.Index) else None
        if not isinstance(params, exp.Expr):
            return []
        return [column.this for column in params.find_all(exp.Column) if isinstance(column.this, exp.Identifier)]

    def _index_column_keys(self, entry: "_FoldedStatement") -> set[str]:
        return {self._ident_key(ident) for ident in self._index_column_refs(entry)}

    def _column_names(self, node: exp.Expr) -> set[str]:
        return {self._name_key(column) for column in node.find_all(exp.Column) if column.name}

    def _find_column(self, schema: exp.Schema, column_key: str) -> exp.ColumnDef | None:
        for item in schema.expressions:
            if isinstance(item, exp.ColumnDef) and self._name_key(item) == column_key:
                return item
        return None

    def _resolve_reference_key(self, table: exp.Table) -> str:
        key = self._object_key(table)
        if key in self._tables or table.args.get("db"):
            return key
        candidates = [candidate for candidate in self._tables if candidate.rsplit(".", 1)[-1] == key]
        return candidates[0] if len(candidates) == 1 else key

    def _relation(self, key: str) -> "_FoldedStatement":
        entry = self._tables.get(key)
        return entry if entry is not None else self._views[key]

    def _render(self, entry: "_FoldedStatement") -> str:
        if not entry.modified:
            return _ensure_semicolon(entry.text)
        return self._render_expr(entry.expr, entry)

    def _render_drop(self, entry: "_FoldedStatement", kind: str, *, materialized: bool = False) -> str:
        target = entry.expr.this.this if isinstance(entry.expr.this, exp.Schema) else entry.expr.this
        if not isinstance(target, exp.Expr):
            msg = "the dropped object name could not be read"
            raise self._refusal(msg, entry)
        drop = exp.Drop(tables=[target.copy()], kind=kind, exists=True, materialized=materialized)
        return self._render_expr(drop, entry)

    def _render_expr(self, expr: exp.Expr, entry: "_FoldedStatement") -> str:
        try:
            sql = expr.sql(dialect=self._dialect, unsupported_level=ErrorLevel.RAISE)
        except UnsupportedError as error:
            msg = f"SQLGlot cannot render it for this dialect ({error})"
            raise self._refusal(msg, entry) from error
        return _ensure_semicolon(sql)

    def _is_faithful(self, expr: exp.Expr, tokens: list[Token], spans: list[tuple[exp.Expr, str, int, int]]) -> bool:
        """Return whether rendering ``expr`` reproduces the original statement faithfully.

        Declared column data types are compared as parsed type nodes rendered for the dialect,
        so column type synonyms (``VARCHAR`` versus SQLite ``TEXT``) are faithful while rewrites
        that change a column definition (Postgres ``SERIAL`` becoming an identity column) are not.
        Column type spans are then masked on both sides, while non-column types (such as ``CAST``
        targets inside ``DEFAULT`` or ``CHECK`` expressions) remain unmasked so dialect rewrites
        that alter runtime evaluation are rejected. Every other keyword token type of the original
        must appear in the rendering (allowing a small set of synonyms) and every identifier,
        literal, and non-function word must survive. Function names may be canonicalized because
        SQLGlot maps them to equivalent spellings.

        Args:
            expr: Parsed statement, unchanged on return.
            tokens: Original tokens of the statement.
            spans: Declared column type spans from :meth:`_column_type_spans`.

        Returns:
            True when the rendering is faithful to the original text.
        """
        try:
            rendered = expr.sql(dialect=self._dialect, unsupported_level=ErrorLevel.IGNORE)
            reparsed = [node for node in self._dialect_impl.parse(rendered) if node is not None]
        except (ParseError, TokenError):
            return False
        if self._type_signature([expr]) != self._type_signature(reparsed):
            return False
        original_kinds = [(node, arg_key, node.args.get(arg_key)) for node, arg_key, _, _ in spans]
        for node, arg_key, _ in original_kinds:
            node.set(arg_key, exp.DataType(this=exp.DataType.Type.USERDEFINED, kind=_TYPE_PLACEHOLDER))
        try:
            masked = expr.sql(dialect=self._dialect, unsupported_level=ErrorLevel.IGNORE)
        finally:
            for node, arg_key, kind in original_kinds:
                node.set(arg_key, kind)
        try:
            masked_tokens = self._dialect_impl.tokenize(masked)
        except TokenError:
            return False
        skipped = {position for _, _, first, last in spans for position in range(first, last + 1)}
        original_types, original_words = self._token_signature([
            token for position, token in enumerate(tokens) if position not in skipped
        ])
        original_words[_TYPE_PLACEHOLDER] += len(spans)
        rendered_types, rendered_words = self._token_signature(masked_tokens)
        for token_type in original_types - rendered_types:
            equivalent = _EQUIVALENT_TOKEN_TYPES.get(token_type)
            if equivalent is None or equivalent not in rendered_types:
                return False
        return not original_words - rendered_words

    def _type_signature(self, expressions: list[exp.Expr]) -> "Counter[str]":
        return Counter(
            data_type.sql(dialect=self._dialect) for node in expressions for data_type in node.find_all(exp.DataType)
        )

    def _refusal(self, reason: str, entry: "_FoldedStatement | None" = None) -> SquashValidationError:
        text = entry.text if entry is not None else self._current_text
        origin = entry.origin if entry is not None else self._current_origin
        location = f"migration {origin} statement" if origin else "statement"
        return SquashValidationError(
            f"Cannot fold {location} '{self._snippet(text)}' with the ddl squash strategy: {reason}. {_CONCAT_HINT}"
        )

    def _object_key(self, node: exp.Expr | None) -> str:
        target = node.this if isinstance(node, exp.Schema) else node
        if target is None:
            return ""
        if isinstance(target, exp.Table):
            parts = [target.args.get("catalog"), target.args.get("db"), target.this]
            keys = [self._part_key(part) for part in parts if isinstance(part, exp.Expr)]
            return ".".join(key for key in keys if key)
        return self._part_key(target)

    def _part_key(self, part: exp.Expr) -> str:
        if isinstance(part, exp.Identifier):
            return self._ident_key(part)
        return part.name.lower()

    def _name_key(self, node: exp.Expr | None) -> str:
        if node is None:
            return ""
        if isinstance(node, exp.Identifier):
            return self._ident_key(node)
        if isinstance(node.this, exp.Identifier):
            return self._ident_key(node.this)
        return node.name.lower()

    def _ident_key(self, ident: exp.Identifier) -> str:
        return self._dialect_impl.normalize_identifier(ident.copy()).name

    def _index_catalog_key(self, base_name: str, table_key: str) -> str:
        if (self._dialect or "") in _MYSQL_FAMILY | {"tsql", "mssql"}:
            return f"{table_key}.{base_name}"
        if "." in table_key:
            schema_prefix = table_key.rsplit(".", 1)[0]
            return f"{schema_prefix}.{base_name}"
        return base_name

    def _resolve_index_key(self, target_key: str, *, owner: str | None = None) -> str:
        if owner is not None:
            scoped_key = self._index_catalog_key(target_key.rsplit(".", 1)[-1], owner)
            if scoped_key in self._indexes:
                return scoped_key
        if target_key in self._indexes:
            return target_key
        if "." not in target_key:
            matches = [
                key
                for key, entry in self._indexes.items()
                if key.rsplit(".", 1)[-1] == target_key and (owner is None or entry.owner == owner)
            ]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                msg = f"index reference {target_key} is ambiguous across {', '.join(matches)}"
                raise self._refusal(msg)
        return target_key

    @staticmethod
    def _token_signature(tokens: list[Token]) -> tuple[set[TokenType], "Counter[str]"]:
        """Return keyword token types and words for non-masked statement tokens."""
        types: set[TokenType] = set()
        words: Counter[str] = Counter()
        for position, token in enumerate(tokens):
            token_type = token.token_type
            if token_type == TokenType.SEMICOLON:
                continue
            if token_type not in _TEXT_TOKEN_TYPES:
                types.add(token_type)
                continue
            next_type = tokens[position + 1].token_type if position + 1 < len(tokens) else None
            if token_type == TokenType.VAR and next_type == TokenType.L_PAREN:
                continue
            words[token.text if token_type in _CASE_SENSITIVE_TOKEN_TYPES else token.text.lower()] += 1
        return types, words

    @staticmethod
    def _snippet(text: str) -> str:
        collapsed = " ".join(text.split())
        if len(collapsed) > _SNIPPET_LENGTH:
            return f"{collapsed[: _SNIPPET_LENGTH - 3]}..."
        return collapsed

    @staticmethod
    def _table_schema(entry: "_FoldedStatement") -> exp.Schema:
        schema = entry.expr.this
        if not isinstance(schema, exp.Schema):
            schema = exp.Schema(this=entry.expr.this, expressions=[])
            entry.expr.set("this", schema)
        return schema

    @staticmethod
    def _set_create_table_node(create_expr: exp.Expr, new_table: exp.Table) -> None:
        if isinstance(create_expr.this, exp.Schema):
            create_expr.this.set("this", new_table)
        else:
            create_expr.set("this", new_table)

    @staticmethod
    def _reference_table(reference: exp.Reference) -> exp.Table | None:
        target = reference.this.this if isinstance(reference.this, exp.Schema) else reference.this
        return target if isinstance(target, exp.Table) else None

    @staticmethod
    def _is_materialized(entry: "_FoldedStatement") -> bool:
        return entry.expr.find(exp.MaterializedProperty) is not None

    @staticmethod
    def _drop_targets(expr: exp.Drop) -> list[exp.Expr]:
        targets: list[exp.Expr] = [target for target in expr.args.get("tables") or [] if isinstance(target, exp.Expr)]
        if isinstance(expr.this, exp.Expr) and all(target is not expr.this for target in targets):
            targets.append(expr.this)
        return targets

    @staticmethod
    def _dml_targets(expr: exp.Expr) -> list[exp.Table]:
        if isinstance(expr, exp.TruncateTable):
            return [table for table in expr.expressions if isinstance(table, exp.Table)]
        target = expr.this.this if isinstance(expr.this, exp.Schema) else expr.this
        return [target] if isinstance(target, exp.Table) else []


@dataclass(slots=True)
class SquashPlan:
    """Represents a planned squash operation.

    Attributes:
        source_migrations: List of (version, path) tuples for migrations being squashed.
        target_version: The version string for the squashed migration.
        target_path: Output file path for the squashed migration.
        description: Combined description for the squashed migration.
        source_versions: List of version strings being replaced (for tracking table updates).
        transactional: Optional transactional directive preserved from source SQL migrations.
        schema: Optional schema directive preserved from source SQL migrations.
        strategy: Squash strategy ("concat", "ddl", or "schema").
        schema_ddl_up: Optional pre-extracted schema CREATE DDL statements.
        schema_ddl_down: Optional pre-extracted schema DROP DDL statements.
        missing_down_versions: Optional list of source versions that lacked a down block.
    """

    source_migrations: list[tuple[str, Path]]
    target_version: str
    target_path: Path
    description: str
    source_versions: list[str]
    transactional: bool | None = None
    schema: str | None = None
    strategy: str = "concat"
    schema_ddl_up: list[str] | None = None
    schema_ddl_down: list[str] | None = None
    missing_down_versions: list[str] | None = None


class MigrationSquasher:
    """Core squash engine for combining migrations.

    Provides functionality to plan, validate, and execute migration squash operations.
    Combines multiple sequential or timestamped migrations into a single file.
    """

    __slots__ = (
        "backup_path",
        "migrations_path",
        "runner",
        "schema_ddl",
        "strategy",
        "template_settings",
        "version_table",
    )

    def __init__(
        self,
        migrations_path: Path,
        runner: "SyncMigrationRunner",
        template_settings: "MigrationTemplateSettings | None" = None,
        *,
        strategy: str = "concat",
        schema_ddl: tuple[list[str], list[str]] | None = None,
        version_table: str = "ddl_migrations",
    ) -> None:
        """Initialize the migration squasher.

        Args:
            migrations_path: Path to the migrations directory.
            runner: SyncMigrationRunner instance for loading migrations.
            template_settings: Optional template settings for generating squashed file.
            strategy: Default squash strategy ("concat", "ddl", or "schema").
            schema_ddl: Optional pre-fetched (up_ddl, down_ddl) tuple for "schema" strategy.
            version_table: Migration tracking table excluded from live "schema" extraction.
        """
        self.migrations_path = migrations_path
        self.runner = runner
        self.template_settings = template_settings
        self.backup_path: Path | None = None
        self.strategy = self._resolve_strategy(strategy)
        self.schema_ddl = schema_ddl
        self.version_table = version_table

    def plan_squash(
        self,
        start_version: str,
        end_version: str,
        description: str,
        *,
        allow_gaps: bool = False,
        output_format: str = "sql",
        strategy: str | None = None,
        single_file: bool | None = None,
        schema_ddl: tuple[list[str], list[str]] | None = None,
    ) -> list[SquashPlan]:
        """Plan a squash operation for a range of migrations.

        Args:
            start_version: First version in the range to squash (inclusive).
            end_version: Last version in the range to squash (inclusive).
            description: Description for the squashed migration file.
            allow_gaps: If True, allow gaps in version sequence.
            output_format: Output file format ("sql" or "py").
            strategy: Squash strategy override ("concat", "ddl", or "schema").
            single_file: When True, emit a single SquashPlan across mixed file types/directives.
            schema_ddl: Optional pre-fetched (up_ddl, down_ddl) tuple for "schema" strategy.

        Returns:
            List of SquashPlan objects with details of planned operations.
        """
        resolved_strategy = self._resolve_strategy(strategy or self.strategy)
        resolved_schema_ddl = schema_ddl if schema_ddl is not None else self.schema_ddl
        all_migrations = self.runner.get_migration_files()
        source_migrations = validate_squash_range(all_migrations, start_version, end_version, allow_gaps=allow_gaps)

        safe_description = slugify(description, separator="_")[:50] or "migration"

        if output_format == "py":
            directive_groups: list[tuple[str, bool | None, str | None, list[tuple[str, Path]]]] = [
                ("py", None, None, source_migrations)
            ]
        elif single_file is True:
            file_type = "py" if all(p.suffix == ".py" for _, p in source_migrations) else "sql"
            tx_dir, schema_dir = _coalesce_sql_directives(source_migrations) if file_type == "sql" else (None, None)
            directive_groups = [(file_type, tx_dir, schema_dir, source_migrations)]
        else:
            directive_groups = _group_migrations_with_directives(
                source_migrations, split_by_directives=single_file is False
            )

        parsed_start = parse_version(start_version)
        if parsed_start.sequence is not None:
            version_counter = parsed_start.sequence
        else:
            prior_sequences: list[int] = []
            for ver_str, _ in all_migrations:
                try:
                    parsed_ver = parse_version(ver_str)
                except ValueError:
                    continue
                if (
                    parsed_ver.extension == parsed_start.extension
                    and parsed_ver < parsed_start
                    and parsed_ver.sequence is not None
                ):
                    prior_sequences.append(parsed_ver.sequence)
            version_counter = (max(prior_sequences) + 1) if prior_sequences else 1

        ext_prefix = f"ext_{parsed_start.extension}_" if parsed_start.extension else ""

        plans: list[SquashPlan] = []

        for file_type, transactional, schema, group_migrations in directive_groups:
            group_versions = [v for v, _ in group_migrations]
            target_version = f"{ext_prefix}{_format_sequential_version(version_counter)}"
            extension = ".py" if file_type == "py" else ".sql"
            target_path = self.migrations_path / f"{target_version}_{safe_description}{extension}"

            plans.append(
                SquashPlan(
                    source_migrations=group_migrations,
                    target_version=target_version,
                    target_path=target_path,
                    description=description,
                    source_versions=group_versions,
                    transactional=transactional,
                    schema=schema,
                    strategy=resolved_strategy,
                    schema_ddl_up=list(resolved_schema_ddl[0]) if resolved_schema_ddl is not None else None,
                    schema_ddl_down=list(resolved_schema_ddl[1]) if resolved_schema_ddl is not None else None,
                )
            )
            version_counter += 1

        return plans

    def extract_sql(
        self,
        migrations: list[tuple[str, Path]],
        *,
        strategy: str | None = None,
        dialect: str | None = None,
        schema_ddl: tuple[list[str], list[str]] | None = None,
    ) -> tuple[list[str], list[str]]:
        """Extract UP and DOWN SQL statements from migrations.

        Args:
            migrations: List of (version, path) tuples to extract SQL from.
            strategy: Optional strategy ("concat", "ddl", or "schema"). When None,
                returns the raw UP statements and the reversed raw DOWN statements.
            dialect: Optional SQLGlot dialect name for DDL folding.
            schema_ddl: Optional pre-fetched (up_ddl, down_ddl) tuple for "schema" strategy.

        Returns:
            Tuple of (up_statements, down_statements) lists.
        """
        if strategy is None:
            up_entries, down_statements, _ = self._extract_raw_sql(migrations)
            return [sql for _, sql in up_entries], down_statements
        up_statements, down_statements, _ = self._extract_with_strategy(
            migrations, self._resolve_strategy(strategy), dialect=dialect, schema_ddl=schema_ddl
        )
        return up_statements, down_statements

    def extract_plan_sql(self, plan: SquashPlan) -> tuple[list[str], list[str]]:
        """Extract the UP and DOWN SQL for a squash plan using the plan's strategy.

        Records versions lacking a down block on ``plan.missing_down_versions`` when the
        squashed migration is irreversible.

        Args:
            plan: The SquashPlan to extract SQL for.

        Returns:
            Tuple of (up_statements, down_statements) lists.
        """
        plan_schema_ddl = (plan.schema_ddl_up, plan.schema_ddl_down or []) if plan.schema_ddl_up is not None else None
        up_statements, down_statements, missing_downs = self._extract_with_strategy(
            plan.source_migrations,
            self._resolve_strategy(plan.strategy or self.strategy),
            dialect=None,
            schema_ddl=plan_schema_ddl,
        )
        if missing_downs:
            plan.missing_down_versions = missing_downs
        return up_statements, down_statements

    def generate_squashed_content(self, plan: SquashPlan, up_sql: list[str], down_sql: list[str]) -> str:
        """Generate the content for a squashed migration file.

        Args:
            plan: The SquashPlan describing the squash operation.
            up_sql: List of UP SQL statements (in execution order).
            down_sql: List of DOWN SQL statements (in rollback order).

        Returns:
            Complete SQL file content as a string.
        """
        lines: list[str] = []

        title = "SQLSpec Migration"
        if self.template_settings and self.template_settings.profile:
            title = self.template_settings.profile.title

        lines.extend((
            f"-- {title}",
            f"-- Version: {plan.target_version}",
            f"-- Description: {plan.description}",
            f"-- Squashed from: {', '.join(plan.source_versions)}",
        ))
        if plan.transactional is not None:
            lines.append(f"-- transactional: {'true' if plan.transactional else 'false'}")
        if plan.schema is not None:
            lines.append(f"-- schema: {plan.schema}")
        lines.extend(("", f"-- name: migrate-{plan.target_version}-up"))
        lines.extend(statement.rstrip() for statement in up_sql)
        lines.append("")

        if down_sql:
            lines.append(f"-- name: migrate-{plan.target_version}-down")
            lines.extend(statement.rstrip() for statement in down_sql)
            lines.append("")
        elif plan.missing_down_versions:
            lines.append(f"-- Note: Irreversible squash (missing down in {', '.join(plan.missing_down_versions)})")
            lines.append("")

        return "\n".join(lines)

    def generate_python_squash(self, plan: SquashPlan, up_sql: list[str], down_sql: list[str]) -> str:
        """Generate Python migration file content instead of SQL.

        Creates a Python migration file with up() and down() functions
        that return the SQL statements as lists.

        Args:
            plan: The SquashPlan describing the squash operation.
            up_sql: List of UP SQL statements (in execution order).
            down_sql: List of DOWN SQL statements (in rollback order).

        Returns:
            Complete Python file content as a string.
        """
        lines: list[str] = []

        title = "SQLSpec Migration"
        if self.template_settings and self.template_settings.profile:
            title = self.template_settings.profile.title

        lines.extend([
            '"""' + title + ".",
            "",
            f"Version: {plan.target_version}",
            f"Description: {plan.description}",
            f"Squashed from: {', '.join(plan.source_versions)}",
            '"""',
            "",
        ])

        if plan.source_versions:
            lines.extend([f"__replaces__ = {list(plan.source_versions)!r}", ""])

        lines.extend(["def up() -> list[str]:", '    """Return UP migration SQL statements."""', "    return ["])
        lines.extend(f"        {statement!r}," for statement in up_sql)
        lines.extend(["    ]", ""])

        lines.extend(["def down() -> list[str] | None:", '    """Return DOWN migration SQL statements."""'])
        if down_sql:
            lines.append("    return [")
            lines.extend(f"        {statement!r}," for statement in down_sql)
            lines.extend(["    ]", ""])
        else:
            lines.extend(["    return None", ""])

        return "\n".join(lines)

    def apply_squash(self, plans: list[SquashPlan], *, dry_run: bool = False, cleanup_backup: bool = True) -> None:
        """Apply the squash operation for one or more plans.

        Renders every squashed file before touching the filesystem, so a refusal from the
        ``ddl`` strategy leaves the migrations directory unchanged. Then creates a backup,
        writes squashed files, deletes source migrations, and cleans up the backup on
        success. Rolls back on error.

        Args:
            plans: List of SquashPlan objects to execute.
            dry_run: If True, no files are modified (preview only).
            cleanup_backup: If True (default), remove the backup directory before returning.
                Set False when callers need to keep the backup until database tracking is updated.
        """
        if dry_run:
            logger.debug("Dry run mode - no changes will be made")
            return

        all_source_files = {path for plan in plans for _, path in plan.source_migrations}
        all_target_files = {plan.target_path for plan in plans}
        ready_plans: list[SquashPlan] = []
        for plan in plans:
            validate_extension_consistency(plan.source_migrations)
            source_files = [path for _, path in plan.source_migrations]
            status = validate_squash_idempotency(
                source_files, plan.target_path, all_source_files=all_source_files, all_target_files=all_target_files
            )
            if status == "already_squashed":
                logger.debug("Squash target already applied: %s", plan.target_path)
                continue
            if status == "partial":
                msg = f"Squash plan for {plan.target_path.name} is partially applied"
                raise SquashValidationError(msg)
            ready_plans.append(plan)

        if ready_plans and len(ready_plans) != len(plans) and (all_source_files & all_target_files):
            msg = "Multi-plan squash batch with overlapping target filenames is partially applied"
            raise SquashValidationError(msg)

        if not ready_plans:
            return

        planned_contents: list[tuple[SquashPlan, str]] = []
        for plan in ready_plans:
            up_sql, down_sql = self.extract_plan_sql(plan)
            if plan.target_path.suffix == ".py":
                content = self.generate_python_squash(plan, up_sql, down_sql)
            else:
                content = self.generate_squashed_content(plan, up_sql, down_sql)
            planned_contents.append((plan, content))

        self._create_backup()

        try:
            for plan, content in planned_contents:
                plan.target_path.write_text(content, encoding="utf-8")
                logger.debug("Wrote squashed migration to %s", plan.target_path)

            target_paths = {plan.target_path for plan in ready_plans}
            all_source_paths = {
                source_path
                for plan in ready_plans
                for _, source_path in plan.source_migrations
                if source_path not in target_paths
            }

            for source_path in all_source_paths:
                if source_path.exists():
                    source_path.unlink()
                    logger.debug("Deleted source migration %s", source_path)

            if cleanup_backup:
                self.cleanup_backup()

        except Exception:
            self.rollback_backup()
            raise

    def cleanup_backup(self) -> None:
        """Remove the backup directory after a successful operation."""
        if not self.backup_path or not self.backup_path.exists():
            return

        remove_backup(self.backup_path)
        logger.debug("Cleaned up backup at %s", self.backup_path)
        self.backup_path = None

    def rollback_backup(self) -> None:
        """Restore migration files from the backup directory after an error."""
        if not self.backup_path or not self.backup_path.exists():
            return

        backup_dir = self.backup_path
        restore_backup(self.migrations_path, backup_dir, delete_backup=True)
        self.backup_path = None

        logger.debug("Rolled back from backup at %s", backup_dir)

    def _extract_with_strategy(
        self,
        migrations: list[tuple[str, Path]],
        strategy: str,
        *,
        dialect: str | None,
        schema_ddl: tuple[list[str], list[str]] | None,
    ) -> tuple[list[str], list[str], list[str]]:
        if strategy == "schema":
            effective_schema_ddl = schema_ddl if schema_ddl is not None else self.schema_ddl
            if effective_schema_ddl is not None:
                return list(effective_schema_ddl[0]), list(effective_schema_ddl[1]), []
            schema_up, schema_down = self._extract_schema_ddl_from_context()
            return schema_up, schema_down, []

        up_entries, down_statements, missing_downs = self._extract_raw_sql(migrations)
        if strategy == "concat":
            return [sql for _, sql in up_entries], ([] if missing_downs else down_statements), missing_downs

        folded_up, folded_down = fold_migration_ddl(
            [sql for _, sql in up_entries],
            dialect=dialect if dialect is not None else self._resolve_dialect(),
            origins=[version for version, _ in up_entries],
        )
        return folded_up, folded_down, []

    def _extract_raw_sql(
        self, migrations: list[tuple[str, Path]]
    ) -> tuple[list[tuple[str, str]], list[str], list[str]]:
        up_entries: list[tuple[str, str]] = []
        down_statements: list[str] = []
        missing_down_versions: list[str] = []
        migration_down_sql: list[list[str]] = []

        for version, path in migrations:
            migration_data = self.runner.load_migration(path, version)
            loader = migration_data["loader"]

            up_sql = _load_migration_sql(loader, path, "up")
            if up_sql:
                up_entries.extend((version, statement) for statement in up_sql)

            cleaned_down = [stmt for stmt in _load_migration_sql(loader, path, "down") or [] if stmt.strip()]
            if not cleaned_down:
                missing_down_versions.append(version)
            migration_down_sql.append(cleaned_down)

        for down_sql_group in migration_down_sql[::-1]:
            down_statements.extend(down_sql_group)

        return up_entries, down_statements, missing_down_versions

    def _resolve_dialect(self) -> str | None:
        context = self.runner.context
        dialect = context.dialect if context is not None else None
        if dialect and dialect not in {"generic", "unknown"}:
            return dialect
        return None

    def _extract_schema_ddl_from_context(self) -> tuple[list[str], list[str]]:
        context = self.runner.context
        driver = context.driver if context is not None else None
        if not isinstance(driver, SyncDriverAdapterBase):
            msg = (
                "Schema squash strategy requires an active synchronous database driver; "
                "pass schema_ddl when reflecting through an async driver"
            )
            raise SquashValidationError(msg)
        version_table = self.version_table.rsplit(".", 1)[-1].strip('"`[]')
        data_dictionary = driver.data_dictionary
        create_res = data_dictionary.get_schema_ddl(driver, exclude_tables=(version_table,), direction="create")
        drop_res = data_dictionary.get_schema_ddl(driver, exclude_tables=(version_table,), direction="drop")
        return schema_ddl_statements(create_res, drop_res)

    def _create_backup(self) -> Path:
        """Create timestamped backup directory with all migration files.

        Returns:
            Path to created backup directory.
        """
        self.backup_path = create_backup(self.migrations_path)
        logger.debug("Created backup at %s", self.backup_path)
        return self.backup_path

    @staticmethod
    def _resolve_strategy(strategy: str) -> str:
        resolved = strategy.lower()
        if resolved not in _SQUASH_STRATEGIES:
            msg = f"Unsupported squash strategy: '{strategy}'. Expected 'concat', 'ddl', or 'schema'."
            raise SquashValidationError(msg)
        return resolved


def fold_migration_ddl(
    statements: Sequence[str], *, dialect: str | None = None, origins: Sequence[str] | None = None
) -> tuple[list[str], list[str]]:
    """Fold a chronological sequence of migration SQL statements into net canonical DDL.

    Args:
        statements: Chronological sequence of SQL text from ``up`` migrations.
        dialect: Optional SQLGlot dialect name used to parse and render DDL.
        origins: Optional labels (usually migration versions) parallel to ``statements``,
            used to name the offending migration in refusal messages.

    Returns:
        Tuple of ``(up_statements, down_statements)`` representing net schema DDL
        and reverse-dependency ``DROP`` statements.

    Raises:
        SquashValidationError: If the statements cannot be folded faithfully.
    """
    folder = DDLStateFolder(dialect=dialect)
    for position, statement in enumerate(statements):
        folder.feed(statement, origin=origins[position] if origins is not None else None)
    return folder.to_statements()


def parse_version_range(range_str: str) -> tuple[str, str]:
    """Parse a version range string into (start, end) tuple.

    Accepts multiple formats: ``START:END``, ``START..END``, or ``START-END``.

    Args:
        range_str: Version range string.

    Returns:
        Tuple of (start_version, end_version) zero-padded to 4 digits.

    Raises:
        ValueError: If the format is not recognised or an endpoint is empty.
    """
    for sep in (":", "..", "-"):
        if sep in range_str:
            raw_start, raw_end = (part.strip() for part in range_str.split(sep, 1))
            if not raw_start or not raw_end:
                msg = f"Invalid VERSION_RANGE '{range_str}': both START and END are required"
                raise ValueError(msg)
            return _normalize_range_endpoint(raw_start), _normalize_range_endpoint(raw_end)
    msg = f"Invalid VERSION_RANGE format: '{range_str}'. Use START:END, START..END, or START-END"
    raise ValueError(msg)


def group_migrations_by_type(migrations: list[tuple[str, Path]]) -> list[tuple[str, list[tuple[str, Path]]]]:
    """Group consecutive migrations by file type (sql or py).

    Partitions a list of migrations into groups where each group contains
    consecutive migrations of the same type. This enables squashing mixed
    SQL and Python migrations into separate output files.

    Args:
        migrations: List of (version, path) tuples to group.

    Returns:
        List of (type, migrations) tuples where type is "sql" or "py"
        and migrations is the list of (version, path) for that group.
    """
    if not migrations:
        return []

    groups: list[tuple[str, list[tuple[str, Path]]]] = []
    current_type: str | None = None
    current_group: list[tuple[str, Path]] = []

    for version, path in migrations:
        file_type = "py" if path.suffix == ".py" else "sql"

        if file_type != current_type:
            if current_group and current_type is not None:
                groups.append((current_type, current_group))
            current_type = file_type
            current_group = [(version, path)]
        else:
            current_group.append((version, path))

    if current_group and current_type is not None:
        groups.append((current_type, current_group))

    return groups


def schema_ddl_statements(create_res: Iterable[object], drop_res: Iterable[object]) -> tuple[list[str], list[str]]:
    """Convert ``get_schema_ddl`` create/drop results into semicolon-terminated UP and DOWN SQL lists.

    Args:
        create_res: Result of ``get_schema_ddl(direction="create")``.
        drop_res: Result of ``get_schema_ddl(direction="drop")``.

    Returns:
        Tuple of ``(up_ddl, down_ddl)`` statement lists.

    Raises:
        SquashValidationError: If any reflected object has no DDL text, since omitting it
            would silently produce an incomplete schema.
    """
    return _ddl_texts(create_res, "create"), _ddl_texts(drop_res, "drop")


def _ddl_texts(results: Iterable[object], direction: str) -> list[str]:
    """Read the DDL text of each schema DDL result, refusing results without DDL."""
    statements: list[str] = []
    for item in results:
        if not isinstance(item, DDLResult):
            msg = f"Unexpected schema DDL result {item!r} in {direction} direction"
            raise SquashValidationError(msg)
        if not item.ddl:
            identity = item.identity
            qualified = f"{identity.schema}.{identity.name}" if identity.schema else identity.name
            msg = (
                f"The data dictionary returned no {direction} DDL for {identity.object_type} {qualified} "
                f"(status {item.status}); the schema strategy cannot represent it"
            )
            raise SquashValidationError(msg)
        statements.append(_ensure_semicolon(item.ddl))
    return statements


def _ensure_semicolon(sql_text: str) -> str:
    """Return ``sql_text`` stripped and terminated by exactly one semicolon."""
    stripped = sql_text.strip()
    return stripped if stripped.endswith(";") else f"{stripped};"


class _FoldedStatement:
    """Mutable catalog entry for one statement tracked by :class:`DDLStateFolder`.

    Attributes:
        expr: Parsed statement, mutated in place as later statements are folded into it.
        text: Original statement text, emitted verbatim while the entry is unmodified.
        origin: Label of the migration that produced the statement.
        order: Monotonic creation order used for deterministic output.
        faithful: Whether SQLGlot reproduces ``text`` faithfully, required before rewriting.
        owner: Catalog key of the owning table for indexes.
        modified: Whether ``expr`` diverged from ``text`` and must be re-rendered.
    """

    __slots__ = ("expr", "faithful", "modified", "order", "origin", "owner", "text")

    def __init__(
        self, expr: exp.Expr, text: str, origin: str | None, order: int, faithful: bool, owner: str = ""
    ) -> None:
        self.expr = expr
        self.text = text
        self.origin = origin
        self.order = order
        self.faithful = faithful
        self.owner = owner
        self.modified = False


def _normalize_range_endpoint(endpoint: str) -> str:
    """Normalize a squash range endpoint, preserving extension prefixes and timestamps."""
    min_timestamp_len = 8
    if endpoint.startswith("ext_"):
        parsed = parse_extension_stem(endpoint)
        if parsed is not None:
            ext_name, ext_version = parsed
            if ext_version.isdigit() and len(ext_version) >= min_timestamp_len:
                return f"ext_{ext_name}_{ext_version}"
            return f"ext_{ext_name}_{_format_sequential_version(ext_version)}"
    if endpoint.isdigit() and len(endpoint) >= min_timestamp_len:
        return endpoint
    return _format_sequential_version(endpoint)


def _extract_sql_directives(path: Path) -> tuple[bool | None, str | None]:
    """Extract transactional and schema directives from a SQL migration file if it exists."""
    if path.suffix != ".sql" or not path.exists():
        return None, None
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return None, None
    tx_match = _TRANSACTIONAL_DIRECTIVE_RE.search(content)
    transactional = tx_match.group(1).lower() == "true" if tx_match else None
    schema_match = _SCHEMA_DIRECTIVE_RE.search(content)
    schema = schema_match.group(1) if schema_match else None
    return transactional, schema


def _coalesce_sql_directives(migrations: list[tuple[str, Path]]) -> tuple[bool | None, str | None]:
    """Coalesce uniform transactional and schema directives across SQL migrations."""
    sql_paths = [path for _, path in migrations if path.suffix == ".sql"]
    if not sql_paths:
        return None, None
    extracted = [_extract_sql_directives(path) for path in sql_paths]
    tx_values = {tx for tx, _ in extracted}
    schema_values = {sc for _, sc in extracted}
    uniform_tx = next(iter(tx_values)) if len(tx_values) == 1 else None
    uniform_schema = next(iter(schema_values)) if len(schema_values) == 1 else None
    return uniform_tx, uniform_schema


def _group_migrations_with_directives(
    migrations: list[tuple[str, Path]], *, split_by_directives: bool = False
) -> list[tuple[str, bool | None, str | None, list[tuple[str, Path]]]]:
    """Group consecutive migrations by file type and optionally SQL execution directives."""
    if not migrations:
        return []

    if not split_by_directives:
        type_groups = group_migrations_by_type(migrations)
        result: list[tuple[str, bool | None, str | None, list[tuple[str, Path]]]] = []
        for file_type, group_items in type_groups:
            tx_dir, schema_dir = _coalesce_sql_directives(group_items) if file_type == "sql" else (None, None)
            result.append((file_type, tx_dir, schema_dir, group_items))
        return result

    groups: list[tuple[str, bool | None, str | None, list[tuple[str, Path]]]] = []
    current_key: tuple[str, bool | None, str | None] | None = None
    current_group: list[tuple[str, Path]] = []

    for version, path in migrations:
        file_type = "py" if path.suffix == ".py" else "sql"
        transactional, schema = _extract_sql_directives(path) if file_type == "sql" else (None, None)
        key = (file_type, transactional, schema)
        if key != current_key:
            if current_group and current_key is not None:
                groups.append((current_key[0], current_key[1], current_key[2], current_group))
            current_key = key
            current_group = [(version, path)]
        else:
            current_group.append((version, path))

    if current_group and current_key is not None:
        groups.append((current_key[0], current_key[1], current_key[2], current_group))

    return groups
