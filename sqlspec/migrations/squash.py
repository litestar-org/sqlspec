"""Migration squash engine for combining multiple migrations into a single file.

This module provides utilities to consolidate multiple sequential migrations
into a single "release" migration file, following the Django-style squash workflow.
"""

import re
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

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
)

logger = get_logger("sqlspec.migrations.squash")

_TRANSACTIONAL_DIRECTIVE_RE = re.compile(r"^--\s*transactional:\s*(true|false)\s*$", re.MULTILINE | re.IGNORECASE)
_SCHEMA_DIRECTIVE_RE = re.compile(r"^--\s*schema:\s*(\S+)\s*$", re.MULTILINE | re.IGNORECASE)
_RENAME_TABLE_COMMAND_RE = re.compile(r"^\s*TABLE\s+([^\s;]+)\s+TO\s+([^\s;]+)\s*$", re.IGNORECASE)


class DDLStateFolder:
    """Fold a chronological sequence of SQL DDL statements into net schema DDL."""

    __slots__ = (
        "_comments",
        "_dialect",
        "_indexes",
        "_post_ddl_statements",
        "_schemas",
        "_sequences",
        "_tables",
        "_views",
    )

    def __init__(self, dialect: str | None = None) -> None:
        self._dialect = dialect
        self._schemas: OrderedDict[str, exp.Create] = OrderedDict()
        self._sequences: OrderedDict[str, exp.Create] = OrderedDict()
        self._tables: OrderedDict[str, exp.Create] = OrderedDict()
        self._indexes: OrderedDict[str, tuple[str, exp.Create]] = OrderedDict()
        self._views: OrderedDict[str, exp.Create] = OrderedDict()
        self._comments: OrderedDict[tuple[str, str], exp.Comment] = OrderedDict()
        self._post_ddl_statements: list[tuple[str | None, str]] = []

    def feed(self, statement: str) -> None:
        """Parse and fold a SQL statement into the accumulated schema catalog.

        Args:
            statement: Raw SQL statement string (may contain one or more semicolon-delimited statements).
        """
        stripped = statement.strip()
        if not stripped:
            return
        try:
            parsed_expressions = sqlglot.parse(stripped, read=self._dialect)
        except ParseError:
            self._post_ddl_statements.append((None, self._ensure_semicolon(stripped)))
            return

        for expr in parsed_expressions:
            if expr is None:
                continue
            self._apply_expression(expr)

    def to_statements(self) -> tuple[list[str], list[str]]:
        """Render the folded net UP DDL/DML statements and reverse-dependency DOWN DROP statements.

        Returns:
            Tuple of ``(up_statements, down_statements)``.
        """
        up_statements: list[str] = [self._render_expr(create_schema) for create_schema in self._schemas.values()]
        down_statements: list[str] = []

        up_statements.extend(self._render_expr(create_seq) for create_seq in self._sequences.values())

        ordered_table_keys = self._topologically_order_tables()
        up_statements.extend(self._render_expr(self._tables[table_key]) for table_key in ordered_table_keys)

        for table_key, create_index in self._indexes.values():
            if not table_key or table_key in self._tables:
                up_statements.append(self._render_expr(create_index))

        up_statements.extend(self._render_expr(create_view) for create_view in self._views.values())

        for (comment_kind, target_key), comment_expr in self._comments.items():
            if self._is_comment_target_alive(comment_kind, target_key):
                up_statements.append(self._render_expr(comment_expr))

        for target_table_key, raw_stmt in self._post_ddl_statements:
            if target_table_key is None or target_table_key in self._tables:
                up_statements.append(raw_stmt)

        for create_view in reversed(list(self._views.values())):
            view_kind = str(create_view.args.get("kind") or "VIEW").upper()
            drop_view = self._build_drop_expr(create_view, view_kind)
            if drop_view is not None:
                down_statements.append(self._render_expr(drop_view))

        for table_key in reversed(ordered_table_keys):
            drop_table = self._build_drop_expr(self._tables[table_key], "TABLE")
            if drop_table is not None:
                down_statements.append(self._render_expr(drop_table))

        for create_seq in reversed(list(self._sequences.values())):
            drop_seq = self._build_drop_expr(create_seq, "SEQUENCE")
            if drop_seq is not None:
                down_statements.append(self._render_expr(drop_seq))

        for create_schema in reversed(list(self._schemas.values())):
            drop_schema = self._build_drop_expr(create_schema, "SCHEMA")
            if drop_schema is not None:
                down_statements.append(self._render_expr(drop_schema))

        return up_statements, down_statements

    def _apply_expression(self, expr: exp.Expr) -> None:
        if isinstance(expr, exp.Create):
            self._handle_create(expr)
            return
        if isinstance(expr, exp.Alter):
            self._handle_alter(expr)
            return
        if isinstance(expr, exp.Drop):
            self._handle_drop(expr)
            return
        if isinstance(expr, exp.Comment):
            self._handle_comment(expr)
            return
        if isinstance(expr, exp.Command) and str(expr.this).upper() == "RENAME":
            literal_text = expr.expression.name if isinstance(expr.expression, exp.Expr) else str(expr.expression or "")
            match = _RENAME_TABLE_COMMAND_RE.match(literal_text)
            if match is not None:
                old_table = exp.to_table(match.group(1), dialect=self._dialect)
                new_table = exp.to_table(match.group(2), dialect=self._dialect)
                self._rename_table_in_catalog(old_table, new_table)
                return

        dml_table_key = self._extract_dml_target_table_key(expr)
        self._post_ddl_statements.append((dml_table_key, self._render_expr(expr)))

    def _handle_create(self, expr: exp.Create) -> None:
        kind = str(expr.args.get("kind") or "").upper()
        expr_copy = expr.copy()
        if kind == "SCHEMA":
            key = self._object_key(expr_copy.this)
            if key:
                self._schemas[key] = expr_copy
            return
        if kind == "SEQUENCE":
            key = self._object_key(expr_copy.this)
            if key:
                self._sequences[key] = expr_copy
            return
        if kind == "TABLE":
            key = self._object_key(expr_copy.this)
            if key:
                self._tables[key] = expr_copy
            return
        if kind == "INDEX":
            index_node = expr_copy.this
            index_key = self._index_key(index_node)
            table_node = (
                index_node.args.get("table") if isinstance(index_node, exp.Index) else expr_copy.find(exp.Table)
            )
            table_key = self._object_key(table_node) if isinstance(table_node, exp.Expr) else ""
            if index_key:
                self._indexes[index_key] = (table_key, expr_copy)
            return
        if kind in {"VIEW", "MATERIALIZED VIEW"}:
            key = self._object_key(expr_copy.this)
            if key:
                if kind == "VIEW":
                    expr_copy.set("replace", False)
                self._views[key] = expr_copy
            return
        self._post_ddl_statements.append((None, self._render_expr(expr_copy)))

    def _handle_drop(self, expr: exp.Drop) -> None:
        kind = str(expr.args.get("kind") or "").upper()
        targets: list[exp.Expr] = list(expr.args.get("tables") or [])
        if expr.this is not None and isinstance(expr.this, exp.Expr):
            targets.append(expr.this)

        for target in targets:
            key = self._object_key(target)
            if not key:
                continue
            if kind == "TABLE":
                self._tables.pop(key, None)
                stale_indexes = [idx_key for idx_key, (tbl_key, _) in self._indexes.items() if tbl_key == key]
                for idx_key in stale_indexes:
                    self._indexes.pop(idx_key, None)
                stale_comments = [
                    c_key
                    for c_key in self._comments
                    if c_key == ("TABLE", key) or (c_key[0] == "COLUMN" and c_key[1].startswith(f"{key}."))
                ]
                for c_key in stale_comments:
                    self._comments.pop(c_key, None)
                self._post_ddl_statements = [
                    (tbl_key, stmt) for tbl_key, stmt in self._post_ddl_statements if tbl_key != key
                ]
            elif kind == "INDEX":
                unqualified = key.rsplit(".", 1)[-1]
                self._indexes.pop(key, None)
                self._indexes.pop(unqualified, None)
            elif kind in {"VIEW", "MATERIALIZED VIEW"}:
                self._views.pop(key, None)
                self._comments.pop(("VIEW", key), None)
            elif kind == "SEQUENCE":
                self._sequences.pop(key, None)
            elif kind == "SCHEMA":
                self._schemas.pop(key, None)

    def _handle_comment(self, expr: exp.Comment) -> None:
        kind = str(expr.args.get("kind") or "").upper()
        target = expr.this
        if not isinstance(target, exp.Expr):
            return
        key = self._comment_target_key(kind, target)
        if key:
            self._comments[(kind, key)] = expr.copy()

    def _handle_alter(self, expr: exp.Alter) -> None:
        kind = str(expr.args.get("kind") or "TABLE").upper()
        if kind != "TABLE" or not isinstance(expr.this, exp.Table):
            self._post_ddl_statements.append((None, self._render_expr(expr)))
            return

        target_table = expr.this
        table_key = self._object_key(target_table)
        if table_key not in self._tables:
            self._post_ddl_statements.append((table_key or None, self._render_expr(expr)))
            return

        actions = expr.args.get("actions") or []
        for action in actions:
            if not isinstance(action, exp.Expr):
                continue
            self._apply_alter_table_action(table_key, target_table, action)
            table_key = self._object_key(target_table)

    def _apply_alter_table_action(self, table_key: str, target_table: exp.Table, action: exp.Expr) -> None:
        create_expr = self._tables.get(table_key)
        if create_expr is None:
            return

        if isinstance(action, exp.AlterRename) or action.__class__.__name__ == "RenameTable":
            new_table = action.this
            if isinstance(new_table, exp.Table):
                target_db = target_table.args.get("db")
                if isinstance(target_db, exp.Expr) and not new_table.db:
                    new_table = new_table.copy()
                    new_table.set("db", target_db.copy())
                self._rename_table_in_catalog(target_table, new_table)
                target_table.set("this", new_table.args.get("this"))
                target_table.set("db", new_table.args.get("db"))
            return

        schema_expr = self._ensure_table_schema(create_expr)
        expressions: list[exp.Expr] = list(schema_expr.expressions)

        if isinstance(action, exp.ColumnDef):
            col_name = action.name.lower()
            replaced = False
            for idx, item in enumerate(expressions):
                if isinstance(item, exp.ColumnDef) and item.name.lower() == col_name:
                    expressions[idx] = action.copy()
                    replaced = True
                    break
            if not replaced:
                expressions.append(action.copy())
            schema_expr.set("expressions", expressions)
            return

        if isinstance(action, exp.Drop):
            drop_kind = str(action.args.get("kind") or "").upper()
            drop_targets: list[exp.Expr] = list(action.args.get("tables") or [])
            if action.this is not None and isinstance(action.this, exp.Expr):
                drop_targets.append(action.this)
            drop_names = {self._unqualified_name(t) for t in drop_targets if self._unqualified_name(t)}
            if drop_kind == "COLUMN":
                self._drop_columns_from_table(table_key, schema_expr, expressions, drop_names)
            elif drop_kind == "CONSTRAINT":
                filtered = [
                    item
                    for item in expressions
                    if not (isinstance(item, exp.Constraint) and self._unqualified_name(item.this) in drop_names)
                ]
                schema_expr.set("expressions", filtered)
            return

        if isinstance(action, exp.RenameColumn):
            old_name = self._unqualified_name(action.this)
            new_target = action.args.get("to")
            new_name = self._unqualified_name(new_target) if isinstance(new_target, exp.Expr) else ""
            if old_name and new_name:
                new_ident = self._extract_identifier(new_target, new_name)
                self._rename_column_in_table(table_key, schema_expr, expressions, old_name, new_ident)
            return

        if isinstance(action, exp.AlterColumn):
            self._alter_column_in_table(schema_expr, expressions, action)
            return

        if isinstance(action, exp.AddConstraint):
            constraint_exprs = action.args.get("expressions") or ([action.this] if action.this is not None else [])
            expressions.extend(
                constraint_expr.copy() for constraint_expr in constraint_exprs if isinstance(constraint_expr, exp.Expr)
            )
            schema_expr.set("expressions", expressions)
            return

        if isinstance(action, (exp.Constraint, exp.PrimaryKey, exp.ForeignKey, exp.UniqueColumnConstraint)):
            expressions.append(action.copy())
            schema_expr.set("expressions", expressions)

    def _drop_columns_from_table(
        self, table_key: str, schema_expr: exp.Schema, expressions: list[exp.Expr], drop_names: set[str]
    ) -> None:
        new_expressions: list[exp.Expr] = []
        for item in expressions:
            if isinstance(item, exp.ColumnDef) and item.name.lower() in drop_names:
                continue
            if not isinstance(item, exp.ColumnDef):
                referenced_cols = self._referenced_columns_in_constraint(item)
                if referenced_cols and referenced_cols.issubset(drop_names):
                    continue
            new_expressions.append(item)
        schema_expr.set("expressions", new_expressions)

        stale_indexes: list[str] = []
        for idx_key, (idx_table_key, idx_expr) in self._indexes.items():
            if idx_table_key != table_key:
                continue
            idx_cols = self._referenced_columns_in_index(idx_expr)
            if idx_cols and idx_cols.issubset(drop_names):
                stale_indexes.append(idx_key)
        for idx_key in stale_indexes:
            self._indexes.pop(idx_key, None)

        for col_name in drop_names:
            self._comments.pop(("COLUMN", f"{table_key}.{col_name}"), None)

    def _rename_column_in_table(
        self,
        table_key: str,
        schema_expr: exp.Schema,
        expressions: list[exp.Expr],
        old_name: str,
        new_ident: exp.Identifier,
    ) -> None:
        for item in expressions:
            if isinstance(item, exp.ColumnDef) and item.name.lower() == old_name:
                item.set("this", new_ident.copy())
            elif not isinstance(item, exp.ColumnDef):
                self._replace_column_identifier_in_tree(item, old_name, new_ident)
        schema_expr.set("expressions", expressions)

        for idx_table_key, idx_expr in self._indexes.values():
            if idx_table_key == table_key:
                params = idx_expr.this.args.get("params") if isinstance(idx_expr.this, exp.Index) else None
                if isinstance(params, exp.Expr):
                    self._replace_column_identifier_in_tree(params, old_name, new_ident)

        old_comment_key = ("COLUMN", f"{table_key}.{old_name}")
        if old_comment_key in self._comments:
            comment_expr = self._comments.pop(old_comment_key)
            if isinstance(comment_expr.this, exp.Column):
                comment_expr.this.set("this", new_ident.copy())
            self._comments[("COLUMN", f"{table_key}.{new_ident.name.lower()}")] = comment_expr

    def _alter_column_in_table(
        self, schema_expr: exp.Schema, expressions: list[exp.Expr], action: exp.AlterColumn
    ) -> None:
        col_name = action.name.lower()
        target_col: exp.ColumnDef | None = None
        for item in expressions:
            if isinstance(item, exp.ColumnDef) and item.name.lower() == col_name:
                target_col = item
                break
        if target_col is None:
            return

        dtype = action.args.get("dtype")
        if isinstance(dtype, exp.DataType):
            target_col.set("kind", dtype.copy())

        constraints: list[exp.ColumnConstraint] = list(target_col.args.get("constraints") or [])
        default_expr = action.args.get("default")
        allow_null = action.args.get("allow_null")
        is_drop = bool(action.args.get("drop"))

        if isinstance(default_expr, exp.Expr):
            constraints = [c for c in constraints if not isinstance(c.kind, exp.DefaultColumnConstraint)]
            constraints.append(exp.ColumnConstraint(kind=exp.DefaultColumnConstraint(this=default_expr.copy())))
        elif is_drop and allow_null is None:
            constraints = [c for c in constraints if not isinstance(c.kind, exp.DefaultColumnConstraint)]

        if allow_null is False:
            has_not_null = any(
                isinstance(c.kind, exp.NotNullColumnConstraint) and not c.kind.args.get("allow_null")
                for c in constraints
            )
            if not has_not_null:
                constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))
        elif allow_null is True:
            constraints = [
                c
                for c in constraints
                if not (isinstance(c.kind, exp.NotNullColumnConstraint) and not c.kind.args.get("allow_null"))
            ]

        target_col.set("constraints", constraints)
        schema_expr.set("expressions", expressions)

    def _rename_table_in_catalog(self, old_table: exp.Table, new_table: exp.Table) -> None:
        old_key = self._object_key(old_table)
        new_key = self._object_key(new_table)
        if not old_key or not new_key or old_key == new_key:
            return

        if old_key in self._tables:
            rebuilt_tables: OrderedDict[str, exp.Create] = OrderedDict()
            for key, create_expr in self._tables.items():
                if key == old_key:
                    self._set_create_table_node(create_expr, new_table.copy())
                    rebuilt_tables[new_key] = create_expr
                else:
                    rebuilt_tables[key] = create_expr
            self._tables = rebuilt_tables

        rebuilt_indexes: OrderedDict[str, tuple[str, exp.Create]] = OrderedDict()
        for idx_key, (tbl_key, idx_expr) in self._indexes.items():
            if tbl_key == old_key:
                if isinstance(idx_expr.this, exp.Index):
                    idx_expr.this.set("table", new_table.copy())
                rebuilt_indexes[idx_key] = (new_key, idx_expr)
            else:
                rebuilt_indexes[idx_key] = (tbl_key, idx_expr)
        self._indexes = rebuilt_indexes

        rebuilt_comments: OrderedDict[tuple[str, str], exp.Comment] = OrderedDict()
        for (c_kind, c_target), comment_expr in self._comments.items():
            if c_kind == "TABLE" and c_target == old_key:
                comment_expr.set("this", new_table.copy())
                rebuilt_comments[(c_kind, new_key)] = comment_expr
            elif c_kind == "COLUMN" and c_target.startswith(f"{old_key}."):
                col_suffix = c_target[len(old_key) + 1 :]
                if isinstance(comment_expr.this, exp.Column):
                    new_this = new_table.args.get("this")
                    if isinstance(new_this, exp.Expr):
                        comment_expr.this.set("table", new_this.copy())
                    new_db = new_table.args.get("db")
                    if isinstance(new_db, exp.Expr):
                        comment_expr.this.set("db", new_db.copy())
                rebuilt_comments[(c_kind, f"{new_key}.{col_suffix}")] = comment_expr
            else:
                rebuilt_comments[(c_kind, c_target)] = comment_expr
        self._comments = rebuilt_comments

        self._post_ddl_statements = [
            (new_key if tbl_key == old_key else tbl_key, stmt) for tbl_key, stmt in self._post_ddl_statements
        ]

    def _topologically_order_tables(self) -> list[str]:
        keys = list(self._tables.keys())
        if len(keys) <= 1:
            return keys

        unqualified_map: dict[str, str] = {}
        for key in keys:
            short_name = key.rsplit(".", 1)[-1]
            if short_name not in unqualified_map:
                unqualified_map[short_name] = key

        deps: dict[str, set[str]] = {key: set() for key in keys}
        for key, create_expr in self._tables.items():
            for ref in create_expr.find_all(exp.Reference):
                ref_target = ref.this.this if isinstance(ref.this, exp.Schema) else ref.this
                ref_key = self._object_key(ref_target) if isinstance(ref_target, exp.Expr) else ""
                resolved_key = ref_key if ref_key in deps else unqualified_map.get(ref_key.rsplit(".", 1)[-1], "")
                if resolved_key and resolved_key != key and resolved_key in deps:
                    deps[key].add(resolved_key)

        ordered: list[str] = []
        visited: set[str] = set()
        visiting: set[str] = set()

        def _visit(node: str) -> None:
            if node in visited or node in visiting:
                return
            visiting.add(node)
            for parent in keys:
                if parent in deps[node]:
                    _visit(parent)
            visiting.remove(node)
            visited.add(node)
            ordered.append(node)

        for key in keys:
            _visit(key)
        return ordered

    def _is_comment_target_alive(self, comment_kind: str, target_key: str) -> bool:
        if comment_kind == "TABLE":
            return target_key in self._tables
        if comment_kind == "COLUMN":
            table_key = target_key.rsplit(".", 1)[0] if "." in target_key else ""
            return table_key in self._tables
        if comment_kind in {"VIEW", "MATERIALIZED VIEW"}:
            return target_key in self._views
        if comment_kind == "INDEX":
            return target_key in self._indexes
        if comment_kind == "SEQUENCE":
            return target_key in self._sequences
        if comment_kind == "SCHEMA":
            return target_key in self._schemas
        return True

    def _render_expr(self, expr: exp.Expr) -> str:
        return self._ensure_semicolon(expr.sql(dialect=self._dialect))

    @staticmethod
    def _ensure_semicolon(sql_text: str) -> str:
        stripped = sql_text.strip()
        return stripped if stripped.endswith(";") else f"{stripped};"

    @staticmethod
    def _ensure_table_schema(create_expr: exp.Create) -> exp.Schema:
        if isinstance(create_expr.this, exp.Schema):
            return create_expr.this
        schema_expr = exp.Schema(this=create_expr.this, expressions=[])
        create_expr.set("this", schema_expr)
        return schema_expr

    @staticmethod
    def _set_create_table_node(create_expr: exp.Create, new_table: exp.Table) -> None:
        if isinstance(create_expr.this, exp.Schema):
            create_expr.this.set("this", new_table)
        else:
            create_expr.set("this", new_table)

    @staticmethod
    def _build_drop_expr(create_expr: exp.Create, kind: str) -> exp.Drop | None:
        target = create_expr.this.this if isinstance(create_expr.this, exp.Schema) else create_expr.this
        if not isinstance(target, exp.Expr):
            return None
        return exp.Drop(this=target.copy(), tables=[target.copy()], kind=kind, exists=True)

    @staticmethod
    def _object_key(node: exp.Expr | None) -> str:
        if node is None:
            return ""
        target = node.this if isinstance(node, exp.Schema) else node
        if isinstance(target, exp.Table):
            name = target.name.lower()
            db = target.db.lower()
            if db and name:
                return f"{db}.{name}"
            return name or db
        if isinstance(target, exp.Index):
            return target.name.lower()
        return target.name.lower()

    @classmethod
    def _index_key(cls, node: exp.Expr | None) -> str:
        if isinstance(node, exp.Index):
            return node.name.lower()
        return cls._object_key(node)

    @classmethod
    def _comment_target_key(cls, kind: str, target: exp.Expr) -> str:
        if kind == "COLUMN" and isinstance(target, exp.Column):
            col_name = target.name.lower()
            table_name = target.table.lower()
            db_arg = target.args.get("db")
            db_name = str(db_arg.name).lower() if isinstance(db_arg, exp.Expr) else ""
            table_key = f"{db_name}.{table_name}" if db_name and table_name else table_name
            return f"{table_key}.{col_name}" if table_key and col_name else col_name
        return cls._object_key(target)

    @classmethod
    def _extract_dml_target_table_key(cls, expr: exp.Expr) -> str | None:
        if isinstance(expr, (exp.Insert, exp.Update, exp.Delete)):
            table_node = expr.this.this if isinstance(expr.this, exp.Schema) else expr.this
            if isinstance(table_node, exp.Table):
                key = cls._object_key(table_node)
                return key or None
        return None

    @staticmethod
    def _unqualified_name(node: exp.Expr | None) -> str:
        if node is None:
            return ""
        return node.name.lower()

    @staticmethod
    def _extract_identifier(node: exp.Expr | None, fallback_name: str) -> exp.Identifier:
        if isinstance(node, exp.Identifier):
            return node.copy()
        if isinstance(node, exp.Column) and isinstance(node.this, exp.Identifier):
            return node.this.copy()
        return exp.to_identifier(fallback_name)

    @staticmethod
    def _referenced_columns_in_constraint(node: exp.Expr) -> set[str]:
        cols: set[str] = set()
        for col in node.find_all(exp.Column):
            if col.name:
                cols.add(col.name.lower())
        for ident in node.find_all(exp.Identifier):
            parent = ident.parent
            if isinstance(parent, (exp.Schema, exp.ForeignKey, exp.PrimaryKey, exp.UniqueColumnConstraint)) and (
                parent is node or (isinstance(parent, exp.Schema) and parent.this is None)
            ):
                cols.add(ident.name.lower())
        return cols

    @staticmethod
    def _referenced_columns_in_index(create_index: exp.Create) -> set[str]:
        cols: set[str] = set()
        index_node = create_index.this
        params = index_node.args.get("params") if isinstance(index_node, exp.Index) else None
        search_root = params if isinstance(params, exp.Expr) else create_index
        for col in search_root.find_all(exp.Column):
            if col.name:
                cols.add(col.name.lower())
        return cols

    @staticmethod
    def _replace_column_identifier_in_tree(node: exp.Expr, old_name: str, new_ident: exp.Identifier) -> None:
        for col in node.find_all(exp.Column):
            if col.name.lower() == old_name:
                col.set("this", new_ident.copy())
        for ident in node.find_all(exp.Identifier):
            parent = ident.parent
            if (
                ident.name.lower() == old_name
                and isinstance(parent, (exp.Schema, exp.ForeignKey, exp.PrimaryKey, exp.UniqueColumnConstraint))
                and not isinstance(parent.parent, exp.Reference)
            ):
                ident.replace(new_ident.copy())


def fold_migration_ddl(statements: Sequence[str], *, dialect: str | None = None) -> tuple[list[str], list[str]]:
    """Fold a chronological sequence of migration SQL statements into net canonical DDL.

    Args:
        statements: Chronological sequence of SQL statements from ``up`` migrations.
        dialect: Optional SQLGlot dialect name used to parse and render DDL.

    Returns:
        Tuple of ``(up_statements, down_statements)`` representing net schema DDL
        and reverse-dependency ``DROP`` statements.
    """
    folder = DDLStateFolder(dialect=dialect)
    for statement in statements:
        folder.feed(statement)
    return folder.to_statements()


def parse_version_range(range_str: str) -> tuple[str, str]:
    """Parse a version range string into (start, end) tuple.

    Accepts multiple formats: ``START:END``, ``START..END``, or ``START-END``.

    Args:
        range_str: Version range string.

    Returns:
        Tuple of (start_version, end_version) zero-padded to 4 digits.

    Raises:
        ValueError: If the format is not recognised.
    """
    for sep in (":", "..", "-"):
        if sep in range_str:
            parts = range_str.split(sep, 1)
            start = _normalize_range_endpoint(parts[0].strip())
            end = _normalize_range_endpoint(parts[1].strip())
            return start, end
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
        strategy: Squash strategy ("ddl", "schema", or "concat").
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
    strategy: str = "ddl"
    schema_ddl_up: list[str] | None = None
    schema_ddl_down: list[str] | None = None
    missing_down_versions: list[str] | None = None


class MigrationSquasher:
    """Core squash engine for combining migrations.

    Provides functionality to plan, validate, and execute migration squash operations.
    Combines multiple sequential or timestamped migrations into a single file.
    """

    __slots__ = ("backup_path", "migrations_path", "runner", "schema_ddl", "strategy", "template_settings")

    def __init__(
        self,
        migrations_path: Path,
        runner: "SyncMigrationRunner",
        template_settings: "MigrationTemplateSettings | None" = None,
        *,
        strategy: str = "ddl",
        schema_ddl: tuple[list[str], list[str]] | None = None,
    ) -> None:
        """Initialize the migration squasher.

        Args:
            migrations_path: Path to the migrations directory.
            runner: SyncMigrationRunner instance for loading migrations.
            template_settings: Optional template settings for generating squashed file.
            strategy: Default squash strategy ("ddl", "schema", or "concat").
            schema_ddl: Optional pre-fetched (up_ddl, down_ddl) tuple for "schema" strategy.
        """
        resolved_strategy = strategy.lower()
        if resolved_strategy not in {"ddl", "schema", "concat"}:
            msg = f"Unsupported squash strategy: '{strategy}'. Expected 'ddl', 'schema', or 'concat'."
            raise SquashValidationError(msg)
        self.migrations_path = migrations_path
        self.runner = runner
        self.template_settings = template_settings
        self.backup_path: Path | None = None
        self.strategy = resolved_strategy
        self.schema_ddl = schema_ddl

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
            strategy: Squash strategy override ("ddl", "schema", or "concat").
            single_file: When True, emit a single SquashPlan across mixed file types/directives.
            schema_ddl: Optional pre-fetched (up_ddl, down_ddl) tuple for "schema" strategy.

        Returns:
            List of SquashPlan objects with details of planned operations.
        """
        resolved_strategy = (strategy or self.strategy or "ddl").lower()
        if resolved_strategy not in {"ddl", "schema", "concat"}:
            msg = f"Unsupported squash strategy: '{resolved_strategy}'. Expected 'ddl', 'schema', or 'concat'."
            raise SquashValidationError(msg)

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
            strategy: Optional strategy ("ddl", "schema", or "concat"). When None,
                returns raw extracted UP and reverse DOWN statements.
            dialect: Optional SQLGlot dialect name for DDL folding.
            schema_ddl: Optional pre-fetched (up_ddl, down_ddl) tuple for "schema" strategy.

        Returns:
            Tuple of (up_statements, down_statements) lists.
        """
        if strategy is None:
            up_statements, down_statements, _ = self._extract_raw_sql(migrations)
            return up_statements, down_statements

        resolved_strategy = strategy.lower()
        if resolved_strategy == "schema":
            effective_schema_ddl = schema_ddl if schema_ddl is not None else self.schema_ddl
            if effective_schema_ddl is not None:
                return list(effective_schema_ddl[0]), list(effective_schema_ddl[1])
            return self._extract_schema_ddl_from_context()

        up_statements, down_statements, missing_downs = self._extract_raw_sql(migrations)
        if resolved_strategy == "concat":
            return up_statements, ([] if missing_downs else down_statements)

        if resolved_strategy == "ddl":
            if not up_statements:
                return [], []
            resolved_dialect = dialect if dialect is not None else self._resolve_dialect()
            folded_up, folded_down = fold_migration_ddl(up_statements, dialect=resolved_dialect)
            effective_down = folded_down or ([] if missing_downs else down_statements)
            return folded_up, effective_down

        msg = f"Unsupported squash strategy: '{strategy}'. Expected 'ddl', 'schema', or 'concat'."
        raise SquashValidationError(msg)

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

        Creates backup, writes squashed files, deletes source migrations,
        and cleans up backup on success. Rolls back on error.

        Args:
            plans: List of SquashPlan objects to execute.
            dry_run: If True, no files are modified (preview only).
            cleanup_backup: If True (default), remove the backup directory before returning.
                Set False when callers need to keep the backup until database tracking is updated.
        """
        if dry_run:
            logger.debug("Dry run mode - no changes will be made")
            return

        ready_plans: list[SquashPlan] = []
        for plan in plans:
            validate_extension_consistency(plan.source_migrations)
            source_files = [path for _, path in plan.source_migrations]
            status = validate_squash_idempotency(source_files, plan.target_path)
            if status == "already_squashed":
                logger.debug("Squash target already applied: %s", plan.target_path)
                continue
            if status == "partial":
                msg = f"Squash plan for {plan.target_path.name} is partially applied"
                raise SquashValidationError(msg)
            ready_plans.append(plan)

        if not ready_plans:
            return

        self._create_backup()

        try:
            planned_contents: list[tuple[SquashPlan, str]] = []
            for plan in ready_plans:
                up_sql, down_sql = self._extract_plan_sql(plan)

                if plan.target_path.suffix == ".py":
                    content = self.generate_python_squash(plan, up_sql, down_sql)
                else:
                    content = self.generate_squashed_content(plan, up_sql, down_sql)
                planned_contents.append((plan, content))

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
                self._cleanup_backup()

        except Exception:
            self._rollback_backup()
            raise

    def _extract_raw_sql(self, migrations: list[tuple[str, Path]]) -> tuple[list[str], list[str], list[str]]:
        up_statements: list[str] = []
        down_statements: list[str] = []
        missing_down_versions: list[str] = []
        migration_down_sql: list[list[str]] = []

        for version, path in migrations:
            migration_data = self.runner.load_migration(path, version)
            loader = migration_data["loader"]

            up_sql = _load_migration_sql(loader, path, "up")
            if up_sql:
                if isinstance(up_sql, list):
                    up_statements.extend(up_sql)
                else:
                    up_statements.append(up_sql)

            down_sql = _load_migration_sql(loader, path, "down")
            if down_sql:
                if isinstance(down_sql, list):
                    cleaned_down = [stmt for stmt in down_sql if isinstance(stmt, str) and stmt.strip()]
                    if cleaned_down:
                        migration_down_sql.append(cleaned_down)
                    else:
                        missing_down_versions.append(version)
                        migration_down_sql.append([])
                elif isinstance(down_sql, str) and down_sql.strip():
                    migration_down_sql.append([down_sql])
                else:
                    missing_down_versions.append(version)
                    migration_down_sql.append([])
            else:
                missing_down_versions.append(version)
                migration_down_sql.append([])

        for down_sql_group in migration_down_sql[::-1]:
            down_statements.extend(down_sql_group)

        return up_statements, down_statements, missing_down_versions

    def _extract_plan_sql(self, plan: SquashPlan) -> tuple[list[str], list[str]]:
        plan_strategy = (plan.strategy or self.strategy or "ddl").lower()
        if plan_strategy == "schema":
            plan_schema_ddl = (
                (plan.schema_ddl_up, plan.schema_ddl_down or []) if plan.schema_ddl_up is not None else None
            )
            return self.extract_sql(plan.source_migrations, strategy="schema", schema_ddl=plan_schema_ddl)

        raw_up, raw_down, missing_downs = self._extract_raw_sql(plan.source_migrations)
        if plan_strategy == "concat":
            if missing_downs:
                plan.missing_down_versions = missing_downs
                return raw_up, []
            return raw_up, raw_down

        if not raw_up:
            return [], []
        folded_up, folded_down = fold_migration_ddl(raw_up, dialect=self._resolve_dialect())
        effective_down = folded_down or ([] if missing_downs else raw_down)
        return folded_up, effective_down

    def _resolve_dialect(self) -> str | None:
        context = getattr(self.runner, "context", None)
        dialect = getattr(context, "dialect", None) if context is not None else None
        if isinstance(dialect, str) and dialect and dialect not in {"generic", "unknown"}:
            return dialect
        return None

    def _extract_schema_ddl_from_context(self) -> tuple[list[str], list[str]]:
        context = getattr(self.runner, "context", None)
        driver = getattr(context, "driver", None) if context is not None else None
        data_dict = getattr(driver, "data_dictionary", None) if driver is not None else None
        if driver is None or data_dict is None or not hasattr(data_dict, "get_schema_ddl"):
            msg = "Schema squash strategy requires an active database driver with data_dictionary.get_schema_ddl"
            raise SquashValidationError(msg)

        excluded: tuple[str, ...] = ("ddl_migrations", "sqlspec_migrations")
        create_res = data_dict.get_schema_ddl(driver, exclude_tables=excluded, direction="create")
        drop_res = data_dict.get_schema_ddl(driver, exclude_tables=excluded, direction="drop")
        return _normalize_schema_ddl_results(create_res, drop_res)

    def _create_backup(self) -> Path:
        """Create timestamped backup directory with all migration files.

        Returns:
            Path to created backup directory.
        """
        self.backup_path = create_backup(self.migrations_path)
        logger.debug("Created backup at %s", self.backup_path)
        return self.backup_path

    def _cleanup_backup(self) -> None:
        """Remove backup directory after successful operation."""
        if not self.backup_path or not self.backup_path.exists():
            return

        remove_backup(self.backup_path)
        logger.debug("Cleaned up backup at %s", self.backup_path)
        self.backup_path = None

    def _rollback_backup(self) -> None:
        """Restore migration files from backup on error."""
        if not self.backup_path or not self.backup_path.exists():
            return

        backup_dir = self.backup_path
        restore_backup(self.migrations_path, backup_dir, delete_backup=True)
        self.backup_path = None

        logger.debug("Rolled back from backup at %s", backup_dir)


def _normalize_schema_ddl_results(create_res: Any, drop_res: Any) -> tuple[list[str], list[str]]:
    """Convert get_schema_ddl create/drop results into semicolon-terminated UP and DOWN SQL lists."""
    create_items: Sequence[Any] = getattr(create_res, "items", create_res) or ()
    drop_items: Sequence[Any] = getattr(drop_res, "items", drop_res) or ()
    up_ddl: list[str] = [
        DDLStateFolder._ensure_semicolon(str(item.ddl)) for item in create_items if getattr(item, "ddl", None)
    ]
    down_ddl: list[str] = []
    for item in drop_items:
        ddl_raw = getattr(item, "ddl", None)
        ident = getattr(item, "identity", None)
        if isinstance(ddl_raw, str) and ddl_raw.strip().upper().startswith("DROP "):
            down_ddl.append(DDLStateFolder._ensure_semicolon(ddl_raw))
        elif ident is not None:
            obj_type = str(getattr(ident, "object_type", "table") or "table").lower()
            if obj_type in {"index", "trigger"}:
                continue
            name = str(getattr(ident, "name", "") or "")
            schema = str(getattr(ident, "schema", "") or "")
            if not name:
                continue
            qualified = f"{schema}.{name}" if schema and schema.lower() != "main" else name
            keyword = (
                "MATERIALIZED VIEW"
                if obj_type == "materialized_view"
                else "VIEW"
                if "view" in obj_type
                else "SEQUENCE"
                if obj_type == "sequence"
                else "SCHEMA"
                if obj_type == "schema"
                else "TABLE"
            )
            down_ddl.append(f"DROP {keyword} IF EXISTS {qualified};")
    return up_ddl, down_ddl


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
