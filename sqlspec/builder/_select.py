"""SELECT statement builder.

Provides a fluent interface for building SQL SELECT queries with
parameter binding and validation.
"""

import re
from typing import TYPE_CHECKING, Any, Final, cast

from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import QueryBuilder, SafeQuery
from sqlspec.builder.mixins import (
    CommonTableExpressionMixin,
    HavingClauseMixin,
    JoinClauseMixin,
    LimitOffsetClauseMixin,
    OrderByClauseMixin,
    PivotClauseMixin,
    SelectClauseMixin,
    SetOperationMixin,
    UnpivotClauseMixin,
    WhereClauseMixin,
)
from sqlspec.core.result import SQLResult
from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("Select",)


TABLE_HINT_PATTERN: Final[str] = r"\b{}\b(\s+AS\s+\w+)?"


class Select(
    QueryBuilder,
    WhereClauseMixin,
    OrderByClauseMixin,
    LimitOffsetClauseMixin,
    SelectClauseMixin,
    JoinClauseMixin,
    HavingClauseMixin,
    SetOperationMixin,
    CommonTableExpressionMixin,
    PivotClauseMixin,
    UnpivotClauseMixin,
):
    """Builder for SELECT queries.

    Provides a fluent interface for constructing SQL SELECT statements
    with parameter binding and validation.

    Example:
        >>> class User(BaseModel):
        ...     id: int
        ...     name: str
        >>> builder = Select("id", "name").from_("users")
        >>> result = driver.execute(builder)
    """

    __slots__ = ("_hints", "_with_parts")
    _expression: exp.Expression | None

    def __init__(self, *columns: str, **kwargs: Any) -> None:
        """Initialize SELECT with optional columns.

        Args:
            *columns: Column names to select (e.g., "id", "name", "u.email")
            **kwargs: Additional QueryBuilder arguments (dialect, schema, etc.)

        Examples:
            Select("id", "name")  # Shorthand for Select().select("id", "name")
            Select()              # Same as Select() - start empty
        """
        super().__init__(**kwargs)

        self._with_parts: dict[str, exp.CTE | Select] = {}
        self._hints: list[dict[str, object]] = []

        self._initialize_expression()

        if columns:
            self.select(*columns)

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Get the expected result type for SELECT operations.

        Returns:
            type: The SelectResult type.
        """
        return SQLResult

    def _create_base_expression(self) -> exp.Select:
        """Create base SELECT expression."""
        if self._expression is None or not isinstance(self._expression, exp.Select):
            self._expression = exp.Select()
        return self._expression

    def with_hint(
        self, hint: "str", *, location: "str" = "statement", table: "str | None" = None, dialect: "str | None" = None
    ) -> "Self":
        """Attach an optimizer or dialect-specific hint to the query.

        Args:
            hint: The raw hint string (e.g., 'INDEX(users idx_users_name)').
            location: Where to apply the hint ('statement', 'table').
            table: Table name if the hint is for a specific table.
            dialect: Restrict the hint to a specific dialect (optional).

        Returns:
            The current builder instance for method chaining.
        """
        self._hints.append({"hint": hint, "location": location, "table": table, "dialect": dialect})
        return self

    def build(self) -> "SafeQuery":
        """Builds the SQL query string and parameters with hint injection.

        Returns:
            SafeQuery: A dataclass containing the SQL string and parameters.
        """
        safe_query = super().build()

        if not self._hints:
            return safe_query

        modified_expr = self._expression or self._create_base_expression()

        if isinstance(modified_expr, exp.Select):
            statement_hints = [h["hint"] for h in self._hints if h.get("location") == "statement"]
            if statement_hints:

                def parse_hint_safely(hint: Any) -> exp.Expression:
                    try:
                        hint_str = str(hint)
                        hint_expr: exp.Expression | None = exp.maybe_parse(hint_str, dialect=self.dialect_name)
                        return hint_expr or exp.Anonymous(this=hint_str)
                    except Exception:
                        return exp.Anonymous(this=str(hint))

                hint_expressions: list[exp.Expression] = [parse_hint_safely(hint) for hint in statement_hints]

                if hint_expressions:
                    modified_expr.set("hint", exp.Hint(expressions=hint_expressions))

        modified_sql = modified_expr.sql(dialect=self.dialect_name, pretty=True)

        for hint_dict in self._hints:
            if hint_dict.get("location") == "table" and hint_dict.get("table"):
                table = str(hint_dict["table"])
                hint = str(hint_dict["hint"])
                pattern = TABLE_HINT_PATTERN.format(re.escape(table))

                def make_replacement(hint_val: str, table_val: str) -> "Callable[[re.Match[str]], str]":
                    def replacement_func(match: re.Match[str]) -> str:
                        alias_part = match.group(1) or ""
                        return f"/*+ {hint_val} */ {table_val}{alias_part}"

                    return replacement_func

                modified_sql = re.sub(
                    pattern, make_replacement(hint, table), modified_sql, count=1, flags=re.IGNORECASE
                )

        return SafeQuery(sql=modified_sql, parameters=safe_query.parameters, dialect=safe_query.dialect)

    def _validate_select_expression(self) -> None:
        """Validate that current expression is a valid SELECT statement.

        Raises:
            SQLBuilderError: If expression is None or not a SELECT statement
        """
        if self._expression is None or not isinstance(self._expression, exp.Select):
            msg = "Locking clauses can only be applied to SELECT statements"
            raise SQLBuilderError(msg)

    def _validate_lock_parameters(self, skip_locked: bool, nowait: bool) -> None:
        """Validate locking parameters for conflicting options.

        Args:
            skip_locked: Whether SKIP LOCKED option is enabled
            nowait: Whether NOWAIT option is enabled

        Raises:
            SQLBuilderError: If both skip_locked and nowait are True
        """
        if skip_locked and nowait:
            msg = "Cannot use both skip_locked and nowait"
            raise SQLBuilderError(msg)

    def for_update(
        self, *, skip_locked: bool = False, nowait: bool = False, of: "str | list[str] | None" = None
    ) -> "Self":
        """Add FOR UPDATE clause to SELECT statement for row-level locking.

        Args:
            skip_locked: Skip rows that are already locked (SKIP LOCKED)
            nowait: Return immediately if row is locked (NOWAIT)
            of: Table names/aliases to lock (FOR UPDATE OF table)

        Returns:
            Self for method chaining
        """
        self._validate_select_expression()
        self._validate_lock_parameters(skip_locked, nowait)

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock_args = {"update": True}

        if skip_locked:
            lock_args["wait"] = False
        elif nowait:
            lock_args["wait"] = True

        if of:
            tables = [of] if isinstance(of, str) else of
            lock_args["expressions"] = [exp.table_(t) for t in tables]  # type: ignore[assignment]

        lock = exp.Lock(**lock_args)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self

    def for_share(
        self, *, skip_locked: bool = False, nowait: bool = False, of: "str | list[str] | None" = None
    ) -> "Self":
        """Add FOR SHARE clause for shared row-level locking.

        Args:
            skip_locked: Skip rows that are already locked (SKIP LOCKED)
            nowait: Return immediately if row is locked (NOWAIT)
            of: Table names/aliases to lock (FOR SHARE OF table)

        Returns:
            Self for method chaining
        """
        self._validate_select_expression()
        self._validate_lock_parameters(skip_locked, nowait)

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock_args = {"update": False}

        if skip_locked:
            lock_args["wait"] = False
        elif nowait:
            lock_args["wait"] = True

        if of:
            tables = [of] if isinstance(of, str) else of
            lock_args["expressions"] = [exp.table_(t) for t in tables]  # type: ignore[assignment]

        lock = exp.Lock(**lock_args)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self

    def for_key_share(self) -> "Self":
        """Add FOR KEY SHARE clause (PostgreSQL-specific).

        FOR KEY SHARE is like FOR SHARE, but the lock is weaker:
        SELECT FOR UPDATE is blocked, but not SELECT FOR NO KEY UPDATE.

        Returns:
            Self for method chaining
        """
        self._validate_select_expression()

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock = exp.Lock(update=False, key=True)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self

    def for_no_key_update(self) -> "Self":
        """Add FOR NO KEY UPDATE clause (PostgreSQL-specific).

        FOR NO KEY UPDATE is like FOR UPDATE, but the lock is weaker:
        it does not block SELECT FOR KEY SHARE commands that attempt to
        acquire a share lock on the same rows.

        Returns:
            Self for method chaining
        """
        self._validate_select_expression()

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock = exp.Lock(update=True, key=False)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self
