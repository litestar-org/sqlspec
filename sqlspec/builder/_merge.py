"""MERGE statement builder.

Provides a fluent interface for building SQL MERGE queries with
parameter binding and validation.
"""

from collections.abc import Mapping, Sequence
from itertools import starmap
from typing import Any

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._parsing_utils import extract_sql_object_expression
from sqlspec.core.result import SQLResult
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters

__all__ = ("Merge",)


class _MergeAssignmentMixin:
    """Shared assignment helpers for MERGE clause mixins."""

    __slots__ = ()

    def add_parameter(self, value: Any, name: str | None = None) -> tuple[Any, str]:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _is_column_reference(self, value: str) -> bool:
        """Check if value is a SQL expression rather than a literal string.

        Returns True for qualified column references, SQL keywords, functions, and expressions.
        Returns False for plain literal strings that should be parameterized.
        """
        if not isinstance(value, str):
            return False

        parsed = exp.maybe_parse(value.strip())
        if parsed is None:
            return False

        if isinstance(parsed, exp.Column):
            return parsed.table is not None and bool(parsed.table)

        return isinstance(
            parsed,
            (
                exp.Dot,
                exp.Add,
                exp.Sub,
                exp.Mul,
                exp.Div,
                exp.Mod,
                exp.Func,
                exp.Anonymous,
                exp.Null,
                exp.CurrentTimestamp,
                exp.CurrentDate,
                exp.CurrentTime,
                exp.Paren,
                exp.Case,
            ),
        )

    def _process_assignment(self, target_column: str, value: Any) -> exp.Expression:
        column_identifier = exp.column(target_column) if isinstance(target_column, str) else target_column

        if hasattr(value, "expression") and hasattr(value, "sql"):
            value_expr = extract_sql_object_expression(value, builder=self)
            return exp.EQ(this=column_identifier, expression=value_expr)
        if isinstance(value, exp.Expression):
            return exp.EQ(this=column_identifier, expression=value)
        if isinstance(value, str) and self._is_column_reference(value):
            parsed_expression: exp.Expression | None = exp.maybe_parse(value)
            if parsed_expression is None:
                msg = f"Could not parse assignment expression: {value}"
                raise SQLBuilderError(msg)
            return exp.EQ(this=column_identifier, expression=parsed_expression)

        column_name = target_column if isinstance(target_column, str) else str(target_column)
        column_leaf = column_name.split(".")[-1]
        param_name = self._generate_unique_parameter_name(column_leaf)
        _, param_name = self.add_parameter(value, name=param_name)
        placeholder = exp.Placeholder(this=param_name)
        return exp.EQ(this=column_identifier, expression=placeholder)


@trait
class MergeIntoClauseMixin:
    """Mixin providing INTO clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def into(self, table: str | exp.Expression, alias: str | None = None) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        current_expr.set("this", exp.to_table(table, alias=alias) if isinstance(table, str) else table)
        return self


@trait
class MergeUsingClauseMixin(_MergeAssignmentMixin):
    """Mixin providing USING clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def add_parameter(self, value: Any, name: str | None = None) -> tuple[Any, str]:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _create_dict_source_expression(
        self, source: "dict[str, Any] | list[dict[str, Any]]", alias: "str | None"
    ) -> "exp.Expression":
        """Create USING clause expression from dict or list of dicts.

        For PostgreSQL: Uses json_populate_record[set]() for type-safe conversion
        For others: Uses SELECT with parameterized values (works on all modern databases)

        Args:
            source: Dict or list of dicts for USING clause
            alias: Optional alias for the source

        Returns:
            Expression for USING clause
        """
        is_list = isinstance(source, list)
        data = source if is_list else [source]

        if not data:
            msg = "Cannot create USING clause from empty list"
            raise SQLBuilderError(msg)

        columns = list(data[0].keys())

        parameterized_values: list[list[exp.Expression]] = []
        for row in data:
            row_params: list[exp.Expression] = []
            for column in columns:
                value = row.get(column)
                column_name = column if isinstance(column, str) else str(column)
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
                param_name = self._generate_unique_parameter_name(column_name)
                _, param_name = self.add_parameter(value, name=param_name)
                row_params.append(exp.Placeholder(this=param_name))
            parameterized_values.append(row_params)

        if is_list:
            union_selects: list[exp.Select] = []
            for row_params in parameterized_values:
                select_expr = exp.Select()
                select_expr.set(
                    "expressions", [exp.alias_(row_params[index], column) for index, column in enumerate(columns)]
                )
                union_selects.append(select_expr)

            if len(union_selects) == 1:
                source_expr = union_selects[0]
            else:
                union_expr = union_selects[0]
                for select in union_selects[1:]:
                    union_expr = exp.Union(this=union_expr, expression=select, distinct=False)
                source_expr = union_expr

            if alias:
                return exp.Subquery(this=source_expr, alias=exp.to_identifier(alias))
            return exp.paren(source_expr)

        select_expr = exp.Select()
        select_expr.set(
            "expressions", [exp.alias_(parameterized_values[0][index], column) for index, column in enumerate(columns)]
        )

        if alias:
            return exp.Subquery(this=select_expr, alias=exp.to_identifier(alias))
        return exp.paren(select_expr)

    def using(self, source: str | exp.Expression | Any, alias: str | None = None) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        source_expr: exp.Expression
        if isinstance(source, str):
            source_expr = exp.to_table(source, alias=alias)
        elif isinstance(source, (dict, list)):
            source_expr = self._create_dict_source_expression(source, alias)
        elif has_query_builder_parameters(source) and hasattr(source, "_expression"):
            parameters_obj = getattr(source, "parameters", None)
            if isinstance(parameters_obj, dict):
                for param_name, param_value in parameters_obj.items():
                    self.add_parameter(param_value, name=param_name)
            elif isinstance(parameters_obj, (list, tuple)):
                for param_value in parameters_obj:
                    self.add_parameter(param_value)
            elif parameters_obj is not None:
                self.add_parameter(parameters_obj)
            subquery_expression_source = getattr(source, "_expression", None)
            if not isinstance(subquery_expression_source, exp.Expression):
                subquery_expression_source = exp.select()

            if alias:
                source_expr = exp.Subquery(this=subquery_expression_source, alias=exp.to_identifier(alias))
            else:
                source_expr = exp.paren(subquery_expression_source)
        elif isinstance(source, exp.Expression):
            # Handle different expression types for MERGE USING
            if isinstance(source, exp.Select):
                # Wrap SELECT in Subquery if alias provided
                source_expr = exp.Subquery(this=source, alias=exp.to_identifier(alias)) if alias else exp.paren(source)
            elif isinstance(source, exp.Paren) and alias:
                # Convert Paren to Subquery with alias
                inner = source.this
                source_expr = exp.Subquery(this=inner, alias=exp.to_identifier(alias))
            elif isinstance(source, exp.Subquery) and alias:
                # Update existing Subquery's alias
                source.set("alias", exp.to_identifier(alias))
                source_expr = source
            else:
                # Table name or other expression - use standard aliasing
                source_expr = exp.alias_(source, alias) if alias else source
        else:
            msg = f"Unsupported source type for USING clause: {type(source)}"
            raise SQLBuilderError(msg)

        current_expr.set("using", source_expr)
        return self


@trait
class MergeOnClauseMixin:
    """Mixin providing ON clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def on(self, condition: str | exp.Expression) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        if isinstance(condition, str):
            parsed_condition: exp.Expression | None = exp.maybe_parse(condition, dialect=getattr(self, "dialect", None))
            if parsed_condition is None:
                msg = f"Could not parse ON condition: {condition}"
                raise SQLBuilderError(msg)
            condition_expr = parsed_condition
        elif isinstance(condition, exp.Expression):
            condition_expr = condition
        else:
            msg = f"Unsupported condition type for ON clause: {type(condition)}"
            raise SQLBuilderError(msg)

        current_expr.set("on", exp.paren(condition_expr))
        return self


@trait
class MergeMatchedClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def add_parameter(self, value: Any, name: str | None = None) -> tuple[Any, str]:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def when_matched_then_update(
        self,
        set_values: dict[str, Any] | None = None,
        condition: str | exp.Expression | None = None,
        **assignments: Any,
    ) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        combined_assignments: dict[str, Any] = {}
        if set_values:
            combined_assignments.update(set_values)
        if assignments:
            combined_assignments.update(assignments)

        if not combined_assignments:
            msg = "No update values provided. Use set_values or keyword arguments."
            raise SQLBuilderError(msg)

        set_expressions = list(starmap(self._process_assignment, combined_assignments.items()))
        update_expression = exp.Update(expressions=set_expressions)

        when_kwargs: dict[str, Any] = {"matched": True, "then": update_expression}
        if condition is not None:
            if isinstance(condition, str):
                parsed_condition: exp.Expression | None = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
                if parsed_condition is None:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                when_kwargs["this"] = parsed_condition
            elif isinstance(condition, exp.Expression):
                when_kwargs["this"] = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)

        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(**when_kwargs))
        return self

    def when_matched_then_delete(self, condition: str | exp.Expression | None = None) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        when_kwargs: dict[str, Any] = {"matched": True, "then": exp.Var(this="DELETE")}
        if condition is not None:
            if isinstance(condition, str):
                parsed_condition: exp.Expression | None = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
                if parsed_condition is None:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                when_kwargs["condition"] = parsed_condition
            elif isinstance(condition, exp.Expression):
                when_kwargs["condition"] = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)

        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(**when_kwargs))
        return self


@trait
class MergeNotMatchedClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN NOT MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def add_parameter(self, value: Any, name: str | None = None) -> tuple[Any, str]:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def when_not_matched_then_insert(
        self,
        columns: Mapping[str, Any] | Sequence[str] | None = None,
        values: Sequence[Any] | None = None,
        **value_kwargs: Any,
    ) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        insert_expr = exp.Insert()
        column_names: list[str]
        column_values: list[Any]

        if isinstance(columns, Mapping):
            combined = dict(columns)
            if value_kwargs:
                combined.update(value_kwargs)
            column_names = list(combined.keys())
            column_values = list(combined.values())
        elif value_kwargs:
            column_names = list(value_kwargs.keys())
            column_values = list(value_kwargs.values())
        else:
            if columns is None:
                msg = "Columns must be provided when not using keyword arguments."
                raise SQLBuilderError(msg)
            column_names = [str(column) for column in columns]
            if values is None:
                using_alias = None
                using_expr = current_expr.args.get("using")
                if isinstance(using_expr, (exp.Subquery, exp.Table)) or hasattr(using_expr, "alias"):
                    using_alias = using_expr.alias
                column_values = [f"{using_alias}.{col}" for col in column_names] if using_alias else column_names
            else:
                column_values = list(values)
                if len(column_names) != len(column_values):
                    msg = "Number of columns must match number of values for MERGE insert"
                    raise SQLBuilderError(msg)

        insert_columns = [exp.column(name) for name in column_names]

        insert_values: list[exp.Expression] = []
        for column_name, value in zip(column_names, column_values, strict=True):
            if hasattr(value, "expression") and hasattr(value, "sql"):
                insert_values.append(extract_sql_object_expression(value, builder=self))
            elif isinstance(value, exp.Expression):
                insert_values.append(value)
            elif isinstance(value, str):
                if self._is_column_reference(value):
                    parsed_value: exp.Expression | None = exp.maybe_parse(value, dialect=getattr(self, "dialect", None))
                    if parsed_value is None:
                        msg = f"Could not parse column reference: {value}"
                        raise SQLBuilderError(msg)
                    insert_values.append(parsed_value)
                else:
                    param_name = self._generate_unique_parameter_name(column_name.split(".")[-1])
                    _, param_name = self.add_parameter(value, name=param_name)
                    insert_values.append(exp.Placeholder(this=param_name))
            else:
                param_name = self._generate_unique_parameter_name(column_name.split(".")[-1])
                _, param_name = self.add_parameter(value, name=param_name)
                insert_values.append(exp.Placeholder(this=param_name))

        insert_expr.set("this", exp.Tuple(expressions=insert_columns))
        insert_expr.set("expression", exp.Tuple(expressions=insert_values))
        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(matched=False, then=insert_expr))
        return self


@trait
class MergeNotMatchedBySourceClauseMixin(_MergeAssignmentMixin):
    """Mixin providing WHEN NOT MATCHED BY SOURCE THEN ... clauses."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def add_parameter(self, value: Any, name: str | None = None) -> tuple[Any, str]:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def when_not_matched_by_source_then_update(
        self, set_values: dict[str, Any] | None = None, **assignments: Any
    ) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        combined_assignments: dict[str, Any] = {}
        if set_values:
            combined_assignments.update(set_values)
        if assignments:
            combined_assignments.update(assignments)

        if not combined_assignments:
            msg = "No update values provided. Use set_values or keyword arguments."
            raise SQLBuilderError(msg)

        set_expressions: list[exp.Expression] = []
        for column_name, value in combined_assignments.items():
            column_identifier = exp.column(column_name)
            if hasattr(value, "expression") and hasattr(value, "sql"):
                value_expr = extract_sql_object_expression(value, builder=self)
            elif isinstance(value, exp.Expression):
                value_expr = value
            elif isinstance(value, str) and self._is_column_reference(value):
                parsed_value: exp.Expression | None = exp.maybe_parse(value)
                if parsed_value is None:
                    msg = f"Could not parse assignment expression: {value}"
                    raise SQLBuilderError(msg)
                value_expr = parsed_value
            else:
                param_name = self._generate_unique_parameter_name(column_name)
                _, param_name = self.add_parameter(value, name=param_name)
                value_expr = exp.Placeholder(this=param_name)
            set_expressions.append(exp.EQ(this=column_identifier, expression=value_expr))

        update_expr = exp.Update(expressions=set_expressions)
        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(matched=False, source=True, then=update_expr))
        return self

    def when_not_matched_by_source_then_delete(self) -> Self:
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        whens = current_expr.args.get("whens")
        if not isinstance(whens, exp.Whens):
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)
        whens.append("expressions", exp.When(matched=False, source=True, then=exp.Delete()))
        return self


class Merge(
    QueryBuilder,
    MergeUsingClauseMixin,
    MergeOnClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeIntoClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
):
    """Builder for MERGE statements.

    Constructs SQL MERGE statements (also known as UPSERT in some databases)
    with parameter binding and validation.
    """

    __slots__ = ()
    _expression: exp.Expression | None

    def __init__(self, target_table: str | None = None, **kwargs: Any) -> None:
        """Initialize MERGE with optional target table.

        Args:
            target_table: Target table name
            **kwargs: Additional QueryBuilder arguments
        """
        super().__init__(**kwargs)
        self._initialize_expression()

        if target_table:
            self.into(target_table)

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Return the expected result type for this builder.

        Returns:
            The SQLResult type for MERGE statements.
        """
        return SQLResult

    def _create_base_expression(self) -> "exp.Merge":
        """Create a base MERGE expression.

        Returns:
            A new sqlglot Merge expression with empty clauses.
        """
        return exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
