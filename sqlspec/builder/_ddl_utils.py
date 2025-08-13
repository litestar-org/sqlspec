"""DDL builder utilities."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

if TYPE_CHECKING:
    from sqlspec.builder._ddl import ColumnDefinition, ConstraintDefinition

__all__ = ("build_column_expression", "build_constraint_expression")


def build_column_expression(col: "ColumnDefinition") -> "exp.Expression":
    """Build SQLGlot expression for a column definition."""
    col_def = exp.ColumnDef(this=exp.to_identifier(col.name), kind=exp.DataType.build(col.dtype))

    constraints: list[exp.ColumnConstraint] = []

    if col.not_null:
        constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))

    if col.primary_key:
        constraints.append(exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint()))

    if col.unique:
        constraints.append(exp.ColumnConstraint(kind=exp.UniqueColumnConstraint()))

    if col.default is not None:
        default_expr: Optional[exp.Expression] = None
        if isinstance(col.default, str):
            if col.default.upper() in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"} or "(" in col.default:
                default_expr = exp.maybe_parse(col.default)
            else:
                default_expr = exp.convert(col.default)
        else:
            default_expr = exp.convert(col.default)

        constraints.append(exp.ColumnConstraint(kind=default_expr))

    if col.check:
        check_expr = exp.Check(this=exp.maybe_parse(col.check))
        constraints.append(exp.ColumnConstraint(kind=check_expr))

    if col.comment:
        constraints.append(exp.ColumnConstraint(kind=exp.CommentColumnConstraint(this=exp.convert(col.comment))))

    if col.generated:
        generated_expr = exp.GeneratedAsIdentityColumnConstraint(this=exp.maybe_parse(col.generated))
        constraints.append(exp.ColumnConstraint(kind=generated_expr))

    if col.collate:
        constraints.append(exp.ColumnConstraint(kind=exp.CollateColumnConstraint(this=exp.to_identifier(col.collate))))

    if constraints:
        col_def.set("constraints", constraints)

    return col_def


def build_constraint_expression(constraint: "ConstraintDefinition") -> "Optional[exp.Expression]":
    """Build SQLGlot expression for a table constraint."""
    if constraint.constraint_type == "PRIMARY KEY":
        pk_cols = [exp.to_identifier(col) for col in constraint.columns]
        pk_constraint = exp.PrimaryKey(expressions=pk_cols)

        if constraint.name:
            return exp.Constraint(this=exp.to_identifier(constraint.name), expression=pk_constraint)
        return pk_constraint

    if constraint.constraint_type == "FOREIGN KEY":
        fk_cols = [exp.to_identifier(col) for col in constraint.columns]
        ref_cols = [exp.to_identifier(col) for col in constraint.references_columns]

        fk_constraint = exp.ForeignKey(
            expressions=fk_cols,
            reference=exp.Reference(
                this=exp.to_table(constraint.references_table) if constraint.references_table else None,
                expressions=ref_cols,
                on_delete=constraint.on_delete,
                on_update=constraint.on_update,
            ),
        )

        if constraint.name:
            return exp.Constraint(this=exp.to_identifier(constraint.name), expression=fk_constraint)
        return fk_constraint

    if constraint.constraint_type == "UNIQUE":
        unique_cols = [exp.to_identifier(col) for col in constraint.columns]
        unique_constraint = exp.UniqueKeyProperty(expressions=unique_cols)

        if constraint.name:
            return exp.Constraint(this=exp.to_identifier(constraint.name), expression=unique_constraint)
        return unique_constraint

    if constraint.constraint_type == "CHECK":
        check_expr = exp.Check(this=exp.maybe_parse(constraint.condition) if constraint.condition else None)

        if constraint.name:
            return exp.Constraint(this=exp.to_identifier(constraint.name), expression=check_expr)
        return check_expr

    return None
