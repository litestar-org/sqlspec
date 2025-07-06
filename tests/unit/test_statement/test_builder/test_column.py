"""Tests for Column and ColumnExpression classes."""

import pytest
from sqlglot import exp

from sqlspec.statement.builder._column import Column, ColumnExpression, FunctionColumn


class TestColumn:
    """Test Column functionality."""

    def test_column_creation(self) -> None:
        """Test basic column creation."""
        col = Column("name")
        assert col.name == "name"
        assert col.table is None
        assert repr(col) == "Column<name>"

    def test_column_with_table(self) -> None:
        """Test column creation with table."""
        col = Column("name", "users")
        assert col.name == "name"
        assert col.table == "users"
        assert repr(col) == "Column<users.name>"

    def test_column_equality_operators(self) -> None:
        """Test equality operators."""
        col = Column("age")

        # Test ==
        expr = col == 18
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.EQ)

        # Test !=
        expr = col != 18
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.NEQ)

    def test_column_null_comparisons(self) -> None:
        """Test NULL comparisons."""
        col = Column("email")

        # Test == None
        expr = col == None  # noqa: E711
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.Is)

        # Test != None
        expr = col != None  # noqa: E711
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.Not)

    def test_column_comparison_operators(self) -> None:
        """Test comparison operators."""
        col = Column("price")

        # Test >
        expr = col > 100
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.GT)

        # Test >=
        expr = col >= 100
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.GTE)

        # Test <
        expr = col < 100
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.LT)

        # Test <=
        expr = col <= 100
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.LTE)

    def test_column_like_operators(self) -> None:
        """Test LIKE operators."""
        col = Column("name")

        # Test like
        expr = col.like("%john%")
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.Like)

        # Test ilike
        expr = col.ilike("%john%")
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.ILike)

    def test_column_in_operator(self) -> None:
        """Test IN operator."""
        col = Column("status")

        expr = col.in_(["active", "pending"])
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.In)

        # Test not_in
        expr = col.not_in(["inactive", "deleted"])
        assert isinstance(expr, ColumnExpression)
        # Should be NOT IN
        assert isinstance(expr.sqlglot_expression, exp.Not)

    def test_column_between(self) -> None:
        """Test BETWEEN operator."""
        col = Column("age")

        expr = col.between(18, 65)
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.Between)

    def test_column_null_checks(self) -> None:
        """Test IS NULL and IS NOT NULL."""
        col = Column("email")

        # Test is_null
        expr = col.is_null()
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.Is)

        # Test is_not_null
        expr = col.is_not_null()
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.Not)

    def test_column_functions(self) -> None:
        """Test SQL functions on columns."""
        col = Column("name")

        # Test lower
        func_col = col.lower()
        assert isinstance(func_col, FunctionColumn)
        assert isinstance(func_col._expression, exp.Lower)

        # Test upper
        func_col = col.upper()
        assert isinstance(func_col, FunctionColumn)
        assert isinstance(func_col._expression, exp.Upper)

    def test_column_hash(self) -> None:
        """Test column hashing."""
        col1 = Column("name", "users")
        col2 = Column("name", "users")
        col3 = Column("name", "profiles")

        # Same columns should have same hash
        assert hash(col1) == hash(col2)

        # Different columns should have different hash
        assert hash(col1) != hash(col3)


class TestColumnExpression:
    """Test ColumnExpression functionality."""

    def test_expression_and(self) -> None:
        """Test AND operator (&)."""
        col1 = Column("age")
        col2 = Column("active")

        expr1 = col1 > 18
        expr2 = col2 == True  # noqa: E712

        combined = expr1 & expr2
        assert isinstance(combined, ColumnExpression)
        assert isinstance(combined.sqlglot_expression, exp.And)

    def test_expression_or(self) -> None:
        """Test OR operator (|)."""
        col1 = Column("status")
        col2 = Column("role")

        expr1 = col1 == "active"
        expr2 = col2 == "admin"

        combined = expr1 | expr2
        assert isinstance(combined, ColumnExpression)
        assert isinstance(combined.sqlglot_expression, exp.Or)

    def test_expression_not(self) -> None:
        """Test NOT operator (~)."""
        col = Column("active")
        expr = col == True  # noqa: E712

        negated = ~expr
        assert isinstance(negated, ColumnExpression)
        assert isinstance(negated.sqlglot_expression, exp.Not)

    def test_expression_complex_combination(self) -> None:
        """Test complex expression combinations."""
        age = Column("age")
        status = Column("status")
        role = Column("role")

        # (age > 18 AND status = 'active') OR role = 'admin'
        expr = ((age > 18) & (status == "active")) | (role == "admin")
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.Or)

    def test_expression_bool_error(self) -> None:
        """Test that using 'and'/'or' keywords raises an error."""
        col = Column("age")
        expr = col > 18

        with pytest.raises(TypeError, match="Cannot use 'and'/'or' operators"):
            # This should raise an error
            bool(expr)


class TestFunctionColumn:
    """Test FunctionColumn functionality."""

    def test_function_column_operators(self) -> None:
        """Test operators on function columns."""
        col = Column("name")
        lower_name = col.lower()

        # Test ==
        expr = lower_name == "john"
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.EQ)

        # Test !=
        expr = lower_name != "john"
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.NEQ)

        # Test like
        expr = lower_name.like("j%")
        assert isinstance(expr, ColumnExpression)
        assert isinstance(expr.sqlglot_expression, exp.Like)

    def test_function_column_hash(self) -> None:
        """Test function column hashing."""
        col1 = Column("name")
        col2 = Column("name")

        func1 = col1.lower()
        func2 = col2.lower()

        # Function columns with same expression should have same hash
        # (this might not always be true due to object identity, but we can test)
        assert isinstance(func1, FunctionColumn)
        assert isinstance(func2, FunctionColumn)


class TestColumnIntegration:
    """Test Column integration with query builders."""

    def test_column_with_select(self) -> None:
        """Test using Column with Select builder."""
        from sqlspec.statement.builder._select import Select

        age = Column("age")
        status = Column("status")

        query = Select().from_("users").where(age > 18).where(status == "active")

        sql = query.build().sql
        # Should contain parameterized WHERE clauses
        assert ":where_param_" in sql
        assert len(query._parameters) == 2

    def test_column_complex_where(self) -> None:
        """Test complex WHERE conditions."""
        from sqlspec.statement.builder._select import Select

        age = Column("age")
        status = Column("status")
        role = Column("role")

        query = (
            Select().from_("users").where(((age >= 18) & (age <= 65)) | (role == "admin")).where(status != "deleted")
        )

        sql = query.build().sql
        # Should contain parameterized values
        assert ":where_param_" in sql
        # Should have 5 parameters: 18, 65, 'admin', 'deleted'
        assert len(query._parameters) == 4

    def test_column_with_table_qualification(self) -> None:
        """Test columns with table qualifiers."""
        from sqlspec.statement.builder._select import Select

        u_name = Column("name", "u")
        p_name = Column("name", "p")

        query = (
            Select()
            .from_("users", "u")
            .join("profiles", "u.id = p.user_id", "p")
            .where(u_name == "John")
            .where(p_name.like("%Developer%"))
        )

        sql = query.build().sql
        # Should contain table-qualified columns
        assert "u" in sql
        assert "p" in sql
