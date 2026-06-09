"""Unit tests for SQL class query modification methods.

This module tests the new parameterized WHERE methods, pagination methods,
and select_only functionality added to the SQL class.
"""

import pytest

from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.exceptions import SQLSpecError


def _named_colon_config() -> StatementConfig:
    """Create a statement config that preserves named-colon placeholders."""
    return StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NAMED_COLON,
            supported_parameter_styles={ParameterStyle.NAMED_COLON},
            default_execution_parameter_style=ParameterStyle.NAMED_COLON,
            supported_execution_parameter_styles={ParameterStyle.NAMED_COLON},
        )
    )


def test_sql_where_eq_where_eq_creates_equality_condition() -> None:
    """Test that where_eq creates WHERE column = :param."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_eq("status", "active")
    assert "WHERE" in modified.raw_sql
    assert "status" in modified.raw_sql
    assert "param_status" in modified.named_parameters
    assert modified.named_parameters["param_status"] == "active"


def test_sql_where_eq_where_eq_preserves_original() -> None:
    """Test that where_eq returns new instance without modifying original."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_eq("status", "active")
    assert "WHERE" not in stmt.raw_sql
    assert len(stmt.named_parameters) == 0
    assert "WHERE" in modified.raw_sql


def test_sql_where_eq_where_eq_chains_with_and() -> None:
    """Test that chained where_eq uses AND."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_eq("status", "active").where_eq("role", "admin")
    assert "AND" in modified.raw_sql
    assert "param_status" in modified.named_parameters
    assert "param_role" in modified.named_parameters


def test_sql_where_neq_where_neq_creates_not_equal_condition() -> None:
    """Test that where_neq creates WHERE column != :param."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_neq("status", "deleted")
    assert "WHERE" in modified.raw_sql
    assert "<>" in modified.raw_sql or "!=" in modified.raw_sql
    assert "param_status" in modified.named_parameters
    assert modified.named_parameters["param_status"] == "deleted"


def test_sql_where_comparisons_where_lt() -> None:
    """Test where_lt creates less-than condition."""
    stmt = SQL("SELECT * FROM products")
    modified = stmt.where_lt("price", 100)
    assert "WHERE" in modified.raw_sql
    assert "<" in modified.raw_sql
    assert modified.named_parameters["param_price"] == 100


def test_sql_where_comparisons_where_lte() -> None:
    """Test where_lte creates less-than-or-equal condition."""
    stmt = SQL("SELECT * FROM products")
    modified = stmt.where_lte("price", 100)
    assert "WHERE" in modified.raw_sql
    assert "<=" in modified.raw_sql
    assert modified.named_parameters["param_price"] == 100


def test_sql_where_comparisons_where_gt() -> None:
    """Test where_gt creates greater-than condition."""
    stmt = SQL("SELECT * FROM products")
    modified = stmt.where_gt("price", 50)
    assert "WHERE" in modified.raw_sql
    assert ">" in modified.raw_sql
    assert modified.named_parameters["param_price"] == 50


def test_sql_where_comparisons_where_gte() -> None:
    """Test where_gte creates greater-than-or-equal condition."""
    stmt = SQL("SELECT * FROM products")
    modified = stmt.where_gte("price", 50)
    assert "WHERE" in modified.raw_sql
    assert ">=" in modified.raw_sql
    assert modified.named_parameters["param_price"] == 50


def test_sql_where_like_where_like() -> None:
    """Test where_like creates LIKE condition."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_like("name", "%john%")
    assert "WHERE" in modified.raw_sql
    assert "LIKE" in modified.raw_sql
    assert modified.named_parameters["param_name"] == "%john%"


def test_sql_where_like_where_ilike() -> None:
    """Test where_ilike creates ILIKE condition."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_ilike("name", "%john%")
    assert "WHERE" in modified.raw_sql
    assert "ILIKE" in modified.raw_sql
    assert modified.named_parameters["param_name"] == "%john%"


def test_sql_where_null_where_is_null() -> None:
    """Test where_is_null creates IS NULL condition."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_is_null("deleted_at")
    assert "WHERE" in modified.raw_sql
    assert "IS NULL" in modified.raw_sql
    assert len(modified.named_parameters) == 0


def test_sql_where_null_where_is_not_null() -> None:
    """Test where_is_not_null creates IS NOT NULL condition."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_is_not_null("email")
    assert "WHERE" in modified.raw_sql
    assert "NOT" in modified.raw_sql
    assert "NULL" in modified.raw_sql
    assert len(modified.named_parameters) == 0


def test_sql_where_in_where_in_creates_in_clause() -> None:
    """Test where_in creates IN condition with multiple placeholders."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_in("status", ["active", "pending", "review"])
    assert "WHERE" in modified.raw_sql
    assert "IN" in modified.raw_sql
    assert len(modified.named_parameters) == 3
    values = list(modified.named_parameters.values())
    assert "active" in values
    assert "pending" in values
    assert "review" in values


def test_sql_where_in_where_in_empty_returns_false_condition() -> None:
    """Test where_in with empty list returns false condition."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_in("status", [])
    assert "WHERE" in modified.raw_sql
    assert "1 = 0" in modified.raw_sql
    assert len(modified.named_parameters) == 0


def test_sql_where_not_in_where_not_in_creates_not_in_clause() -> None:
    """Test where_not_in creates NOT IN condition."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_not_in("status", ["deleted", "banned"])
    assert "WHERE" in modified.raw_sql
    assert "NOT" in modified.raw_sql
    assert "IN" in modified.raw_sql
    assert len(modified.named_parameters) == 2


def test_sql_where_not_in_where_not_in_empty_returns_unchanged() -> None:
    """Test where_not_in with empty list returns original statement."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_not_in("status", [])
    assert modified is stmt


def test_sql_where_between_where_between_creates_between_condition() -> None:
    """Test where_between creates BETWEEN condition."""
    stmt = SQL("SELECT * FROM orders")
    modified = stmt.where_between("total", 100, 500)
    assert "WHERE" in modified.raw_sql
    assert "BETWEEN" in modified.raw_sql
    assert "AND" in modified.raw_sql
    assert "param_total_low" in modified.named_parameters
    assert "param_total_high" in modified.named_parameters
    assert modified.named_parameters["param_total_low"] == 100
    assert modified.named_parameters["param_total_high"] == 500


def test_sql_limit_limit_adds_limit_clause() -> None:
    """Test that limit adds LIMIT clause."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.limit(10)
    assert "LIMIT 10" in modified.raw_sql


def test_sql_limit_limit_preserves_where() -> None:
    """Test that limit preserves existing WHERE clause."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_eq("status", "active").limit(10)
    assert "WHERE" in modified.raw_sql
    assert "LIMIT 10" in modified.raw_sql


def test_sql_offset_offset_adds_offset_clause() -> None:
    """Test that offset adds OFFSET clause."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.offset(20)
    assert "OFFSET 20" in modified.raw_sql


def test_sql_paginate_paginate_adds_limit_and_offset() -> None:
    """Test that paginate adds both LIMIT and OFFSET."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.paginate(page=3, page_size=20)
    assert "LIMIT 20" in modified.raw_sql
    assert "OFFSET 40" in modified.raw_sql


def test_sql_paginate_paginate_first_page() -> None:
    """Test paginate with page 1 has zero offset."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.paginate(page=1, page_size=10)
    assert "LIMIT 10" in modified.raw_sql
    assert "OFFSET 0" in modified.raw_sql


def test_sql_paginate_paginate_rejects_zero_or_negative_page() -> None:
    """Test paginate rejects page values less than 1."""
    stmt = SQL("SELECT * FROM users")
    with pytest.raises(SQLSpecError):
        stmt.paginate(page=0, page_size=10)
    with pytest.raises(SQLSpecError):
        stmt.paginate(page=-1, page_size=10)


def test_sql_paginate_paginate_rejects_non_positive_page_size() -> None:
    """Test paginate rejects page_size values less than 1."""
    stmt = SQL("SELECT * FROM users")
    with pytest.raises(SQLSpecError):
        stmt.paginate(page=1, page_size=0)
    with pytest.raises(SQLSpecError):
        stmt.paginate(page=1, page_size=-5)


def test_sql_select_only_select_only_replaces_columns() -> None:
    """Test that select_only replaces SELECT columns."""
    stmt = SQL("SELECT * FROM users WHERE active = 1")
    modified = stmt.select_only("id", "name", "email")
    assert "id" in modified.raw_sql
    assert "name" in modified.raw_sql
    assert "email" in modified.raw_sql
    assert "*" not in modified.raw_sql
    assert "WHERE" in modified.raw_sql


def test_sql_select_only_select_only_empty_returns_unchanged() -> None:
    """Test that select_only with no columns returns unchanged."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.select_only()
    assert modified is stmt


def test_sql_select_only_select_only_preserves_conditions() -> None:
    """Test select_only preserves WHERE and other clauses."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_eq("status", "active").order_by("name").select_only("id", "name")
    assert "WHERE" in modified.raw_sql
    assert "ORDER BY" in modified.raw_sql
    assert "id" in modified.raw_sql
    assert "name" in modified.raw_sql


def test_parameter_generation_same_column_generates_unique_params() -> None:
    """Test that using same column twice generates unique parameter names."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_eq("status", "active").where_eq("status", "pending")
    assert len(modified.named_parameters) == 2
    params = list(modified.named_parameters.keys())
    assert params[0] != params[1]


def test_parameter_generation_params_dont_collide_with_user_params() -> None:
    """Test auto-generated params don't collide with user-provided params."""
    stmt = SQL("SELECT * FROM users WHERE id = :status", {"status": 1})
    modified = stmt.where_eq("status", "active")
    assert "status" in modified.named_parameters
    assert modified.named_parameters["status"] == 1
    assert "param_status" in modified.named_parameters


def test_parameter_generation_avoids_generated_prefix_collision() -> None:
    """Test generated params append suffixes when the namespace already exists."""
    stmt = SQL("SELECT * FROM users WHERE id = :param_status", {"param_status": 1})
    modified = stmt.where_eq("status", "active")
    assert modified.named_parameters["param_status"] == 1
    assert modified.named_parameters["param_status_1"] == "active"


def test_sql_where_in_uses_oracle_safe_generated_names() -> None:
    """Test where_in creates letter-leading generated parameters."""
    stmt = SQL("SELECT * FROM users")
    modified = stmt.where_in("status", ["active", "pending"])
    assert "param_status_in_0" in modified.named_parameters
    assert "param_status_in_1" in modified.named_parameters
    assert "_sqlspec_status_in_0" not in modified.named_parameters


def test_generated_parameter_names_are_safe_for_named_colon_placeholders() -> None:
    """Test named-colon compilation does not expose underscore-leading binds."""
    stmt = SQL("SELECT * FROM users", statement_config=_named_colon_config())
    modified = stmt.where_eq("status", "active")
    compiled_sql, parameters = modified.compile()
    assert ":param_status" in compiled_sql
    assert ":_sqlspec" not in compiled_sql
    assert parameters == {"param_status": "active"}


def test_cte_preservation_where_eq_preserves_cte() -> None:
    """Test that where_eq preserves CTE in query."""
    stmt = SQL(
        "\n            WITH active_users AS (\n                SELECT * FROM users WHERE active = 1\n            )\n            SELECT * FROM active_users\n            "
    )
    modified = stmt.where_eq("name", "John")
    assert "WITH" in modified.raw_sql
    assert "active_users" in modified.raw_sql
    assert "WHERE" in modified.raw_sql


def test_cte_preservation_limit_preserves_cte() -> None:
    """Test that limit preserves CTE in query."""
    stmt = SQL(
        "\n            WITH top_orders AS (\n                SELECT * FROM orders ORDER BY total DESC\n            )\n            SELECT * FROM top_orders\n            "
    )
    modified = stmt.limit(10)
    assert "WITH" in modified.raw_sql
    assert "top_orders" in modified.raw_sql
    assert "LIMIT 10" in modified.raw_sql


def test_method_chaining_complex_chain() -> None:
    """Test complex method chain produces correct SQL."""
    stmt = SQL("SELECT * FROM orders")
    modified = (
        stmt
        .where_eq("customer_id", 123)
        .where_gte("total", 100)
        .where_lt("total", 1000)
        .where_in("status", ["pending", "processing"])
        .limit(50)
        .offset(100)
    )
    assert "WHERE" in modified.raw_sql
    assert "customer_id" in modified.raw_sql
    assert "total" in modified.raw_sql
    assert "IN" in modified.raw_sql
    assert "LIMIT 50" in modified.raw_sql
    assert "OFFSET 100" in modified.raw_sql
    assert len(modified.named_parameters) == 5


def test_method_chaining_immutability_in_chain() -> None:
    """Test that chaining doesn't modify intermediate results."""
    stmt = SQL("SELECT * FROM users")
    step1 = stmt.where_eq("status", "active")
    step2 = step1.limit(10)
    step3 = step2.select_only("id", "name")
    assert "LIMIT" not in step1.raw_sql
    assert "id" not in step1.raw_sql
    assert "id" not in step2.raw_sql
    assert "LIMIT" in step2.raw_sql
    assert "id" in step3.raw_sql
    assert "LIMIT" in step3.raw_sql
