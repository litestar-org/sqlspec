"""Factory convenience tests for merge(table)."""

import pytest

from sqlspec.builder import sql
from sqlspec.exceptions import SQLBuilderError


def test_merge_factory_sets_target_table_from_positional_arg() -> None:
    """sql.merge(table) should set INTO target without separate into()."""

    query = (
        sql.merge("products")
        .using("staging", alias="s")
        .on("products.id = s.id")
        .when_matched_then_delete()
    )

    stmt = query.build()

    assert "products" in stmt.sql.lower()
    assert "merge" in stmt.sql.lower()


def test_merge_factory_rejects_non_merge_sql() -> None:
    """sql.merge() with non-MERGE SQL should raise helpful error."""

    bad_sql = "SELECT * FROM products"

    with pytest.raises(SQLBuilderError):
        sql.merge(bad_sql)
