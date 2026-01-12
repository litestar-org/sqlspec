"""Unit tests for sql.merge_ property shorthand."""

from sqlspec import sql
from sqlspec.builder import Merge


def test_merge_property_returns_merge_builder() -> None:
    """Test sql.merge_ property returns Merge builder instance."""
    builder = sql.merge_
    assert isinstance(builder, Merge)


def test_merge_property_creates_new_instance_each_time() -> None:
    """Test sql.merge_ property creates new instance on each access."""
    builder1 = sql.merge_
    builder2 = sql.merge_

    assert builder1 is not builder2
    assert isinstance(builder1, Merge)
    assert isinstance(builder2, Merge)


def test_merge_property_uses_factory_default_dialect() -> None:
    """Test sql.merge_ property uses factory default dialect."""
    factory_with_postgres = sql.__class__(dialect="postgres")
    builder = factory_with_postgres.merge_

    assert isinstance(builder, Merge)
    assert builder.dialect_name == "postgres"


def test_merge_property_equivalent_to_method() -> None:
    """Test sql.merge_ property is equivalent to sql.merge() method."""
    factory = sql.__class__(dialect="postgres")

    property_builder = factory.merge_
    method_builder = factory.merge()

    assert isinstance(property_builder, type(method_builder))
    assert property_builder.dialect_name == method_builder.dialect_name


def test_merge_property_supports_full_chain() -> None:
    """Test sql.merge_ property supports full method chain."""
    factory = sql.__class__(dialect="postgres")
    query = (
        factory.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Test"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    assert isinstance(query, Merge)
    built = query.build()
    assert "MERGE INTO" in built.sql


def test_merge_property_multiple_accesses_independent() -> None:
    """Test multiple accesses to sql.merge_ property are independent."""
    builder1 = sql.merge_.into("table1", alias="t")
    builder2 = sql.merge_.into("table2", alias="t")

    assert builder1 is not builder2

    query1 = (
        builder1
        .using({"id": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id")
    )

    query2 = (
        builder2
        .using({"id": 2}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(price="src.price")
        .when_not_matched_then_insert(id="src.id")
    )

    sql1 = query1.build().sql
    sql2 = query2.build().sql

    assert "table1" in sql1
    assert "table2" in sql2


def test_merge_property_backward_compatible() -> None:
    """Test sql.merge() method still works (backward compatibility)."""
    builder = sql.merge(dialect="postgres")
    assert isinstance(builder, Merge)

    query = (
        builder
        .into("products", alias="t")
        .using({"id": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id")
    )

    built = query.build()
    assert "MERGE INTO" in built.sql


def test_merge_property_with_bulk_data() -> None:
    """Test sql.merge_ property works with bulk data."""
    factory = sql.__class__(dialect="postgres")
    bulk_data = [{"id": i, "name": f"Product {i}"} for i in range(10)]

    query = (
        factory.merge_
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    built = query.build()
    assert "MERGE INTO" in built.sql


def test_merge_property_with_all_when_clauses() -> None:
    """Test sql.merge_ property supports all WHEN clause types."""
    factory = sql.__class__(dialect="postgres")
    query = (
        factory.merge_
        .into("products", alias="t")
        .using({"id": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
        .when_not_matched_by_source_then_delete()
    )

    built = query.build()
    assert "WHEN MATCHED THEN UPDATE" in built.sql
    assert "WHEN NOT MATCHED THEN INSERT" in built.sql
    assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in built.sql


def test_merge_property_does_not_mutate_factory() -> None:
    """Test sql.merge_ property does not mutate factory state."""
    factory = sql.__class__(dialect="postgres")
    original_dialect_obj = factory.dialect

    _ = factory.merge_
    _ = factory.merge_
    _ = factory.merge_

    assert factory.dialect is original_dialect_obj


def test_merge_property_type_consistency() -> None:
    """Test sql.merge_ property type is consistent with sql.merge()."""
    property_type = type(sql.merge_)
    method_type = type(sql.merge())

    assert property_type == method_type
    assert property_type == Merge
    assert method_type == Merge


def test_merge_property_to_sql_works() -> None:
    """Test sql.merge_ property result works with to_sql()."""
    factory = sql.__class__(dialect="postgres")
    query = (
        factory.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Test"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    sql_str = query.to_sql()
    assert "MERGE INTO" in sql_str

    sql_with_params = query.to_sql(show_parameters=True)
    assert "MERGE INTO" in sql_with_params
