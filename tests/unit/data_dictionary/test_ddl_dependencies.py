"""Unit tests for DDL dependency graph helpers."""

from typing import cast

import pytest

from sqlspec.data_dictionary import (
    DDLResult,
    DependencyCycleError,
    DependencyDirection,
    DependencyEdge,
    DependencyEdgeKind,
    DependencyMetadata,
    DependencySortResult,
    DependencyStrength,
    ForeignKeyMetadata,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    dependency_edges_from_foreign_keys,
    dependency_edges_from_metadata,
    sort_ddl_results,
    sort_dependencies,
)
from sqlspec.driver._common import DataDictionaryMixin


def test_dependency_graph_sorts_views_sequences_and_tables() -> None:
    """Creation order includes non-table dependencies before dependent objects."""
    sequence = ObjectIdentity("order_id_seq", "sequence", schema="public")
    users = ObjectIdentity("users", "table", schema="public")
    orders = ObjectIdentity("orders", "table", schema="public")
    order_summary = ObjectIdentity("order_summary", "view", schema="public")

    result = sort_dependencies(
        (order_summary, orders, sequence, users),
        (
            DependencyEdge(orders, sequence, DependencyEdgeKind.SEQUENCE_USE),
            DependencyEdge(orders, users, DependencyEdgeKind.FOREIGN_KEY),
            DependencyEdge(order_summary, orders, DependencyEdgeKind.VIEW_REFERENCE),
        ),
    )

    assert isinstance(result, DependencySortResult)
    assert result.cycles == ()
    positions = {identity: index for index, identity in enumerate(result.ordered)}
    assert positions[sequence] < positions[orders]
    assert positions[users] < positions[orders]
    assert positions[orders] < positions[order_summary]

    drop_result = sort_dependencies(
        (order_summary, orders, sequence, users),
        (
            DependencyEdge(orders, sequence, DependencyEdgeKind.SEQUENCE_USE),
            DependencyEdge(orders, users, DependencyEdgeKind.FOREIGN_KEY),
            DependencyEdge(order_summary, orders, DependencyEdgeKind.VIEW_REFERENCE),
        ),
        order="drop",
    )
    drop_positions = {identity: index for index, identity in enumerate(drop_result.ordered)}
    assert drop_positions[order_summary] < drop_positions[orders]
    assert drop_positions[orders] < drop_positions[sequence]
    assert drop_positions[orders] < drop_positions[users]


def test_dependency_graph_reports_cycle_with_edge_kind() -> None:
    """Cycle diagnostics name participating objects and dependency edge kinds."""
    invoice_view = ObjectIdentity("invoice_view", "view", schema="public")
    refresh_routine = ObjectIdentity("refresh_invoice_view", "routine", schema="public")
    edges = (
        DependencyEdge(invoice_view, refresh_routine, DependencyEdgeKind.ROUTINE_REFERENCE),
        DependencyEdge(refresh_routine, invoice_view, DependencyEdgeKind.VIEW_REFERENCE),
    )

    result = sort_dependencies((invoice_view, refresh_routine), edges)

    assert result.ordered == ()
    assert len(result.cycles) == 1
    cycle = result.cycles[0]
    assert cycle.objects == (invoice_view, refresh_routine)
    assert {edge.kind for edge in cycle.edges} == {
        DependencyEdgeKind.ROUTINE_REFERENCE,
        DependencyEdgeKind.VIEW_REFERENCE,
    }
    with pytest.raises(DependencyCycleError) as exc_info:
        result.raise_for_cycles()
    message = str(exc_info.value)
    assert "invoice_view" in message
    assert "refresh_invoice_view" in message
    assert "routine_reference" in message
    assert "view_reference" in message


def test_ddl_result_requires_fidelity_status_and_dependency_context() -> None:
    """DDL payloads expose fidelity, source, status, redaction, context, and dependencies."""
    sequence = ObjectIdentity("order_id_seq", "sequence", schema="public")
    orders = ObjectIdentity("orders", "table", schema="public")
    dependency = DependencyEdge(orders, sequence, DependencyEdgeKind.SEQUENCE_USE, source=MetadataSource.CATALOG)

    result = DDLResult(
        orders,
        status=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.HYBRID,
        source=MetadataSource.GENERATED,
        ddl="CREATE TABLE public.orders (id integer DEFAULT nextval('order_id_seq'))",
        dependencies=(dependency,),
        redactions=("owner",),
        context={"dialect": "postgres", "server_version": "16.0"},
    )

    payload = result.to_dict()

    assert payload["status"] == "supported"
    assert payload["fidelity"] == "hybrid"
    assert payload["source"] == "generated"
    assert payload["redactions"] == ("owner",)
    assert payload["context"] == {"dialect": "postgres", "server_version": "16.0"}
    dependencies = cast("tuple[dict[str, object], ...]", payload["dependencies"])
    assert dependencies[0]["kind"] == "sequence_use"

    unsupported = DDLResult.unsupported(
        orders, source=MetadataSource.UNKNOWN, warnings=("DDL extraction is not implemented for this dialect",)
    )
    assert unsupported.ddl is None
    assert unsupported.status == MetadataSupport.UNSUPPORTED
    assert unsupported.fidelity == MetadataFidelity.UNSUPPORTED
    assert unsupported.warnings == ("DDL extraction is not implemented for this dialect",)


def test_fk_topology_is_represented_as_dependency_edges() -> None:
    """The legacy FK sorter is backed by typed dependency edges."""
    fk = ForeignKeyMetadata(
        table_name="orders",
        column_name="user_id",
        referenced_table="users",
        referenced_column="id",
        constraint_name="orders_user_id_fkey",
        schema="public",
        referenced_schema="public",
    )

    edges = dependency_edges_from_foreign_keys((fk,))

    assert len(edges) == 1
    edge = edges[0]
    assert edge.from_object.name == "orders"
    assert edge.to_object.name == "users"
    assert edge.kind == DependencyEdgeKind.FOREIGN_KEY
    assert edge.strength == DependencyStrength.HARD
    assert edge.create_direction == DependencyDirection.TO_BEFORE_FROM
    assert edge.drop_direction == DependencyDirection.FROM_BEFORE_TO

    sorted_tables = DataDictionaryMixin().sort_tables_topologically(["orders", "users"], [fk])
    assert sorted_tables == ["users", "orders"]


def test_postgres_pilot_sorts_catalog_dependency_metadata_and_native_ddl() -> None:
    """PostgreSQL pg_depend-style metadata can drive typed DDL ordering."""
    sequence = ObjectIdentity("order_id_seq", "sequence", schema="public", dialect="postgres")
    table = ObjectIdentity("orders", "table", schema="public", dialect="postgres")
    view = ObjectIdentity("order_summary", "view", schema="public", dialect="postgres")
    dependencies = (
        DependencyMetadata(
            table,
            source=MetadataSource.CATALOG,
            attributes={"referenced_identity": sequence, "kind": "sequence_use", "confidence": 1.0},
        ),
        DependencyMetadata(
            view,
            source=MetadataSource.CATALOG,
            attributes={
                "referenced_name": "orders",
                "referenced_schema": "public",
                "referenced_type": "table",
                "kind": "view_reference",
            },
        ),
    )
    edges = dependency_edges_from_metadata(dependencies, dialect="postgres")
    ddl_results = (
        DDLResult(
            view,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            ddl="CREATE VIEW public.order_summary AS SELECT count(*) FROM public.orders",
            dependencies=(edges[1],),
        ),
        DDLResult(
            table,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.HYBRID,
            source=MetadataSource.GENERATED,
            ddl="CREATE TABLE public.orders (id integer DEFAULT nextval('public.order_id_seq'))",
            dependencies=(edges[0],),
            warnings=("Table DDL combines generated table text with native catalog dependencies.",),
        ),
        DDLResult(
            sequence,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            ddl="CREATE SEQUENCE public.order_id_seq",
        ),
    )

    ordered = sort_ddl_results(ddl_results)

    assert [result.identity.name for result in ordered] == ["order_id_seq", "orders", "order_summary"]
    assert ordered[1].fidelity == MetadataFidelity.HYBRID
    assert ordered[1].dependencies[0].kind == DependencyEdgeKind.SEQUENCE_USE


def test_sqlite_duckdb_pilot_sorts_native_schema_sql_dependencies() -> None:
    """Embedded native schema SQL can be ordered with parsed dependency edges."""
    users = ObjectIdentity("users", "table", schema="main", dialect="sqlite")
    orders = ObjectIdentity("orders", "table", schema="main", dialect="sqlite")
    orders_view = ObjectIdentity("orders_view", "view", schema="main", dialect="sqlite")
    trigger = ObjectIdentity("orders_ai", "trigger", schema="main", dialect="sqlite")
    edges = (
        DependencyEdge(orders, users, DependencyEdgeKind.FOREIGN_KEY, source=MetadataSource.PARSED_SQL, confidence=0.8),
        DependencyEdge(
            orders_view, orders, DependencyEdgeKind.VIEW_REFERENCE, source=MetadataSource.PARSED_SQL, confidence=0.8
        ),
        DependencyEdge(
            trigger, orders, DependencyEdgeKind.TRIGGER_TARGET, source=MetadataSource.PARSED_SQL, confidence=0.8
        ),
    )
    ddl_results = (
        DDLResult(
            trigger,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.LOSSY,
            source=MetadataSource.PARSED_SQL,
            ddl="CREATE TRIGGER orders_ai AFTER INSERT ON orders BEGIN SELECT 1; END",
            dependencies=(edges[2],),
            warnings=("Dependency edges were parsed from native schema SQL.",),
        ),
        DDLResult(
            orders_view,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            ddl="CREATE VIEW orders_view AS SELECT * FROM orders",
            dependencies=(edges[1],),
        ),
        DDLResult(
            orders,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            ddl="CREATE TABLE orders (id integer primary key, user_id integer references users(id))",
            dependencies=(edges[0],),
        ),
        DDLResult(
            users,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            ddl="CREATE TABLE users (id integer primary key)",
        ),
    )

    create_order = sort_ddl_results(ddl_results)
    drop_order = sort_ddl_results(ddl_results, order="drop")
    create_positions = {result.identity.name: index for index, result in enumerate(create_order)}
    drop_positions = {result.identity.name: index for index, result in enumerate(drop_order)}

    assert create_positions["users"] < create_positions["orders"]
    assert create_positions["orders"] < create_positions["orders_view"]
    assert create_positions["orders"] < create_positions["orders_ai"]
    assert drop_positions["orders_view"] < drop_positions["orders"]
    assert drop_positions["orders_ai"] < drop_positions["orders"]
    assert drop_positions["orders"] < drop_positions["users"]
    assert create_order[-1].dependencies[0].confidence == 0.8
