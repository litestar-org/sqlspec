"""Unit tests for DDL dependency graph helpers."""

from typing import cast

import pytest

from sqlspec.data_dictionary import (
    DDLResult,
    DependencyCycleError,
    DependencyDirection,
    DependencyEdge,
    DependencyEdgeKind,
    DependencySortResult,
    DependencyStrength,
    ForeignKeyMetadata,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    dependency_edges_from_foreign_keys,
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
