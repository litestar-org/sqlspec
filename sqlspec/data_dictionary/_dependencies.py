"""Typed dependency graph helpers for data-dictionary DDL ordering."""

from collections import defaultdict
from enum import Enum
from typing import TYPE_CHECKING

from sqlspec.data_dictionary._types import ForeignKeyMetadata, MetadataSource, ObjectIdentity
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from typing import Literal

__all__ = (
    "DependencyCycle",
    "DependencyCycleError",
    "DependencyDirection",
    "DependencyEdge",
    "DependencyEdgeKind",
    "DependencySortResult",
    "DependencyStrength",
    "dependency_edges_from_foreign_keys",
    "sort_dependencies",
)


class DependencyEdgeKind(str, Enum):
    """Kinds of metadata dependency edges used for DDL ordering."""

    CHECK_EXPRESSION = "check_expression"
    DEFAULT_EXPRESSION = "default_expression"
    EXTENSION_OWNED = "extension_owned"
    FOREIGN_KEY = "foreign_key"
    GENERATED_EXPRESSION = "generated_expression"
    INDEX_EXPRESSION = "index_expression"
    MATERIALIZED_VIEW = "materialized_view"
    PARTITION_PARENT = "partition_parent"
    ROLE_GRANT = "role_grant"
    ROUTINE_REFERENCE = "routine_reference"
    SEQUENCE_OWNER = "sequence_owner"
    SEQUENCE_USE = "sequence_use"
    TRIGGER_TARGET = "trigger_target"
    VIEW_REFERENCE = "view_reference"


class DependencyStrength(str, Enum):
    """Strength of a dependency for ordering and diagnostics."""

    HARD = "hard"
    INFORMATIONAL = "informational"
    SOFT = "soft"


class DependencyDirection(str, Enum):
    """Relative direction for create and drop ordering."""

    FROM_BEFORE_TO = "from_before_to"
    TO_BEFORE_FROM = "to_before_from"


class DependencyEdge:
    """Typed dependency edge between two metadata objects.

    ``from_object`` is the dependent object and ``to_object`` is the prerequisite object.
    Creation order normally places ``to_object`` before ``from_object``; drop order reverses it.
    """

    __slots__ = (
        "confidence",
        "create_direction",
        "drop_direction",
        "from_object",
        "kind",
        "source",
        "strength",
        "to_object",
    )

    def __init__(
        self,
        from_object: ObjectIdentity,
        to_object: ObjectIdentity,
        kind: "DependencyEdgeKind | str",
        *,
        strength: "DependencyStrength | str" = DependencyStrength.HARD,
        create_direction: "DependencyDirection | str" = DependencyDirection.TO_BEFORE_FROM,
        drop_direction: "DependencyDirection | str" = DependencyDirection.FROM_BEFORE_TO,
        source: "MetadataSource | str" = MetadataSource.UNKNOWN,
        confidence: float = 1.0,
    ) -> None:
        if confidence < 0.0 or confidence > 1.0:
            msg = "Dependency confidence must be between 0.0 and 1.0"
            raise ValueError(msg)
        self.from_object = from_object
        self.to_object = to_object
        self.kind = _coerce_edge_kind(kind)
        self.strength = _coerce_strength(strength)
        self.create_direction = _coerce_direction(create_direction)
        self.drop_direction = _coerce_direction(drop_direction)
        self.source = _coerce_source(source)
        self.confidence = confidence

    def to_dict(self) -> "dict[str, object]":
        """Serialize the edge with stable string enum values."""
        return {
            "from": self.from_object.to_dict(),
            "to": self.to_object.to_dict(),
            "kind": self.kind.value,
            "strength": self.strength.value,
            "create_direction": self.create_direction.value,
            "drop_direction": self.drop_direction.value,
            "source": self.source.value,
            "confidence": self.confidence,
        }

    def __repr__(self) -> str:
        return (
            f"DependencyEdge(from_object={self.from_object!r}, to_object={self.to_object!r}, "
            f"kind={self.kind!r}, strength={self.strength!r}, create_direction={self.create_direction!r}, "
            f"drop_direction={self.drop_direction!r}, source={self.source!r}, confidence={self.confidence!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DependencyEdge):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((
            self.from_object,
            self.to_object,
            self.kind,
            self.strength,
            self.create_direction,
            self.drop_direction,
            self.source,
            self.confidence,
        ))


class DependencyCycle:
    """Named dependency cycle with participating objects and edge diagnostics."""

    __slots__ = ("edges", "objects")

    def __init__(self, objects: "tuple[ObjectIdentity, ...]", edges: "tuple[DependencyEdge, ...]") -> None:
        self.objects = objects
        self.edges = edges

    def describe(self) -> str:
        """Return a compact human-readable cycle diagnostic."""
        if not self.edges:
            return " -> ".join(_format_identity(identity) for identity in self.objects)
        return "; ".join(
            f"{_format_identity(edge.from_object)} --{edge.kind.value}--> {_format_identity(edge.to_object)}"
            for edge in self.edges
        )

    def to_dict(self) -> "dict[str, object]":
        """Serialize the cycle for structured diagnostics."""
        return {
            "objects": tuple(identity.to_dict() for identity in self.objects),
            "edges": tuple(edge.to_dict() for edge in self.edges),
        }

    def __repr__(self) -> str:
        return f"DependencyCycle(objects={self.objects!r}, edges={self.edges!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DependencyCycle):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((self.objects, self.edges))


class DependencyCycleError(SQLSpecError):
    """Raised when dependency sorting finds one or more cycles."""

    def __init__(self, cycles: "Sequence[DependencyCycle]") -> None:
        self.cycles = tuple(cycles)
        detail = "Dependency cycle detected"
        if self.cycles:
            detail = f"{detail}: " + " | ".join(cycle.describe() for cycle in self.cycles)
        super().__init__(detail)


class DependencySortResult:
    """Result from dependency graph sorting."""

    __slots__ = ("cycles", "ordered")

    def __init__(self, ordered: "tuple[ObjectIdentity, ...]", cycles: "tuple[DependencyCycle, ...]" = ()) -> None:
        self.ordered = ordered
        self.cycles = cycles

    @property
    def is_acyclic(self) -> bool:
        """Return whether sorting completed without cycles."""
        return not self.cycles

    def raise_for_cycles(self) -> None:
        """Raise a diagnostic exception if cycles were found."""
        if self.cycles:
            raise DependencyCycleError(self.cycles)

    def to_dict(self) -> "dict[str, object]":
        """Serialize the sort result."""
        return {
            "ordered": tuple(identity.to_dict() for identity in self.ordered),
            "cycles": tuple(cycle.to_dict() for cycle in self.cycles),
        }

    def __repr__(self) -> str:
        return f"DependencySortResult(ordered={self.ordered!r}, cycles={self.cycles!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DependencySortResult):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((self.ordered, self.cycles))


def dependency_edges_from_foreign_keys(
    foreign_keys: "Iterable[ForeignKeyMetadata]",
    *,
    dialect: str | None = None,
    source: "MetadataSource | str" = MetadataSource.CATALOG,
) -> "tuple[DependencyEdge, ...]":
    """Convert FK metadata into typed dependency edges."""
    metadata_source = _coerce_source(source)
    edges: list[DependencyEdge] = []
    for fk in foreign_keys:
        referenced_schema = fk.referenced_schema or fk.schema
        if fk.table_name == fk.referenced_table and fk.schema == referenced_schema:
            continue
        from_object = ObjectIdentity(fk.table_name, "table", schema=fk.schema, dialect=dialect, source=metadata_source)
        to_object = ObjectIdentity(
            fk.referenced_table, "table", schema=referenced_schema, dialect=dialect, source=metadata_source
        )
        edges.append(
            DependencyEdge(
                from_object,
                to_object,
                DependencyEdgeKind.FOREIGN_KEY,
                strength=DependencyStrength.HARD,
                source=metadata_source,
            )
        )
    return tuple(edges)


def sort_dependencies(
    objects: "Iterable[ObjectIdentity]",
    edges: "Iterable[DependencyEdge]",
    *,
    order: "Literal['create', 'drop']" = "create",
) -> DependencySortResult:
    """Sort metadata objects by typed dependency edges.

    Args:
        objects: Initial object identities to include in the graph.
        edges: Dependency edges. Edge endpoints are added to the graph automatically.
        order: ``"create"`` for dependencies before dependents, ``"drop"`` for dependents before dependencies.

    Returns:
        Ordered objects plus cycle diagnostics if cycles prevented a complete order.
    """
    edge_tuple = tuple(edges)
    nodes = _dedupe_objects((
        *tuple(objects),
        *(edge.from_object for edge in edge_tuple),
        *(edge.to_object for edge in edge_tuple),
    ))
    dependencies: dict[ObjectIdentity, set[ObjectIdentity]] = {node: set() for node in nodes}
    dependents: dict[ObjectIdentity, set[ObjectIdentity]] = defaultdict(set)

    for edge in edge_tuple:
        dependent, dependency = _edge_order(edge, order)
        dependencies.setdefault(dependent, set()).add(dependency)
        dependencies.setdefault(dependency, set())
        dependents[dependency].add(dependent)

    ordered: list[ObjectIdentity] = []
    ready = [node for node in nodes if not dependencies[node]]
    ready_seen = set(ready)

    while ready:
        node = ready.pop(0)
        ordered.append(node)
        for dependent in _ordered_nodes(dependents.get(node, ()), nodes):
            dependencies[dependent].discard(node)
            if not dependencies[dependent] and dependent not in ready_seen and dependent not in ordered:
                ready.append(dependent)
                ready_seen.add(dependent)

    unresolved = tuple(node for node in nodes if node not in ordered)
    if not unresolved:
        return DependencySortResult(tuple(ordered))

    unresolved_set = set(unresolved)
    cycle_edges = tuple(
        edge for edge in edge_tuple if edge.from_object in unresolved_set and edge.to_object in unresolved_set
    )
    return DependencySortResult(tuple(ordered), (DependencyCycle(unresolved, cycle_edges),))


def _edge_order(edge: DependencyEdge, order: "Literal['create', 'drop']") -> "tuple[ObjectIdentity, ObjectIdentity]":
    direction = edge.create_direction if order == "create" else edge.drop_direction
    if direction == DependencyDirection.TO_BEFORE_FROM:
        return edge.from_object, edge.to_object
    return edge.to_object, edge.from_object


def _dedupe_objects(objects: "Iterable[ObjectIdentity]") -> "tuple[ObjectIdentity, ...]":
    seen: set[ObjectIdentity] = set()
    ordered: list[ObjectIdentity] = []
    for identity in objects:
        if identity in seen:
            continue
        ordered.append(identity)
        seen.add(identity)
    return tuple(ordered)


def _ordered_nodes(
    nodes: "Iterable[ObjectIdentity]", preferred_order: "Sequence[ObjectIdentity]"
) -> "tuple[ObjectIdentity, ...]":
    node_set = set(nodes)
    return tuple(node for node in preferred_order if node in node_set)


def _format_identity(identity: ObjectIdentity) -> str:
    name = identity.name
    if identity.schema:
        name = f"{identity.schema}.{name}"
    if identity.catalog:
        name = f"{identity.catalog}.{name}"
    return f"{name} ({identity.object_type})"


def _coerce_edge_kind(value: "DependencyEdgeKind | str") -> DependencyEdgeKind:
    if isinstance(value, DependencyEdgeKind):
        return value
    return DependencyEdgeKind(value)


def _coerce_strength(value: "DependencyStrength | str") -> DependencyStrength:
    if isinstance(value, DependencyStrength):
        return value
    return DependencyStrength(value)


def _coerce_direction(value: "DependencyDirection | str") -> DependencyDirection:
    if isinstance(value, DependencyDirection):
        return value
    return DependencyDirection(value)


def _coerce_source(value: "MetadataSource | str") -> MetadataSource:
    if isinstance(value, MetadataSource):
        return value
    return MetadataSource(value)
