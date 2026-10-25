"""Database-family integration collection rules."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from _pytest.nodes import Node

_FAMILY_ADAPTERS = {
    "bigquery": frozenset({"bigquery"}),
    "cockroach": frozenset({"cockroach_asyncpg", "cockroach_psycopg"}),
    "db2": frozenset({"db2"}),
    "duckdb": frozenset({"duckdb"}),
    "mssql": frozenset({"arrow_odbc", "mssql_python", "pymssql"}),
    "mysql": frozenset({"aiomysql", "asyncmy", "mysqlconnector", "pymysql"}),
    "oracle": frozenset({"oracledb"}),
    "postgres": frozenset({"asyncpg", "psqlpy", "psycopg"}),
    "spanner": frozenset({"spanner"}),
    "sqlite": frozenset({"aiosqlite", "sqlite"}),
}
_SHARED_TEST_FAMILIES = frozenset((*_FAMILY_ADAPTERS, "gizmosql", "snowflake"))
_FAMILY_XDIST_GROUPS = {family: "cockroachdb" if family == "cockroach" else family for family in _SHARED_TEST_FAMILIES}
_PRESERVED_CONTAINER_XDIST_GROUPS: frozenset[str] = frozenset({
    "alloydb",
    "gizmosql_sqlite",
    "mariadb",
    "oracle_18c",
    "oracle_adb",
    "paradedb",
    "pgvector",
    "snowflake",
})


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Keep shared cases in their family while normalizing each item to a single xdist group."""
    retained: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    resolved_groups: list[tuple[pytest.Item, str]] = []
    for item in items:
        item_family = next((part for part in item.path.parts if part in _SHARED_TEST_FAMILIES), None)
        target_group = _resolve_target_xdist_group(item, item_family) if item_family is not None else None
        if item.path.name != "test_shared.py" or item.path.parent.name not in _SHARED_TEST_FAMILIES:
            retained.append(item)
            if target_group is not None:
                resolved_groups.append((item, target_group))
            continue
        family = item.path.parent.name
        callspec = getattr(item, "callspec", None)
        values = () if callspec is None else callspec.params.values()
        case_families = {case_family for value in values if (case_family := _case_family(value)) is not None}
        if family in case_families or (not case_families and family == "sqlite"):
            retained.append(item)
            if target_group is not None:
                resolved_groups.append((item, target_group))
        else:
            deselected.append(item)

    cleaned_nodes: set[Node] = set()
    for item, target_group in resolved_groups:
        for node in item.listchain():
            if node not in cleaned_nodes:
                node.own_markers[:] = [mark for mark in node.own_markers if mark.name != "xdist_group"]
                cleaned_nodes.add(node)
        item.add_marker(pytest.mark.xdist_group(target_group))

    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = retained


def _resolve_target_xdist_group(item: pytest.Item, item_family: str) -> str:
    for mark in item.iter_markers(name="xdist_group"):
        group_name = _xdist_group_name(mark)
        if group_name in _PRESERVED_CONTAINER_XDIST_GROUPS:
            return group_name
    return _FAMILY_XDIST_GROUPS[item_family]


def _xdist_group_name(mark: pytest.Mark) -> str | None:
    if mark.args:
        return str(mark.args[0])
    if (name := mark.kwargs.get("name")) is not None:
        return str(name)
    return None


def _case_family(value: object) -> str | None:
    case = getattr(value, "case", value)
    adapter = getattr(case, "adapter", None)
    identity = " ".join(
        str(part).lower()
        for part in (
            getattr(case, "id", ""),
            adapter or "",
            getattr(case, "fixture_name", ""),
            getattr(case, "factory_fixture", ""),
            getattr(case, "config_fixture_name", ""),
        )
    )
    if "gizmosql" in identity:
        return "gizmosql"
    if "cockroach" in identity:
        return "cockroach"
    if "adbc" in identity:
        if "sqlite" in identity:
            return "sqlite"
        if "duckdb" in identity:
            return "duckdb"
        if "bigquery" in identity:
            return "bigquery"
        if "oracle" in identity:
            return "oracle"
        if "db2" in identity:
            return "db2"
        if "snowflake" in identity:
            return "snowflake"
        return "postgres"
    for family, adapters in _FAMILY_ADAPTERS.items():
        if adapter in adapters:
            return family
    return None
