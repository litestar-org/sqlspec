"""Database-family integration collection rules."""

import pytest

_FAMILY_ADAPTERS = {
    "bigquery": frozenset({"bigquery"}),
    "cockroach": frozenset({"cockroach_asyncpg", "cockroach_psycopg"}),
    "duckdb": frozenset({"duckdb"}),
    "mssql": frozenset({"arrow_odbc", "mssql_python", "pymssql"}),
    "mysql": frozenset({"aiomysql", "asyncmy", "mysqlconnector", "pymysql"}),
    "oracle": frozenset({"oracledb"}),
    "postgres": frozenset({"asyncpg", "psqlpy", "psycopg"}),
    "spanner": frozenset({"spanner"}),
    "sqlite": frozenset({"aiosqlite", "sqlite"}),
}
_SHARED_TEST_FAMILIES = frozenset((*_FAMILY_ADAPTERS, "gizmosql"))
_FAMILY_XDIST_GROUPS = {family: "cockroachdb" if family == "cockroach" else family for family in _SHARED_TEST_FAMILIES}


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Keep shared cases in their family and serialize every family on one worker."""
    retained: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        item_family = next((part for part in item.path.parts if part in _SHARED_TEST_FAMILIES), None)
        if item_family is not None:
            item.own_markers[:] = [mark for mark in item.own_markers if mark.name != "xdist_group"]
            item.add_marker(pytest.mark.xdist_group(_FAMILY_XDIST_GROUPS[item_family]))
        if item.path.name != "test_shared.py" or item.path.parent.name not in _SHARED_TEST_FAMILIES:
            retained.append(item)
            continue
        family = item.path.parent.name
        callspec = getattr(item, "callspec", None)
        values = () if callspec is None else callspec.params.values()
        case_families = {case_family for value in values if (case_family := _case_family(value)) is not None}
        if family in case_families or (not case_families and family == "sqlite"):
            retained.append(item)
        else:
            deselected.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = retained


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
        return "postgres"
    for family, adapters in _FAMILY_ADAPTERS.items():
        if adapter in adapters:
            return family
    return None
