"""Tests for database-family collection and xdist group assignment."""

from pathlib import Path

import msgspec
import pytest

from tests.integration.adapters.conftest import (
    _PRESERVED_CONTAINER_XDIST_GROUPS,
    _case_family,
    pytest_collection_modifyitems,
)


def test_case_family_routes_adbc_dialects() -> None:
    """ADBC case identifiers route to their backing database family instead of defaulting to postgres."""
    expected_routes = {
        "adbc-oracle": "oracle",
        "adbc-db2": "db2",
        "adbc-snowflake": "snowflake",
        "adbc-bigquery": "bigquery",
        "adbc-duckdb": "duckdb",
        "adbc-sqlite": "sqlite",
        "adbc-postgres": "postgres",
        "adbc-pgvector": "postgres",
        "adbc-gizmosql-duckdb": "gizmosql",
    }
    observed_routes = {case_id: _case_family(_StubCaseId(id=case_id)) for case_id in expected_routes}
    assert observed_routes == expected_routes


def test_collection_preserves_container_distinct_xdist_groups(
    request: pytest.FixtureRequest, pytestconfig: pytest.Config
) -> None:
    """Container-distinct xdist_group markers survive multi-level collector normalization without compound leakage."""
    adapters_root = Path(__file__).resolve().parent
    group_family_map = {
        "alloydb": "postgres",
        "gizmosql_sqlite": "gizmosql",
        "mariadb": "mysql",
        "oracle_18c": "oracle",
        "oracle_adb": "oracle",
        "paradedb": "postgres",
        "pgvector": "postgres",
        "snowflake": "snowflake",
    }
    items: list[pytest.Item] = []
    preserved_expectations: list[tuple[pytest.Item, str]] = []
    for group_name in sorted(_PRESERVED_CONTAINER_XDIST_GROUPS):
        family = group_family_map.get(group_name, "postgres")
        use_keyword = group_name in {"gizmosql_sqlite", "mariadb", "oracle_adb", "paradedb"}
        family_pkg = _StubAdapterCollector.from_parent(
            request.session, name=family, path=adapters_root / family / "__init__.py"
        )
        family_pkg.add_marker(pytest.mark.xdist_group("cockroachdb" if family == "cockroach" else family))
        module_collector = _StubAdapterCollector.from_parent(
            family_pkg, name=f"test_{group_name}.py", path=adapters_root / family / f"test_{group_name}.py"
        )
        group_marker = pytest.mark.xdist_group(name=group_name) if use_keyword else pytest.mark.xdist_group(group_name)
        module_collector.add_marker(group_marker)
        first_item = _StubAdapterItem.from_parent(
            module_collector, name=f"test_{group_name}_first", path=module_collector.path
        )
        second_item = _StubAdapterItem.from_parent(
            module_collector, name=f"test_{group_name}_second", path=module_collector.path
        )
        items.extend((first_item, second_item))
        preserved_expectations.extend(((first_item, group_name), (second_item, group_name)))

    oracle_pkg = _StubAdapterCollector.from_parent(
        request.session, name="oracledb", path=adapters_root / "oracle" / "oracledb" / "__init__.py"
    )
    oracle_pkg.add_marker(pytest.mark.xdist_group("oracle"))
    oracle_json_mod = _StubAdapterCollector.from_parent(
        oracle_pkg,
        name="test_json_storage_modes.py",
        path=adapters_root / "oracle" / "oracledb" / "test_json_storage_modes.py",
    )
    oracle_json_mod.add_marker(pytest.mark.xdist_group("oracle"))
    default_oracle_item = _StubAdapterItem.from_parent(
        oracle_json_mod, name="test_native_json_round_trip_matrix", path=oracle_json_mod.path
    )
    oracle_18c_item = _StubAdapterItem.from_parent(
        oracle_json_mod, name="test_blob_is_json_round_trip_on_oracle_18c", path=oracle_json_mod.path
    )
    oracle_18c_item.add_marker(pytest.mark.xdist_group("oracle_18c"))
    oracle_adb_cls = _StubAdapterCollector.from_parent(
        oracle_json_mod, name="TestOracleAdbWallet", path=oracle_json_mod.path
    )
    oracle_adb_cls.add_marker(pytest.mark.xdist_group(name="oracle_adb"))
    oracle_adb_class_item = _StubAdapterItem.from_parent(
        oracle_adb_cls, name="test_wallet_mtls", path=oracle_json_mod.path
    )

    cloud_mod = _StubAdapterCollector.from_parent(
        request.session,
        name="test_cloud_connectors.py",
        path=adapters_root / "postgres" / "asyncpg" / "test_cloud_connectors.py",
    )
    cloud_mod.add_marker(pytest.mark.xdist_group("google_cloud"))
    cloud_postgres_item = _StubAdapterItem.from_parent(cloud_mod, name="test_cloud_sql_connector", path=cloud_mod.path)

    ungrouped_postgres = _StubAdapterItem.from_parent(
        request.session, name="test_ungrouped_postgres", path=adapters_root / "postgres" / "test_driver.py"
    )
    overridden_postgres = _StubAdapterItem.from_parent(
        request.session, name="test_overridden_postgres", path=adapters_root / "postgres" / "test_driver.py"
    )
    overridden_postgres.add_marker(pytest.mark.xdist_group(name="custom_transient_group"))
    ungrouped_cockroach = _StubAdapterItem.from_parent(
        request.session, name="test_ungrouped_cockroach", path=adapters_root / "cockroach" / "test_retry.py"
    )
    ungrouped_snowflake = _StubAdapterItem.from_parent(
        request.session, name="test_ungrouped_snowflake", path=adapters_root / "snowflake" / "test_adbc.py"
    )
    items.extend((
        default_oracle_item,
        oracle_18c_item,
        oracle_adb_class_item,
        cloud_postgres_item,
        ungrouped_postgres,
        overridden_postgres,
        ungrouped_cockroach,
        ungrouped_snowflake,
    ))

    pytest_collection_modifyitems(pytestconfig, items)

    for item, expected_group in preserved_expectations:
        assert _xdist_groups(item) == [expected_group]
    assert _xdist_groups(default_oracle_item) == ["oracle"]
    assert _xdist_groups(oracle_18c_item) == ["oracle_18c"]
    assert _xdist_groups(oracle_adb_class_item) == ["oracle_adb"]
    assert _xdist_groups(cloud_postgres_item) == ["postgres"]
    assert _xdist_groups(ungrouped_postgres) == ["postgres"]
    assert _xdist_groups(overridden_postgres) == ["postgres"]
    assert _xdist_groups(ungrouped_cockroach) == ["cockroachdb"]
    assert _xdist_groups(ungrouped_snowflake) == ["snowflake"]


class _StubCaseId(msgspec.Struct, frozen=True, gc=False):
    id: str


class _StubAdapterCollector(pytest.Collector):
    def collect(self) -> list[pytest.Item | pytest.Collector]:
        return []


class _StubAdapterItem(pytest.Item):
    def runtest(self) -> None:
        return None


def _xdist_groups(item: pytest.Item) -> list[str]:
    groups: list[str] = []
    for mark in item.iter_markers(name="xdist_group"):
        if mark.args:
            groups.append(str(mark.args[0]))
        elif (name := mark.kwargs.get("name")) is not None:
            groups.append(str(name))
    return groups
