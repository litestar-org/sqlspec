"""Integration tests for in_fields and not_in_fields filter dependencies with Litestar (issue #405)."""

import tempfile
from typing import Any

import pytest
from litestar import Litestar, get
from litestar.params import Dependency
from litestar.testing import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.core import FilterTypes, InCollectionFilter, NotInCollectionFilter
from sqlspec.extensions.litestar import SQLSpecPlugin
from sqlspec.extensions.litestar.providers import FieldNameType, create_filter_dependencies, dep_cache
from sqlspec.typing import LITESTAR_INSTALLED

pytestmark = pytest.mark.xdist_group("sqlite")

if not LITESTAR_INSTALLED:
    pytest.skip("Litestar not installed", allow_module_level=True)


@pytest.fixture(autouse=True)
def _clear_dependency_cache() -> Any:
    dep_cache.dependencies.clear()
    yield
    dep_cache.dependencies.clear()


def test_litestar_in_fields_filter_dependency() -> None:
    """Test in_fields filter dependency with actual Litestar HTTP request (issue #405)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        filter_deps = create_filter_dependencies({"in_fields": {FieldNameType(name="status", type_hint=str)}})

        @get("/users", dependencies=filter_deps)
        async def list_users(
            filters: list[FilterTypes] = Dependency(skip_validation=True),  # type: ignore[assignment]
        ) -> dict[str, Any]:
            in_filters = [f for f in filters if isinstance(f, InCollectionFilter)]
            if in_filters:
                return {
                    "filter_count": len(filters),
                    "field_name": in_filters[0].field_name,
                    "values": list(in_filters[0].values) if in_filters[0].values else [],
                }
            return {"filter_count": len(filters), "field_name": None, "values": []}

        app = Litestar(route_handlers=[list_users], plugins=[SQLSpecPlugin(sqlspec=sql)])

        with TestClient(app=app) as client:
            # No query params - should return empty filters
            response = client.get("/users")
            assert response.status_code == 200
            assert response.json()["filter_count"] == 0

            # With in-collection values
            response = client.get("/users?statusIn=active&statusIn=archived")
            assert response.status_code == 200
            data = response.json()
            assert data["filter_count"] == 1
            assert data["field_name"] == "status"
            assert set(data["values"]) == {"active", "archived"}


def test_litestar_not_in_fields_filter_dependency() -> None:
    """Test not_in_fields filter dependency with actual Litestar HTTP request (issue #405)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        filter_deps = create_filter_dependencies({"not_in_fields": {FieldNameType(name="status", type_hint=str)}})

        @get("/users", dependencies=filter_deps)
        async def list_users(
            filters: list[FilterTypes] = Dependency(skip_validation=True),  # type: ignore[assignment]
        ) -> dict[str, Any]:
            not_in_filters = [f for f in filters if isinstance(f, NotInCollectionFilter)]
            if not_in_filters:
                return {
                    "filter_count": len(filters),
                    "field_name": not_in_filters[0].field_name,
                    "values": list(not_in_filters[0].values) if not_in_filters[0].values else [],
                }
            return {"filter_count": len(filters), "field_name": None, "values": []}

        app = Litestar(route_handlers=[list_users], plugins=[SQLSpecPlugin(sqlspec=sql)])

        with TestClient(app=app) as client:
            # No query params - should return empty filters
            response = client.get("/users")
            assert response.status_code == 200
            assert response.json()["filter_count"] == 0

            # With not-in-collection values
            response = client.get("/users?statusNotIn=deleted&statusNotIn=archived")
            assert response.status_code == 200
            data = response.json()
            assert data["filter_count"] == 1
            assert data["field_name"] == "status"
            assert set(data["values"]) == {"deleted", "archived"}


def test_litestar_multiple_in_fields() -> None:
    """Test multiple in_fields filters with Litestar (issue #405)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        filter_deps = create_filter_dependencies({
            "in_fields": {FieldNameType(name="status", type_hint=str), FieldNameType(name="role", type_hint=str)}
        })

        @get("/users", dependencies=filter_deps)
        async def list_users(
            filters: list[FilterTypes] = Dependency(skip_validation=True),  # type: ignore[assignment]
        ) -> dict[str, Any]:
            return {"filter_count": len(filters), "filter_types": sorted(type(f).__name__ for f in filters)}

        app = Litestar(route_handlers=[list_users], plugins=[SQLSpecPlugin(sqlspec=sql)])

        with TestClient(app=app) as client:
            # Both in-collection filters provided
            response = client.get("/users?statusIn=active&roleIn=admin")
            assert response.status_code == 200
            data = response.json()
            assert data["filter_count"] == 2
            assert data["filter_types"] == ["InCollectionFilter", "InCollectionFilter"]


def test_litestar_in_fields_single_value() -> None:
    """Test in_fields with a single query param value (issue #405)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        filter_deps = create_filter_dependencies({"in_fields": {FieldNameType(name="status", type_hint=str)}})

        @get("/users", dependencies=filter_deps)
        async def list_users(
            filters: list[FilterTypes] = Dependency(skip_validation=True),  # type: ignore[assignment]
        ) -> dict[str, Any]:
            in_filters = [f for f in filters if isinstance(f, InCollectionFilter)]
            if in_filters:
                return {
                    "filter_count": len(filters),
                    "field_name": in_filters[0].field_name,
                    "values": list(in_filters[0].values) if in_filters[0].values else [],
                }
            return {"filter_count": len(filters), "field_name": None, "values": []}

        app = Litestar(route_handlers=[list_users], plugins=[SQLSpecPlugin(sqlspec=sql)])

        with TestClient(app=app) as client:
            response = client.get("/users?statusIn=active")
            assert response.status_code == 200
            data = response.json()
            assert data["filter_count"] == 1
            assert data["field_name"] == "status"
            assert data["values"] == ["active"]
