"""Integration tests for in_fields and not_in_fields filter dependencies with Litestar (issue #405)."""

import tempfile
from typing import Any
from uuid import UUID

import pytest
from litestar import Litestar, get
from litestar.params import Dependency
from litestar.testing import AsyncTestClient, TestClient

from sqlspec import sql as sql_builder
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.core import (
    BeforeAfterFilter,
    FilterTypes,
    InCollectionFilter,
    NotInCollectionFilter,
    NotNullFilter,
    NullFilter,
)
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


@pytest.mark.anyio
async def test_litestar_qualified_column_search_filter() -> None:
    """Verify that filtering on a qualified column name in a JOIN works correctly with SearchFilter."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        # Setup tables with overlapping column names
        async with sql.provide_session(config) as session:
            await session.execute_script("""
                CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT);
                INSERT INTO parent (id, name) VALUES (1, 'parent1');
                INSERT INTO child (id, parent_id, name) VALUES (1, 1, 'child1');
            """)
            await session.commit()

        # We want to search on 'p.name'. This should now work!
        filter_deps = create_filter_dependencies({"search": "p.name"})

        @get("/joined", dependencies=filter_deps)
        async def joined_route(filters: list[Any] = Dependency(skip_validation=True)) -> dict[str, Any]:
            async with sql.provide_session(config) as session:
                query = (
                    sql_builder
                    .select("p.name as parent_name", "c.name as child_name")
                    .from_("parent p")
                    .join("child c", "p.id = c.parent_id")
                )
                results = await session.select(query, *filters)
                return {"items": results}

        app = Litestar(route_handlers=[joined_route], plugins=[SQLSpecPlugin(sqlspec=sql)])

        async with AsyncTestClient(app=app) as client:
            response = await client.get("/joined", params={"searchString": "parent"})

            assert response.status_code == 200
            data = response.json()
            assert len(data["items"]) == 1
            assert data["items"][0]["parent_name"] == "parent1"


@pytest.mark.anyio
async def test_litestar_qualified_column_order_by_filter() -> None:
    """Verify that filtering on a qualified column name in a JOIN works correctly with OrderByFilter."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        # Setup tables
        async with sql.provide_session(config) as session:
            await session.execute_script("""
                CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO parent (id, name) VALUES (1, 'b');
                INSERT INTO parent (id, name) VALUES (2, 'a');
            """)
            await session.commit()

        # We want to order by 'p.name'
        filter_deps = create_filter_dependencies({"sort_field": "p.name"})

        @get("/ordered", dependencies=filter_deps)
        async def ordered_route(filters: list[Any] = Dependency(skip_validation=True)) -> dict[str, Any]:
            async with sql.provide_session(config) as session:
                query = sql_builder.select("p.name").from_("parent p")
                results = await session.select(query, *filters)
                return {"items": results}

        app = Litestar(route_handlers=[ordered_route], plugins=[SQLSpecPlugin(sqlspec=sql)])

        async with AsyncTestClient(app=app) as client:
            # Ascending
            response = await client.get("/ordered", params={"orderBy": "p.name", "sortOrder": "asc"})
            assert response.status_code == 200
            items = response.json()["items"]
            assert items[0]["name"] == "a"
            assert items[1]["name"] == "b"

            # Descending
            response = await client.get("/ordered", params={"orderBy": "p.name", "sortOrder": "desc"})
            assert response.status_code == 200
            items = response.json()["items"]
            assert items[0]["name"] == "b"
            assert items[1]["name"] == "a"


@pytest.mark.anyio
async def test_litestar_order_by_accepts_camelized_sort_field_alias() -> None:
    """Camelized orderBy values normalize to SQL-facing fields before filtering."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        async with sql.provide_session(config) as session:
            await session.execute_script("""
                CREATE TABLE upload_stats (id INTEGER PRIMARY KEY, uploaded_collections INTEGER);
                INSERT INTO upload_stats (id, uploaded_collections) VALUES (1, 2);
                INSERT INTO upload_stats (id, uploaded_collections) VALUES (2, 3);
                INSERT INTO upload_stats (id, uploaded_collections) VALUES (3, 1);
            """)
            await session.commit()

        filter_deps = create_filter_dependencies({"sort_field": ["created_at", "uploaded_collections"]})

        @get("/ordered", dependencies=filter_deps)
        async def ordered_route(filters: list[Any] = Dependency(skip_validation=True)) -> dict[str, Any]:
            async with sql.provide_session(config) as session:
                query = sql_builder.select("id", "uploaded_collections").from_("upload_stats")
                results = await session.select(query, *filters)
                return {"items": results}

        app = Litestar(route_handlers=[ordered_route], plugins=[SQLSpecPlugin(sqlspec=sql)])

        async with AsyncTestClient(app=app) as client:
            response = await client.get("/ordered", params={"orderBy": "uploadedCollections", "sortOrder": "asc"})
            assert response.status_code == 200
            items = response.json()["items"]
            assert [item["id"] for item in items] == [3, 1, 2]


@pytest.mark.anyio
async def test_litestar_order_by_accepts_dotted_camelized_sort_field_alias() -> None:
    """Qualified dotted orderBy fields accept dotted camel aliases."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        async with sql.provide_session(config) as session:
            await session.execute_script("""
                CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT, created_at TEXT);
                INSERT INTO parent (id, name, created_at) VALUES (1, 'middle', '2024-02-01');
                INSERT INTO parent (id, name, created_at) VALUES (2, 'first', '2024-01-01');
                INSERT INTO parent (id, name, created_at) VALUES (3, 'last', '2024-03-01');
            """)
            await session.commit()

        filter_deps = create_filter_dependencies({"sort_field": ["p.created_at", "p.name"]})

        @get("/ordered", dependencies=filter_deps)
        async def ordered_route(filters: list[Any] = Dependency(skip_validation=True)) -> dict[str, Any]:
            async with sql.provide_session(config) as session:
                query = sql_builder.select("p.id", "p.name", "p.created_at").from_("parent p")
                results = await session.select(query, *filters)
                return {"items": results}

        app = Litestar(route_handlers=[ordered_route], plugins=[SQLSpecPlugin(sqlspec=sql)])

        async with AsyncTestClient(app=app) as client:
            camel_response = await client.get("/ordered", params={"orderBy": "p.createdAt", "sortOrder": "asc"})
            assert camel_response.status_code == 200
            assert [item["id"] for item in camel_response.json()["items"]] == [2, 1, 3]

            snake_response = await client.get("/ordered", params={"orderBy": "p.created_at", "sortOrder": "asc"})
            assert snake_response.status_code == 200
            assert [item["id"] for item in snake_response.json()["items"]] == [2, 1, 3]


@pytest.mark.anyio
async def test_litestar_order_by_invalid_alias_error_uses_display_aliases() -> None:
    """Invalid alias errors list wire aliases instead of raw snake_case fields."""
    sql = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sql.add_config(config)

    filter_deps = create_filter_dependencies({"sort_field": ["created_at", "uploaded_collections"]})

    @get("/ordered", dependencies=filter_deps)
    async def ordered_route(filters: list[Any] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {"items": []}

    app = Litestar(route_handlers=[ordered_route], plugins=[SQLSpecPlugin(sqlspec=sql)])

    async with AsyncTestClient(app=app) as client:
        response = await client.get("/ordered", params={"orderBy": "uploadedCollectionz", "sortOrder": "asc"})

    assert response.status_code == 400
    assert "Invalid orderBy field 'uploadedCollectionz'" in response.text
    assert "Allowed fields: createdAt, uploadedCollections" in response.text
    assert "uploaded_collections" not in response.text


def test_litestar_order_by_openapi_schema() -> None:
    """OrderByFilter must appear as a string in OpenAPI even with expression support."""
    from typing import Any

    sql = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sql.add_config(config)

    filter_deps = create_filter_dependencies({"sort_field": "p.name"})

    @get("/ordered", dependencies=filter_deps)
    async def ordered_route(filters: list[Any] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {"items": []}

    app = Litestar(route_handlers=[ordered_route], plugins=[SQLSpecPlugin(sqlspec=sql)])

    schema = app.openapi_schema
    paths = schema.paths
    assert paths is not None

    operation = paths["/ordered"].get
    assert operation is not None
    params = operation.parameters
    assert params is not None

    def _named_param(name: str) -> Any:
        return next((p for p in params if getattr(p, "name", None) == name), None)

    order_by_param = _named_param("orderBy")
    assert order_by_param is not None

    # In newer Litestar, types can be a list or a single value, and might be in one_of
    def is_string_type(schema: Any) -> bool:
        if not schema:
            return False
        stype = schema.type
        if isinstance(stype, list):
            return any("string" in str(t).lower() for t in stype)
        if stype:
            return "string" in str(stype).lower()
        if schema.one_of:
            return any(is_string_type(s) for s in schema.one_of)
        return False

    assert is_string_type(order_by_param.schema)

    sort_order_param = _named_param("sortOrder")
    assert sort_order_param is not None
    assert is_string_type(sort_order_param.schema)


def test_litestar_order_by_openapi_schema_uses_alias_default() -> None:
    """Alias mode keeps orderBy as a string and exposes the alias-shaped default."""
    from typing import Any

    sql = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sql.add_config(config)

    filter_deps = create_filter_dependencies({"sort_field": ["created_at", "uploaded_collections"]})

    @get("/ordered", dependencies=filter_deps)
    async def ordered_route(filters: list[Any] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {"items": []}

    app = Litestar(route_handlers=[ordered_route], plugins=[SQLSpecPlugin(sqlspec=sql)])

    schema = app.openapi_schema
    paths = schema.paths
    assert paths is not None

    operation = paths["/ordered"].get
    assert operation is not None
    params = operation.parameters
    assert params is not None

    order_by_param = next((p for p in params if getattr(p, "name", None) == "orderBy"), None)
    assert order_by_param is not None
    order_by_schema = getattr(order_by_param, "schema", None)
    assert order_by_schema is not None
    assert getattr(order_by_schema, "default", None) == "createdAt"

    def is_string_type(schema: Any) -> bool:
        if not schema:
            return False
        stype = schema.type
        if isinstance(stype, list):
            return any("string" in str(t).lower() for t in stype)
        if stype:
            return "string" in str(stype).lower()
        if schema.one_of:
            return any(is_string_type(s) for s in schema.one_of)
        return False

    assert is_string_type(order_by_schema)


# Regression tests for issue #435 (cross-binding across providers in the same family).


def test_litestar_in_fields_two_str_fields_do_not_cross_bind() -> None:
    """Two `in_fields` with matching `str` types must not share a value (issue #435)."""
    filter_deps = create_filter_dependencies({"in_fields": [FieldNameType("a", str), FieldNameType("b", str)]})

    @get("/x", dependencies=filter_deps)
    async def handler(filters: list[FilterTypes] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {"got": [(f.field_name, sorted(f.values or ())) for f in filters if isinstance(f, InCollectionFilter)]}

    app = Litestar(route_handlers=[handler])
    with TestClient(app=app) as client:
        response = client.get("/x", params={"aIn": "HR,SYSTEM", "bIn": "ADMIN"})
        assert response.status_code == 200
        got = dict(response.json()["got"])
        assert got == {"a": sorted(["HR,SYSTEM"]), "b": sorted(["ADMIN"])}


def test_litestar_in_fields_mixed_types_do_not_400() -> None:
    """`in_fields` with mixed (`str`, `UUID`) types must bind separately, not 400 (issue #435)."""
    filter_deps = create_filter_dependencies({
        "in_fields": [FieldNameType("role", str), FieldNameType("owner_id", UUID)]
    })

    @get("/x", dependencies=filter_deps)
    async def handler(filters: list[FilterTypes] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {
            "got": [
                (f.field_name, [str(v) for v in (f.values or ())]) for f in filters if isinstance(f, InCollectionFilter)
            ]
        }

    app = Litestar(route_handlers=[handler])
    valid_uuid = "11111111-2222-3333-4444-555555555555"
    with TestClient(app=app) as client:
        response = client.get("/x", params={"roleIn": "HR", "ownerIdIn": valid_uuid})
        assert response.status_code == 200, response.text
        got = dict(response.json()["got"])
        assert got == {"role": ["HR"], "owner_id": [valid_uuid]}

        # Sending only the str field should not trigger UUID validation on the other.
        response = client.get("/x", params={"roleIn": "HR"})
        assert response.status_code == 200, response.text
        got = dict(response.json()["got"])
        assert got == {"role": ["HR"]}


def test_litestar_not_in_fields_two_fields_do_not_cross_bind() -> None:
    """Two `not_in_fields` providers must not share their `values` parameter (issue #435)."""
    filter_deps = create_filter_dependencies({"not_in_fields": [FieldNameType("a", str), FieldNameType("b", str)]})

    @get("/x", dependencies=filter_deps)
    async def handler(filters: list[FilterTypes] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {
            "got": [(f.field_name, sorted(f.values or ())) for f in filters if isinstance(f, NotInCollectionFilter)]
        }

    app = Litestar(route_handlers=[handler])
    with TestClient(app=app) as client:
        response = client.get("/x", params={"aNotIn": "HR", "bNotIn": "ADMIN"})
        assert response.status_code == 200
        got = dict(response.json()["got"])
        assert got == {"a": ["HR"], "b": ["ADMIN"]}


def test_litestar_null_fields_two_fields_do_not_cross_bind() -> None:
    """Two `null_fields` providers must not share their `is_null` parameter (issue #435)."""
    filter_deps = create_filter_dependencies({"null_fields": ["a", "b"]})

    @get("/x", dependencies=filter_deps)
    async def handler(filters: list[FilterTypes] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {"fields": sorted(str(f.field_name) for f in filters if isinstance(f, NullFilter))}

    app = Litestar(route_handlers=[handler])
    with TestClient(app=app) as client:
        response = client.get("/x", params={"aIsNull": "true"})
        assert response.status_code == 200
        assert response.json() == {"fields": ["a"]}

        response = client.get("/x", params={"bIsNull": "true"})
        assert response.status_code == 200
        assert response.json() == {"fields": ["b"]}

        response = client.get("/x", params={"aIsNull": "true", "bIsNull": "true"})
        assert response.status_code == 200
        assert response.json() == {"fields": ["a", "b"]}


def test_litestar_not_null_fields_two_fields_do_not_cross_bind() -> None:
    """Two `not_null_fields` providers must not share their `is_not_null` parameter (issue #435)."""
    filter_deps = create_filter_dependencies({"not_null_fields": ["a", "b"]})

    @get("/x", dependencies=filter_deps)
    async def handler(filters: list[FilterTypes] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {"fields": sorted(str(f.field_name) for f in filters if isinstance(f, NotNullFilter))}

    app = Litestar(route_handlers=[handler])
    with TestClient(app=app) as client:
        response = client.get("/x", params={"aIsNotNull": "true", "bIsNotNull": "true"})
        assert response.status_code == 200
        assert response.json() == {"fields": ["a", "b"]}


def test_litestar_created_at_and_updated_at_do_not_cross_bind() -> None:
    """`created_at` + `updated_at` providers share `before`/`after` names; aliases must still be honored."""
    filter_deps = create_filter_dependencies({"created_at": True, "updated_at": True})

    @get("/x", dependencies=filter_deps)
    async def handler(filters: list[FilterTypes] = Dependency(skip_validation=True)) -> dict[str, Any]:
        return {
            "got": [
                (f.field_name, f.before.isoformat() if f.before else None, f.after.isoformat() if f.after else None)
                for f in filters
                if isinstance(f, BeforeAfterFilter)
            ]
        }

    app = Litestar(route_handlers=[handler])
    with TestClient(app=app) as client:
        response = client.get(
            "/x",
            params={
                "createdBefore": "2025-01-01T00:00:00",
                "createdAfter": "2024-01-01T00:00:00",
                "updatedBefore": "2025-06-01T00:00:00",
                "updatedAfter": "2024-06-01T00:00:00",
            },
        )
        assert response.status_code == 200, response.text
        got = {field: (before, after) for field, before, after in response.json()["got"]}
        assert got["created_at"] == ("2025-01-01T00:00:00", "2024-01-01T00:00:00")
        assert got["updated_at"] == ("2025-06-01T00:00:00", "2024-06-01T00:00:00")
