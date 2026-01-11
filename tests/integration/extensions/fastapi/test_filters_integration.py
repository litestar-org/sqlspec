"""Integration tests for FastAPI filter dependencies with real HTTP requests."""

from collections.abc import Generator
from typing import Annotated, Any
from uuid import UUID, uuid4

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.base import SQLSpec
from sqlspec.core import BeforeAfterFilter, FilterTypes, LimitOffsetFilter, OrderByFilter
from sqlspec.extensions.fastapi import SQLSpecPlugin
from sqlspec.extensions.fastapi.providers import dep_cache

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.fixture(autouse=True)
def _clear_dependency_cache() -> Generator[None, None, None]:
    """Clear the dependency cache before each test to prevent test pollution."""
    dep_cache.dependencies.clear()
    yield
    dep_cache.dependencies.clear()


def test_fastapi_id_filter_dependency() -> None:
    """Test ID filter dependency with actual HTTP request."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.get("/users")
    async def list_users(
        filters: Annotated[list[FilterTypes], Depends(db_ext.provide_filters({"id_filter": UUID}))],
    ) -> dict[str, Any]:
        return {"filter_count": len(filters), "filters": [type(f).__name__ for f in filters]}

    with TestClient(app) as client:
        # No query params - should return empty filters
        response = client.get("/users")
        assert response.status_code == 200
        assert response.json() == {"filter_count": 0, "filters": []}

        # With IDs - should return InCollectionFilter
        test_id1 = str(uuid4())
        test_id2 = str(uuid4())
        response = client.get(f"/users?ids={test_id1}&ids={test_id2}")
        assert response.status_code == 200
        data = response.json()
        assert data["filter_count"] == 1
        assert data["filters"] == ["InCollectionFilter"]


def test_fastapi_search_filter_dependency() -> None:
    """Test search filter dependency with actual HTTP request."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "manual"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.get("/users")
    async def list_users(
        filters: Annotated[
            list[FilterTypes], Depends(db_ext.provide_filters({"search": "name,email", "search_ignore_case": True}))
        ],
    ) -> dict[str, Any]:
        return {"filter_count": len(filters), "filters": [type(f).__name__ for f in filters]}

    with TestClient(app) as client:
        # No search string - empty filters
        response = client.get("/users")
        assert response.status_code == 200
        assert response.json() == {"filter_count": 0, "filters": []}

        # With search string
        response = client.get("/users?searchString=john")
        assert response.status_code == 200
        data = response.json()
        assert data["filter_count"] == 1
        assert data["filters"] == ["SearchFilter"]

        # With case sensitivity override
        response = client.get("/users?searchString=JOHN&searchIgnoreCase=false")
        assert response.status_code == 200
        data = response.json()
        assert data["filter_count"] == 1


def test_fastapi_pagination_filter_dependency() -> None:
    """Test pagination filter dependency with actual HTTP request."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "manual"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.get("/users")
    async def list_users(
        filters: Annotated[list[FilterTypes], Depends(db_ext.provide_filters({"pagination_type": "limit_offset"}))],
    ) -> dict[str, Any]:
        pagination = next((f for f in filters if isinstance(f, LimitOffsetFilter)), None)
        if pagination:
            return {"limit": pagination.limit, "offset": pagination.offset}
        return {"limit": None, "offset": None}

    with TestClient(app) as client:
        # Default pagination (page 1, size 20)
        response = client.get("/users")
        assert response.status_code == 200
        assert response.json() == {"limit": 20, "offset": 0}

        # Page 2
        response = client.get("/users?currentPage=2")
        assert response.status_code == 200
        assert response.json() == {"limit": 20, "offset": 20}

        # Custom page size
        response = client.get("/users?pageSize=50&currentPage=1")
        assert response.status_code == 200
        assert response.json() == {"limit": 50, "offset": 0}

        # Page 3 with custom size
        response = client.get("/users?currentPage=3&pageSize=25")
        assert response.status_code == 200
        assert response.json() == {"limit": 25, "offset": 50}


def test_fastapi_order_by_filter_dependency() -> None:
    """Test order by filter dependency with actual HTTP request."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "manual"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.get("/users")
    async def list_users(
        filters: Annotated[list[FilterTypes], Depends(db_ext.provide_filters({"sort_field": "created_at"}))],
    ) -> dict[str, Any]:
        order_by = next((f for f in filters if isinstance(f, OrderByFilter)), None)
        if order_by:
            return {"field": order_by.field_name, "order": order_by.sort_order}
        return {"field": None, "order": None}

    with TestClient(app) as client:
        # Default ordering
        response = client.get("/users")
        assert response.status_code == 200
        assert response.json() == {"field": "created_at", "order": "desc"}

        # Custom field
        response = client.get("/users?orderBy=name")
        assert response.status_code == 200
        assert response.json() == {"field": "name", "order": "desc"}

        # Custom order
        response = client.get("/users?sortOrder=asc")
        assert response.status_code == 200
        assert response.json() == {"field": "created_at", "order": "asc"}

        # Both custom
        response = client.get("/users?orderBy=email&sortOrder=asc")
        assert response.status_code == 200
        assert response.json() == {"field": "email", "order": "asc"}


def test_fastapi_date_range_filter_dependency() -> None:
    """Test date range filter dependency with actual HTTP request."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "manual"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.get("/users")
    async def list_users(
        filters: Annotated[list[FilterTypes], Depends(db_ext.provide_filters({"created_at": True}))],
    ) -> dict[str, Any]:
        date_filter = next((f for f in filters if isinstance(f, BeforeAfterFilter)), None)
        if date_filter:
            return {
                "field": date_filter.field_name,
                "before": date_filter.before.isoformat() if date_filter.before else None,
                "after": date_filter.after.isoformat() if date_filter.after else None,
            }
        return {"field": None, "before": None, "after": None}

    with TestClient(app) as client:
        # No dates - no filter
        response = client.get("/users")
        assert response.status_code == 200
        assert response.json() == {"field": None, "before": None, "after": None}

        # Before date only
        response = client.get("/users?createdBefore=2025-01-15T10:30:00Z")
        assert response.status_code == 200
        data = response.json()
        assert data["field"] == "created_at"
        assert data["before"] == "2025-01-15T10:30:00+00:00"
        assert data["after"] is None

        # After date only
        response = client.get("/users?createdAfter=2024-01-01T00:00:00Z")
        assert response.status_code == 200
        data = response.json()
        assert data["after"] == "2024-01-01T00:00:00+00:00"
        assert data["before"] is None

        # Both dates
        response = client.get("/users?createdBefore=2025-01-15T10:30:00Z&createdAfter=2024-01-01T00:00:00Z")
        assert response.status_code == 200
        data = response.json()
        assert data["before"] is not None
        assert data["after"] is not None


def test_fastapi_multiple_filters_combined() -> None:
    """Test combining multiple filter types in one request."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "manual"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.get("/users")
    async def list_users(
        filters: Annotated[
            list[FilterTypes],
            Depends(
                db_ext.provide_filters({
                    "id_filter": UUID,
                    "search": "name",
                    "pagination_type": "limit_offset",
                    "sort_field": "created_at",
                })
            ),
        ],
    ) -> dict[str, Any]:
        return {"filter_count": len(filters), "filter_types": sorted(type(f).__name__ for f in filters)}

    with TestClient(app) as client:
        # All filters provided
        test_id = str(uuid4())
        response = client.get(
            f"/users?ids={test_id}&searchString=john&currentPage=2&pageSize=25&orderBy=name&sortOrder=asc"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["filter_count"] == 4
        assert set(data["filter_types"]) == {"InCollectionFilter", "SearchFilter", "LimitOffsetFilter", "OrderByFilter"}

        # Partial filters
        response = client.get(f"/users?ids={test_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["filter_count"] == 3  # ID + default pagination + default order by
        assert "InCollectionFilter" in data["filter_types"]


def test_fastapi_filter_with_actual_query_execution() -> None:
    """Test filters applied to actual SQL query execution."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "autocommit"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.post("/setup")
    async def setup(db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))]) -> dict[str, Any]:
        await db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        await db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
        await db.execute("INSERT INTO users (name, age) VALUES ('Charlie', 35)")
        return {"created": True}

    @app.get("/users")
    async def list_users(
        filters: Annotated[
            list[FilterTypes],
            Depends(
                db_ext.provide_filters({"pagination_type": "limit_offset", "sort_field": "name", "sort_order": "desc"})
            ),
        ],
        db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))],
    ) -> dict[str, Any]:
        result = await db.select("SELECT * FROM users", *filters)
        return {"users": result, "applied_filters": len(filters)}

    with TestClient(app) as client:
        # Setup database
        response = client.post("/setup")
        assert response.status_code == 200

        # Query with pagination
        response = client.get("/users?currentPage=1&pageSize=2")
        assert response.status_code == 200
        data = response.json()
        assert data["applied_filters"] == 2  # pagination + order by
        assert len(data["users"]) == 2

        # Query with different order
        response = client.get("/users?orderBy=age&sortOrder=asc")
        assert response.status_code == 200
        data = response.json()
        # Should be ordered by age ascending: Bob(25), Alice(30), Charlie(35)
        assert data["users"][0]["name"] == "Bob"
        assert data["users"][0]["age"] == 25


def test_fastapi_openapi_schema_includes_filter_params() -> None:
    """Test that OpenAPI schema includes filter query parameters."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "manual"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.get("/users")
    async def list_users(
        filters: Annotated[
            list[FilterTypes],
            Depends(db_ext.provide_filters({"id_filter": UUID, "search": "name", "pagination_type": "limit_offset"})),
        ],
    ) -> dict[str, Any]:
        return {"filters": len(filters)}

    # Get OpenAPI schema
    openapi_schema = app.openapi()

    # Check that query parameters are in schema
    user_endpoint = openapi_schema["paths"]["/users"]["get"]
    param_names = {param["name"] for param in user_endpoint.get("parameters", [])}

    # Should include filter query parameters
    assert "ids" in param_names  # ID filter
    assert "searchString" in param_names  # Search
    assert "currentPage" in param_names  # Pagination
    assert "pageSize" in param_names  # Pagination


def test_fastapi_filter_validation_error() -> None:
    """Test that invalid filter values return proper validation errors."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "manual"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app=app)

    @app.get("/users")
    async def list_users(
        filters: Annotated[list[FilterTypes], Depends(db_ext.provide_filters({"created_at": True}))],
    ) -> dict[str, Any]:
        return {"filters": len(filters)}

    with TestClient(app) as client:
        # Invalid date format should return 422 validation error
        response = client.get("/users?createdBefore=invalid-date")
        assert response.status_code == 422
        error_data = response.json()
        assert "detail" in error_data
        # Should mention the validation error for createdBefore
        assert any("createdBefore" in str(error) for error in error_data["detail"])
