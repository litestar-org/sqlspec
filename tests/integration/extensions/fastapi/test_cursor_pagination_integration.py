"""HTTP cursor pagination against a real SQLite database."""

import sqlite3
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.core import CursorKey, CursorPagination, FilterTypes
from sqlspec.extensions.fastapi import SQLSpecPlugin
from sqlspec.extensions.fastapi.providers import dep_cache
from sqlspec.service import SQLSpecAsyncService

pytestmark = pytest.mark.xdist_group("sqlite")


@dataclass
class Item:
    id: int
    name: str


@pytest.fixture
def cursor_client(tmp_path: Path) -> Generator[TestClient, None, None]:
    dep_cache.dependencies.clear()
    database = tmp_path / "cursor.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        connection.executemany("INSERT INTO items VALUES (?, ?)", [(i, f"item-{i}") for i in range(1, 6)])
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": str(database)}))
    app = FastAPI()
    plugin = SQLSpecPlugin(spec, app=app)
    filters_dependency = plugin.provide_filters({
        "pagination_type": "cursor",
        "cursor_keys": [CursorKey("id")],
        "pagination_size": 2,
    })

    @app.get("/items")
    async def items(
        filters: Annotated[list[FilterTypes], Depends(filters_dependency)],
        db_session: Annotated[AiosqliteDriver, Depends(plugin.provide_session(config))],
    ) -> CursorPagination[Item]:
        return await SQLSpecAsyncService(session=db_session).paginate_cursor(
            "SELECT id, name FROM items", *filters, schema_type=Item
        )

    with TestClient(app) as client:
        yield client
    dep_cache.dependencies.clear()


def test_cursor_walk(cursor_client: TestClient) -> None:
    first = cursor_client.get("/items").json()
    assert set(first) == {"items", "limit", "next_cursor", "previous_cursor", "has_next", "has_previous"}
    second = cursor_client.get("/items", params={"cursor": first["next_cursor"]}).json()
    third = cursor_client.get("/items", params={"cursor": second["next_cursor"]}).json()
    assert [item["id"] for page in (first, second, third) for item in page["items"]] == [1, 2, 3, 4, 5]
    assert third["has_next"] is False
    previous = cursor_client.get("/items", params={"cursor": third["previous_cursor"]}).json()
    assert previous["items"] == second["items"]
    beginning = cursor_client.get("/items", params={"cursor": previous["previous_cursor"]}).json()
    assert beginning["items"] == first["items"]


def test_invalid_cursor(cursor_client: TestClient) -> None:
    response = cursor_client.get("/items", params={"cursor": "garbage"})
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", "cursor"]
    assert response.json()["detail"][0]["msg"] == "Invalid pagination cursor"


def test_cursor_page_size_above_limit_is_rejected(cursor_client: TestClient) -> None:
    assert cursor_client.get("/items", params={"pageSize": 1001}).status_code == 422


def test_cursor_openapi(cursor_client: TestClient) -> None:
    schema = cursor_client.get("/openapi.json").json()
    parameters = schema["paths"]["/items"]["get"]["parameters"]
    assert {parameter["name"] for parameter in parameters} == {"cursor", "pageSize"}
    assert next(parameter["schema"]["maximum"] for parameter in parameters if parameter["name"] == "pageSize") == 1000
    components = schema["components"]["schemas"]
    pagination = next(body for name, body in components.items() if name.startswith("CursorPagination"))
    assert set(pagination["properties"]) == {
        "items",
        "limit",
        "next_cursor",
        "previous_cursor",
        "has_next",
        "has_previous",
    }
    assert set(pagination["required"]) == set(pagination["properties"])
    assert "Item" in components
