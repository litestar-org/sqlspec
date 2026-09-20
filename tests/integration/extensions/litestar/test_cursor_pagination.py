"""HTTP cursor pagination against a real SQLite database."""

import sqlite3
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, cast

import msgspec
import pytest
from litestar import Litestar, get
from litestar.di import NamedDependency
from litestar.params import QueryParameter, SkipValidation
from litestar.testing import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.core import (
    CursorFilter,
    CursorKey,
    CursorPagination,
    FilterTypes,
    LimitOffsetFilter,
    OrderByFilter,
    Pagination,
)
from sqlspec.extensions.litestar import SQLSpecPlugin
from sqlspec.extensions.litestar.providers import create_filter_dependencies, dep_cache
from sqlspec.service import SQLSpecAsyncService

pytestmark = pytest.mark.xdist_group("sqlite")


@dataclass
class Item:
    id: int
    name: str


class MsgspecItem(msgspec.Struct):
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
    spec.add_config(AiosqliteConfig(connection_config={"database": str(database)}))
    dependencies = create_filter_dependencies({
        "pagination_type": "cursor",
        "cursor_keys": [CursorKey("id")],
        "pagination_size": 2,
    })

    @get("/items", dependencies=dependencies)
    async def items(
        filters: NamedDependency[SkipValidation[list[FilterTypes]]], db_session: NamedDependency[AiosqliteDriver]
    ) -> CursorPagination[Item]:
        return cast(
            "CursorPagination[Item]",
            await SQLSpecAsyncService(session=db_session).paginate(
                "SELECT id, name FROM items", *filters, schema_type=Item
            ),
        )

    @get("/pages")
    async def pages(
        db_session: NamedDependency[AiosqliteDriver],
        mode: Annotated[Literal["cursor", "offset"], QueryParameter()] = "cursor",
        cursor: Annotated[str | None, QueryParameter()] = None,
        offset: Annotated[int, QueryParameter()] = 0,
    ) -> Pagination[MsgspecItem]:
        filters: list[FilterTypes] = (
            [CursorFilter("id", 2, cursor)] if mode == "cursor" else [LimitOffsetFilter(2, offset), OrderByFilter("id")]
        )
        return await SQLSpecAsyncService(session=db_session).paginate(
            "SELECT id, name FROM items", *filters, schema_type=MsgspecItem
        )

    app = Litestar([items, pages], plugins=[SQLSpecPlugin(sqlspec=spec)])
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client
    dep_cache.dependencies.clear()


def test_cursor_walk(cursor_client: TestClient) -> None:
    response = cursor_client.get("/items")
    assert response.status_code == 200, response.text
    first = response.json()
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
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid pagination cursor"


def test_cursor_page_size_above_limit_is_rejected(cursor_client: TestClient) -> None:
    assert cursor_client.get("/items", params={"pageSize": 1001}).status_code == 400


def test_cursor_openapi(cursor_client: TestClient) -> None:
    schema = cursor_client.app.openapi_schema.to_schema()
    parameters = schema["paths"]["/items"]["get"]["parameters"]
    assert {parameter["name"] for parameter in parameters} == {"cursor", "pageSize"}
    assert next(parameter["schema"]["maximum"] for parameter in parameters if parameter["name"] == "pageSize") == 1000
    components = schema["components"]["schemas"]
    response = schema["paths"]["/items"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    pagination = components[response["$ref"].rsplit("/", 1)[-1]]
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


@pytest.mark.parametrize("mode", ["cursor", "offset"])
def test_msgspec_pagination_union_response(cursor_client: TestClient, mode: str) -> None:
    response = cursor_client.get("/pages", params={"mode": mode})
    assert response.status_code == 200, response.text
    page = response.json()
    assert page["items"] == [{"id": 1, "name": "item-1"}, {"id": 2, "name": "item-2"}]
    assert page["limit"] == 2
    if mode == "cursor":
        assert set(page) == {"items", "limit", "next_cursor", "previous_cursor", "has_next", "has_previous"}
        assert page["has_next"] is True
        assert page["has_previous"] is False
        assert page["previous_cursor"] is None
        continuation = {"mode": mode, "cursor": page["next_cursor"]}
    else:
        assert set(page) == {"items", "limit", "offset", "total"}
        assert page["total"] == 5
        assert page["offset"] == 0
        continuation = {"mode": mode, "offset": "2"}
    following = cursor_client.get("/pages", params=continuation)
    assert following.status_code == 200, following.text
    assert following.json()["items"] == [{"id": 3, "name": "item-3"}, {"id": 4, "name": "item-4"}]


def test_msgspec_pagination_union_openapi(cursor_client: TestClient) -> None:
    schema = cursor_client.app.openapi_schema.to_schema()
    response = schema["paths"]["/pages"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    variants = response["oneOf"]
    assert len(variants) == 2
    components = schema["components"]["schemas"]
    envelopes = [components[variant["$ref"].rsplit("/", 1)[-1]] for variant in variants]
    assert {frozenset(envelope["properties"]) for envelope in envelopes} == {
        frozenset({"items", "limit", "next_cursor", "previous_cursor", "has_next", "has_previous"}),
        frozenset({"items", "limit", "offset", "total"}),
    }
    for envelope in envelopes:
        item_ref = envelope["properties"]["items"]["items"]["$ref"]
        item_schema = components[item_ref.rsplit("/", 1)[-1]]
        assert item_schema["properties"] == {"id": {"type": "integer"}, "name": {"type": "string"}}
        assert set(item_schema["required"]) == {"id", "name"}
