"""Integration test for the route-handler-deepcopy crash reported against 0.48.1.

Litestar's `Controller.get_route_handlers()` performs `copy.deepcopy(self_handler)`
at controller registration time -- *before* the app attaches the `.app` / `.owner`
back-pointer to the handler. Pre-fix, that deepcopy walked into
`handler.dependencies` -> `Provide` -> `functools.partial(provide_fn, _*Provider)`
and crashed on the mypyc-compiled provider's `__new__` -> `__init__()` chain
with `TypeError: __init__() missing required argument`.

These tests exercise that same pre-registration deepcopy via the public Litestar
API: `@get(..., dependencies=...)` returns a `BaseRouteHandler` whose
`dependencies` dict already contains the filter `Provide` objects, and
`copy.deepcopy(handler)` reproduces the user's exact stack trace path.
"""

import copy
from typing import Any

import pytest
from litestar import Controller, get
from litestar.router import Router

from sqlspec.extensions.litestar.providers import ChoiceField, FieldNameType, create_filter_dependencies, dep_cache
from sqlspec.typing import LITESTAR_INSTALLED

if not LITESTAR_INSTALLED:
    pytest.skip("Litestar not installed", allow_module_level=True)


@pytest.fixture(autouse=True)
def _clear_dependency_cache() -> Any:
    dep_cache.dependencies.clear()
    yield
    dep_cache.dependencies.clear()


def _dep_keys(handler: Any) -> "set[str]":
    return set(getattr(handler, "dependencies", None) or ())


def test_handler_deepcopy_with_limit_offset_filter() -> None:
    """Minimum repro: a handler with a single LimitOffsetFilter dep must deepcopy."""
    deps = create_filter_dependencies({"pagination_type": "limit_offset", "pagination_size": 10})

    @get("/probe", dependencies=deps)
    async def probe() -> "list[Any]":
        return []

    out = copy.deepcopy(probe)
    assert out is not probe
    assert _dep_keys(out) == _dep_keys(probe)


def test_handler_deepcopy_with_full_filter_set() -> None:
    """Every supported filter type wired in. Each filter `Provide` must survive deepcopy."""
    deps = create_filter_dependencies({
        "id_filter": int,
        "id_field": "id",
        "pagination_type": "limit_offset",
        "pagination_size": 25,
        "search": "name",
        "search_ignore_case": False,
        "sort_field": "id",
        "created_at": True,
        "updated_at": True,
        "in_fields": [FieldNameType("name", str)],
        "not_in_fields": [FieldNameType("name", str)],
        "null_fields": "deleted_at",
        "not_null_fields": "created_at",
        "boolean_fields": "is_active",
        "choice_fields": [ChoiceField("status", ["active", "pending"])],
    })

    @get("/probe", dependencies=deps)
    async def probe() -> "list[Any]":
        return []

    out = copy.deepcopy(probe)
    assert out is not probe
    assert _dep_keys(out) == _dep_keys(probe)


def test_handler_repeated_deepcopy_is_idempotent() -> None:
    """A deepcopied handler must itself deepcopy."""
    deps = create_filter_dependencies({"pagination_type": "limit_offset"})

    @get("/probe", dependencies=deps)
    async def probe() -> "list[Any]":
        return []

    first = copy.deepcopy(probe)
    second = copy.deepcopy(first)
    assert second is not first
    assert _dep_keys(second) == _dep_keys(first)


def test_controller_get_route_handlers_with_filter_dependencies() -> None:
    """Reproduce the user's exact Controller.get_route_handlers() stack trace path.

    `Controller.get_route_handlers()` is what crashed in the user's report. It
    calls `copy.deepcopy(self_handler)` on each handler attribute of the
    Controller class. To force the controller's handlers to carry the filter
    `Provide` objects at the point of that deepcopy, we attach `dependencies` to
    the `@get` decorator (Litestar 2.22's Controller-level `dependencies` are
    merged at request time, not at `get_route_handlers` time).
    """
    deps = create_filter_dependencies({"pagination_type": "limit_offset", "search": "name", "sort_field": "id"})

    class _ProbeController(Controller):
        path = "/probe"

        @get("/", dependencies=deps)
        async def list_items(self) -> "list[Any]":
            return []

    owner = Router(path="/api", route_handlers=[])
    handlers = list(_ProbeController(owner=owner).get_route_handlers())

    assert len(handlers) == 1
    assert _dep_keys(handlers[0]) == set(deps)
