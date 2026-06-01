"""Case records for shared adapter contract tests."""

from dataclasses import dataclass

import pytest
from _pytest.mark.structures import Mark, MarkDecorator


@dataclass(frozen=True)
class DriverCase:
    """Driver fixture and capability metadata for contract tests."""

    id: str
    fixture_name: str
    adapter: str
    dialect: str
    marks: tuple[Mark | MarkDecorator, ...] = ()
    supports_arrow: bool = False
    supports_explain: bool = False
    supports_execute_many: bool = True
    supports_migrations: bool = False
    supports_storage_bridge: bool = False


@dataclass(frozen=True)
class DriverCaseContext:
    """Resolved driver instance paired with its case metadata."""

    case: DriverCase
    driver: object


SQLITE_XDIST_MARK = pytest.mark.xdist_group("sqlite")

SYNC_DRIVER_CASES = (
    DriverCase(
        id="sqlite-sync",
        fixture_name="contract_sqlite_driver",
        adapter="sqlite",
        dialect="sqlite",
        marks=(SQLITE_XDIST_MARK,),
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
    ),
)

ASYNC_DRIVER_CASES = (
    DriverCase(
        id="aiosqlite-async",
        fixture_name="contract_aiosqlite_driver",
        adapter="aiosqlite",
        dialect="sqlite",
        marks=(SQLITE_XDIST_MARK, pytest.mark.anyio),
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
    ),
)

DRIVER_CASES = SYNC_DRIVER_CASES + ASYNC_DRIVER_CASES
SYNC_DRIVER_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in SYNC_DRIVER_CASES)
ASYNC_DRIVER_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in ASYNC_DRIVER_CASES)
DRIVER_PARAMS = SYNC_DRIVER_PARAMS
