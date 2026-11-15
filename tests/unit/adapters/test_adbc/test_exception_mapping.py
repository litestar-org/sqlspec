"""ADBC exception mapping for Arrow conversion failures."""

from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.exceptions import DataError


@pytest.fixture
def adbc_sqlite_session() -> "Any":
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_sqlite", "uri": ":memory:"})
    with config.provide_session() as session:
        yield session


@pytest.mark.parametrize("parameter", [[1, "x"], object()], ids=["nested-list", "object"])
def test_adbc_arrow_errors_map_to_data_error(adbc_sqlite_session: "Any", parameter: object) -> None:
    with pytest.raises(DataError, match="could not convert"):
        adbc_sqlite_session.execute("select ? as v", [parameter])


def test_adbc_arrow_errors_map_to_data_error_for_mixed_many(adbc_sqlite_session: "Any") -> None:
    adbc_sqlite_session.execute("create table t(a, b)")

    with pytest.raises(DataError, match="could not convert"):
        adbc_sqlite_session.execute_many("insert into t values (?, ?)", [(1, "a"), ("b", 2)])
