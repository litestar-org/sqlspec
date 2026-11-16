"""Table name validation for Litestar stores."""

import pytest

from sqlspec.extensions.litestar.store import BaseSQLSpecStore


@pytest.mark.parametrize(
    "table_name", ["sess\n", "sess;drop table x", "sess\x00", "se\u017f\u017f", "", "1sess", "a b"]
)
def test_invalid_table_name_rejected(table_name: str) -> None:
    with pytest.raises(ValueError, match=r"Table name|Invalid table name"):
        BaseSQLSpecStore._ensure_table_name(table_name)


@pytest.mark.parametrize("table_name", ["sess", "litestar_session", "_x1"])
def test_valid_table_name_accepted(table_name: str) -> None:
    BaseSQLSpecStore._ensure_table_name(table_name)
