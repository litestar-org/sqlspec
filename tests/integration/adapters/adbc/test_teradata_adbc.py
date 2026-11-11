"""Live Columnar Teradata ADBC and Flight SQL acceptance."""

from collections.abc import Generator
from uuid import uuid4

import pytest

from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.exceptions import NotFoundError, TransactionError, UniqueViolationError

pytestmark = [pytest.mark.teradata, pytest.mark.adbc]


@pytest.fixture
def teradata_adbc_table(adbc_teradata_session: AdbcDriver) -> Generator[str, None, None]:
    name = f"sqlspec_adbc_td_{uuid4().hex[:8]}"
    adbc_teradata_session.execute_script(
        f"CREATE MULTISET TABLE {name} (id INTEGER NOT NULL, name VARCHAR(128) NOT NULL, UNIQUE(id))"
    )
    try:
        yield name
    finally:
        adbc_teradata_session.execute_script(f"DROP TABLE {name}")


def test_teradata_adbc_select_to_arrow_and_qmark_binds(
    adbc_teradata_session: AdbcDriver, teradata_adbc_table: str
) -> None:
    adbc_teradata_session.execute(f"INSERT INTO {teradata_adbc_table} (id, name) VALUES (?, ?)", (1, "alpha"))
    assert adbc_teradata_session.select_value(f"SELECT name FROM {teradata_adbc_table} WHERE id = ?", (1,)) == "alpha"
    assert adbc_teradata_session.select_to_arrow(f"SELECT id, name FROM {teradata_adbc_table}").to_dict() == [
        {"id": 1, "name": "alpha"}
    ]


def test_teradata_adbc_load_from_arrow_bulk_ingest(adbc_teradata_session: AdbcDriver, teradata_adbc_table: str) -> None:
    pa = pytest.importorskip("pyarrow")
    adbc_teradata_session.load_from_arrow(teradata_adbc_table, pa.table({"id": [10, 11], "name": ["gamma", "delta"]}))
    assert adbc_teradata_session.select_value(f"SELECT COUNT(*) FROM {teradata_adbc_table}") == 2


def test_teradata_adbc_error_mapping_and_autocommit_begin(
    adbc_teradata_session: AdbcDriver, teradata_adbc_table: str
) -> None:
    adbc_teradata_session.execute(f"INSERT INTO {teradata_adbc_table} (id, name) VALUES (?, ?)", (1, "alpha"))
    with pytest.raises(UniqueViolationError):
        adbc_teradata_session.execute(f"INSERT INTO {teradata_adbc_table} (id, name) VALUES (?, ?)", (1, "duplicate"))
    with pytest.raises(NotFoundError):
        adbc_teradata_session.select("SELECT * FROM nonexistent_td_tbl_xyz")
    try:
        adbc_teradata_session.begin()
    except TransactionError:
        pass
    else:
        adbc_teradata_session.rollback()


def test_teradata_adbc_data_dictionary_reflection(adbc_teradata_session: AdbcDriver, teradata_adbc_table: str) -> None:
    dictionary = adbc_teradata_session.data_dictionary
    version = dictionary.get_version(adbc_teradata_session)
    if version is None:
        pytest.fail("Teradata server version was not discovered")
    assert version.major >= 15
    columns = dictionary.get_columns(adbc_teradata_session, teradata_adbc_table)
    assert {column["column_name"].lower() for column in columns} == {"id", "name"}


def test_teradata_flightsql_dialect_and_arrow_query(adbc_teradata_flightsql_session: AdbcDriver) -> None:
    assert adbc_teradata_flightsql_session.dialect == "teradata"
    assert adbc_teradata_flightsql_session.select_value("SELECT 1 AS ok") == 1
    assert adbc_teradata_flightsql_session.select_to_arrow("SELECT 1 AS ok").to_dict() == [{"ok": 1}]
