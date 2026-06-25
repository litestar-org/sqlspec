"""PyMySQL LOAD DATA LOCAL INFILE bulk-load helpers and config gate."""

import pytest

from sqlspec.adapters.pymysql.config import PyMysqlConfig
from sqlspec.adapters.pymysql.core import build_load_data_statement, encode_records_for_local_infile
from sqlspec.exceptions import ImproperConfigurationError


def test_encode_records_handles_none_bool_and_escapes() -> None:
    records = [("hi", None, True, "x\ty"), ("a\\b", 42, False, "line\nbreak")]
    payload = encode_records_for_local_infile(records)
    expected = b"hi\t\\N\t1\tx\\ty\na\\\\b\t42\t0\tline\\nbreak\n"
    assert payload == expected


def test_encode_records_escapes_carriage_return() -> None:
    payload = encode_records_for_local_infile([("a\rb",)])
    assert payload == b"a\\rb\n"


def test_encode_records_always_ends_with_newline() -> None:
    payload = encode_records_for_local_infile([("only",)])
    assert payload.endswith(b"\n")


def test_build_load_data_statement_exact_string() -> None:
    statement = build_load_data_statement("orders", ["id", "name"], "/tmp/data.tsv")
    assert statement == (
        "LOAD DATA LOCAL INFILE '/tmp/data.tsv' INTO TABLE `orders` "
        "CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' "
        "LINES TERMINATED BY '\\n' (`id`, `name`)"
    )


def test_config_gate_raises_when_local_infile_disabled() -> None:
    with pytest.raises(ImproperConfigurationError):
        PyMysqlConfig(
            connection_config={"local_infile": False}, driver_features={"enable_local_infile_bulk_load": True}
        )


def test_config_gate_allows_when_local_infile_enabled() -> None:
    config = PyMysqlConfig(
        connection_config={"local_infile": True}, driver_features={"enable_local_infile_bulk_load": True}
    )
    assert config.driver_features["enable_local_infile_bulk_load"] is True
