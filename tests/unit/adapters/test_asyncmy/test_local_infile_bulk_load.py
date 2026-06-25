"""Asyncmy LOAD DATA LOCAL INFILE bulk-load helpers and config gate."""

import pytest

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.adapters.asyncmy.core import build_load_data_statement, encode_records_for_local_infile
from sqlspec.exceptions import ImproperConfigurationError


def test_encode_records_handles_none_bool_and_escapes() -> None:
    records = [("hi", None, True, "x\ty"), ("a\\b", 42, False, "line\nbreak")]
    payload = encode_records_for_local_infile(records)
    assert payload == b"hi\t\\N\t1\tx\\ty\na\\\\b\t42\t0\tline\\nbreak\n"


def test_encode_records_escapes_carriage_return() -> None:
    assert encode_records_for_local_infile([("a\rb",)]) == b"a\\rb\n"


def test_build_load_data_statement_exact_string() -> None:
    statement = build_load_data_statement("orders", ["id", "name"], "/tmp/data.tsv")
    assert statement == (
        "LOAD DATA LOCAL INFILE '/tmp/data.tsv' INTO TABLE `orders` "
        "CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' "
        "LINES TERMINATED BY '\\n' (`id`, `name`)"
    )


def test_config_gate_raises_when_local_infile_disabled() -> None:
    with pytest.raises(ImproperConfigurationError):
        AsyncmyConfig(
            connection_config={"allow_local_infile": True}, driver_features={"enable_local_infile_bulk_load": True}
        )


def test_config_gate_allows_when_local_infile_and_allow_enabled() -> None:
    config = AsyncmyConfig(
        connection_config={"local_infile": True, "allow_local_infile": True},
        driver_features={"enable_local_infile_bulk_load": True},
    )
    assert config.driver_features["enable_local_infile_bulk_load"] is True
