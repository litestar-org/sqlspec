"""mysql-connector compiled core helpers."""

from sqlspec.adapters.mysqlconnector.core import collect_stream_rows
from sqlspec.utils.serializers import from_json


def test_collect_stream_rows_preserves_dict_values_and_decodes_json() -> None:
    """Dict rows from a dictionary=True cursor keep their values and still decode JSON."""
    rows = [{"id": 1, "payload": '{"name": "alpha"}'}, {"id": 2, "payload": '{"name": "beta"}'}]

    collected = collect_stream_rows(rows, (["id", "payload"], [1]), from_json)

    assert collected == [{"id": 1, "payload": {"name": "alpha"}}, {"id": 2, "payload": {"name": "beta"}}]


def test_collect_stream_rows_still_zips_tuple_rows() -> None:
    """Tuple rows from a plain cursor keep the positional zip path."""
    collected = collect_stream_rows([(1, '{"name": "alpha"}')], (["id", "payload"], [1]), from_json)

    assert collected == [{"id": 1, "payload": {"name": "alpha"}}]
