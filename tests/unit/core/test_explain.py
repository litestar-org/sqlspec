"""Unit tests for EXPLAIN option value objects."""

from sqlspec.core.explain import ExplainFormat, ExplainOptions


def test_explain_options_value_semantics() -> None:
    options = ExplainOptions(analyze=True, verbose=True, format=ExplainFormat.JSON, costs=False, buffers=True)
    same = ExplainOptions(analyze=True, verbose=True, format="json", costs=False, buffers=True)
    different = ExplainOptions(analyze=False, verbose=True, format=ExplainFormat.JSON, costs=False, buffers=True)

    assert options == same
    assert options != different
    assert hash(options) == hash(same)
    assert repr(options) == "ExplainOptions(analyze=True, verbose=True, format='json', costs=False, buffers=True)"
    assert options.to_dict() == {"analyze": True, "verbose": True, "format": "JSON", "costs": False, "buffers": True}
