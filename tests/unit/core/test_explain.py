"""Unit tests for EXPLAIN option value objects."""

from pathlib import Path

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


def test_c4_explain_options_source_uses_shared_key_and_fields() -> None:
    source = Path("sqlspec/core/explain.py").read_text()

    assert "EXPLAIN_OPTION_FIELDS: Final" in source
    assert "def _key(self)" in source
    assert "return self._key() == other._key()" in source
    assert "return hash(self._key())" in source
    assert "for field_name in EXPLAIN_OPTION_FIELDS" in source
