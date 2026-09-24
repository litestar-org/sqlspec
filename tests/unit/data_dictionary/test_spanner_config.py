"""Unit tests for Spanner data dictionary dialect configuration."""

from sqlglot import exp

from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary
from sqlspec.data_dictionary import get_dialect_config
from sqlspec.data_dictionary.dialects.spanner.config import SPANNER_CONFIG, SPANNER_FEATURE_FLAGS


def test_spanner_supports_for_update_feature_flag() -> None:
    """Verify that Spanner feature flag supports_for_update evaluates to False."""
    assert SPANNER_FEATURE_FLAGS["supports_for_update"] is False
    assert SPANNER_CONFIG.get_feature_flag("supports_for_update") is False

    dictionary = SpannerDataDictionary()
    assert dictionary.get_feature_flag(None, "supports_for_update") is False


def test_spanner_type_mappings_include_float32_and_tokenlist() -> None:
    """Verify that Spanner config includes float32 and tokenlist mappings."""
    cfg = get_dialect_config("spanner")
    assert cfg.get_optimal_type("float32") == "FLOAT32"
    assert cfg.get_optimal_type("tokenlist") == "TOKENLIST"
    assert isinstance(exp.DataType.build("FLOAT32", dialect="spanner"), exp.DataType)
