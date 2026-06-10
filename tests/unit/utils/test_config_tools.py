"""Tests for sqlspec.utils.config_tools sensitive feature gates."""

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.config_tools import SENSITIVE_FLAG_PREFIX, assert_sensitive_feature_enabled


def test_sensitive_flag_prefix_is_allow() -> None:
    assert SENSITIVE_FLAG_PREFIX == "allow_"


def test_gate_raises_when_requested_without_opt_in() -> None:
    with pytest.raises(ImproperConfigurationError, match="allow_local_infile=True"):
        assert_sensitive_feature_enabled(
            "Asyncmy local_infile=True",
            True,
            False,
            flag_name="allow_local_infile",
            risk="LOAD DATA LOCAL INFILE can read client files",
        )


def test_gate_error_message_format() -> None:
    with pytest.raises(
        ImproperConfigurationError,
        match=r"Asyncmy local_infile=True requires allow_local_infile=True "
        r"because LOAD DATA LOCAL INFILE can read client files\.",
    ):
        assert_sensitive_feature_enabled(
            "Asyncmy local_infile=True",
            True,
            False,
            flag_name="allow_local_infile",
            risk="LOAD DATA LOCAL INFILE can read client files",
        )


def test_gate_passes_when_allowed() -> None:
    assert_sensitive_feature_enabled("feature", True, True, flag_name="allow_x", risk="risk")


def test_gate_passes_when_not_requested() -> None:
    assert_sensitive_feature_enabled("feature", False, False, flag_name="allow_x", risk="risk")
