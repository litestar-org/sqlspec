"""Tests for documented mypyc boundary decisions in config surfaces."""

import sqlspec.config as config_module
from sqlspec.config import DatabaseConfigProtocol


def test_config_module_documents_interpreted_boundary() -> None:
    """The config module documents why it stays interpreted."""
    assert config_module.__doc__ is not None
    assert "stability-critical" in config_module.__doc__
    assert "interpreted" in config_module.__doc__


def test_database_config_protocol_documents_compiled_caller_contract() -> None:
    """DatabaseConfigProtocol documents the contract compiled callers rely on."""
    assert DatabaseConfigProtocol.__doc__ is not None
    assert "stability-critical" in DatabaseConfigProtocol.__doc__
    assert "compiled callers" in DatabaseConfigProtocol.__doc__.lower()
