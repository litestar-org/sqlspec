"""JSON serialization utilities for SQLSpec.

Re-exports common JSON encoding and decoding functions from the core
serialization module for convenient access.
"""

from sqlspec._serialization import decode_json as from_json
from sqlspec._serialization import encode_json as to_json

__all__ = ("from_json", "to_json")
