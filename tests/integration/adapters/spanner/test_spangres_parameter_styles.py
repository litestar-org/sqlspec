"""Spangres parameter-style residual documentation.

Runtime PostgreSQL-style parameter binding for Spanner is not active until the
suite has PostgreSQL-dialect Spanner fixtures. The GoogleSQL Spanner driver
continues to use named-at parameters, which is the only executable assertion
this file can make today.
"""

from sqlspec.adapters.spanner.core import driver_profile
from sqlspec.core import ParameterStyle


def test_spangres_parameter_style_differs_from_googlesql() -> None:
    """GoogleSQL Spanner keeps @name style while Spangres would use $1 style."""
    assert driver_profile.default_style == ParameterStyle.NAMED_AT
