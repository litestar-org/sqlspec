"""Sanic test config.

Sanic registers its own ``warnings.filterwarnings("once", category=DeprecationWarning,
module="sanic.*")`` at ``Config._configure_warnings`` time, which prepends a filter
that takes precedence over anything in pyproject's ``[tool.pytest.ini_options]``.
The env var below makes Sanic's own filter ``"ignore"`` instead of ``"once"`` so the
v26.6 ``loop`` listener-argument deprecation stays out of the warnings summary.
"""

import os

os.environ.setdefault("SANIC_DEPRECATION_FILTER", "ignore")
