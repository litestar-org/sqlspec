from __future__ import annotations

# Import necessary modules for the decorator
import functools
from typing import Any, Callable, TypeVar, cast

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import Adbc

F = TypeVar("F", bound=Callable[..., Any])


def xfail_if_driver_missing(func: F) -> F:
    """Decorator to xfail a test if the ADBC driver shared object is missing."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "cannot open shared object file" in str(e):
                pytest.xfail(f"ADBC driver shared object file not found: {e}")
            raise e  # Reraise other exceptions

    return cast(F, wrapper)


@pytest.fixture(scope="session")
def adbc_session(postgres_service: PostgresService) -> Adbc:
    """Create an ADBC session for PostgreSQL."""
    return Adbc(
        uri=f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="postgresql",
    )
