"""Public typed feature dictionaries accept disabled tenant strategies."""

from sqlspec.adapters.asyncpg import AsyncpgDriverFeatures
from sqlspec.adapters.psqlpy import PsqlpyDriverFeatures
from sqlspec.adapters.psycopg import PsycopgDriverFeatures

asyncpg_features: AsyncpgDriverFeatures = {"tenant_strategy": None}
psycopg_features: PsycopgDriverFeatures = {"tenant_strategy": None}
psqlpy_features: PsqlpyDriverFeatures = {"tenant_strategy": None}
