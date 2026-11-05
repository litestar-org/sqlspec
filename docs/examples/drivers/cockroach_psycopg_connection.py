"""Configure a Psycopg connection pool for CockroachDB."""

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgSyncConfig

spec = SQLSpec()
config = spec.add_config(
    CockroachPsycopgSyncConfig(
        connection_config={
            "conninfo": "postgresql://root@localhost:26257/defaultdb",
            "min_size": 2,
            "max_size": 10,
        },
        driver_features={"enable_auto_retry": True, "max_retries": 5},
    )
)
# --8<-- [end:example]

__all__ = ("main",)


def main() -> CockroachPsycopgSyncConfig:
    """Construct a CockroachPsycopgSyncConfig targeting a CockroachDB cluster."""
    assert "conninfo" in config.connection_config
    return config


if __name__ == "__main__":
    print(main())
