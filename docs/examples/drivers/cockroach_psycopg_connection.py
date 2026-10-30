"""Configure a Psycopg connection pool for CockroachDB."""

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgSyncConfig

__all__ = ("main",)


def main() -> CockroachPsycopgSyncConfig:
    """Construct a CockroachPsycopgSyncConfig targeting a CockroachDB cluster."""
    config = CockroachPsycopgSyncConfig(connection_config={"dsn": "postgresql://user:pass@localhost:26257/defaultdb"})
    assert "conninfo" in config.connection_config
    return config


if __name__ == "__main__":
    print(main())
