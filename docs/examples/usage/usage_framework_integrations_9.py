# start-example
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        extension_config={
            "litestar": {
                "enable_correlation_middleware": True,  # Default: True
                "correlation_header": "x-request-id",
                "correlation_headers": ["x-client-trace"],
                "auto_trace_headers": True,
             }
        }
    )
)
# Queries will include correlation IDs in logs (header or generated UUID)
# Format: [correlation_id=abc123] SELECT * FROM users
# end-example

def test_stub():
    assert True
