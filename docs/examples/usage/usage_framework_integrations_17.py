# start-example
from flask import Flask

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_stub",)


app = Flask(__name__)

# Initialize SQLSpec
spec = SQLSpec()
db = spec.add_config(SqliteConfig(pool_config={"database": "app.db"}))
# end-example


def test_stub() -> None:
    assert app is not None
