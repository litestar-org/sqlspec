# start-example
from flask import Flask, g
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

app = Flask(__name__)

# Initialize SQLSpec
spec = SQLSpec()
db = spec.add_config(SqliteConfig(pool_config={"database": "app.db"}))
# end-example

def test_stub():
    assert app is not None
