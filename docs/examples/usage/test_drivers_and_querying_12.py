# Example from docs/usage/drivers_and_querying.rst - code-block 12
from sqlspec.adapters.bigquery import BigQueryConfig
import datetime

config = BigQueryConfig(
    pool_config={
        "project": "my-project",
        "credentials": None,
    }
)

# with spec.provide_session(config) as session:
#     result = session.execute("SELECT DATE(timestamp) as date, COUNT(*) as events FROM `project.dataset.events` WHERE timestamp >= @start_date GROUP BY date", start_date=datetime.date(2025,1,1))

