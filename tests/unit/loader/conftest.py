"""Shared SQL-file content for fragment and slot tests."""

import pytest

EFFORT_SQL = """\
-- fragment: decision_fact_ctes
decisions AS (
    SELECT d.id, d.workspace_id, d.strategy
    FROM decision d
    WHERE d.workspace_id = :workspace_id
),
classified AS (
    SELECT id, workspace_id, CASE WHEN strategy = 'lift' THEN 'homogeneous' ELSE 'modernize' END AS journey
    FROM decisions
)

-- name: list_efforts
-- param: workspace_id str
-- slot: predicates = TRUE
-- slot: order_by = e.id
WITH
/* include: decision_fact_ctes */
SELECT e.id, e.journey
FROM classified e
WHERE /* slot: predicates */
ORDER BY /* slot: order_by */

-- name: count_efforts
-- param: workspace_id str
WITH
/* include: decision_fact_ctes */
SELECT count(*) FROM classified e WHERE /* slot: predicates */
"""


@pytest.fixture
def effort_sql() -> str:
    """SQL file text that shares CTEs through a fragment and fills WHERE and ORDER BY slots."""
    return EFFORT_SQL
