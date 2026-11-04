-- # --8<-- [start:queries]
-- fragment: team_source
WITH team_source AS (
    SELECT id, name
    FROM teams
    WHERE workspace_id = :workspace_id
)

-- name: list_teams
-- slot: predicates = 1 = 1
-- slot: ordering = t.id
/* include: team_source */
SELECT t.id, t.name
FROM team_source AS t
WHERE /* slot: predicates */
ORDER BY /* slot: ordering */;

-- name: count_teams
/* include: team_source */
SELECT COUNT(*)
FROM team_source AS t
WHERE /* slot: predicates */
-- # --8<-- [end:queries]
