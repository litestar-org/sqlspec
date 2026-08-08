-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    trigger_name,
    trigger_type,
    triggering_event,
    table_owner,
    base_object_type,
    table_name,
    column_name,
    referencing_names,
    when_clause,
    status,
    description,
    action_type,
    trigger_body,
    crossedition,
    before_statement,
    before_row,
    after_row,
    after_statement,
    instead_of_row,
    fire_once,
    apply_server_only
FROM all_triggers
WHERE owner = COALESCE(:schema_name, USER)
  AND (:trigger_name IS NULL OR trigger_name = :trigger_name)
ORDER BY owner, trigger_name;
