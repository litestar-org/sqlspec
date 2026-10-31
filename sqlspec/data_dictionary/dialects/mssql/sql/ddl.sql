-- name: index_inputs_by_table
-- dialect: mssql
/* sqlspec:mssql:ddl_index_inputs */
WITH index_key_columns AS (
    SELECT
        ic.object_id,
        ic.index_id,
        STRING_AGG(CAST(c.name AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE ic.is_included_column = 0
    GROUP BY ic.object_id, ic.index_id
),
index_included_columns AS (
    SELECT
        ic.object_id,
        ic.index_id,
        STRING_AGG(CAST(c.name AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY ic.index_column_id) AS included_columns
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE ic.is_included_column = 1
    GROUP BY ic.object_id, ic.index_id
)
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc,
    CAST(i.is_unique AS BIT) AS is_unique,
    CAST(i.has_filter AS BIT) AS has_filter,
    i.filter_definition,
    kc.columns AS columns,
    inc.included_columns AS included_columns
FROM sys.indexes AS i
INNER JOIN sys.tables AS t ON i.object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
LEFT JOIN index_key_columns AS kc ON kc.object_id = i.object_id AND kc.index_id = i.index_id
LEFT JOIN index_included_columns AS inc ON inc.object_id = i.object_id AND inc.index_id = i.index_id
WHERE s.name = :schema_name
  AND t.name = :table_name
  AND i.name IS NOT NULL
  AND i.is_primary_key = 0
  AND i.is_unique_constraint = 0
ORDER BY i.name;

-- name: table_inputs_by_table
-- dialect: mssql
/* sqlspec:mssql:ddl_table_inputs */
WITH primary_key_columns AS (
    SELECT
        pk_col.object_id,
        pk_col.column_id,
        pk.name AS primary_key_name,
        pk_col.key_ordinal AS primary_key_ordinal
    FROM sys.indexes AS pk
    INNER JOIN sys.index_columns AS pk_col
        ON pk.object_id = pk_col.object_id
       AND pk.index_id = pk_col.index_id
    WHERE pk.is_primary_key = 1
      AND pk_col.is_included_column = 0
)
SELECT
    s.name AS schema_name,
    tab.name AS table_name,
    c.name AS column_name,
    typ.name AS data_type,
    c.max_length,
    c.precision AS numeric_precision,
    c.scale AS numeric_scale,
    CAST(c.is_nullable AS BIT) AS is_nullable,
    c.column_id AS ordinal_position,
    OBJECT_DEFINITION(c.default_object_id) AS column_default,
    CAST(CASE WHEN cc.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_computed,
    cc.definition AS computed_definition,
    CAST(CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_identity,
    CONVERT(BIGINT, ic.seed_value) AS identity_seed,
    CONVERT(BIGINT, ic.increment_value) AS identity_increment,
    pk.primary_key_name AS primary_key_name,
    pk.primary_key_ordinal AS primary_key_ordinal
FROM sys.columns AS c
INNER JOIN sys.types AS typ ON c.user_type_id = typ.user_type_id
INNER JOIN sys.tables AS tab ON c.object_id = tab.object_id
INNER JOIN sys.schemas AS s ON tab.schema_id = s.schema_id
LEFT JOIN sys.computed_columns AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
LEFT JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
LEFT JOIN primary_key_columns AS pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
WHERE s.name = :schema_name
  AND tab.name = :table_name
ORDER BY c.column_id;
