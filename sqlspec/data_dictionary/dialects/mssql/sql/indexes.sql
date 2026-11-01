-- name: by_schema
-- dialect: mssql
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
    CAST(i.is_primary_key AS BIT) AS is_primary,
    CAST(i.is_unique_constraint AS BIT) AS is_unique_constraint,
    CAST(i.has_filter AS BIT) AS has_filter,
    i.filter_definition,
    ds.name AS data_space_name,
    ps.name AS partition_scheme_name,
    kc.columns,
    inc.included_columns
FROM sys.indexes AS i
INNER JOIN sys.tables AS t ON i.object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
LEFT JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id
LEFT JOIN sys.partition_schemes AS ps ON ds.data_space_id = ps.data_space_id
LEFT JOIN index_key_columns AS kc ON kc.object_id = i.object_id AND kc.index_id = i.index_id
LEFT JOIN index_included_columns AS inc ON inc.object_id = i.object_id AND inc.index_id = i.index_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
  AND i.name IS NOT NULL
ORDER BY s.name, t.name, i.name;

-- name: by_table
-- dialect: mssql
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
    CAST(i.is_primary_key AS BIT) AS is_primary,
    CAST(i.is_unique_constraint AS BIT) AS is_unique_constraint,
    CAST(i.has_filter AS BIT) AS has_filter,
    i.filter_definition,
    ds.name AS data_space_name,
    ps.name AS partition_scheme_name,
    kc.columns,
    inc.included_columns
FROM sys.indexes AS i
INNER JOIN sys.tables AS t ON i.object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
LEFT JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id
LEFT JOIN sys.partition_schemes AS ps ON ds.data_space_id = ps.data_space_id
LEFT JOIN index_key_columns AS kc ON kc.object_id = i.object_id AND kc.index_id = i.index_id
LEFT JOIN index_included_columns AS inc ON inc.object_id = i.object_id AND inc.index_id = i.index_id
WHERE s.name = :schema_name
  AND t.name = :table_name
  AND i.name IS NOT NULL
ORDER BY i.name;
