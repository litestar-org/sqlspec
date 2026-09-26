-- name: by_schema
-- dialect: db2
SELECT schema_name, object_name, object_type, created, remarks
FROM (
    SELECT
        RTRIM(t.TABSCHEMA) AS schema_name,
        RTRIM(t.TABNAME) AS object_name,
        CASE t.TYPE
            WHEN 'T' THEN 'TABLE'
            WHEN 'V' THEN 'VIEW'
            WHEN 'A' THEN 'ALIAS'
            WHEN 'S' THEN 'MATERIALIZED QUERY TABLE'
            WHEN 'N' THEN 'NICKNAME'
            WHEN 'G' THEN 'CREATED TEMPORARY TABLE'
            ELSE t.TYPE
        END AS object_type,
        t.CREATE_TIME AS created,
        t.REMARKS AS remarks
    FROM SYSCAT.TABLES t
    WHERE t.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:object_name AS VARCHAR(128)) IS NULL OR t.TABNAME = :object_name)

    UNION ALL

    SELECT
        RTRIM(s.SEQSCHEMA) AS schema_name,
        RTRIM(s.SEQNAME) AS object_name,
        'SEQUENCE' AS object_type,
        s.CREATE_TIME AS created,
        s.REMARKS AS remarks
    FROM SYSCAT.SEQUENCES s
    WHERE s.SEQTYPE = 'S'
      AND s.SEQSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:object_name AS VARCHAR(128)) IS NULL OR s.SEQNAME = :object_name)

    UNION ALL

    SELECT
        RTRIM(r.ROUTINESCHEMA) AS schema_name,
        RTRIM(r.ROUTINENAME) AS object_name,
        CASE r.ROUTINETYPE
            WHEN 'F' THEN 'FUNCTION'
            WHEN 'P' THEN 'PROCEDURE'
            WHEN 'M' THEN 'METHOD'
            ELSE r.ROUTINETYPE
        END AS object_type,
        r.CREATE_TIME AS created,
        r.REMARKS AS remarks
    FROM SYSCAT.ROUTINES r
    WHERE r.ROUTINESCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:object_name AS VARCHAR(128)) IS NULL OR r.ROUTINENAME = :object_name)
) objects
ORDER BY schema_name, object_type, object_name;
