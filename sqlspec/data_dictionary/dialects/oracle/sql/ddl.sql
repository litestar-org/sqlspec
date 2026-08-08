-- name: dbms_metadata
-- dialect: oracle
SELECT DBMS_METADATA.GET_DDL(:object_type, :object_name, :owner) AS ddl
FROM dual;
