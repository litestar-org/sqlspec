-- name: by_object
-- dialect: snowflake
SELECT GET_DDL(:object_type, :object_name, TRUE) AS "ddl";
