-- name: event_by_name
-- dialect: mysql
SELECT 'SHOW CREATE EVENT' AS statement_template;

-- name: function_by_name
-- dialect: mysql
SELECT 'SHOW CREATE FUNCTION' AS statement_template;

-- name: procedure_by_name
-- dialect: mysql
SELECT 'SHOW CREATE PROCEDURE' AS statement_template;

-- name: sequence_by_name
-- dialect: mysql
SELECT 'SHOW CREATE SEQUENCE' AS statement_template;

-- name: table_by_name
-- dialect: mysql
SELECT 'SHOW CREATE TABLE' AS statement_template;

-- name: trigger_by_name
-- dialect: mysql
SELECT 'SHOW CREATE TRIGGER' AS statement_template;

-- name: view_by_name
-- dialect: mysql
SELECT 'SHOW CREATE VIEW' AS statement_template;
