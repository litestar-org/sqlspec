/*
 Copyright 2024 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
-- name: readiness-check-init-get-db-count$
select count(*) as db_count
from collection_postgres_all_databases;

-- name:  readiness-check-init-get-execution-id$
select 'postgres_' || current_setting('server_version_num') || '_' || to_char(current_timestamp, 'YYYYMMDDHH24MISSMS') as execution_id;

-- name:  readiness-check-init-get-source-id$
select system_identifier::VARCHAR as source_id
from pg_control_system();
