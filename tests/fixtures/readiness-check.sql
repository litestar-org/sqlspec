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
-- name: ddl-readiness-check-01-ddl!
create or replace table database_summary(
    collection_key varchar,
    database_name varchar,
    database_type varchar,
    database_version varchar
  );

create or replace table readiness_check_summary(
    migration_target ENUM (
      'CLOUDSQL',
      'ALLOYDB',
      'BMS',
      'SPANNER',
      'BIGQUERY'
    ),
    severity ENUM ('INFO', 'PASS', 'WARNING', 'ACTION REQUIRED', 'ERROR'),
    rule_code varchar,
    info varchar
  );
