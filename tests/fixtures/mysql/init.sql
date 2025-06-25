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
-- name: init-get-db-version$
select if(
    version() rlike '^[0-9]+\.[0-9]+\.[0-9]+$' = 1,
    version(),
    concat(SUBSTRING_INDEX(VERSION(), '.', 2), '.0')
  ) as db_version;

-- name: init-get-execution-id$
select concat(
    'mysql_',
    a.db_version,
    '_',
    DATE_FORMAT(SYSDATE(), '%Y%m%d%H%i%s')
  ) as execution_id
from (
    select if(
        version() rlike '^[0-9]+\.[0-9]+\.[0-9]+$' = 1,
        version(),
        concat(SUBSTRING_INDEX(VERSION(), '.', 2), '.0')
      ) as db_version
  ) a;

-- name: init-get-source-id$
select @@server_uuid as source_id;
