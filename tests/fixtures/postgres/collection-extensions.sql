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
-- name: collection-postgres-extensions
with src as (
  select e.oid as extension_id,
    e.extname as extension_name,
    a.rolname as extension_owner,
    a.rolsuper as is_super_user,
    n.nspname as extension_schema,
    e.extrelocatable as is_relocatable,
    e.extversion as extension_version,
    current_database() as database_name
  from pg_extension e
    join pg_roles a on (e.extowner = a.oid)
    join pg_namespace n on (e.extnamespace = n.oid)
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.extension_id,
  src.extension_name,
  src.extension_owner,
  src.extension_schema,
  src.is_relocatable,
  src.extension_version,
  src.database_name,
  src.is_super_user
from src;
