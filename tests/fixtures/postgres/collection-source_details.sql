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
-- name: collection-postgres-source-details
with src as (
  select p.oid as object_id,
    n.nspname as schema_name,
    case
      when p.prokind = 'f' then 'FUNCTION'
      when p.prokind = 'p' then 'PROCEDURE'
      when p.prokind = 'a' then 'AGGREGATE_FUNCTION'
      when p.prokind = 'w' then 'WINDOW_FUNCTION'
      else 'UNCATEGORIZED_PROCEDURE'
    end as object_type,
    p.proname as object_name,
    pg_get_function_result(p.oid) as result_data_types,
    pg_get_function_arguments(p.oid) as argument_data_types,
    pg_get_userbyid(p.proowner) as object_owner,
    length(p.prosrc) as number_of_chars,
    (LENGTH(p.prosrc) + 1) - LENGTH(replace(p.prosrc, E'\n', '')) as number_of_lines,
    case
      when p.prosecdef then 'definer'
      else 'invoker'
    end as object_security,
    array_to_string(p.proacl, '') as access_privileges,
    l.lanname as procedure_language,
    case
      when n.nspname <> all (ARRAY ['pg_catalog', 'information_schema']) then false
      else true
    end as system_object
  from pg_proc p
    left join pg_namespace n on n.oid = p.pronamespace
    left join pg_language l on l.oid = p.prolang
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.object_id,
  replace (src.schema_name, chr(34), chr(39)) as schema_name,
  src.object_type,
  replace (src.object_name, chr(34), chr(39)) as object_name,
  replace (src.result_data_types, chr(34), chr(39)) as result_data_types,
  replace (src.argument_data_types, chr(34), chr(39)) as argument_data_types,
  replace (src.object_owner, chr(34), chr(39)) as object_owner,
  src.number_of_chars,
  src.number_of_lines,
  src.object_security,
  src.access_privileges,
  src.procedure_language,
  src.system_object
from src;
