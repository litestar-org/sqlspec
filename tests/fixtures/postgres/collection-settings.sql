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
-- name: collection-postgres-settings
with src as (
  select s.category as setting_category,
    s.name as setting_name,
    s.setting as setting_value,
    s.unit as setting_unit,
    s.context as context,
    s.vartype as variable_type,
    s.source as setting_source,
    s.min_val as min_value,
    s.max_val as max_value,
    s.enumvals as enum_values,
    s.boot_val as boot_value,
    s.reset_val as reset_value,
    s.sourcefile as source_file,
    s.pending_restart as pending_restart,
    case
      when s.source not in ('override', 'default') then 1
      else 0
    end as is_default
  from pg_settings s
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  replace(src.setting_category, chr(34), chr(39)) as setting_category,
  replace(src.setting_name, chr(34), chr(39)) as setting_name,
  replace(src.setting_value, chr(34), chr(39)) as setting_value,
  src.setting_unit,
  src.context,
  src.variable_type,
  src.setting_source,
  src.min_value,
  src.max_value,
  replace(src.enum_values::text, chr(34), chr(39)) as enum_values,
  replace(src.boot_value::text, chr(34), chr(39)) as boot_value,
  replace(src.reset_value::text, chr(34), chr(39)) as reset_value,
  src.source_file,
  src.pending_restart,
  src.is_default
from src;
