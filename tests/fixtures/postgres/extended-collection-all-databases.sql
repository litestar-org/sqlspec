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
-- name: extended-collection-postgres-all-databases
with src as (
  select datname
  from pg_catalog.pg_database
  where datname not in (
      'template0',
      'template1',
      'rdsadmin',
      'cloudsqladmin',
      'alloydbadmin',
      'alloydbmetadata',
      'azure_maintenance',
      'azure_sys'
    )
    and not datistemplate
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.datname as database_name
from src;
