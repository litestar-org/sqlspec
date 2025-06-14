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
-- name: ddl-collection-scripts-02!
create or replace table collection_mysql_config (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    variable_category varchar,
    variable_name varchar,
    variable_value varchar
  );

create or replace table collection_mysql_users (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    user_host varchar,
    user_count numeric
  );

drop view if exists collection_resource_groups;

create or replace table collection_mysql_5_resource_groups (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    resource_group_name varchar,
    resource_group_type varchar,
    resource_group_enabled varchar,
    vcpu_ids varchar,
    thread_priority varchar
  );

create or replace table collection_mysql_base_resource_groups (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    resource_group_name varchar,
    resource_group_type varchar,
    resource_group_enabled varchar,
    vcpu_ids varchar,
    thread_priority varchar
  );

create or replace view collection_resource_groups as
select pkey,
  dma_source_id,
  dma_manual_id,
  resource_group_name,
  resource_group_type,
  resource_group_enabled,
  vcpu_ids,
  thread_priority
from collection_mysql_base_resource_groups
union all
select pkey,
  dma_source_id,
  dma_manual_id,
  resource_group_name,
  resource_group_type,
  resource_group_enabled,
  vcpu_ids,
  thread_priority
from collection_mysql_5_resource_groups;

create or replace table collection_mysql_config (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    variable_category varchar,
    variable_name varchar,
    variable_value varchar
  );

create or replace table collection_mysql_data_types (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    table_catalog varchar,
    table_schema varchar,
    table_name varchar,
    data_type varchar,
    data_type_count numeric
  );

create or replace table collection_mysql_database_details (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    table_schema varchar,
    total_table_count numeric,
    innodb_table_count numeric,
    non_innodb_table_count numeric,
    total_row_count numeric,
    innodb_table_row_count numeric,
    non_innodb_table_row_count numeric,
    total_data_size_bytes numeric,
    innodb_data_size_bytes numeric,
    non_innodb_data_size_bytes numeric,
    total_index_size_bytes numeric,
    innodb_index_size_bytes numeric,
    non_innodb_index_size_bytes numeric,
    total_size_bytes numeric,
    innodb_total_size_bytes numeric,
    non_innodb_total_size_bytes numeric,
    total_index_count numeric,
    innodb_index_count numeric,
    non_innodb_index_count numeric
  );

create or replace table collection_mysql_engines (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    engine_name varchar,
    engine_support varchar,
    engine_transactions varchar,
    engine_xa varchar,
    engine_savepoints varchar,
    engine_comment varchar
  );

create or replace table collection_mysql_plugins (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    plugin_name varchar,
    plugin_version varchar,
    plugin_status varchar,
    plugin_type varchar,
    plugin_type_version varchar,
    plugin_library varchar,
    plugin_library_version varchar,
    plugin_author varchar,
    plugin_description varchar,
    plugin_license varchar,
    load_option varchar
  );

drop view if exists collection_mysql_process_list;

create or replace table collection_mysql_base_process_list (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    process_id numeric,
    process_host varchar,
    process_db varchar,
    process_command varchar,
    process_time numeric,
    process_state varchar
  );

create or replace table collection_mysql_5_process_list (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    process_id numeric,
    process_host varchar,
    process_db varchar,
    process_command varchar,
    process_time numeric,
    process_state varchar
  );

create or replace view collection_mysql_process_list as
select pkey,
  dma_source_id,
  dma_manual_id,
  process_id,
  process_host,
  process_db,
  process_command,
  process_time,
  process_state
from collection_mysql_base_process_list
union all
select pkey,
  dma_source_id,
  dma_manual_id,
  process_id,
  process_host,
  process_db,
  process_command,
  process_time,
  process_state
from collection_mysql_5_process_list;

create or replace table collection_mysql_schema_details (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    table_schema varchar,
    table_name varchar,
    table_engine varchar,
    table_rows numeric,
    data_length numeric,
    index_length numeric,
    is_compressed numeric,
    is_partitioned numeric,
    partition_count numeric,
    index_count numeric,
    fulltext_index_count numeric,
    is_encrypted numeric,
    spatial_index_count numeric,
    has_primary_key numeric,
    row_format varchar,
    table_type varchar
  );

create or replace table collection_mysql_schema_objects (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    object_catalog varchar,
    object_schema varchar,
    object_category varchar,
    object_type varchar,
    object_owner_schema varchar,
    object_owner varchar,
    object_name varchar
  );

create or replace table collection_mysql_table_details (
    pkey varchar,
    dma_source_id varchar,
    dma_manual_id varchar,
    table_schema varchar,
    table_name varchar,
    table_engine varchar,
    table_rows numeric,
    data_length numeric,
    index_length numeric,
    is_compressed numeric,
    is_partitioned numeric,
    partition_count numeric,
    index_count numeric,
    fulltext_index_count numeric,
    is_encrypted numeric,
    spatial_index_count numeric,
    has_primary_key numeric,
    row_format varchar,
    table_type varchar
  );
