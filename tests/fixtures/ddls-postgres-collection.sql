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
-- name: ddl-collection-scripts-01!
create or replace table collection_postgres_12_database_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    database_oid BIGINT,
    database_name VARCHAR,
    database_version VARCHAR,
    database_version_number VARCHAR,
    max_connection_limit BIGINT,
    is_template_database BOOLEAN,
    character_set_encoding VARCHAR,
    total_disk_size_bytes BIGINT,
    backends_connected BIGINT,
    txn_commit_count BIGINT,
    txn_rollback_count BIGINT,
    blocks_read_count BIGINT,
    blocks_hit_count BIGINT,
    tup_returned_count BIGINT,
    tup_fetched_count BIGINT,
    tup_inserted_count BIGINT,
    tup_updated_count BIGINT,
    tup_deleted_count BIGINT,
    query_conflict_count BIGINT,
    temporary_file_count BIGINT,
    temporary_file_bytes_written BIGINT,
    detected_deadlocks_count BIGINT,
    checksum_failure_count INTEGER,
    last_checksum_failure INTEGER,
    block_read_time_ms DOUBLE,
    block_write_time_ms DOUBLE,
    session_time_ms INTEGER,
    active_time_ms INTEGER,
    idle_in_transaction_time_ms INTEGER,
    sessions_count INTEGER,
    fatal_sessions_count INTEGER,
    killed_sessions_count INTEGER,
    statistics_last_reset_on VARCHAR,
    inet_server_addr VARCHAR,
    database_collation VARCHAR
  );

create or replace table collection_postgres_13_database_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    database_oid BIGINT,
    database_name VARCHAR,
    database_version VARCHAR,
    database_version_number VARCHAR,
    max_connection_limit BIGINT,
    is_template_database BOOLEAN,
    character_set_encoding VARCHAR,
    total_disk_size_bytes BIGINT,
    backends_connected BIGINT,
    txn_commit_count BIGINT,
    txn_rollback_count BIGINT,
    blocks_read_count BIGINT,
    blocks_hit_count BIGINT,
    tup_returned_count BIGINT,
    tup_fetched_count BIGINT,
    tup_inserted_count BIGINT,
    tup_updated_count BIGINT,
    tup_deleted_count BIGINT,
    query_conflict_count BIGINT,
    temporary_file_count BIGINT,
    temporary_file_bytes_written BIGINT,
    detected_deadlocks_count BIGINT,
    checksum_failure_count INTEGER,
    last_checksum_failure INTEGER,
    block_read_time_ms DOUBLE,
    block_write_time_ms DOUBLE,
    session_time_ms INTEGER,
    active_time_ms INTEGER,
    idle_in_transaction_time_ms INTEGER,
    sessions_count INTEGER,
    fatal_sessions_count INTEGER,
    killed_sessions_count INTEGER,
    statistics_last_reset_on VARCHAR,
    inet_server_addr VARCHAR,
    database_collation VARCHAR
  );

create or replace table collection_postgres_base_database_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    database_oid BIGINT,
    database_name VARCHAR,
    database_version VARCHAR,
    database_version_number VARCHAR,
    max_connection_limit BIGINT,
    is_template_database BOOLEAN,
    character_set_encoding VARCHAR,
    total_disk_size_bytes BIGINT,
    backends_connected BIGINT,
    txn_commit_count BIGINT,
    txn_rollback_count BIGINT,
    blocks_read_count BIGINT,
    blocks_hit_count BIGINT,
    tup_returned_count BIGINT,
    tup_fetched_count BIGINT,
    tup_inserted_count BIGINT,
    tup_updated_count BIGINT,
    tup_deleted_count BIGINT,
    query_conflict_count BIGINT,
    temporary_file_count BIGINT,
    temporary_file_bytes_written BIGINT,
    detected_deadlocks_count BIGINT,
    checksum_failure_count INTEGER,
    last_checksum_failure INTEGER,
    block_read_time_ms DOUBLE,
    block_write_time_ms DOUBLE,
    session_time_ms DOUBLE,
    active_time_ms DOUBLE,
    idle_in_transaction_time_ms DOUBLE,
    sessions_count BIGINT,
    fatal_sessions_count BIGINT,
    killed_sessions_count BIGINT,
    statistics_last_reset_on VARCHAR,
    inet_server_addr VARCHAR,
    database_collation VARCHAR
  );

create or replace view collection_postgres_database_details as
select pkey,
  dma_source_id,
  dma_manual_id,
  database_oid,
  database_name,
  database_version,
  database_version_number,
  max_connection_limit,
  is_template_database,
  character_set_encoding,
  total_disk_size_bytes,
  backends_connected,
  txn_commit_count,
  txn_rollback_count,
  blocks_read_count,
  blocks_hit_count,
  tup_returned_count,
  tup_fetched_count,
  tup_inserted_count,
  tup_updated_count,
  tup_deleted_count,
  query_conflict_count,
  temporary_file_count,
  temporary_file_bytes_written,
  detected_deadlocks_count,
  checksum_failure_count,
  last_checksum_failure,
  block_read_time_ms,
  block_write_time_ms,
  session_time_ms,
  active_time_ms,
  idle_in_transaction_time_ms,
  sessions_count,
  fatal_sessions_count,
  killed_sessions_count,
  statistics_last_reset_on,
  inet_server_addr,
  database_collation
from collection_postgres_base_database_details
union all
select pkey,
  dma_source_id,
  dma_manual_id,
  database_oid,
  database_name,
  database_version,
  database_version_number,
  max_connection_limit,
  is_template_database,
  character_set_encoding,
  total_disk_size_bytes,
  backends_connected,
  txn_commit_count,
  txn_rollback_count,
  blocks_read_count,
  blocks_hit_count,
  tup_returned_count,
  tup_fetched_count,
  tup_inserted_count,
  tup_updated_count,
  tup_deleted_count,
  query_conflict_count,
  temporary_file_count,
  temporary_file_bytes_written,
  detected_deadlocks_count,
  checksum_failure_count,
  last_checksum_failure,
  block_read_time_ms,
  block_write_time_ms,
  session_time_ms,
  active_time_ms,
  idle_in_transaction_time_ms,
  sessions_count,
  fatal_sessions_count,
  killed_sessions_count,
  statistics_last_reset_on,
  inet_server_addr,
  database_collation
from collection_postgres_13_database_details
union all
select pkey,
  dma_source_id,
  dma_manual_id,
  database_oid,
  database_name,
  database_version,
  database_version_number,
  max_connection_limit,
  is_template_database,
  character_set_encoding,
  total_disk_size_bytes,
  backends_connected,
  txn_commit_count,
  txn_rollback_count,
  blocks_read_count,
  blocks_hit_count,
  tup_returned_count,
  tup_fetched_count,
  tup_inserted_count,
  tup_updated_count,
  tup_deleted_count,
  query_conflict_count,
  temporary_file_count,
  temporary_file_bytes_written,
  detected_deadlocks_count,
  checksum_failure_count,
  last_checksum_failure,
  block_read_time_ms,
  block_write_time_ms,
  session_time_ms,
  active_time_ms,
  idle_in_transaction_time_ms,
  sessions_count,
  fatal_sessions_count,
  killed_sessions_count,
  statistics_last_reset_on,
  inet_server_addr,
  database_collation
from collection_postgres_12_database_details;

create or replace table collection_postgres_applications(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    application_name VARCHAR,
    application_count BIGINT
  );

create or replace table collection_postgres_aws_oracle_exists(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    sct_oracle_extension_exists BOOLEAN
  );

create or replace table collection_postgres_aws_extension_dependency(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    schema_name VARCHAR,
    object_language VARCHAR,
    object_type VARCHAR,
    object_name VARCHAR,
    aws_extension_dependency VARCHAR,
    sct_function_reference_count BIGINT,
  );

create or replace table collection_postgres_bg_writer_stats(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    checkpoints_timed BIGINT,
    checkpoints_requested BIGINT,
    checkpoint_write_time DOUBLE,
    checkpoint_sync_time DOUBLE,
    buffers_checkpoint BIGINT,
    buffers_clean BIGINT,
    max_written_clean BIGINT,
    buffers_backend BIGINT,
    buffers_backend_fsync BIGINT,
    buffers_allocated BIGINT,
    stats_reset TIMESTAMP
  );

  create or replace table collection_postgres_bg_writer_stats_from_pg17(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    buffers_clean BIGINT,
    max_written_clean BIGINT,
    buffers_allocated BIGINT,
    stats_reset TIMESTAMP
  );

create or replace table collection_postgres_calculated_metrics(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    metric_category VARCHAR,
    metric_name VARCHAR,
    metric_value VARCHAR
  );

create or replace table collection_postgres_extensions(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    extension_id BIGINT,
    extension_name VARCHAR,
    extension_owner VARCHAR,
    extension_schema VARCHAR,
    is_relocatable BOOLEAN,
    extension_version VARCHAR,
    database_name VARCHAR,
    is_super_user BOOLEAN
  );

create or replace table collection_postgres_schema_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    object_schema VARCHAR,
    schema_owner VARCHAR,
    system_object BOOLEAN,
    table_count BIGINT,
    view_count BIGINT,
    function_count BIGINT,
    table_data_size_bytes DECIMAL(38, 0),
    total_table_size_bytes DECIMAL(38, 0),
    database_name VARCHAR
  );

create or replace table collection_postgres_settings(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    setting_category VARCHAR,
    setting_name VARCHAR,
    setting_value VARCHAR,
    setting_unit VARCHAR,
    context VARCHAR,
    variable_type VARCHAR,
    setting_source VARCHAR,
    min_value VARCHAR,
    max_value VARCHAR,
    enum_values VARCHAR,
    boot_value VARCHAR,
    reset_value VARCHAR,
    source_file VARCHAR,
    pending_restart BOOLEAN,
    is_default BOOLEAN
  );

create or replace table collection_postgres_source_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    object_id BIGINT,
    schema_name VARCHAR,
    object_type VARCHAR,
    object_name VARCHAR,
    result_data_types VARCHAR,
    argument_data_types VARCHAR,
    object_owner VARCHAR,
    number_of_chars BIGINT,
    number_of_lines BIGINT,
    object_security VARCHAR,
    access_privileges VARCHAR,
    procedure_language VARCHAR,
    system_object BOOLEAN
  );

create or replace table collection_postgres_data_types(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    table_schema VARCHAR,
    table_type VARCHAR,
    table_name VARCHAR,
    data_type VARCHAR,
    data_type_count BIGINT
  );

create or replace table collection_postgres_index_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    object_id VARCHAR,
    table_name VARCHAR,
    table_owner VARCHAR,
    index_name VARCHAR,
    index_owner VARCHAR,
    table_object_id VARCHAR,
    indexed_column_count BIGINT,
    indexed_keyed_column_count BIGINT,
    is_unique BOOLEAN,
    is_primary BOOLEAN,
    is_exclusion BOOLEAN,
    is_immediate BOOLEAN,
    is_clustered BOOLEAN,
    is_valid BOOLEAN,
    is_check_xmin BOOLEAN,
    is_ready BOOLEAN,
    is_live BOOLEAN,
    is_replica_identity BOOLEAN,
    index_block_read BIGINT,
    index_blocks_hit BIGINT,
    index_scan BIGINT,
    index_tuples_read BIGINT,
    index_tuples_fetched BIGINT
  );

create or replace table collection_postgres_base_replication_slots(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    slot_name VARCHAR,
    plugin VARCHAR,
    slot_type VARCHAR,
    datoid VARCHAR,
    database VARCHAR,
    temporary VARCHAR,
    active VARCHAR,
    active_pid VARCHAR,
    xmin VARCHAR,
    catalog_xmin VARCHAR,
    restart_lsn VARCHAR,
    confirmed_flush_lsn VARCHAR,
    wal_status VARCHAR,
    safe_wal_size VARCHAR,
    two_phase VARCHAR
  );

create or replace table collection_postgres_12_replication_slots(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    slot_name VARCHAR,
    plugin VARCHAR,
    slot_type VARCHAR,
    datoid VARCHAR,
    database VARCHAR,
    temporary VARCHAR,
    active VARCHAR,
    active_pid VARCHAR,
    xmin VARCHAR,
    catalog_xmin VARCHAR,
    restart_lsn VARCHAR,
    confirmed_flush_lsn VARCHAR,
    wal_status VARCHAR,
    safe_wal_size VARCHAR,
    two_phase VARCHAR
  );

create or replace table collection_postgres_13_replication_slots(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    slot_name VARCHAR,
    plugin VARCHAR,
    slot_type VARCHAR,
    datoid VARCHAR,
    database VARCHAR,
    temporary VARCHAR,
    active VARCHAR,
    active_pid VARCHAR,
    xmin VARCHAR,
    catalog_xmin VARCHAR,
    restart_lsn VARCHAR,
    confirmed_flush_lsn VARCHAR,
    wal_status VARCHAR,
    safe_wal_size VARCHAR,
    two_phase VARCHAR
  );

create or replace view collection_postgres_replication_slots as
select pkey,
  dma_source_id,
  dma_manual_id,
  slot_name,
  plugin,
  slot_type,
  datoid,
  database,
  temporary,
  active,
  active_pid,
  xmin,
  catalog_xmin,
  restart_lsn,
  confirmed_flush_lsn,
  wal_status,
  safe_wal_size,
  two_phase
from collection_postgres_base_replication_slots
union all
select pkey,
  dma_source_id,
  dma_manual_id,
  slot_name,
  plugin,
  slot_type,
  datoid,
  database,
  temporary,
  active,
  active_pid,
  xmin,
  catalog_xmin,
  restart_lsn,
  confirmed_flush_lsn,
  wal_status,
  safe_wal_size,
  two_phase
from collection_postgres_13_replication_slots
union all
select pkey,
  dma_source_id,
  dma_manual_id,
  slot_name,
  plugin,
  slot_type,
  datoid,
  database,
  temporary,
  active,
  active_pid,
  xmin,
  catalog_xmin,
  restart_lsn,
  confirmed_flush_lsn,
  wal_status,
  safe_wal_size,
  two_phase
from collection_postgres_12_replication_slots;

create or replace table collection_postgres_replication_stats(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    pid VARCHAR,
    usesysid VARCHAR,
    usename VARCHAR,
    application_name VARCHAR,
    client_addr VARCHAR,
    client_hostname VARCHAR,
    client_port VARCHAR,
    backend_start VARCHAR,
    backend_xmin VARCHAR,
    state VARCHAR,
    sent_lsn VARCHAR,
    write_lsn VARCHAR,
    flush_lsn VARCHAR,
    replay_lsn VARCHAR,
    write_lag VARCHAR,
    flush_lag VARCHAR,
    replay_lag VARCHAR,
    sync_priority VARCHAR,
    sync_state VARCHAR,
    reply_time VARCHAR
  );

create or replace table collection_postgres_schema_objects(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    object_owner VARCHAR,
    object_category VARCHAR,
    object_type VARCHAR,
    object_schema VARCHAR,
    object_name VARCHAR,
    object_id VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_base_table_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    object_id VARCHAR,
    table_schema VARCHAR,
    table_type VARCHAR,
    table_name VARCHAR,
    total_object_size_bytes DECIMAL(38, 0),
    object_size_bytes DECIMAL(38, 0),
    sequence_scan VARCHAR,
    live_tuples BIGINT,
    dead_tuples BIGINT,
    modifications_since_last_analyzed BIGINT,
    last_analyzed VARCHAR,
    last_autoanalyzed VARCHAR,
    last_autovacuumed VARCHAR,
    last_vacuumed VARCHAR,
    vacuum_count BIGINT,
    analyze_count BIGINT,
    autoanalyze_count BIGINT,
    autovacuum_count BIGINT,
    foreign_server_name VARCHAR,
    foreign_data_wrapper_name VARCHAR,
    heap_blocks_hit BIGINT,
    heap_blocks_read BIGINT,
    index_blocks_hit BIGINT,
    index_blocks_read BIGINT,
    toast_blocks_hit BIGINT,
    toast_blocks_read BIGINT,
    toast_index_hit BIGINT,
    toast_index_read BIGINT,
    database_name VARCHAR
  );

create or replace table collection_postgres_12_table_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    object_id VARCHAR,
    table_schema VARCHAR,
    table_type VARCHAR,
    table_name VARCHAR,
    total_object_size_bytes DECIMAL(38, 0),
    object_size_bytes DECIMAL(38, 0),
    sequence_scan VARCHAR,
    live_tuples BIGINT,
    dead_tuples BIGINT,
    modifications_since_last_analyzed BIGINT,
    last_analyzed VARCHAR,
    last_autoanalyzed VARCHAR,
    last_autovacuumed VARCHAR,
    last_vacuumed VARCHAR,
    vacuum_count BIGINT,
    analyze_count BIGINT,
    autoanalyze_count BIGINT,
    autovacuum_count BIGINT,
    foreign_server_name VARCHAR,
    foreign_data_wrapper_name VARCHAR,
    heap_blocks_hit BIGINT,
    heap_blocks_read BIGINT,
    index_blocks_hit BIGINT,
    index_blocks_read BIGINT,
    toast_blocks_hit BIGINT,
    toast_blocks_read BIGINT,
    toast_index_hit BIGINT,
    toast_index_read BIGINT,
    database_name VARCHAR
  );

create or replace table collection_postgres_13_table_details(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    object_id VARCHAR,
    table_schema VARCHAR,
    table_type VARCHAR,
    table_name VARCHAR,
    total_object_size_bytes DECIMAL(38, 0),
    object_size_bytes DECIMAL(38, 0),
    sequence_scan VARCHAR,
    live_tuples BIGINT,
    dead_tuples BIGINT,
    modifications_since_last_analyzed BIGINT,
    last_analyzed VARCHAR,
    last_autoanalyzed VARCHAR,
    last_autovacuumed VARCHAR,
    last_vacuumed VARCHAR,
    vacuum_count BIGINT,
    analyze_count BIGINT,
    autoanalyze_count BIGINT,
    autovacuum_count BIGINT,
    foreign_server_name VARCHAR,
    foreign_data_wrapper_name VARCHAR,
    heap_blocks_hit BIGINT,
    heap_blocks_read BIGINT,
    index_blocks_hit BIGINT,
    index_blocks_read BIGINT,
    toast_blocks_hit BIGINT,
    toast_blocks_read BIGINT,
    toast_index_hit BIGINT,
    toast_index_read BIGINT,
    database_name VARCHAR
  );

create or replace view collection_postgres_table_details as
select pkey,
  dma_source_id,
  dma_manual_id,
  object_id,
  table_schema,
  table_type,
  table_name,
  total_object_size_bytes,
  object_size_bytes,
  sequence_scan,
  live_tuples,
  dead_tuples,
  modifications_since_last_analyzed,
  last_analyzed,
  last_autoanalyzed,
  last_autovacuumed,
  last_vacuumed,
  vacuum_count,
  analyze_count,
  autoanalyze_count,
  autovacuum_count,
  foreign_server_name,
  foreign_data_wrapper_name,
  heap_blocks_hit,
  heap_blocks_read,
  index_blocks_hit,
  index_blocks_read,
  toast_blocks_hit,
  toast_blocks_read,
  toast_index_hit,
  toast_index_read,
  database_name
from collection_postgres_13_table_details
union all
select pkey,
  dma_source_id,
  dma_manual_id,
  object_id,
  table_schema,
  table_type,
  table_name,
  total_object_size_bytes,
  object_size_bytes,
  sequence_scan,
  live_tuples,
  dead_tuples,
  modifications_since_last_analyzed,
  last_analyzed,
  last_autoanalyzed,
  last_autovacuumed,
  last_vacuumed,
  vacuum_count,
  analyze_count,
  autoanalyze_count,
  autovacuum_count,
  foreign_server_name,
  foreign_data_wrapper_name,
  heap_blocks_hit,
  heap_blocks_read,
  index_blocks_hit,
  index_blocks_read,
  toast_blocks_hit,
  toast_blocks_read,
  toast_index_hit,
  toast_index_read,
  database_name
from collection_postgres_base_table_details
union all
select pkey,
  dma_source_id,
  dma_manual_id,
  object_id,
  table_schema,
  table_type,
  table_name,
  total_object_size_bytes,
  object_size_bytes,
  sequence_scan,
  live_tuples,
  dead_tuples,
  modifications_since_last_analyzed,
  last_analyzed,
  last_autoanalyzed,
  last_autovacuumed,
  last_vacuumed,
  vacuum_count,
  analyze_count,
  autoanalyze_count,
  autovacuum_count,
  foreign_server_name,
  foreign_data_wrapper_name,
  heap_blocks_hit,
  heap_blocks_read,
  index_blocks_hit,
  index_blocks_read,
  toast_blocks_hit,
  toast_blocks_read,
  toast_index_hit,
  toast_index_read,
  database_name
from collection_postgres_12_table_details;

create or replace table extended_collection_postgres_all_databases(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_pglogical_privileges(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    has_tables_select_privilege BOOLEAN,
    has_local_node_select_privilege BOOLEAN,
    has_node_select_privilege BOOLEAN,
    has_node_interface_select_privilege BOOLEAN,
    database_name VARCHAR
  );

create or replace table collection_postgres_pglogical_schema_usage_privilege(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    has_schema_usage_privilege BOOLEAN,
    database_name VARCHAR
  );

create or replace table collection_postgres_user_schemas_without_privilege(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    namespace_name VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_user_tables_without_privilege(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    schema_name VARCHAR,
    table_name VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_user_views_without_privilege(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    schema_name VARCHAR,
    view_name VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_user_sequences_without_privilege(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    namespace_name VARCHAR,
    rel_name VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_db_machine_specs(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    machine_name VARCHAR,
    physical_cpu_count NUMERIC,
    logical_cpu_count NUMERIC,
    total_os_memory_mb NUMERIC,
    total_size_bytes NUMERIC,
    used_size_bytes NUMERIC,
    primary_mac VARCHAR,
    ip_addresses VARCHAR
  );

create or replace table collection_postgres_tables_with_no_primary_key(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    nspname VARCHAR,
    relname VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_tables_with_primary_key_replica_identity(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    nspname VARCHAR,
    relname VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_replication_role(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    rolname VARCHAR,
    rolreplication VARCHAR,
    database_name VARCHAR
  );

create or replace table collection_postgres_pglogical_provider_node(
    pkey VARCHAR,
    dma_source_id VARCHAR,
    dma_manual_id VARCHAR,
    node_id VARCHAR,
    node_name VARCHAR,
    database_name VARCHAR
);
