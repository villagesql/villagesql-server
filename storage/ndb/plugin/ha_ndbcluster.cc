/* Copyright (c) 2004, 2024, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is designed to work with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have either included with
  the program or referenced in the documentation.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file

  @brief
  This file defines the NDB Cluster handler: the interface between
  MySQL and NDB Cluster
*/

#include "storage/ndb/plugin/ha_ndbcluster.h"

#include <algorithm>  // std::min(),std::max()
#include <memory>
#include <sstream>
#include <string>

#include "my_config.h"  // WORDS_BIGENDIAN
#include "my_dbug.h"
#include "mysql/psi/mysql_thread.h"
#include "mysql/strings/m_ctype.h"
#include "nulls.h"
#include "sql/current_thd.h"
#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/derror.h"      // ER_THD
#include "sql/filesort.h"
#include "sql/join_optimizer/walk_access_paths.h"
#include "sql/mysqld.h"  // global_system_variables table_alias_charset ...
#include "sql/partition_info.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"  // QEP_TAB
#include "sql/sql_lex.h"
#include "sql/sql_plugin_var.h"  // SYS_VAR
#include "sql/transaction.h"
#ifndef NDEBUG
#include "sql/sql_test.h"  // print_where
#endif
#include "sql/strfunc.h"
#include "storage/ndb/include/ndb_global.h"
#include "storage/ndb/include/ndb_version.h"
#include "storage/ndb/include/ndbapi/NdbApi.hpp"
#include "storage/ndb/include/util/SparseBitmask.hpp"
#include "storage/ndb/plugin/ha_ndb_index_stat.h"
#include "storage/ndb/plugin/ha_ndbcluster_binlog.h"
#include "storage/ndb/plugin/ha_ndbcluster_cond.h"
#include "storage/ndb/plugin/ha_ndbcluster_connection.h"
#include "storage/ndb/plugin/ha_ndbcluster_push.h"
#include "storage/ndb/plugin/ndb_anyvalue.h"
#include "storage/ndb/plugin/ndb_applier.h"
#include "storage/ndb/plugin/ndb_binlog_client.h"
#include "storage/ndb/plugin/ndb_binlog_extra_row_info.h"
#include "storage/ndb/plugin/ndb_binlog_thread.h"
#include "storage/ndb/plugin/ndb_bitmap.h"
#include "storage/ndb/plugin/ndb_conflict.h"
#include "storage/ndb/plugin/ndb_conflict_trans.h"  // DependencyTracker
#include "storage/ndb/plugin/ndb_create_helper.h"
#include "storage/ndb/plugin/ndb_dd.h"
#include "storage/ndb/plugin/ndb_dd_client.h"
#include "storage/ndb/plugin/ndb_dd_disk_data.h"
#include "storage/ndb/plugin/ndb_dd_table.h"
#include "storage/ndb/plugin/ndb_ddl_definitions.h"
#include "storage/ndb/plugin/ndb_ddl_transaction_ctx.h"
#include "storage/ndb/plugin/ndb_dist_priv_util.h"
#include "storage/ndb/plugin/ndb_dummy_ts.h"
#include "storage/ndb/plugin/ndb_event_data.h"
#include "storage/ndb/plugin/ndb_fk_util.h"
#include "storage/ndb/plugin/ndb_global_schema_lock.h"
#include "storage/ndb/plugin/ndb_local_connection.h"
#include "storage/ndb/plugin/ndb_log.h"
#include "storage/ndb/plugin/ndb_metadata.h"
#include "storage/ndb/plugin/ndb_metadata_change_monitor.h"
#include "storage/ndb/plugin/ndb_metadata_sync.h"
#include "storage/ndb/plugin/ndb_modifiers.h"
#include "storage/ndb/plugin/ndb_mysql_services.h"
#include "storage/ndb/plugin/ndb_name_util.h"
#include "storage/ndb/plugin/ndb_ndbapi_errors.h"
#include "storage/ndb/plugin/ndb_pfs_init.h"
#include "storage/ndb/plugin/ndb_replica.h"
#include "storage/ndb/plugin/ndb_require.h"
#include "storage/ndb/plugin/ndb_schema_dist.h"
#include "storage/ndb/plugin/ndb_schema_trans_guard.h"
#include "storage/ndb/plugin/ndb_server_hooks.h"
#include "storage/ndb/plugin/ndb_sleep.h"
#include "storage/ndb/plugin/ndb_table_guard.h"
#include "storage/ndb/plugin/ndb_table_stats.h"
#include "storage/ndb/plugin/ndb_tdc.h"
#include "storage/ndb/plugin/ndb_thd.h"
#include "storage/ndb/src/common/util/parse_mask.hpp"
#include "storage/ndb/src/ndbapi/NdbQueryBuilder.hpp"
#include "storage/ndb/src/ndbapi/NdbQueryOperation.hpp"
#include "string_with_len.h"
#include "strxnmov.h"
#include "template_utils.h"

typedef NdbDictionary::Column NDBCOL;
typedef NdbDictionary::Table NDBTAB;
typedef NdbDictionary::Dictionary NDBDICT;

// ndb interface initialization/cleanup
extern "C" void ndb_init_internal(Uint32);
extern "C" void ndb_end_internal(Uint32);

static const int DEFAULT_PARALLELISM = 0;
static const ha_rows DEFAULT_AUTO_PREFETCH = 32;
static const ulong ONE_YEAR_IN_SECONDS = (ulong)3600L * 24L * 365L;

static constexpr unsigned DEFAULT_REPLICA_BATCH_SIZE = 2UL * 1024 * 1024;
static constexpr unsigned MAX_BLOB_ROW_SIZE = 14000;
static constexpr unsigned DEFAULT_MAX_BLOB_PART_SIZE =
    MAX_BLOB_ROW_SIZE - 4 * 13;

ulong opt_ndb_extra_logging;
static ulong opt_ndb_wait_connected;
static ulong opt_ndb_wait_setup;
static ulong opt_ndb_replica_batch_size;
static uint opt_ndb_replica_blob_write_batch_bytes;
static uint opt_ndb_cluster_connection_pool;
static char *opt_connection_pool_nodeids_str;
static uint opt_ndb_recv_thread_activation_threshold;
static char *opt_ndb_recv_thread_cpu_mask;
static char *opt_ndb_index_stat_option;
static char *opt_ndb_connectstring;
static uint opt_ndb_nodeid;
static bool opt_ndb_read_backup;
static ulong opt_ndb_data_node_neighbour;
static bool opt_ndb_fully_replicated;
static ulong opt_ndb_row_checksum;

char *opt_ndb_tls_search_path;
ulong opt_ndb_mgm_tls_level;

// The version where ndbcluster uses DYNAMIC by default when creating columns
static const ulong NDB_VERSION_DYNAMIC_IS_DEFAULT = 50711;
enum ndb_default_colum_format_enum {
  NDB_DEFAULT_COLUMN_FORMAT_FIXED = 0,
  NDB_DEFAULT_COLUMN_FORMAT_DYNAMIC = 1
};
static const char *default_column_format_names[] = {"FIXED", "DYNAMIC", NullS};
static ulong opt_ndb_default_column_format;
static TYPELIB default_column_format_typelib = {
    array_elements(default_column_format_names) - 1, "",
    default_column_format_names, nullptr};
static MYSQL_SYSVAR_ENUM(
    default_column_format,         /* name */
    opt_ndb_default_column_format, /* var */
    PLUGIN_VAR_RQCMDARG,
    "Change COLUMN_FORMAT default value (fixed or dynamic) "
    "for backward compatibility. Also affects the default value "
    "of ROW_FORMAT.",
    nullptr,                         /* check func. */
    nullptr,                         /* update func. */
    NDB_DEFAULT_COLUMN_FORMAT_FIXED, /* default */
    &default_column_format_typelib   /* typelib */
);

static MYSQL_THDVAR_UINT(
    autoincrement_prefetch_sz, /* name */
    PLUGIN_VAR_RQCMDARG,
    "Specify number of autoincrement values that are prefetched.",
    nullptr, /* check func. */
    nullptr, /* update func. */
    512,     /* default */
    1,       /* min */
    65535,   /* max */
    0        /* block */
);

static MYSQL_THDVAR_BOOL(
    force_send, /* name */
    PLUGIN_VAR_OPCMDARG,
    "Force send of buffers to ndb immediately without waiting for "
    "other threads.",
    nullptr, /* check func. */
    nullptr, /* update func. */
    1        /* default */
);

static MYSQL_THDVAR_BOOL(
    use_exact_count, /* name */
    PLUGIN_VAR_OPCMDARG,
    "Use exact records count during query planning and for fast "
    "select count(*), disable for faster queries.",
    nullptr, /* check func. */
    nullptr, /* update func. */
    0        /* default */
);

static MYSQL_THDVAR_BOOL(
    use_transactions, /* name */
    PLUGIN_VAR_OPCMDARG,
    "Use transactions for large inserts, if enabled then large "
    "inserts will be split into several smaller transactions",
    nullptr, /* check func. */
    nullptr, /* update func. */
    1        /* default */
);

static MYSQL_THDVAR_BOOL(
    use_copying_alter_table, /* name */
    PLUGIN_VAR_OPCMDARG,
    "Force ndbcluster to always copy tables at alter table (should "
    "only be used if online alter table fails).",
    nullptr, /* check func. */
    nullptr, /* update func. */
    0        /* default */
);

static MYSQL_THDVAR_BOOL(
    allow_copying_alter_table, /* name */
    PLUGIN_VAR_OPCMDARG,
    "Specifies if implicit copying alter table is allowed. Can be overridden "
    "by using ALGORITHM=COPY in the alter table command.",
    nullptr, /* check func. */
    nullptr, /* update func. */
    1        /* default */
);

/**
   @brief Determine if copying alter table is allowed for current query

   @param thd Pointer to current THD
   @return true if allowed
 */
static bool is_copying_alter_table_allowed(THD *thd) {
  if (THDVAR(thd, allow_copying_alter_table)) {
    //  Copying alter table is allowed
    return true;
  }
  if (thd->lex->alter_info->requested_algorithm ==
      Alter_info::ALTER_TABLE_ALGORITHM_COPY) {
    // User have specified ALGORITHM=COPY, thus overriding the fact that
    // --ndb-allow-copying-alter-table is OFF
    return true;
  }
  return false;
}

static MYSQL_THDVAR_UINT(optimized_node_selection, /* name */
                         PLUGIN_VAR_OPCMDARG,
                         "Select nodes for transactions in a more optimal way.",
                         nullptr, /* check func. */
                         nullptr, /* update func. */
                         3,       /* default */
                         0,       /* min */
                         3,       /* max */
                         0        /* block */
);

static MYSQL_THDVAR_ULONG(batch_size, /* name */
                          PLUGIN_VAR_RQCMDARG, "Batch size in bytes.",
                          nullptr,                  /* check func. */
                          nullptr,                  /* update func. */
                          32768,                    /* default */
                          0,                        /* min */
                          2UL * 1024 * 1024 * 1024, /* max */
                          0                         /* block */
);

static MYSQL_THDVAR_ULONG(
    optimization_delay, /* name */
    PLUGIN_VAR_RQCMDARG,
    "For optimize table, specifies the delay in milliseconds "
    "for each batch of rows sent.",
    nullptr, /* check func. */
    nullptr, /* update func. */
    10,      /* default */
    0,       /* min */
    100000,  /* max */
    0        /* block */
);

static MYSQL_THDVAR_BOOL(index_stat_enable, /* name */
                         PLUGIN_VAR_OPCMDARG,
                         "Use ndb index statistics in query optimization.",
                         nullptr, /* check func. */
                         nullptr, /* update func. */
                         true     /* default */
);

static MYSQL_THDVAR_BOOL(table_no_logging,                 /* name */
                         PLUGIN_VAR_NOCMDARG, "", nullptr, /* check func. */
                         nullptr,                          /* update func. */
                         false                             /* default */
);

static MYSQL_THDVAR_BOOL(table_temporary,                  /* name */
                         PLUGIN_VAR_NOCMDARG, "", nullptr, /* check func. */
                         nullptr,                          /* update func. */
                         false                             /* default */
);

static MYSQL_THDVAR_UINT(blob_read_batch_bytes, /* name */
                         PLUGIN_VAR_RQCMDARG,
                         "Specifies the bytesize large Blob reads "
                         "should be batched into.  0 == No limit.",
                         nullptr,  /* check func */
                         nullptr,  /* update func */
                         65536,    /* default */
                         0,        /* min */
                         UINT_MAX, /* max */
                         0         /* block */
);

static MYSQL_THDVAR_UINT(blob_write_batch_bytes, /* name */
                         PLUGIN_VAR_RQCMDARG,
                         "Specifies the bytesize large Blob writes "
                         "should be batched into.  0 == No limit.",
                         nullptr,  /* check func */
                         nullptr,  /* update func */
                         65536,    /* default */
                         0,        /* min */
                         UINT_MAX, /* max */
                         0         /* block */
);

static MYSQL_THDVAR_UINT(
    deferred_constraints, /* name */
    PLUGIN_VAR_RQCMDARG,
    "Specified that constraints should be checked deferred (when supported)",
    nullptr, /* check func */
    nullptr, /* update func */
    0,       /* default */
    0,       /* min */
    1,       /* max */
    0        /* block */
);

static MYSQL_THDVAR_BOOL(
    show_foreign_key_mock_tables, /* name */
    PLUGIN_VAR_OPCMDARG,
    "Show the mock tables which is used to support foreign_key_checks= 0. "
    "Extra info warnings are shown when creating and dropping the tables. "
    "The real table name is show in SHOW CREATE TABLE",
    nullptr, /* check func. */
    nullptr, /* update func. */
    0        /* default */
);

static MYSQL_THDVAR_BOOL(join_pushdown, /* name */
                         PLUGIN_VAR_OPCMDARG,
                         "Enable pushing down of join to datanodes",
                         nullptr, /* check func. */
                         nullptr, /* update func. */
                         true     /* default */
);

static MYSQL_THDVAR_BOOL(log_exclusive_reads, /* name */
                         PLUGIN_VAR_OPCMDARG,
                         "Log primary key reads with exclusive locks "
                         "to allow conflict resolution based on read conflicts",
                         nullptr, /* check func. */
                         nullptr, /* update func. */
                         0        /* default */
);

/*
  Required in index_stat.cc but available only from here
  thanks to use of top level anonymous structs.
*/
bool ndb_index_stat_get_enable(THD *thd) {
  const bool value = THDVAR(thd, index_stat_enable);
  return value;
}

bool ndb_show_foreign_key_mock_tables(THD *thd) {
  const bool value = THDVAR(thd, show_foreign_key_mock_tables);
  return value;
}

int ndbcluster_push_to_engine(THD *, AccessPath *, JOIN *);

static bool inplace_ndb_column_comment_changed(std::string_view old_comment,
                                               std::string_view new_comment,
                                               const char **reason);

static int ndbcluster_end(handlerton *, ha_panic_function);
static bool ndbcluster_show_status(handlerton *, THD *, stat_print_fn *,
                                   enum ha_stat_type);

static int ndbcluster_alter_tablespace(handlerton *, THD *thd,
                                       st_alter_tablespace *info,
                                       const dd::Tablespace *,
                                       dd::Tablespace *);
static bool ndbcluster_get_tablespace_statistics(
    const char *tablespace_name, const char *file_name,
    const dd::Properties &ts_se_private_data, ha_tablespace_statistics *stats);
static void ndbcluster_pre_dd_shutdown(handlerton *);

static handler *ndbcluster_create_handler(handlerton *hton, TABLE_SHARE *table,
                                          bool /* partitioned */,
                                          MEM_ROOT *mem_root) {
  return new (mem_root) ha_ndbcluster(hton, table);
}

static uint ndbcluster_partition_flags() {
  return (HA_CAN_UPDATE_PARTITION_KEY | HA_CAN_PARTITION_UNIQUE |
          HA_USE_AUTO_PARTITION);
}

uint ha_ndbcluster::alter_flags(uint flags) const {
  const uint f = HA_PARTITION_FUNCTION_SUPPORTED | 0;

  if (flags & Alter_info::ALTER_DROP_PARTITION) return 0;

  return f;
}

static constexpr uint NDB_AUTO_INCREMENT_RETRIES = 100;

#define ERR_PRINT(err) \
  DBUG_PRINT("error", ("%d  message: %s", err.code, err.message))

#define ERR_RETURN(err)              \
  {                                  \
    const NdbError &tmp = err;       \
    return ndb_to_mysql_error(&tmp); \
  }

#define ERR_SET(err, code)           \
  {                                  \
    const NdbError &tmp = err;       \
    code = ndb_to_mysql_error(&tmp); \
  }

static int ndbcluster_inited = 0;

extern Ndb *g_ndb;
extern Ndb_cluster_connection *g_ndb_cluster_connection;

static const char *ndbcluster_hton_name = "ndbcluster";
static const int ndbcluster_hton_name_length = sizeof(ndbcluster_hton_name) - 1;

static ulong multi_range_fixed_size(int num_ranges);

static ulong multi_range_max_entry(NDB_INDEX_TYPE keytype, ulong reclength);

struct st_ndb_status {
  st_ndb_status() { memset(this, 0, sizeof(struct st_ndb_status)); }
  long cluster_node_id;
  const char *connected_host;
  long connected_port;
  long config_generation;
  long number_of_data_nodes;
  long number_of_ready_data_nodes;
  long connect_count;
  long execute_count;
  long trans_hint_count;
  long scan_count;
  long pruned_scan_count;
  long schema_locks_count;
  long sorted_scan_count;
  long pushed_queries_defined;
  long pushed_queries_dropped;
  long pushed_queries_executed;
  long pushed_reads;
  long long last_commit_epoch_server;
  long long last_commit_epoch_session;
  long long api_client_stats[Ndb::NumClientStatistics];
  const char *system_name;
  long fetch_table_stats;
};

/* Status variables shown with 'show status like 'Ndb%' */
static st_ndb_status g_ndb_status;

static long long g_server_api_client_stats[Ndb::NumClientStatistics];

static int update_status_variables(Thd_ndb *thd_ndb, st_ndb_status *ns,
                                   Ndb_cluster_connection *c) {
  ns->connected_port = c->get_connected_port();
  ns->connected_host = c->get_connected_host();
  if (ns->cluster_node_id != (int)c->node_id()) {
    ns->cluster_node_id = c->node_id();
    if (&g_ndb_status == ns && g_ndb_cluster_connection == c)
      ndb_log_info("NodeID is %lu, management server '%s:%lu'",
                   ns->cluster_node_id, ns->connected_host, ns->connected_port);
  }
  {
    int n = c->get_no_ready();
    ns->number_of_ready_data_nodes = n > 0 ? n : 0;
  }
  ns->config_generation = c->get_config_generation();
  ns->number_of_data_nodes = c->no_db_nodes();
  ns->connect_count = c->get_connect_count();
  ns->system_name = c->get_system_name();
  ns->last_commit_epoch_server = ndb_get_latest_trans_gci();
  if (thd_ndb) {
    ns->execute_count = thd_ndb->m_execute_count;
    ns->trans_hint_count = thd_ndb->hinted_trans_count();
    ns->scan_count = thd_ndb->m_scan_count;
    ns->pruned_scan_count = thd_ndb->m_pruned_scan_count;
    ns->sorted_scan_count = thd_ndb->m_sorted_scan_count;
    ns->pushed_queries_defined = thd_ndb->m_pushed_queries_defined;
    ns->pushed_queries_dropped = thd_ndb->m_pushed_queries_dropped;
    ns->pushed_queries_executed = thd_ndb->m_pushed_queries_executed;
    ns->pushed_reads = thd_ndb->m_pushed_reads;
    ns->last_commit_epoch_session = thd_ndb->m_last_commit_epoch_session;
    for (int i = 0; i < Ndb::NumClientStatistics; i++) {
      ns->api_client_stats[i] = thd_ndb->ndb->getClientStat(i);
    }
    ns->schema_locks_count = thd_ndb->schema_locks_count;
    ns->fetch_table_stats = thd_ndb->m_fetch_table_stats;
  }
  return 0;
}

/* Helper macro for definitions of NdbApi status variables */

#define NDBAPI_COUNTERS(NAME_SUFFIX, ARRAY_LOCATION)                          \
  {"api_wait_exec_complete_count" NAME_SUFFIX,                                \
   (char *)ARRAY_LOCATION[Ndb::WaitExecCompleteCount], SHOW_LONGLONG,         \
   SHOW_SCOPE_GLOBAL},                                                        \
      {"api_wait_scan_result_count" NAME_SUFFIX,                              \
       (char *)ARRAY_LOCATION[Ndb::WaitScanResultCount], SHOW_LONGLONG,       \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_wait_meta_request_count" NAME_SUFFIX,                             \
       (char *)ARRAY_LOCATION[Ndb::WaitMetaRequestCount], SHOW_LONGLONG,      \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_wait_nanos_count" NAME_SUFFIX,                                    \
       (char *)ARRAY_LOCATION[Ndb::WaitNanosCount], SHOW_LONGLONG,            \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_bytes_sent_count" NAME_SUFFIX,                                    \
       (char *)ARRAY_LOCATION[Ndb::BytesSentCount], SHOW_LONGLONG,            \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_bytes_received_count" NAME_SUFFIX,                                \
       (char *)ARRAY_LOCATION[Ndb::BytesRecvdCount], SHOW_LONGLONG,           \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_trans_start_count" NAME_SUFFIX,                                   \
       (char *)ARRAY_LOCATION[Ndb::TransStartCount], SHOW_LONGLONG,           \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_trans_commit_count" NAME_SUFFIX,                                  \
       (char *)ARRAY_LOCATION[Ndb::TransCommitCount], SHOW_LONGLONG,          \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_trans_abort_count" NAME_SUFFIX,                                   \
       (char *)ARRAY_LOCATION[Ndb::TransAbortCount], SHOW_LONGLONG,           \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_trans_close_count" NAME_SUFFIX,                                   \
       (char *)ARRAY_LOCATION[Ndb::TransCloseCount], SHOW_LONGLONG,           \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_pk_op_count" NAME_SUFFIX, (char *)ARRAY_LOCATION[Ndb::PkOpCount], \
       SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},                                     \
      {"api_uk_op_count" NAME_SUFFIX, (char *)ARRAY_LOCATION[Ndb::UkOpCount], \
       SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},                                     \
      {"api_table_scan_count" NAME_SUFFIX,                                    \
       (char *)ARRAY_LOCATION[Ndb::TableScanCount], SHOW_LONGLONG,            \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_range_scan_count" NAME_SUFFIX,                                    \
       (char *)ARRAY_LOCATION[Ndb::RangeScanCount], SHOW_LONGLONG,            \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_pruned_scan_count" NAME_SUFFIX,                                   \
       (char *)ARRAY_LOCATION[Ndb::PrunedScanCount], SHOW_LONGLONG,           \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_scan_batch_count" NAME_SUFFIX,                                    \
       (char *)ARRAY_LOCATION[Ndb::ScanBatchCount], SHOW_LONGLONG,            \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_read_row_count" NAME_SUFFIX,                                      \
       (char *)ARRAY_LOCATION[Ndb::ReadRowCount], SHOW_LONGLONG,              \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_trans_local_read_row_count" NAME_SUFFIX,                          \
       (char *)ARRAY_LOCATION[Ndb::TransLocalReadRowCount], SHOW_LONGLONG,    \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_adaptive_send_forced_count" NAME_SUFFIX,                          \
       (char *)ARRAY_LOCATION[Ndb::ForcedSendsCount], SHOW_LONGLONG,          \
       SHOW_SCOPE_GLOBAL},                                                    \
      {"api_adaptive_send_unforced_count" NAME_SUFFIX,                        \
       (char *)ARRAY_LOCATION[Ndb::UnforcedSendsCount], SHOW_LONGLONG,        \
       SHOW_SCOPE_GLOBAL},                                                    \
  {                                                                           \
    "api_adaptive_send_deferred_count" NAME_SUFFIX,                           \
        (char *)ARRAY_LOCATION[Ndb::DeferredSendsCount], SHOW_LONGLONG,       \
        SHOW_SCOPE_GLOBAL                                                     \
  }

static SHOW_VAR ndb_status_vars_dynamic[] = {
    {"cluster_node_id", (char *)&g_ndb_status.cluster_node_id, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"config_from_host", (char *)&g_ndb_status.connected_host, SHOW_CHAR_PTR,
     SHOW_SCOPE_GLOBAL},
    {"config_from_port", (char *)&g_ndb_status.connected_port, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"config_generation", (char *)&g_ndb_status.config_generation, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"number_of_data_nodes", (char *)&g_ndb_status.number_of_data_nodes,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"number_of_ready_data_nodes",
     (char *)&g_ndb_status.number_of_ready_data_nodes, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"connect_count", (char *)&g_ndb_status.connect_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"execute_count", (char *)&g_ndb_status.execute_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"scan_count", (char *)&g_ndb_status.scan_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"pruned_scan_count", (char *)&g_ndb_status.pruned_scan_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"schema_locks_count", (char *)&g_ndb_status.schema_locks_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    NDBAPI_COUNTERS("_session", &g_ndb_status.api_client_stats),
    {"trans_hint_count_session",
     reinterpret_cast<char *>(&g_ndb_status.trans_hint_count), SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"sorted_scan_count", (char *)&g_ndb_status.sorted_scan_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"pushed_queries_defined", (char *)&g_ndb_status.pushed_queries_defined,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"pushed_queries_dropped", (char *)&g_ndb_status.pushed_queries_dropped,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"pushed_queries_executed", (char *)&g_ndb_status.pushed_queries_executed,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"pushed_reads", (char *)&g_ndb_status.pushed_reads, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"last_commit_epoch_server", (char *)&g_ndb_status.last_commit_epoch_server,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"last_commit_epoch_session",
     (char *)&g_ndb_status.last_commit_epoch_session, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"system_name", (char *)&g_ndb_status.system_name, SHOW_CHAR_PTR,
     SHOW_SCOPE_GLOBAL},
    {"fetch_table_stats", (char *)&g_ndb_status.fetch_table_stats, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

// Global instance of stats for the default replication channel, populated
// from Ndb_replica when the channel state changes
static Ndb_replica::Channel_stats g_default_channel_stats;
// List of status variables for the default replication channel
static SHOW_VAR ndb_status_vars_replica[] = {
    NDBAPI_COUNTERS("_slave", &g_default_channel_stats.api_stats),
    NDBAPI_COUNTERS("_replica", &g_default_channel_stats.api_stats),
    {"slave_max_replicated_epoch",
     (char *)&g_default_channel_stats.max_rep_epoch, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"replica_max_replicated_epoch",
     (char *)&g_default_channel_stats.max_rep_epoch, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_fn_max",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_MAX],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_fn_old",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_OLD],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_fn_max_del_win",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_MAX_DEL_WIN],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_fn_max_ins",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_MAX_INS],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_fn_max_del_win_ins",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_MAX_DEL_WIN_INS],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_fn_epoch",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_EPOCH],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_fn_epoch_trans",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_EPOCH_TRANS],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_fn_epoch2",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_EPOCH2],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_fn_epoch2_trans",
     (char *)&g_default_channel_stats.violation_count[CFT_NDB_EPOCH2_TRANS],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_trans_row_conflict_count",
     (char *)&g_default_channel_stats.trans_row_conflict_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_trans_row_reject_count",
     (char *)&g_default_channel_stats.trans_row_reject_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_trans_reject_count",
     (char *)&g_default_channel_stats.trans_in_conflict_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_trans_detect_iter_count",
     (char *)&g_default_channel_stats.trans_detect_iter_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_trans_conflict_commit_count",
     (char *)&g_default_channel_stats.trans_conflict_commit_count,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"conflict_epoch_delete_delete_count",
     (char *)&g_default_channel_stats.delete_delete_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_reflected_op_prepare_count",
     (char *)&g_default_channel_stats.reflect_op_prepare_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_reflected_op_discard_count",
     (char *)&g_default_channel_stats.reflect_op_discard_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_refresh_op_count",
     (char *)&g_default_channel_stats.refresh_op_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_last_conflict_epoch",
     (char *)&g_default_channel_stats.last_conflicted_epoch, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},
    {"conflict_last_stable_epoch",
     (char *)&g_default_channel_stats.last_stable_epoch, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

static SHOW_VAR ndb_status_vars_server_api[] = {
    NDBAPI_COUNTERS("", &g_server_api_client_stats),
    {"api_event_data_count",
     (char *)&g_server_api_client_stats[Ndb::DataEventsRecvdCount],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"api_event_nondata_count",
     (char *)&g_server_api_client_stats[Ndb::NonDataEventsRecvdCount],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {"api_event_bytes_count",
     (char *)&g_server_api_client_stats[Ndb::EventBytesRecvdCount],
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

/*
   Called when SHOW STATUS or performance_schema.[global|session]_status
   wants to see the status variables. We use this opportunity to:
   1) Update the globals with current values
   2) Return an array of var definitions, pointing to
      the updated globals
*/

static int show_ndb_status_server_api(THD *, SHOW_VAR *var, char *) {
  ndb_get_connection_stats((Uint64 *)&g_server_api_client_stats[0]);

  var->type = SHOW_ARRAY;
  var->value = (char *)ndb_status_vars_server_api;
  var->scope = SHOW_SCOPE_GLOBAL;

  return 0;
}

/*
  Error handling functions
*/

/* Note for merge: old mapping table, moved to storage/ndb/ndberror.c */

int ndb_to_mysql_error(const NdbError *ndberr) {
  /* read the mysql mapped error code */
  int error = ndberr->mysql_code;

  switch (error) {
      /* errors for which we do not add warnings, just return mapped error code
       */
    case HA_ERR_NO_SUCH_TABLE:
    case HA_ERR_KEY_NOT_FOUND:
      return error;

      /* Mapping missing, go with the ndb error code */
    case -1:
    case 0:
      /* Never map to errors below HA_ERR_FIRST */
      if (ndberr->code < HA_ERR_FIRST)
        error = HA_ERR_INTERNAL_ERROR;
      else
        error = ndberr->code;
      break;
      /* Mapping exists, go with the mapped code */
    default:
      break;
  }

  {
    /*
      Push the NDB error message as warning
      - Used to be able to use SHOW WARNINGS to get more info
        on what the error is
      - Used by replication to see if the error was temporary
    */
    if (ndberr->status == NdbError::TemporaryError)
      push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                          ER_GET_TEMPORARY_ERRMSG,
                          ER_THD(current_thd, ER_GET_TEMPORARY_ERRMSG),
                          ndberr->code, ndberr->message, "NDB");
    else
      push_warning_printf(current_thd, Sql_condition::SL_WARNING, ER_GET_ERRMSG,
                          ER_THD(current_thd, ER_GET_ERRMSG), ndberr->code,
                          ndberr->message, "NDB");
  }
  return error;
}

ulong opt_ndb_slave_conflict_role;
ulong opt_ndb_applier_conflict_role;

static int handle_conflict_op_error(Ndb_applier *const applier,
                                    NdbTransaction *trans, const NdbError &err,
                                    const NdbOperation *op);

static bool ndbcluster_notify_alter_table(THD *, const MDL_key *,
                                          ha_notification_type);

static bool ndbcluster_notify_exclusive_mdl(THD *, const MDL_key *,
                                            ha_notification_type, bool *);

static int handle_row_conflict(
    Ndb_applier *const applier, NDB_CONFLICT_FN_SHARE *cfn_share,
    const char *tab_name, const char *handling_type, const NdbRecord *key_rec,
    const NdbRecord *data_rec, const uchar *old_row, const uchar *new_row,
    enum_conflicting_op_type op_type, enum_conflict_cause conflict_cause,
    const NdbError &conflict_error, NdbTransaction *conflict_trans,
    const MY_BITMAP *write_set, Uint64 transaction_id);

// Error code returned when "refresh occurs on a refreshed row"
static constexpr int ERROR_OP_AFTER_REFRESH_OP = 920;

static inline int check_completed_operations_pre_commit(
    Thd_ndb *thd_ndb, NdbTransaction *trans, const NdbOperation *first,
    const NdbOperation *last, uint *ignore_count) {
  uint ignores = 0;
  DBUG_TRACE;

  if (unlikely(first == nullptr)) {
    assert(last == nullptr);
    return 0;
  }

  /*
    Check that all errors are "accepted" errors
    or exceptions to report
  */
  const NdbOperation *lastUserOp = trans->getLastDefinedOperation();
  while (true) {
    const NdbError &err = first->getNdbError();
    const bool op_has_conflict_detection = (first->getCustomData() != nullptr);
    if (!op_has_conflict_detection) {
      assert(err.code != ERROR_OP_AFTER_REFRESH_OP);

      /* 'Normal path' - ignore key (not) present, others are errors */
      if (err.classification != NdbError::NoError &&
          err.classification != NdbError::ConstraintViolation &&
          err.classification != NdbError::NoDataFound) {
        /* Non ignored error, report it */
        DBUG_PRINT("info", ("err.code == %u", err.code));
        return err.code;
      }
    } else {
      /*
         Op with conflict detection, use special error handling method
       */

      if (err.classification != NdbError::NoError) {
        const int res =
            handle_conflict_op_error(thd_ndb->get_applier(), trans, err, first);
        if (res != 0) {
          return res;
        }
      }
    }  // if (!op_has_conflict_detection)
    if (err.classification != NdbError::NoError) ignores++;

    if (first == last) break;

    first = trans->getNextCompletedOperation(first);
  }
  if (ignore_count) *ignore_count = ignores;

  /*
     Conflict detection related error handling above may have defined
     new operations on the transaction.  If so, execute them now
  */
  if (trans->getLastDefinedOperation() != lastUserOp) {
    const NdbOperation *last_conflict_op = trans->getLastDefinedOperation();

    NdbError nonMaskedError;
    assert(nonMaskedError.code == 0);

    if (trans->execute(NdbTransaction::NoCommit, NdbOperation::AO_IgnoreError,
                       thd_ndb->m_force_send)) {
      /* Transaction execute failed, even with IgnoreError... */
      nonMaskedError = trans->getNdbError();
      assert(nonMaskedError.code != 0);
    } else if (trans->getNdbError().code) {
      /* Check the result codes of the operations we added */
      const NdbOperation *conflict_op = nullptr;
      do {
        conflict_op = trans->getNextCompletedOperation(conflict_op);
        assert(conflict_op != nullptr);
        // Ignore 920 (ERROR_OP_AFTER_REFRESH_OP) which represents a refreshOp
        // or other op arriving after a refreshOp
        const NdbError &err = conflict_op->getNdbError();
        if (err.code != 0 && err.code != ERROR_OP_AFTER_REFRESH_OP) {
          /* Found a real error, break out and handle it */
          nonMaskedError = err;
          break;
        }
      } while (conflict_op != last_conflict_op);
    }

    /* Handle errors with extra conflict handling operations */
    if (nonMaskedError.code != 0) {
      if (nonMaskedError.status == NdbError::TemporaryError) {
        /* Slave will roll back and retry entire transaction. */
        ERR_RETURN(nonMaskedError);
      } else {
        thd_ndb->push_ndb_error_warning(nonMaskedError);
        thd_ndb->push_warning(
            ER_EXCEPTIONS_WRITE_ERROR,
            ER_THD(current_thd, ER_EXCEPTIONS_WRITE_ERROR),
            "Failed executing extra operations for conflict handling");
        /* Slave will stop replication. */
        return ER_EXCEPTIONS_WRITE_ERROR;
      }
    }
  }
  return 0;
}

static inline int check_completed_operations(NdbTransaction *trans,
                                             const NdbOperation *first,
                                             const NdbOperation *last,
                                             uint *ignore_count) {
  uint ignores = 0;
  DBUG_TRACE;

  if (unlikely(first == nullptr)) {
    assert(last == nullptr);
    return 0;
  }

  /*
    Check that all errors are "accepted" errors
  */
  while (true) {
    const NdbError &err = first->getNdbError();
    if (err.classification != NdbError::NoError &&
        err.classification != NdbError::ConstraintViolation &&
        err.classification != NdbError::NoDataFound) {
      /* All conflict detection etc should be done before commit */
      assert(err.code != ERROR_CONFLICT_FN_VIOLATION &&
             err.code != ERROR_OP_AFTER_REFRESH_OP);
      return err.code;
    }
    if (err.classification != NdbError::NoError) ignores++;

    if (first == last) break;

    first = trans->getNextCompletedOperation(first);
  }
  if (ignore_count) *ignore_count = ignores;
  return 0;
}

static inline int execute_no_commit(Thd_ndb *thd_ndb, NdbTransaction *trans,
                                    bool ignore_no_key,
                                    uint *ignore_count = nullptr) {
  DBUG_TRACE;

  trans->releaseCompletedOpsAndQueries();

  const NdbOperation *first = trans->getFirstDefinedOperation();
  const NdbOperation *last = trans->getLastDefinedOperation();
  thd_ndb->m_execute_count++;
  thd_ndb->m_unsent_bytes = 0;
  thd_ndb->m_unsent_blob_ops = false;
  DBUG_PRINT("info", ("execute_count: %u", thd_ndb->m_execute_count));
  int rc = 0;
  do {
    if (trans->execute(NdbTransaction::NoCommit, NdbOperation::AO_IgnoreError,
                       thd_ndb->m_force_send)) {
      rc = -1;
      break;
    }
    if (!ignore_no_key || trans->getNdbError().code == 0) {
      rc = trans->getNdbError().code;
      break;
    }

    rc = check_completed_operations_pre_commit(thd_ndb, trans, first, last,
                                               ignore_count);
  } while (0);

  if (unlikely(rc != 0)) {
    Ndb_applier *const applier = thd_ndb->get_applier();
    if (applier) {
      applier->atTransactionAbort();
    }
  }

  DBUG_PRINT("info", ("execute_no_commit rc is %d", rc));
  return rc;
}

static inline int execute_commit(Thd_ndb *thd_ndb, NdbTransaction *trans,
                                 int force_send, int ignore_error,
                                 uint *ignore_count = nullptr) {
  DBUG_TRACE;
  NdbOperation::AbortOption ao = NdbOperation::AO_IgnoreError;
  if (thd_ndb->m_unsent_bytes && !ignore_error) {
    /*
      We have unsent bytes and cannot ignore error.  Calling execute
      with NdbOperation::AO_IgnoreError will result in possible commit
      of a transaction although there is an error.
    */
    ao = NdbOperation::AbortOnError;
  }
  const NdbOperation *first = trans->getFirstDefinedOperation();
  const NdbOperation *last = trans->getLastDefinedOperation();
  thd_ndb->m_execute_count++;
  thd_ndb->m_unsent_bytes = 0;
  thd_ndb->m_unsent_blob_ops = false;
  DBUG_PRINT("info", ("execute_count: %u", thd_ndb->m_execute_count));
  int rc = 0;
  do {
    if (trans->execute(NdbTransaction::Commit, ao, force_send)) {
      rc = -1;
      break;
    }

    if (!ignore_error || trans->getNdbError().code == 0) {
      rc = trans->getNdbError().code;
      break;
    }

    rc = check_completed_operations(trans, first, last, ignore_count);
  } while (0);

  if (likely(rc == 0)) {
    /* Committed ok, update session GCI, if it's available
     * (Not available for reads, empty transactions etc...)
     */
    Uint64 reportedGCI;
    if (trans->getGCI(&reportedGCI) == 0 && reportedGCI != 0) {
      assert(reportedGCI >= thd_ndb->m_last_commit_epoch_session);
      thd_ndb->m_last_commit_epoch_session = reportedGCI;
    }
  }

  Ndb_applier *const applier = thd_ndb->get_applier();
  if (applier) {
    if (likely(rc == 0)) {
      /* Success */
      applier->atTransactionCommit(thd_ndb->m_last_commit_epoch_session);
    } else {
      applier->atTransactionAbort();
    }
  }

  DBUG_PRINT("info", ("execute_commit rc is %d", rc));
  return rc;
}

static inline int execute_no_commit_ie(Thd_ndb *thd_ndb,
                                       NdbTransaction *trans) {
  DBUG_TRACE;

  trans->releaseCompletedOpsAndQueries();

  const int res =
      trans->execute(NdbTransaction::NoCommit, NdbOperation::AO_IgnoreError,
                     thd_ndb->m_force_send);
  thd_ndb->m_unsent_bytes = 0;
  thd_ndb->m_execute_count++;
  thd_ndb->m_unsent_blob_ops = false;
  DBUG_PRINT("info", ("execute_count: %u", thd_ndb->m_execute_count));
  return res;
}

Thd_ndb::Thd_ndb(THD *thd, const char *name)
    : m_thd(thd),
      options(0),
      trans_options(0),
      m_ddl_ctx(nullptr),
      m_thread_name(name),
      m_batch_mem_root(key_memory_thd_ndb_batch_mem_root,
                       BATCH_MEM_ROOT_BLOCK_SIZE),
      global_schema_lock_trans(nullptr),
      global_schema_lock_count(0),
      global_schema_lock_error(0),
      schema_locks_count(0),
      m_last_commit_epoch_session(0) {
  connection = ndb_get_cluster_connection();
  m_connect_count = connection->get_connect_count();
  ndb = new Ndb(connection, "");
  save_point_count = 0;
  trans = nullptr;
  m_handler = nullptr;
  m_unsent_bytes = 0;
  m_unsent_blob_ops = false;
  m_execute_count = 0;
  m_scan_count = 0;
  m_pruned_scan_count = 0;
  m_sorted_scan_count = 0;
  m_pushed_queries_defined = 0;
  m_pushed_queries_dropped = 0;
  m_pushed_queries_executed = 0;
  m_pushed_reads = 0;
}

Thd_ndb::~Thd_ndb() {
  assert(global_schema_lock_count == 0);
  assert(m_ddl_ctx == nullptr);

  // The applier uses the Ndb object when removing its NdbApi table from dict
  // cache, release applier first
  m_applier.reset();

  delete ndb;

  m_batch_mem_root.Clear();
}

void ha_ndbcluster::set_rec_per_key(THD *thd) {
  DBUG_TRACE;
  /*
    Set up the 'records per key' value for keys which there are good knowledge
    about the distribution. The default value for 'records per key' is otherwise
    0 (interpreted as 'unknown' by optimizer), which would force the optimizer
    to use its own heuristic to estimate 'records per key'.
  */
  for (uint i = 0; i < table_share->keys; i++) {
    KEY *const key_info = table->key_info + i;
    switch (m_index[i].type) {
      case UNIQUE_INDEX:
      case PRIMARY_KEY_INDEX: {
        // Index is unique when all 'key_parts' are specified,
        // else distribution is unknown and not specified here.

        // Set 'records per key' to 1 for complete key given
        key_info->set_records_per_key(key_info->user_defined_key_parts - 1,
                                      1.0F);
        break;
      }
      case UNIQUE_ORDERED_INDEX:
      case PRIMARY_KEY_ORDERED_INDEX:
        // Set 'records per key' to 1 for complete key given
        key_info->set_records_per_key(key_info->user_defined_key_parts - 1,
                                      1.0F);

        // intentional fall thru to logic for ordered index
        [[fallthrough]];
      case ORDERED_INDEX:
        // 'records per key' are unknown for non-unique indexes (may change when
        // we get better index statistics).

        {
          const bool index_stat_enable = ndb_index_stat_get_enable(nullptr) &&
                                         ndb_index_stat_get_enable(thd);
          if (index_stat_enable) {
            const int err = ndb_index_stat_set_rpk(i);
            if (err != 0 &&
                /* no stats is not unexpected error */
                err != NdbIndexStat::NoIndexStats &&
                /* warning was printed at first error */
                err != NdbIndexStat::MyHasError &&
                /* stats thread aborted request */
                err != NdbIndexStat::MyAbortReq) {
              push_warning_printf(thd, Sql_condition::SL_WARNING,
                                  ER_CANT_GET_STAT,
                                  "index stats (RPK) for key %s:"
                                  " unexpected error %d",
                                  key_info->name, err);
            }
          }
          // no fallback method...
          break;
        }
      case UNDEFINED_INDEX:  // index is currently unavailable
        break;
    }
  }
}

int ha_ndbcluster::records(ha_rows *num_rows) {
  DBUG_TRACE;

  // Read fresh stats from NDB (one roundtrip)
  const int error = update_stats(table->in_use, true);
  if (error != 0) {
    *num_rows = HA_POS_ERROR;
    return error;
  }

  // Return the "records" from handler::stats::records
  *num_rows = stats.records;
  return 0;
}

int ha_ndbcluster::ndb_err(NdbTransaction *trans) {
  DBUG_TRACE;

  const NdbError &err = trans->getNdbError();
  switch (err.classification) {
    case NdbError::SchemaError: {
      // Mark the NDB table def as invalid, this will cause also all index defs
      // to be invalidate on close
      m_table->setStatusInvalid();
      // Close other open handlers not used by any thread
      ndb_tdc_close_cached_table(current_thd, table->s->db.str,
                                 table->s->table_name.str);
      break;
    }
    default:
      break;
  }
  const int res = ndb_to_mysql_error(&err);
  DBUG_PRINT("info", ("transformed ndbcluster error %d to mysql error %d",
                      err.code, res));
  if (res == HA_ERR_FOUND_DUPP_KEY) {
    char *error_data = err.details;
    uint dupkey = MAX_KEY;

    for (uint i = 0; i < MAX_KEY; i++) {
      if (m_index[i].type == UNIQUE_INDEX ||
          m_index[i].type == UNIQUE_ORDERED_INDEX) {
        const NdbDictionary::Index *unique_index = m_index[i].unique_index;
        if (unique_index &&
            UintPtr(unique_index->getObjectId()) == UintPtr(error_data)) {
          dupkey = i;
          break;
        }
      }
    }
    if (m_rows_to_insert == 1) {
      /*
        We can only distinguish between primary and non-primary
        violations here, so we need to return MAX_KEY for non-primary
        to signal that key is unknown
      */
      m_dupkey = err.code == 630 ? table_share->primary_key : dupkey;
    } else {
      /* We are batching inserts, offending key is not available */
      m_dupkey = (uint)-1;
    }
  }
  return res;
}

extern bool ndb_fk_util_generate_constraint_string(
    THD *thd, Ndb *ndb, const NdbDictionary::ForeignKey &fk,
    const int child_tab_id, String &fk_string);

/**
  Generate error messages when requested by the caller.
  Fetches the error description from NdbError and print it in the caller's
  buffer. This function also additionally handles HA_ROW_REF fk errors.

  @param    error             The error code sent by the caller.
  @param    buf               String buffer to print the error message.

  @retval   true              if the error is permanent
            false             if its temporary
*/

bool ha_ndbcluster::get_error_message(int error, String *buf) {
  DBUG_TRACE;
  DBUG_PRINT("enter", ("error: %d", error));

  Ndb *ndb = check_ndb_in_thd(current_thd);
  if (!ndb) return false;

  bool temporary = false;

  if (unlikely(error == HA_ERR_NO_REFERENCED_ROW ||
               error == HA_ERR_ROW_IS_REFERENCED)) {
    /* Error message to be generated from NdbError in latest trans or dict */
    Thd_ndb *thd_ndb = get_thd_ndb(current_thd);
    NdbDictionary::Dictionary *dict = ndb->getDictionary();
    NdbError err;
    if (thd_ndb->trans != nullptr) {
      err = thd_ndb->trans->getNdbError();
    } else {
      // Drop table failure. get error from dictionary.
      err = dict->getNdbError();
      assert(err.code == 21080);
    }
    temporary = (err.status == NdbError::TemporaryError);

    String fk_string;
    {
      /* copy default error message to be used on failure */
      const char *unknown_fk = "Unknown FK Constraint";
      buf->copy(unknown_fk, (uint32)strlen(unknown_fk), &my_charset_bin);
    }

    /* fk name of format parent_id/child_id/fk_name */
    char fully_qualified_fk_name[MAX_ATTR_NAME_SIZE + (2 * MAX_INT_WIDTH) + 3];
    /* get the fully qualified FK name from ndb using getNdbErrorDetail */
    if (ndb->getNdbErrorDetail(err, &fully_qualified_fk_name[0],
                               sizeof(fully_qualified_fk_name)) == nullptr) {
      assert(false);
      ndb_to_mysql_error(&dict->getNdbError());
      return temporary;
    }

    /* fetch the foreign key */
    NdbDictionary::ForeignKey fk;
    if (dict->getForeignKey(fk, fully_qualified_fk_name) != 0) {
      assert(false);
      ndb_to_mysql_error(&dict->getNdbError());
      return temporary;
    }

    /* generate constraint string from fk object */
    if (!ndb_fk_util_generate_constraint_string(current_thd, ndb, fk, 0,
                                                fk_string)) {
      assert(false);
      return temporary;
    }

    /* fk found and string has been generated. set the buf */
    buf->copy(fk_string);
    return temporary;
  } else {
    /* NdbError code. Fetch error description from ndb */
    const NdbError err = ndb->getNdbError(error);
    temporary = err.status == NdbError::TemporaryError;
    buf->set(err.message, (uint32)strlen(err.message), &my_charset_bin);
  }

  DBUG_PRINT("exit", ("message: %s, temporary: %d", buf->ptr(), temporary));
  return temporary;
}

/*
  field_used_length() returns the number of bytes actually used to
  store the data of the field. So for a varstring it includes both
  length byte(s) and string data, and anything after data_length()
  bytes are unused.
*/
static uint32 field_used_length(const Field *field, ptrdiff_t row_offset = 0) {
  if (field->type() == MYSQL_TYPE_VARCHAR) {
    return field->get_length_bytes() + field->data_length(row_offset);
  }
  return field->pack_length();
}

/**
  Check if MySQL field type forces var part in ndb storage
*/
static bool field_type_forces_var_part(enum_field_types type) {
  switch (type) {
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VECTOR:
      return true;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_JSON:
    case MYSQL_TYPE_GEOMETRY:
      return false;
    default:
      return false;
  }
}

/**
 * findBlobError
 * This method attempts to find an error in the hierarchy of runtime
 * NDBAPI objects from Blob up to transaction.
 * It will return -1 if no error is found, 0 if an error is found.
 */
static int findBlobError(NdbError &error, NdbBlob *pBlob) {
  error = pBlob->getNdbError();
  if (error.code != 0) return 0;

  const NdbOperation *pOp = pBlob->getNdbOperation();
  error = pOp->getNdbError();
  if (error.code != 0) return 0;

  NdbTransaction *pTrans = pOp->getNdbTransaction();
  error = pTrans->getNdbError();
  if (error.code != 0) return 0;

  /* No error on any of the objects */
  return -1;
}

/*
 Calculate the length of the blob/text after applying mysql limits
 on blob/text sizes. If the blob contains multi-byte characters, the length is
 reduced till the end of the last well-formed char, so that data is not
 truncated in the middle of a multi-byte char.
*/
static uint64 calc_ndb_blob_len(const CHARSET_INFO *cs, uchar *blob_ptr,
                                uint64 maxlen) {
  int errors = 0;

  if (!cs) cs = &my_charset_bin;

  const char *begin = (const char *)blob_ptr;
  const char *end = (const char *)(blob_ptr + maxlen);

  // avoid truncation in the middle of a multi-byte character by
  // stopping at end of last well-formed character before max length
  uint32 numchars = cs->cset->numchars(cs, begin, end);
  uint64 len64 = cs->cset->well_formed_len(cs, begin, end, numchars, &errors);
  assert(len64 <= maxlen);

  return len64;
}

int ha_ndbcluster::get_ndb_blobs_value_hook(NdbBlob *ndb_blob, void *arg) {
  ha_ndbcluster *ha = (ha_ndbcluster *)arg;
  DBUG_TRACE;
  DBUG_PRINT("info", ("destination row: %p", ha->m_blob_destination_record));

  if (ha->m_blob_counter == 0) /* Reset total size at start of row */
    ha->m_blobs_row_total_size = 0;

  /* Count the total length needed for blob data. */
  int isNull;
  if (ndb_blob->getNull(isNull) != 0) ERR_RETURN(ndb_blob->getNdbError());
  if (isNull == 0) {
    Uint64 len64 = 0;
    if (ndb_blob->getLength(len64) != 0) ERR_RETURN(ndb_blob->getNdbError());
    /* Align to Uint64. */
    ha->m_blobs_row_total_size += (len64 + 7) & ~((Uint64)7);
    if (ha->m_blobs_row_total_size > 0xffffffff) {
      assert(false);
      return -1;
    }
    DBUG_PRINT("info", ("blob[%d]: size %llu, total size now %llu",
                        ha->m_blob_counter, len64, ha->m_blobs_row_total_size));
  }
  ha->m_blob_counter++;

  if (ha->m_blob_counter < ha->m_blob_expected_count_per_row) {
    // Wait until all blobs in this row are active so that a large buffer
    // with space for all can be allocated
    return 0;
  }

  /* Reset blob counter for next row (scan scenario) */
  ha->m_blob_counter = 0;

  // Check if buffer is large enough or need to be extended
  if (ha->m_blobs_row_total_size > ha->m_blobs_buffer.size()) {
    if (!ha->m_blobs_buffer.allocate(ha->m_blobs_row_total_size)) {
      ha->m_thd_ndb->push_warning(ER_OUTOFMEMORY,
                                  "Failed to allocate blobs buffer, size: %llu",
                                  ha->m_blobs_row_total_size);
      return -1;
    }
  }

  /*
    Now read all blob data.
    If we know the destination mysqld row, we also set the blob null bit and
    pointer/length (if not, it will be done instead in unpack_record()).
  */
  uint32 offset = 0;
  for (uint i = 0; i < ha->table->s->fields; i++) {
    Field *field = ha->table->field[i];
    if (!(field->is_flag_set(BLOB_FLAG) && field->stored_in_db)) continue;
    if (ha->m_row_side_buffer && bitmap_is_set(&ha->m_in_row_side_buffer, i))
      continue;
    NdbValue value = ha->m_value[i];
    if (value.blob == nullptr) {
      DBUG_PRINT("info", ("[%u] skipped", i));
      continue;
    }
    Field_blob *field_blob = (Field_blob *)field;
    NdbBlob *ndb_blob = value.blob;
    int isNull;
    if (ndb_blob->getNull(isNull) != 0) ERR_RETURN(ndb_blob->getNdbError());
    if (isNull == 0) {
      Uint64 len64 = 0;
      if (ndb_blob->getLength(len64) != 0) ERR_RETURN(ndb_blob->getNdbError());
      assert(len64 < 0xffffffff);
      uchar *buf = ha->m_blobs_buffer.get_ptr(offset);
      uint32 len = ha->m_blobs_buffer.size() - offset;
      if (ndb_blob->readData(buf, len) != 0) {
        NdbError err;
        if (findBlobError(err, ndb_blob) == 0) {
          ERR_RETURN(err);
        } else {
          /* Should always have some error code set */
          assert(err.code != 0);
          ERR_RETURN(err);
        }
      }
      DBUG_PRINT("info",
                 ("[%u] offset: %u  buf: %p  len=%u", i, offset, buf, len));
      assert(len == len64);
      if (ha->m_blob_destination_record) {
        ptrdiff_t ptrdiff =
            ha->m_blob_destination_record - ha->table->record[0];
        field_blob->move_field_offset(ptrdiff);

        if (len > field_blob->max_data_length()) {
          len = calc_ndb_blob_len(field_blob->charset(), buf,
                                  field_blob->max_data_length());

          // push a warning
          push_warning_printf(
              current_thd, Sql_condition::SL_WARNING, WARN_DATA_TRUNCATED,
              "Truncated value from TEXT field \'%s\'", field_blob->field_name);
        }

        field_blob->set_ptr(len, buf);
        field_blob->set_notnull();
        field_blob->move_field_offset(-ptrdiff);
      }
      offset += Uint32((len64 + 7) & ~((Uint64)7));
    } else if (ha->m_blob_destination_record) {
      /* Have to set length even in this case. */
      ptrdiff_t ptrdiff = ha->m_blob_destination_record - ha->table->record[0];
      const uchar *buf = ha->m_blobs_buffer.get_ptr(offset);
      field_blob->move_field_offset(ptrdiff);
      field_blob->set_ptr((uint32)0, buf);
      field_blob->set_null();
      field_blob->move_field_offset(-ptrdiff);
      DBUG_PRINT("info", ("[%u] isNull=%d", i, isNull));
    }
  }

  /**
   * For non-scan, non autocommit reads, call NdbBlob::close()
   * to allow Blob read related resources to be freed
   * early
   */
  const bool autocommit = (get_thd_ndb(current_thd)->m_handler != nullptr);
  if (!autocommit && !ha->m_active_cursor) {
    for (uint i = 0; i < ha->table->s->fields; i++) {
      Field *field = ha->table->field[i];
      if (!(field->is_flag_set(BLOB_FLAG) && field->stored_in_db)) continue;
      if (ha->m_row_side_buffer && bitmap_is_set(&ha->m_in_row_side_buffer, i))
        continue;
      NdbValue value = ha->m_value[i];
      if (value.blob == nullptr) {
        DBUG_PRINT("info", ("[%u] skipped", i));
        continue;
      }
      NdbBlob *ndb_blob = value.blob;

      assert(ndb_blob->getState() == NdbBlob::Active);

      /* Call close() with execPendingBlobOps == true
       * For LM_CommittedRead access, this will enqueue
       * an unlock operation, which the Blob framework
       * code invoking this callback will execute before
       * returning control to the caller of execute()
       */
      if (ndb_blob->close(true) != 0) {
        ERR_RETURN(ndb_blob->getNdbError());
      }
    }
  }

  return 0;
}

/*
  Request reading of blob values.

  If dst_record is specified, the blob null bit, pointer, and length will be
  set in that record. Otherwise they must be set later by calling
  unpack_record().
*/
int ha_ndbcluster::get_blob_values(const NdbOperation *ndb_op,
                                   uchar *dst_record, const MY_BITMAP *bitmap) {
  uint i;
  DBUG_TRACE;

  m_blob_counter = 0;
  m_blob_expected_count_per_row = 0;
  m_blob_destination_record = dst_record;
  m_blobs_row_total_size = 0;
  ndb_op->getNdbTransaction()->setMaxPendingBlobReadBytes(
      THDVAR(current_thd, blob_read_batch_bytes));

  for (i = 0; i < table_share->fields; i++) {
    Field *field = table->field[i];
    if (!(field->is_flag_set(BLOB_FLAG) && field->stored_in_db)) continue;
    if (m_row_side_buffer && bitmap_is_set(&m_in_row_side_buffer, i)) continue;

    DBUG_PRINT("info", ("fieldnr=%d", i));
    NdbBlob *ndb_blob;
    if (bitmap_is_set(bitmap, i)) {
      if ((ndb_blob = m_table_map->getBlobHandle(ndb_op, i)) == nullptr ||
          ndb_blob->setActiveHook(get_ndb_blobs_value_hook, this) != 0)
        return 1;
      m_blob_expected_count_per_row++;
    } else
      ndb_blob = nullptr;

    m_value[i].blob = ndb_blob;
  }

  return 0;
}

int ha_ndbcluster::set_blob_values(const NdbOperation *ndb_op,
                                   ptrdiff_t row_offset,
                                   const MY_BITMAP *bitmap, uint *set_count,
                                   bool batch) const {
  uint field_no;
  uint *blob_index, *blob_index_end;
  int res = 0;
  DBUG_TRACE;

  *set_count = 0;

  if (table_share->blob_fields == 0) return 0;

  // Note! This settings seems to be lazily assigned for every row rather than
  // once up front when transaction is started. For many rows, it might be
  // better to do it once.
  m_thd_ndb->trans->setMaxPendingBlobWriteBytes(
      m_thd_ndb->get_blob_write_batch_size());

  blob_index = table_share->blob_field;
  blob_index_end = blob_index + table_share->blob_fields;
  do {
    field_no = *blob_index;
    /* A NULL bitmap sets all blobs. */
    if (bitmap && !bitmap_is_set(bitmap, field_no)) continue;

    if (m_row_side_buffer && bitmap_is_set(&m_in_row_side_buffer, field_no))
      continue;

    Field *field = table->field[field_no];
    if (field->is_virtual_gcol()) continue;

    NdbBlob *ndb_blob = m_table_map->getBlobHandle(ndb_op, field_no);
    if (ndb_blob == nullptr) ERR_RETURN(ndb_op->getNdbError());
    if (field->is_real_null(row_offset)) {
      DBUG_PRINT("info", ("Setting Blob %d to NULL", field_no));
      if (ndb_blob->setNull() != 0) ERR_RETURN(ndb_op->getNdbError());
    } else {
      Field_blob *field_blob = (Field_blob *)field;

      // Get length and pointer to data
      const uint32 blob_len = field_blob->get_length(row_offset);
      const uchar *blob_ptr = field_blob->get_blob_data(row_offset);

      // Looks like NULL ptr signals length 0 blob
      if (blob_ptr == nullptr) {
        assert(blob_len == 0);
        blob_ptr = pointer_cast<const uchar *>("");
      }

      DBUG_PRINT("value", ("set blob ptr: %p  len: %u", blob_ptr, blob_len));
      DBUG_DUMP("value", blob_ptr, std::min(blob_len, (Uint32)26));

      if (batch && blob_len > 0) {
        /*
          The blob data pointer is required to remain valid until execute()
          time. So when batching, copy the blob data to batch memory.
        */
        uchar *blob_copy = m_thd_ndb->copy_to_batch_mem(blob_ptr, blob_len);
        if (!blob_copy) {
          return HA_ERR_OUT_OF_MEM;
        }
        blob_ptr = blob_copy;
      }
      res = ndb_blob->setValue(pointer_cast<const char *>(blob_ptr), blob_len);
      if (res != 0) ERR_RETURN(ndb_op->getNdbError());
    }

    ++(*set_count);
  } while (++blob_index != blob_index_end);

  return res;
}

/**
  Check if any set or get of blob value in current query.
  Not counting blobs that do not use blob hooks.
*/

bool ha_ndbcluster::uses_blob_value(const MY_BITMAP *bitmap) const {
  uint *blob_index, *blob_index_end;
  if (table_share->blob_fields == 0) return false;

  blob_index = table_share->blob_field;
  blob_index_end = blob_index + table_share->blob_fields;
  do {
    Field *field = table->field[*blob_index];
    if (bitmap_is_set(bitmap, field->field_index()) &&
        !field->is_virtual_gcol() &&
        !(m_row_side_buffer &&
          bitmap_is_set(&m_in_row_side_buffer, field->field_index())))
      return true;
  } while (++blob_index != blob_index_end);
  return false;
}

void ha_ndbcluster::release_blobs_buffer() {
  DBUG_TRACE;
  m_blobs_buffer.release();
  m_blobs_row_total_size = 0;
}

/*
  Does type support a default value?
*/
static bool type_supports_default_value(enum_field_types mysql_type) {
  bool ret =
      (mysql_type != MYSQL_TYPE_BLOB && mysql_type != MYSQL_TYPE_TINY_BLOB &&
       mysql_type != MYSQL_TYPE_MEDIUM_BLOB &&
       mysql_type != MYSQL_TYPE_LONG_BLOB && mysql_type != MYSQL_TYPE_JSON &&
       mysql_type != MYSQL_TYPE_GEOMETRY);

  return ret;
}

#ifndef NDEBUG
/**

   Check that NDB table has the same default values
   as the MySQL table def.
   Called as part of a DBUG check when opening table.

   @return true Defaults are ok
*/
bool ha_ndbcluster::check_default_values() const {
  if (!m_table->hasDefaultValues()) {
    // There are no default values in the NDB table
    return true;
  }

  bool defaults_aligned = true;

  /* NDB supports native defaults for non-pk columns */
  my_bitmap_map *old_map = tmp_use_all_columns(table, table->read_set);

  for (uint f = 0; f < table_share->fields; f++) {
    Field *field = table->field[f];
    if (!field->stored_in_db) continue;

    const NdbDictionary::Column *ndbCol =
        m_table_map->getColumn(field->field_index());

    if ((!(field->is_flag_set(PRI_KEY_FLAG) ||
           field->is_flag_set(NO_DEFAULT_VALUE_FLAG))) &&
        type_supports_default_value(field->real_type())) {
      // Expect NDB to have a native default for this column
      ptrdiff_t src_offset =
          table_share->default_values - field->table->record[0];

      /* Move field by offset to refer to default value */
      field->move_field_offset(src_offset);

      const uchar *ndb_default = (const uchar *)ndbCol->getDefaultValue();

      if (ndb_default == nullptr) {
        /* MySQL default must also be NULL */
        defaults_aligned = field->is_null();
      } else {
        if (field->type() != MYSQL_TYPE_BIT) {
          defaults_aligned = (0 == field->cmp(ndb_default));
        } else {
          longlong value = (static_cast<Field_bit *>(field))->val_int();
          /* Map to NdbApi format - two Uint32s */
          Uint32 out[2];
          out[0] = 0;
          out[1] = 0;
          for (int b = 0; b < 64; b++) {
            out[b >> 5] |= (value & 1) << (b & 31);

            value = value >> 1;
          }
          Uint32 defaultLen = field_used_length(field);
          defaultLen = ((defaultLen + 3) & ~(Uint32)0x7);
          defaults_aligned = (0 == memcmp(ndb_default, out, defaultLen));
        }
      }

      field->move_field_offset(-src_offset);

      if (unlikely(!defaults_aligned)) {
        ndb_log_error(
            "Internal error, Default values differ "
            "for column %u, ndb_default: %d",
            field->field_index(), ndb_default != nullptr);
      }
    } else {
      /* Don't expect Ndb to have a native default for this column */
      if (unlikely(ndbCol->getDefaultValue() != nullptr)) {
        /* Didn't expect that */
        ndb_log_error(
            "Internal error, Column %u has native "
            "default, but shouldn't. Flags=%u, type=%u",
            field->field_index(), field->all_flags(), field->real_type());
        defaults_aligned = false;
      }
    }
    if (unlikely(!defaults_aligned)) {
      // Dump field
      ndb_log_error(
          "field[ name: '%s', type: %u, real_type: %u, "
          "flags: 0x%x, is_null: %d]",
          field->field_name, field->type(), field->real_type(),
          field->all_flags(), field->is_null());
      // Dump ndbCol
      ndb_log_error(
          "ndbCol[name: '%s', type: %u, column_no: %d, "
          "nullable: %d]",
          ndbCol->getName(), ndbCol->getType(), ndbCol->getColumnNo(),
          ndbCol->getNullable());
      break;
    }
  }
  tmp_restore_column_map(table->read_set, old_map);

  return defaults_aligned;
}
#endif

int ha_ndbcluster::get_metadata(Ndb *ndb, const char *dbname,
                                const char *tabname,
                                const dd::Table *table_def) {
  DBUG_TRACE;

  // The NDB table should not be open
  assert(m_table == nullptr);
  assert(m_trans_table_stats == nullptr);

  Ndb_dd_handle dd_handle = ndb_dd_table_get_spi_and_version(table_def);
  if (!dd_handle.valid()) {
    DBUG_PRINT("error", ("Could not extract object_id and object_version "
                         "from table definition"));
    return 1;
  }

  Ndb_table_guard ndbtab_g(ndb, dbname, tabname);
  const NDBTAB *tab = ndbtab_g.get_table();
  if (tab == nullptr) {
    ERR_RETURN(ndbtab_g.getNdbError());
  }

  {
    // Check that the id and version from DD
    // matches the id and version of the NDB table
    Ndb_dd_handle curr_handle{tab->getObjectId(), tab->getObjectVersion()};
    if (curr_handle != dd_handle) {
      DBUG_PRINT("error", ("Table id or version mismatch"));
      DBUG_PRINT("error", ("NDB table id: %llu, version: %d", curr_handle.spi,
                           curr_handle.version));
      DBUG_PRINT("error", ("DD table id: %llu, version: %d", dd_handle.spi,
                           dd_handle.version));

      ndb_log_verbose(10,
                      "Table id or version mismatch for table '%s.%s', "
                      "[%llu, %d] != [%llu, %d]",
                      dbname, tabname, dd_handle.spi, dd_handle.version,
                      curr_handle.spi, curr_handle.version);

      ndbtab_g.invalidate();

      // When returning HA_ERR_TABLE_DEF_CHANGED from handler::open()
      // the caller is intended to call ha_discover() in order to let
      // the engine install the correct table definition in the
      // data dictionary, then the open() will be retried and presumably
      // the table definition will be correct
      return HA_ERR_TABLE_DEF_CHANGED;
    }
  }

  if (DBUG_EVALUATE_IF("ndb_get_metadata_fail", true, false)) {
    fprintf(stderr, "ndb_get_metadata_fail\n");
    DBUG_SET("-d,ndb_get_metadata_fail");
    ndbtab_g.invalidate();
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  // Remember the opened NDB table
  m_table = tab;

  // Create field to column map for table
  m_table_map = new Ndb_table_map(table, m_table);

  // Check that NDB default values match those in MySQL table def.
  assert(check_default_values());

  ndb_bitmap_init(&m_bitmap, m_bitmap_buf, table_share->fields);

  NDBDICT *dict = ndb->getDictionary();
  int error = 0;
  if (table_share->primary_key == MAX_KEY) {
    /* Hidden primary key. */
    if ((error = add_hidden_pk_ndb_record(dict)) != 0) goto err;
  }

  if ((error = add_table_ndb_record(dict)) != 0) goto err;

  // Approximate row size
  m_bytes_per_write = 12 + tab->getRowSizeInBytes() + 4 * tab->getNoOfColumns();

  /* Open indexes */
  if ((error = open_indexes(dict)) != 0) goto err;

  /*
    Backward compatibility for tables created without tablespace
    in .frm => read tablespace setting from engine
  */
  if (table_share->mysql_version < 50120 &&
      !table_share->tablespace /* safety */) {
    Uint32 id;
    if (tab->getTablespace(&id)) {
      NdbDictionary::Tablespace ts = dict->getTablespace(id);
      if (ndb_dict_check_NDB_error(dict)) {
        const char *tablespace = ts.getName();
        const size_t tablespace_len = strlen(tablespace);
        if (tablespace_len != 0) {
          DBUG_PRINT("info", ("Found tablespace '%s'", tablespace));
          table_share->tablespace =
              strmake_root(&table_share->mem_root, tablespace, tablespace_len);
        }
      }
    }
  }

  // Tell the Ndb_table_guard to release ownership of the NDB table def since
  // it's now owned by this ha_ndbcluster instance
  ndbtab_g.release();

  return 0;

err:
  // Function failed, release all resources allocated by this function
  // before returning
  release_indexes(dict, true /* invalidate */);

  //  Release field to column map
  if (m_table_map != nullptr) {
    delete m_table_map;
    m_table_map = nullptr;
  }
  // Release NdbRecord's allocated for the table
  if (m_ndb_record != nullptr) {
    dict->releaseRecord(m_ndb_record);
    m_ndb_record = nullptr;
  }
  if (m_ndb_hidden_key_record != nullptr) {
    dict->releaseRecord(m_ndb_hidden_key_record);
    m_ndb_hidden_key_record = nullptr;
  }

  ndbtab_g.invalidate();
  m_table = nullptr;
  return error;
}

/**
   @brief Create Attrid_map for mapping the columns of KEY to a NDB index.
   @param key_info key to create mapping for
   @param index NDB index definition
 */
NDB_INDEX_DATA::Attrid_map::Attrid_map(const KEY *key_info,
                                       const NdbDictionary::Index *index) {
  m_ids.reserve(key_info->user_defined_key_parts);

  for (unsigned i = 0; i < key_info->user_defined_key_parts; i++) {
    const KEY_PART_INFO *key_part = key_info->key_part + i;
    const char *key_part_name = key_part->field->field_name;

    // Find the NDB index column by name
    for (unsigned j = 0; j < index->getNoOfColumns(); j++) {
      const NdbDictionary::Column *column = index->getColumn(j);
      if (strcmp(key_part_name, column->getName()) == 0) {
        // Save id of NDB index column
        m_ids.push_back(j);
        break;
      }
    }
  }
  // Must have found one NDB column for each key
  ndbcluster::ndbrequire(m_ids.size() == key_info->user_defined_key_parts);
  // Check that the map is not ordered
  assert(std::is_sorted(m_ids.begin(), m_ids.end()) == false);
}

/**
   @brief Create Attrid_map for mapping the columns of KEY to a NDB table.
   @param key_info key to create mapping for
   @param table NDB table definition
 */
NDB_INDEX_DATA::Attrid_map::Attrid_map(const KEY *key_info,
                                       const NdbDictionary::Table *table) {
  m_ids.reserve(key_info->user_defined_key_parts);

  uint key_pos = 0;
  int columnnr = 0;
  const KEY_PART_INFO *key_part = key_info->key_part;
  const KEY_PART_INFO *end = key_part + key_info->user_defined_key_parts;
  for (; key_part != end; key_part++) {
    // As NdbColumnImpl::m_keyInfoPos isn't available through
    // NDB API it has to be calculated, else it could have been retrieved with
    //   table->getColumn(key_part->fieldnr-1)->m_impl.m_keyInfoPos;

    if (key_part->fieldnr < columnnr) {
      // PK columns are not in same order as the columns are defined in the
      // table, Restart PK search from first column:
      key_pos = 0;
      columnnr = 0;
    }

    while (columnnr < key_part->fieldnr - 1) {
      if (table->getColumn(columnnr++)->getPrimaryKey()) {
        key_pos++;
      }
    }

    assert(table->getColumn(columnnr)->getPrimaryKey());
    // Save id of NDB column
    m_ids.push_back(key_pos);

    columnnr++;
    key_pos++;
  }
  // Must have found one NDB column for each key
  ndbcluster::ndbrequire(m_ids.size() == key_info->user_defined_key_parts);
  // Check that the map is not ordered
  assert(std::is_sorted(m_ids.begin(), m_ids.end()) == false);
}

void NDB_INDEX_DATA::Attrid_map::fill_column_map(uint column_map[]) const {
  assert(m_ids.size());
  for (size_t i = 0; i < m_ids.size(); i++) {
    column_map[i] = m_ids[i];
  }
}

/**
   @brief Check if columns in KEY is ordered
   @param key_info key to check
   @return true if columns are ordered
   @note the function actually don't check for consecutive numbers. The
   assumption is that if columns are in same order they will be consecutive. i.e
   [0,1,2...] and not [0,3,6,...]
 */
static bool check_ordered_columns(const KEY *key_info) {
  int columnnr = 0;
  const KEY_PART_INFO *key_part = key_info->key_part;
  const KEY_PART_INFO *end = key_part + key_info->user_defined_key_parts;
  for (; key_part != end; key_part++) {
    if (key_part->fieldnr < columnnr) {
      // PK columns are not in same order as the columns in the table
      DBUG_PRINT("info", ("Detected different order in table"));
      return false;
    }

    while (columnnr < key_part->fieldnr - 1) {
      columnnr++;
    }
    columnnr++;
  }
  return true;
}

void NDB_INDEX_DATA::create_attrid_map(const KEY *key_info,
                                       const NdbDictionary::Table *table) {
  DBUG_TRACE;
  assert(!attrid_map);  // Should not already have been created

  if (key_info->user_defined_key_parts == 1) {
    DBUG_PRINT("info", ("Skip creating map for index with only one column"));
    return;
  }

  if (check_ordered_columns(key_info)) {
    DBUG_PRINT("info", ("Skip creating map for table with same order"));
    return;
  }

  attrid_map = new Attrid_map(key_info, table);
}

/**
   Check if columns in KEY matches the order of the index
   @param key_info key to check
   @param index NDB index to compare with
   @return true if columns in KEY and index have same order
 */
static bool check_same_order_in_index(const KEY *key_info,
                                      const NdbDictionary::Index *index) {
  // Check if key and NDB column order is same
  for (unsigned i = 0; i < key_info->user_defined_key_parts; i++) {
    const KEY_PART_INFO *key_part = key_info->key_part + i;
    const char *key_part_name = key_part->field->field_name;
    for (unsigned j = 0; j < index->getNoOfColumns(); j++) {
      const NdbDictionary::Column *column = index->getColumn(j);
      if (strcmp(key_part_name, column->getName()) == 0) {
        if (i != j) {
          DBUG_PRINT("info", ("Detected different order in index"));
          return false;
        }
        break;
      }
    }
  }
  return true;
}

void NDB_INDEX_DATA::create_attrid_map(const KEY *key_info,
                                       const NdbDictionary::Index *index) {
  DBUG_TRACE;
  assert(index);
  assert(!attrid_map);  // Should not already have been created

  if (key_info->user_defined_key_parts == 1) {
    DBUG_PRINT("info", ("Skip creating map for index with only one column"));
    return;
  }

  if (check_same_order_in_index(key_info, index)) {
    DBUG_PRINT("info", ("Skip creating map for index with same order"));
    return;
  }

  attrid_map = new Attrid_map(key_info, index);
}

void NDB_INDEX_DATA::delete_attrid_map() {
  delete attrid_map;
  attrid_map = nullptr;
}

void NDB_INDEX_DATA::fill_column_map(const KEY *key_info,
                                     uint column_map[]) const {
  if (attrid_map) {
    // Use the cached Attrid_map
    attrid_map->fill_column_map(column_map);
    return;
  }
  // Use the default sequential column order
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    column_map[i] = i;
  }
}

/**
  @brief Create all the indexes for a table.
  @note If any index should fail to be created, the error is returned
  immediately
*/
int ha_ndbcluster::create_indexes(THD *thd, TABLE *tab,
                                  const NdbDictionary::Table *ndbtab) const {
  int error = 0;
  const KEY *key_info = tab->key_info;
  const char **key_name = tab->s->keynames.type_names;
  DBUG_TRACE;

  for (uint i = 0; i < tab->s->keys; i++, key_info++, key_name++) {
    const char *index_name = *key_name;
    NDB_INDEX_TYPE idx_type = get_declared_index_type(i);
    error = create_index(thd, index_name, key_info, idx_type, ndbtab);
    if (error) {
      DBUG_PRINT("error", ("Failed to create index %u", i));
      break;
    }
  }

  return error;
}

static void ndb_protect_char(const char *from, char *to, uint to_length,
                             char protect) {
  uint fpos = 0, tpos = 0;

  while (from[fpos] != '\0' && tpos < to_length - 1) {
    if (from[fpos] == protect) {
      int len = 0;
      to[tpos++] = '@';
      if (tpos < to_length - 5) {
        len = sprintf(to + tpos, "00%u", (uint)protect);
        tpos += len;
      }
    } else {
      to[tpos++] = from[fpos];
    }
    fpos++;
  }
  to[tpos] = '\0';
}

int ha_ndbcluster::open_index(NdbDictionary::Dictionary *dict,
                              const KEY *key_info, const char *key_name,
                              uint index_no) {
  DBUG_TRACE;

  NDB_INDEX_TYPE idx_type = get_declared_index_type(index_no);
  NDB_INDEX_DATA &index_data = m_index[index_no];

  char index_name[FN_LEN + 1];
  ndb_protect_char(key_name, index_name, sizeof(index_name) - 1, '/');
  if (idx_type != PRIMARY_KEY_INDEX && idx_type != UNIQUE_INDEX) {
    DBUG_PRINT("info", ("Get handle to index %s", index_name));
    const NdbDictionary::Index *index =
        dict->getIndexGlobal(index_name, *m_table);
    if (index) {
      DBUG_PRINT("info",
                 ("index: %p  id: %d  version: %d.%d  status: %d", index,
                  index->getObjectId(), index->getObjectVersion() & 0xFFFFFF,
                  index->getObjectVersion() >> 24, index->getObjectStatus()));
      assert(index->getObjectStatus() == NdbDictionary::Object::Retrieved);
      index_data.index = index;
    } else {
      const NdbError &err = dict->getNdbError();
      if (err.code != 4243) ERR_RETURN(err);
      // Index Not Found. Proceed with this index unavailable.
    }
  }

  if (idx_type == UNIQUE_ORDERED_INDEX || idx_type == UNIQUE_INDEX) {
    char unique_index_name[FN_LEN + 1];
    static const char *unique_suffix = "$unique";
    strxnmov(unique_index_name, FN_LEN, index_name, unique_suffix, NullS);
    DBUG_PRINT("info", ("Get handle to unique_index %s", unique_index_name));
    const NdbDictionary::Index *index =
        dict->getIndexGlobal(unique_index_name, *m_table);
    if (index) {
      DBUG_PRINT("info",
                 ("index: %p  id: %d  version: %d.%d  status: %d", index,
                  index->getObjectId(), index->getObjectVersion() & 0xFFFFFF,
                  index->getObjectVersion() >> 24, index->getObjectStatus()));
      assert(index->getObjectStatus() == NdbDictionary::Object::Retrieved);
      m_has_unique_index = true;
      index_data.unique_index = index;
      // Create attrid map for unique index
      index_data.create_attrid_map(key_info, index);
    } else {
      const NdbError &err = dict->getNdbError();
      if (err.code != 4243) ERR_RETURN(err);
      // Index Not Found. Proceed with this index unavailable.
    }
  }

  /* Set type of index as actually opened */
  switch (idx_type) {
    case UNDEFINED_INDEX:
      assert(false);
      break;
    case PRIMARY_KEY_INDEX:
      break;
    case PRIMARY_KEY_ORDERED_INDEX:
      if (!index_data.index) idx_type = PRIMARY_KEY_INDEX;
      break;
    case UNIQUE_INDEX:
      if (!index_data.unique_index) idx_type = UNDEFINED_INDEX;
      break;
    case UNIQUE_ORDERED_INDEX:
      if (!(index_data.unique_index || index_data.index))
        idx_type = UNDEFINED_INDEX;
      else if (!index_data.unique_index)
        idx_type = ORDERED_INDEX;
      else if (!index_data.index)
        idx_type = UNIQUE_INDEX;
      break;
    case ORDERED_INDEX:
      if (!index_data.index) idx_type = UNDEFINED_INDEX;
      break;
  }
  index_data.type = idx_type;

  if (idx_type == UNDEFINED_INDEX) return 0;

  if (idx_type == PRIMARY_KEY_ORDERED_INDEX || idx_type == PRIMARY_KEY_INDEX) {
    // Create attrid map for primary key
    index_data.create_attrid_map(key_info, m_table);
  }

  return open_index_ndb_record(dict, key_info, index_no);
}

/*
  We use this function to convert null bit masks, as found in class Field,
  to bit numbers, as used in NdbRecord.
*/
static uint null_bit_mask_to_bit_number(uchar bit_mask) {
  switch (bit_mask) {
    case 0x1:
      return 0;
    case 0x2:
      return 1;
    case 0x4:
      return 2;
    case 0x8:
      return 3;
    case 0x10:
      return 4;
    case 0x20:
      return 5;
    case 0x40:
      return 6;
    case 0x80:
      return 7;
    default:
      assert(false);
      return 0;
  }
}

static void ndb_set_record_specification(
    uint field_no, NdbDictionary::RecordSpecification *spec, const TABLE *table,
    const NdbDictionary::Column *ndb_column, uint32 *row_side_buffer_size,
    MY_BITMAP &in_row_side_buffer, uint fields) {
  DBUG_TRACE;
  assert(ndb_column);
  spec->column = ndb_column;
  spec->offset = Uint32(table->field[field_no]->offset(table->record[0]));
  if (table->field[field_no]->is_nullable()) {
    spec->nullbit_byte_offset = Uint32(table->field[field_no]->null_offset());
    spec->nullbit_bit_in_byte =
        null_bit_mask_to_bit_number(table->field[field_no]->null_bit);
  } else if (table->field[field_no]->type() == MYSQL_TYPE_BIT) {
    /* We need to store the position of the overflow bits. */
    const Field_bit *field_bit =
        static_cast<Field_bit *>(table->field[field_no]);
    spec->nullbit_byte_offset = Uint32(field_bit->bit_ptr - table->record[0]);
    spec->nullbit_bit_in_byte = field_bit->bit_ofs;
  } else {
    spec->nullbit_byte_offset = 0;
    spec->nullbit_bit_in_byte = 0;
  }
  spec->column_flags = 0;
  if (table->field[field_no]->type() == MYSQL_TYPE_STRING &&
      table->field[field_no]->pack_length() == 0) {
    /*
      This is CHAR(0), which we represent as
      a nullable BIT(1) column where we ignore the data bit
    */
    spec->column_flags |=
        NdbDictionary::RecordSpecification::BitColMapsNullBitOnly;
  } else if (table->field[field_no]->type() == MYSQL_TYPE_VECTOR) {
    assert(ndb_column->getType() == NDBCOL::Longvarbinary);
    spec->column_flags |= NdbDictionary::RecordSpecification::MysqldLongBlob;
    *row_side_buffer_size += ndb_column->getLength();
    // If first blob column and no bit map allocated do allocate
    if (!bitmap_is_valid(&in_row_side_buffer))
      bitmap_init(&in_row_side_buffer, nullptr, fields);
    bitmap_set_bit(&in_row_side_buffer, field_no);
  }
  DBUG_PRINT("info",
             ("%s.%s field: %d, col: %d, offset: %d, null bit: %d",
              table->s->table_name.str, ndb_column->getName(), field_no,
              ndb_column->getColumnNo(), spec->offset,
              (8 * spec->nullbit_byte_offset) + spec->nullbit_bit_in_byte));
}

int ha_ndbcluster::add_table_ndb_record(NdbDictionary::Dictionary *dict) {
  DBUG_TRACE;
  NdbDictionary::RecordSpecification spec[NDB_MAX_ATTRIBUTES_IN_TABLE + 2];
  NdbRecord *rec;
  uint fieldId, colId;

  uint32 row_side_buffer_size = 0;
  for (fieldId = 0, colId = 0; fieldId < table_share->fields; fieldId++) {
    if (table->field[fieldId]->stored_in_db) {
      ndb_set_record_specification(
          fieldId, &spec[colId], table, m_table->getColumn(colId),
          &row_side_buffer_size, /*by-ref*/ m_in_row_side_buffer,
          table_share->fields);
      colId++;
    }
  }

  rec = dict->createRecord(
      m_table, (colId > 0) ? spec : nullptr, colId, sizeof(spec[0]),
      NdbDictionary::RecMysqldBitfield | NdbDictionary::RecPerColumnFlags);
  if (!rec) ERR_RETURN(dict->getNdbError());
  m_ndb_record = rec;

  if (row_side_buffer_size) {
    m_row_side_buffer_size = row_side_buffer_size;
    m_row_side_buffer = (uchar *)table->s->mem_root.Alloc(row_side_buffer_size);
  } else {
    m_row_side_buffer_size = 0;
    m_row_side_buffer = nullptr;
  }
  m_mrr_reclength = table_share->reclength + row_side_buffer_size;

  return 0;
}

/* Create NdbRecord for setting hidden primary key from Uint64. */
int ha_ndbcluster::add_hidden_pk_ndb_record(NdbDictionary::Dictionary *dict) {
  DBUG_TRACE;
  NdbDictionary::RecordSpecification spec[1];
  NdbRecord *rec;

  spec[0].column = m_table->getColumn(m_table_map->get_hidden_key_column());
  spec[0].offset = 0;
  spec[0].nullbit_byte_offset = 0;
  spec[0].nullbit_bit_in_byte = 0;

  rec = dict->createRecord(m_table, spec, 1, sizeof(spec[0]));
  if (!rec) ERR_RETURN(dict->getNdbError());
  m_ndb_hidden_key_record = rec;

  return 0;
}

int ha_ndbcluster::open_index_ndb_record(NdbDictionary::Dictionary *dict,
                                         const KEY *key_info, uint index_no) {
  DBUG_TRACE;
  NdbDictionary::RecordSpecification spec[NDB_MAX_ATTRIBUTES_IN_TABLE + 2];
  NdbRecord *rec;

  Uint32 offset = 0;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    KEY_PART_INFO *kp = &key_info->key_part[i];
    spec[i].column = m_table_map->getColumn(kp->fieldnr - 1);
    if (!spec[i].column) ERR_RETURN(dict->getNdbError());
    if (kp->null_bit) {
      /* Nullable column. */
      spec[i].offset = offset + 1;  // First byte is NULL flag
      spec[i].nullbit_byte_offset = offset;
      spec[i].nullbit_bit_in_byte = 0;
    } else {
      /* Not nullable column. */
      spec[i].offset = offset;
      spec[i].nullbit_byte_offset = 0;
      spec[i].nullbit_bit_in_byte = 0;
    }
    offset += kp->store_length;
  }

  if (m_index[index_no].index) {
    /*
      Enable MysqldShrinkVarchar flag so that the two-byte length used by
      mysqld for short varchar keys is correctly converted into a one-byte
      length used by Ndb kernel.
    */
    rec = dict->createRecord(m_index[index_no].index, m_table, spec,
                             key_info->user_defined_key_parts, sizeof(spec[0]),
                             (NdbDictionary::RecMysqldShrinkVarchar |
                              NdbDictionary::RecMysqldBitfield));
    if (!rec) ERR_RETURN(dict->getNdbError());
    m_index[index_no].ndb_record_key = rec;
  } else
    m_index[index_no].ndb_record_key = nullptr;

  if (m_index[index_no].unique_index) {
    rec = dict->createRecord(m_index[index_no].unique_index, m_table, spec,
                             key_info->user_defined_key_parts, sizeof(spec[0]),
                             (NdbDictionary::RecMysqldShrinkVarchar |
                              NdbDictionary::RecMysqldBitfield));
    if (!rec) ERR_RETURN(dict->getNdbError());
    m_index[index_no].ndb_unique_record_key = rec;
  } else if (index_no == table_share->primary_key) {
    /* The primary key is special, there is no explicit NDB index associated. */
    rec = dict->createRecord(m_table, spec, key_info->user_defined_key_parts,
                             sizeof(spec[0]),
                             (NdbDictionary::RecMysqldShrinkVarchar |
                              NdbDictionary::RecMysqldBitfield));
    if (!rec) ERR_RETURN(dict->getNdbError());
    m_index[index_no].ndb_unique_record_key = rec;
  } else
    m_index[index_no].ndb_unique_record_key = nullptr;

  /* Now do the same, but this time with offsets from Field, for row access. */
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    const KEY_PART_INFO *kp = &key_info->key_part[i];

    spec[i].offset = kp->offset;
    if (kp->null_bit) {
      /* Nullable column. */
      spec[i].nullbit_byte_offset = kp->null_offset;
      spec[i].nullbit_bit_in_byte = null_bit_mask_to_bit_number(kp->null_bit);
    } else {
      /* Not nullable column. */
      spec[i].nullbit_byte_offset = 0;
      spec[i].nullbit_bit_in_byte = 0;
    }
  }

  if (m_index[index_no].unique_index) {
    rec = dict->createRecord(m_index[index_no].unique_index, m_table, spec,
                             key_info->user_defined_key_parts, sizeof(spec[0]),
                             NdbDictionary::RecMysqldBitfield);
    if (!rec) ERR_RETURN(dict->getNdbError());
    m_index[index_no].ndb_unique_record_row = rec;
  } else if (index_no == table_share->primary_key) {
    rec = dict->createRecord(m_table, spec, key_info->user_defined_key_parts,
                             sizeof(spec[0]), NdbDictionary::RecMysqldBitfield);
    if (!rec) ERR_RETURN(dict->getNdbError());
    m_index[index_no].ndb_unique_record_row = rec;
  } else
    m_index[index_no].ndb_unique_record_row = nullptr;

  return 0;
}

static bool check_index_fields_not_null(const KEY *key_info) {
  DBUG_TRACE;
  const KEY_PART_INFO *key_part = key_info->key_part;
  const KEY_PART_INFO *end = key_part + key_info->user_defined_key_parts;
  for (; key_part != end; key_part++) {
    const Field *field = key_part->field;
    if (field->is_nullable()) return true;
  }
  return false;
}

/**
  @brief Open handles to physical indexes in NDB and create NdbRecord's for
  accessing NDB via the index. The intention is to setup this handler instance
  for efficient DML processing in the transaction code path.

  @param dict NdbDictionary pointer
  @return 0 if successful, otherwise random error returned from NdbApi, message
  pushed as warning
*/
int ha_ndbcluster::open_indexes(NdbDictionary::Dictionary *dict) {
  DBUG_TRACE;

  // Flag indicating if table has unique index will be turned on as a sideffect
  // of the below loop if table has unique index
  m_has_unique_index = false;

  const KEY *key_info = table->key_info;
  const char **key_name = table->s->keynames.type_names;
  for (uint i = 0; i < table->s->keys; i++, key_info++, key_name++) {
    const int error = open_index(dict, key_info, *key_name, i);
    if (error) {
      return error;
    }
    m_index[i].null_in_unique_index = check_index_fields_not_null(key_info);
  }

  return 0;
}

/**
   @brief Close handles to physical indexes in NDB and release NdbRecord's
   @param dict NdbDictionary pointer
   @param invalidate Invalidate the index in NdbApi dict cache when
   reference to the NdbApi index is released.
 */
void ha_ndbcluster::release_indexes(NdbDictionary::Dictionary *dict,
                                    bool invalidate) {
  DBUG_TRACE;
  for (NDB_INDEX_DATA &index_data : m_index) {
    if (index_data.unique_index) {
      // Release reference to unique index in NdbAPI
      dict->removeIndexGlobal(*index_data.unique_index, invalidate);
      index_data.unique_index = nullptr;
    }
    if (index_data.index) {
      // Release reference to index in NdbAPI
      dict->removeIndexGlobal(*index_data.index, invalidate);
      index_data.index = nullptr;
    }
    index_data.delete_attrid_map();

    if (index_data.ndb_record_key) {
      dict->releaseRecord(index_data.ndb_record_key);
      index_data.ndb_record_key = nullptr;
    }
    if (index_data.ndb_unique_record_key) {
      dict->releaseRecord(index_data.ndb_unique_record_key);
      index_data.ndb_unique_record_key = nullptr;
    }
    if (index_data.ndb_unique_record_row) {
      dict->releaseRecord(index_data.ndb_unique_record_row);
      index_data.ndb_unique_record_row = nullptr;
    }
    index_data.type = UNDEFINED_INDEX;
  }
}

/**
  @brief Drop all physical NDB indexes for one MySQL index from NDB
  @param dict NdbDictionary pointer
  @param index_num Number of the index in m_index array
  @return 0 if successful, otherwise random error returned from NdbApi, message
  pushed as warning
*/
int ha_ndbcluster::inplace__drop_index(NdbDictionary::Dictionary *dict,
                                       uint index_num) {
  DBUG_TRACE;

  const NdbDictionary::Index *unique_index = m_index[index_num].unique_index;
  if (unique_index) {
    DBUG_PRINT("info", ("Drop unique index: %s", unique_index->getName()));
    // Drop unique index from NDB
    if (dict->dropIndexGlobal(*unique_index) != 0) {
      m_dupkey = index_num;  // for HA_ERR_DROP_INDEX_FK
      return ndb_to_mysql_error(&dict->getNdbError());
    }
  }

  const NdbDictionary::Index *index = m_index[index_num].index;
  if (index) {
    DBUG_PRINT("info", ("Drop index: %s", index->getName()));
    // Drop ordered index from NDB
    if (dict->dropIndexGlobal(*index) != 0) {
      m_dupkey = index_num;  // for HA_ERR_DROP_INDEX_FK
      return ndb_to_mysql_error(&dict->getNdbError());
    }
  }

  return 0;
}

/**
  Decode the declared type of an index from information
  provided in table object.
*/
NDB_INDEX_TYPE get_index_type_from_key(uint index_num, const KEY *key_info,
                                       bool primary) {
  const bool is_hash_index = (key_info[index_num].algorithm == HA_KEY_ALG_HASH);
  if (primary)
    return is_hash_index ? PRIMARY_KEY_INDEX : PRIMARY_KEY_ORDERED_INDEX;

  if (!(key_info[index_num].flags & HA_NOSAME)) return ORDERED_INDEX;

  return is_hash_index ? UNIQUE_INDEX : UNIQUE_ORDERED_INDEX;
}

inline NDB_INDEX_TYPE ha_ndbcluster::get_declared_index_type(uint idxno) const {
  return get_index_type_from_key(idxno, table_share->key_info,
                                 idxno == table_share->primary_key);
}

/* Return the actual type of the index as currently available
 */
NDB_INDEX_TYPE ha_ndbcluster::get_index_type(uint idx_no) const {
  assert(idx_no < MAX_KEY);
  assert(m_table);
  return m_index[idx_no].type;
}

void ha_ndbcluster::release_metadata(NdbDictionary::Dictionary *dict,
                                     bool invalidate) {
  DBUG_TRACE;
  DBUG_PRINT("enter", ("invalidate: %d", invalidate));

  if (m_table == nullptr) {
    return;  // table already released
  }

  if (invalidate == false &&
      m_table->getObjectStatus() == NdbDictionary::Object::Invalid) {
    DBUG_PRINT("info", ("table status invalid -> invalidate both table and "
                        "indexes in 'global dict cache'"));
    invalidate = true;
  }

  if (m_ndb_record != nullptr) {
    dict->releaseRecord(m_ndb_record);
    m_ndb_record = nullptr;
  }
  if (m_ndb_hidden_key_record != nullptr) {
    dict->releaseRecord(m_ndb_hidden_key_record);
    m_ndb_hidden_key_record = nullptr;
  }

  dict->removeTableGlobal(*m_table, invalidate);
  m_table = nullptr;

  release_indexes(dict, invalidate);

  // NOTE! Sometimes set here but should really be reset only by trans logic
  m_trans_table_stats = nullptr;

  //  Release field to column map
  delete m_table_map;
  m_table_map = nullptr;
}

/*
  Map from thr_lock_type to NdbOperation::LockMode
*/
static inline NdbOperation::LockMode get_ndb_lock_mode(
    enum thr_lock_type type) {
  if (type >= TL_WRITE_ALLOW_WRITE) return NdbOperation::LM_Exclusive;
  if (type == TL_READ_WITH_SHARED_LOCKS) return NdbOperation::LM_Read;
  return NdbOperation::LM_CommittedRead;
}

inline bool ha_ndbcluster::has_null_in_unique_index(uint idx_no) const {
  assert(idx_no < MAX_KEY);
  return m_index[idx_no].null_in_unique_index;
}

/**
  Get the flags for an index.

  The index currently available in NDB may differ from the one defined in the
  data dictionary, if ndb_restore or ndb_drop_index has caused some component
  of it to be dropped.

  Generally, index_flags() is called after the table has been open, so that the
  NdbDictionary::Table pointer in m_table is non-null, and index_flags()
  can return the flags for the index as actually available.

  But in a small number of cases index_flags() is called without an open table.
  This happens in CREATE TABLE, where index_flags() is called from
  setup_key_part_field(). It also happens in DD code as discussed in a
  long comment at fill_dd_indexes_from_keyinfo() in dd_table.cc. And it can
  happen as the result of an information_schema or SHOW query. In these cases
  index_flags() returns the flags for the index as declared in the dictionary.
*/

ulong ha_ndbcluster::index_flags(uint idx_no, uint, bool) const {
  const NDB_INDEX_TYPE index_type =
      m_table ? get_index_type(idx_no) : get_declared_index_type(idx_no);

  switch (index_type) {
    case UNDEFINED_INDEX:
      return 0;

    case PRIMARY_KEY_INDEX:
      return HA_ONLY_WHOLE_INDEX;

    case UNIQUE_INDEX:
      return HA_ONLY_WHOLE_INDEX | HA_TABLE_SCAN_ON_NULL;

    case PRIMARY_KEY_ORDERED_INDEX:
    case UNIQUE_ORDERED_INDEX:
    case ORDERED_INDEX:
      return HA_READ_NEXT | HA_READ_PREV | HA_READ_RANGE | HA_READ_ORDER |
             HA_KEY_SCAN_NOT_ROR;
  }
  assert(false);  // unreachable
  return 0;
}

bool ha_ndbcluster::primary_key_is_clustered() const {
  if (table->s->primary_key == MAX_KEY) return false;

  /*
    NOTE 1: our ordered indexes are not really clustered
    but since accessing data when scanning index is free
    it's a good approximation

    NOTE 2: We really should consider DD attributes here too
    (for which there is IO to read data when scanning index)
    but that will need to be handled later...
  */
  const NDB_INDEX_TYPE idx_type = m_index[table->s->primary_key].type;
  return (idx_type == PRIMARY_KEY_ORDERED_INDEX ||
          idx_type == UNIQUE_ORDERED_INDEX || idx_type == ORDERED_INDEX);
}

/**
  Read one record from NDB using primary key.
*/

int ha_ndbcluster::pk_read(const uchar *key, uchar *buf, uint32 *part_id) {
  NdbConnection *trans = m_thd_ndb->trans;
  DBUG_TRACE;
  assert(trans);

  NdbOperation::LockMode lm = get_ndb_lock_mode(m_lock.type);

  if (check_if_pushable(NdbQueryOperationDef::PrimaryKeyAccess,
                        table->s->primary_key)) {
    // Is parent of pushed join
    assert(lm == NdbOperation::LM_CommittedRead);
    const int error =
        pk_unique_index_read_key_pushed(table->s->primary_key, key);
    if (unlikely(error)) {
      return error;
    }

    assert(m_active_query != nullptr);
    if (execute_no_commit_ie(m_thd_ndb, trans) != 0 ||
        m_active_query->getNdbError().code)
      return ndb_err(trans);

    int result = fetch_next_pushed();
    if (result == NdbQuery::NextResult_gotRow) {
      assert(pushed_cond == nullptr ||
             const_cast<Item *>(pushed_cond)->val_int());
      return 0;
    } else if (result == NdbQuery::NextResult_scanComplete) {
      return HA_ERR_KEY_NOT_FOUND;
    } else {
      return ndb_err(trans);
    }
  } else {
    const NdbOperation *op;
    if (!(op = pk_unique_index_read_key(
              table->s->primary_key, key, buf, lm,
              (m_user_defined_partitioning ? part_id : nullptr),
              m_row_side_buffer)))
      ERR_RETURN(trans->getNdbError());

    if (execute_no_commit_ie(m_thd_ndb, trans) != 0 || op->getNdbError().code)
      return ndb_err(trans);

    if (unlikely(!m_cond.check_condition())) {
      return HA_ERR_KEY_NOT_FOUND;  // False condition
    }
    assert(pushed_cond == nullptr ||
           const_cast<Item *>(pushed_cond)->val_int());
    return 0;
  }
}

/**
  Update primary key or part id by doing delete insert.
*/

int ha_ndbcluster::ndb_pk_update_row(const uchar *old_data, uchar *new_data) {
  int error;
  DBUG_TRACE;

  DBUG_PRINT("info", ("primary key update or partition change, "
                      "doing delete+insert"));

#ifndef NDEBUG
  /*
   * 'old_data' contains columns as specified in 'read_set'.
   * All PK columns must be included for ::ndb_delete_row()
   */
  assert(bitmap_is_subset(m_pk_bitmap_p, table->read_set));
  /*
   * As a complete 'new_data' row is reinserted after the delete,
   * all columns must be contained in the read+write union.
   */
  bitmap_copy(&m_bitmap, table->read_set);
  bitmap_union(&m_bitmap, table->write_set);
  assert(bitmap_is_set_all(&m_bitmap));
#endif

  // Delete old row
  error = ndb_delete_row(old_data, true);
  if (error) {
    DBUG_PRINT("info", ("delete failed"));
    return error;
  }

  // Insert new row
  DBUG_PRINT("info", ("delete succeded"));
  bool batched_update = (m_active_cursor != nullptr);
  /*
    If we are updating a primary key with auto_increment
    then we need to update the auto_increment counter
  */
  if (table->found_next_number_field &&
      bitmap_is_set(table->write_set,
                    table->found_next_number_field->field_index()) &&
      (error = set_auto_inc(m_thd_ndb->ndb, table->found_next_number_field))) {
    return error;
  }

  /*
    We are mapping a MySQLD PK changing update to an NdbApi delete
    and insert.
    The original PK changing update may not have written new values
    to all columns, so the write set may be partial.
    We set the write set to be all columns so that all values are
    copied from the old row to the new row.
  */
  my_bitmap_map *old_map = tmp_use_all_columns(table, table->write_set);
  error = ndb_write_row(new_data, true, batched_update);
  tmp_restore_column_map(table->write_set, old_map);

  if (error) {
    DBUG_PRINT("info", ("insert failed"));
    if (m_thd_ndb->trans->commitStatus() == NdbConnection::Started) {
      Ndb_applier *const applier = m_thd_ndb->get_applier();
      if (applier) {
        applier->atTransactionAbort();
      }
      m_thd_ndb->m_unsent_bytes = 0;
      m_thd_ndb->m_unsent_blob_ops = false;
      m_thd_ndb->m_execute_count++;
      DBUG_PRINT("info", ("execute_count: %u", m_thd_ndb->m_execute_count));
      m_thd_ndb->trans->execute(NdbTransaction::Rollback);
    }
    return error;
  }
  DBUG_PRINT("info", ("delete+insert succeeded"));

  return 0;
}

bool ha_ndbcluster::peek_index_rows_check_index_fields_in_write_set(
    const KEY *key_info) const {
  DBUG_TRACE;

  KEY_PART_INFO *key_part = key_info->key_part;
  const KEY_PART_INFO *const end = key_part + key_info->user_defined_key_parts;

  for (; key_part != end; key_part++) {
    Field *field = key_part->field;
    if (!bitmap_is_set(table->write_set, field->field_index())) {
      return false;
    }
  }

  return true;
}

/**
  Check if any operation used for the speculative "peek index rows" read has
  succeeded. Finding a successful read indicates that a conflicting key already
  exists and thus the peek has failed.

  @param trans   The transaction owning the operations to check
  @param first   First operation to check
  @param last    Last operation to check (may point to same operation as first)

  @note Function requires that at least one read operation has been defined in
        transactions.

  @return true peek succeeded, no duplicate rows was found
  @return false peek failed, at least one duplicate row was found. The number of
          the index where it was a duplicate key is available in m_dupkey.

 */
bool ha_ndbcluster::peek_index_rows_check_ops(NdbTransaction *trans,
                                              const NdbOperation *first,
                                              const NdbOperation *last) {
  DBUG_TRACE;
  ndbcluster::ndbrequire(first != nullptr);
  ndbcluster::ndbrequire(last != nullptr);

  const NdbOperation *op = first;
  while (op) {
    const NdbError err = op->getNdbError();
    if (err.status == NdbError::Success) {
      // One "peek index rows" read has succeeded, this means there is a
      // duplicate entry in the primary or unique index. Assign the number of
      // that index to m_dupkey and return error.

      switch (op->getType()) {
        case NdbOperation::PrimaryKeyAccess:
          m_dupkey = table_share->primary_key;
          break;

        case NdbOperation::UniqueIndexAccess: {
          const NdbIndexOperation *iop =
              down_cast<const NdbIndexOperation *>(op);
          const NdbDictionary::Index *index = iop->getIndex();
          // Find the number of the index
          for (uint i = 0; i < table_share->keys; i++) {
            if (m_index[i].unique_index == index) {
              m_dupkey = i;
              break;  // for
            }
          }
          break;
        }

        default:
          // Internal error, since only primary and unique indexes are peeked
          // there should never be any other type of operation in the
          // transaction
          ndbcluster::ndbrequire(false);
          break;
      }
      DBUG_PRINT("info", ("m_dupkey: %u", m_dupkey));
      return false;  // Found duplicate key
    }

    // Check that this "peek index rows" read has failed because the row could
    // not be found, otherwise the caller should report this as a NDB error
    if (err.mysql_code != HA_ERR_KEY_NOT_FOUND) {
      return false;  // Some unexpected error occurred while reading from NDB
    }

    if (op == last) {
      break;
    }

    op = trans->getNextCompletedOperation(op);
  }

  return true;  // No duplicates keys found
}

// Check if record contains any null valued columns that are part of a key
static int peek_index_rows_check_null_in_record(const KEY *key_info,
                                                const uchar *record) {
  const KEY_PART_INFO *curr_part = key_info->key_part;
  const KEY_PART_INFO *const end_part =
      curr_part + key_info->user_defined_key_parts;

  while (curr_part != end_part) {
    if (curr_part->null_bit &&
        (record[curr_part->null_offset] & curr_part->null_bit))
      return 1;
    curr_part++;
  }
  return 0;
}

/* Empty mask and dummy row, for reading no attributes using NdbRecord. */
/* Mask will be initialized to all zeros by linker. */
static unsigned char empty_mask[(NDB_MAX_ATTRIBUTES_IN_TABLE + 7) / 8];
static char dummy_row[1];

/**
  Peek to check if any rows already exist with conflicting
  primary key or unique index values
*/

int ha_ndbcluster::peek_indexed_rows(const uchar *record,
                                     NDB_WRITE_OP write_op) {
  DBUG_TRACE;

  int error;
  NdbTransaction *trans;
  if (unlikely(!(trans = get_transaction(error)))) {
    return error;
  }
  const NdbOperation::LockMode lm = get_ndb_lock_mode(m_lock.type);

  const NdbOperation *first = nullptr;
  const NdbOperation *last = nullptr;
  if (write_op != NDB_UPDATE && table_share->primary_key != MAX_KEY) {
    // Define speculative read of row with colliding primary key
    const NdbRecord *key_rec =
        m_index[table->s->primary_key].ndb_unique_record_row;

    NdbOperation::OperationOptions options;
    NdbOperation::OperationOptions *poptions = nullptr;
    options.optionsPresent = 0;

    if (m_user_defined_partitioning) {
      uint32 part_id;
      longlong func_value;
      my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
      const int part_id_error =
          m_part_info->get_partition_id(m_part_info, &part_id, &func_value);
      dbug_tmp_restore_column_map(table->read_set, old_map);
      if (part_id_error) {
        m_part_info->err_value = func_value;
        return part_id_error;
      }
      options.optionsPresent |= NdbOperation::OperationOptions::OO_PARTITION_ID;
      options.partitionId = part_id;
      poptions = &options;
    }

    const NdbOperation *const op = trans->readTuple(
        key_rec, (const char *)record, m_ndb_record, dummy_row, lm, empty_mask,
        poptions, sizeof(NdbOperation::OperationOptions));
    if (op == nullptr) {
      ERR_RETURN(trans->getNdbError());
    }

    first = op;
    last = op;
  }

  // Define speculative read of colliding row(s) in unique indexes
  const KEY *key_info = table->key_info;
  for (uint i = 0; i < table_share->keys; i++, key_info++) {
    if (i == table_share->primary_key) {
      DBUG_PRINT("info", ("skip primary key"));
      continue;
    }

    if (key_info->flags & HA_NOSAME &&
        bitmap_is_overlapping(table->write_set, m_key_fields[i])) {
      // Unique index being written

      if (unlikely(m_index[i].type == UNDEFINED_INDEX))
        return fail_index_offline(table, i);

      /*
        It's not possible to lookup a NULL field value in a unique index. But
        since keys with NULLs are not indexed, such rows cannot conflict anyway
        -> just skip checking the index in that case.
      */
      if (peek_index_rows_check_null_in_record(key_info, record)) {
        DBUG_PRINT("info", ("skipping check for key with NULL"));
        continue;
      }

      if (write_op != NDB_INSERT &&
          !peek_index_rows_check_index_fields_in_write_set(key_info)) {
        DBUG_PRINT("info", ("skipping check for key %u not in write_set", i));
        continue;
      }

      const NdbRecord *const key_rec = m_index[i].ndb_unique_record_row;
      const NdbOperation *const iop =
          trans->readTuple(key_rec, (const char *)record, m_ndb_record,
                           dummy_row, lm, empty_mask);
      if (iop == nullptr) {
        ERR_RETURN(trans->getNdbError());
      }

      if (!first) first = iop;
      last = iop;
    }
  }

  if (first == nullptr) {
    // Table has no keys
    return HA_ERR_KEY_NOT_FOUND;
  }

  (void)execute_no_commit_ie(m_thd_ndb, trans);

  const NdbError ndberr = trans->getNdbError();
  error = ndberr.mysql_code;
  if ((error != 0 && error != HA_ERR_KEY_NOT_FOUND) ||
      peek_index_rows_check_ops(trans, first, last)) {
    return ndb_err(trans);
  }
  return 0;
}

/**
  Read one record from NDB using unique secondary index.
*/

int ha_ndbcluster::unique_index_read(const uchar *key, uchar *buf) {
  NdbTransaction *trans = m_thd_ndb->trans;
  NdbOperation::LockMode lm = get_ndb_lock_mode(m_lock.type);
  DBUG_TRACE;
  DBUG_PRINT("enter", ("index: %u, lm: %u", active_index, (unsigned int)lm));
  assert(trans);

  if (check_if_pushable(NdbQueryOperationDef::UniqueIndexAccess,
                        active_index)) {
    assert(lm == NdbOperation::LM_CommittedRead);
    const int error = pk_unique_index_read_key_pushed(active_index, key);
    if (unlikely(error)) {
      return error;
    }

    assert(m_active_query != nullptr);
    if (execute_no_commit_ie(m_thd_ndb, trans) != 0 ||
        m_active_query->getNdbError().code)
      return ndb_err(trans);

    int result = fetch_next_pushed();
    if (result == NdbQuery::NextResult_gotRow) {
      assert(pushed_cond == nullptr ||
             const_cast<Item *>(pushed_cond)->val_int());
      return 0;
    } else if (result == NdbQuery::NextResult_scanComplete) {
      return HA_ERR_KEY_NOT_FOUND;
    } else {
      return ndb_err(trans);
    }
  } else {
    const NdbOperation *op;

    if (!(op = pk_unique_index_read_key(active_index, key, buf, lm, nullptr,
                                        m_row_side_buffer)))
      ERR_RETURN(trans->getNdbError());

    if (execute_no_commit_ie(m_thd_ndb, trans) != 0 || op->getNdbError().code) {
      return ndb_err(trans);
    }

    if (unlikely(!m_cond.check_condition())) {
      return HA_ERR_KEY_NOT_FOUND;
    }
    assert(pushed_cond == nullptr ||
           const_cast<Item *>(pushed_cond)->val_int());
    return 0;
  }
}

int ha_ndbcluster::scan_handle_lock_tuple(NdbScanOperation *scanOp,
                                          NdbTransaction *trans) {
  DBUG_TRACE;
  if (m_lock_tuple) {
    /*
      Lock level m_lock.type either TL_WRITE_ALLOW_WRITE
      (SELECT FOR UPDATE) or TL_READ_WITH_SHARED_LOCKS (SELECT
      LOCK WITH SHARE MODE) and row was not explicitly unlocked
      with unlock_row() call
    */
    DBUG_PRINT("info", ("Keeping lock on scanned row"));

    if (!(scanOp->lockCurrentTuple(trans, m_ndb_record, dummy_row,
                                   empty_mask))) {
      m_lock_tuple = false;
      ERR_RETURN(trans->getNdbError());
    }

    /* Perform 'empty update' to mark the read in the binlog, iff required */
    /*
     * Lock_mode = exclusive
     * Session_state = marking_exclusive_reads
     * THEN
     * issue updateCurrentTuple with AnyValue explicitly set
     */
    if ((m_lock.type >= TL_WRITE_ALLOW_WRITE) &&
        THDVAR(current_thd, log_exclusive_reads)) {
      if (scan_log_exclusive_read(scanOp, trans)) {
        m_lock_tuple = false;
        ERR_RETURN(trans->getNdbError());
      }
    }

    m_thd_ndb->m_unsent_bytes += 12;
    m_lock_tuple = false;
  }
  return 0;
}

/*
  Some MySQL table locks are mapped to Ndb internal exclusive
  row locks to achieve part of the table locking semantics. If rows are
  not exclusively locked a new batch of rows need to be fetched.
 */
static bool table_lock_not_mapped_to_row_lock(enum thr_lock_type lock_type) {
  return (lock_type < TL_READ_NO_INSERT &&
          lock_type != TL_READ_WITH_SHARED_LOCKS);
}

inline int ha_ndbcluster::fetch_next(NdbScanOperation *cursor) {
  DBUG_TRACE;
  int local_check;
  int error;
  NdbTransaction *trans = m_thd_ndb->trans;

  assert(trans);
  if ((error = scan_handle_lock_tuple(cursor, trans)) != 0) return error;

  bool contact_ndb = table_lock_not_mapped_to_row_lock(m_lock.type);
  do {
    DBUG_PRINT("info", ("Call nextResult, contact_ndb: %d", contact_ndb));
    /*
      We can only handle one tuple with blobs at a time.
    */
    if (m_thd_ndb->m_unsent_blob_ops) {
      if (execute_no_commit(m_thd_ndb, trans, m_ignore_no_key) != 0)
        return ndb_err(trans);
    }

    /* Should be no unexamined completed operations
       nextResult() on Blobs generates Blob part read ops,
       so we will free them here
    */
    trans->releaseCompletedOpsAndQueries();

    if ((local_check = cursor->nextResult(&_m_next_row, contact_ndb,
                                          m_thd_ndb->m_force_send)) == 0) {
      /*
        Explicitly lock tuple if "select for update" or
        "select lock in share mode"
      */
      m_lock_tuple = (m_lock.type == TL_WRITE_ALLOW_WRITE ||
                      m_lock.type == TL_READ_WITH_SHARED_LOCKS);
      return 0;
    } else if (local_check == 1 || local_check == 2) {
      // 1: No more records
      // 2: No more cached records

      /*
        Before fetching more rows and releasing lock(s),
        all pending update or delete operations should
        be sent to NDB
      */
      DBUG_PRINT("info", ("thd_ndb->m_unsent_bytes: %ld",
                          (long)m_thd_ndb->m_unsent_bytes));
      if (m_thd_ndb->m_unsent_bytes) {
        if ((error = flush_bulk_insert()) != 0) return error;
      }
      contact_ndb = (local_check == 2);
    } else {
      return ndb_err(trans);
    }
  } while (local_check == 2);

  return 1;
}

int ha_ndbcluster::fetch_next_pushed() {
  DBUG_TRACE;
  assert(m_pushed_operation);

  /**
   * Only prepare result & status from this operation in pushed join.
   * Consecutive rows are prepared through ::index_read_pushed() and
   * ::index_next_pushed() which unpack and set correct status for each row.
   */
  NdbQuery::NextResultOutcome result;
  while ((result = m_pushed_operation->nextResult(
              true, m_thd_ndb->m_force_send)) == NdbQuery::NextResult_gotRow) {
    assert(m_next_row != nullptr);
    DBUG_PRINT("info", ("One more record found"));
    const int ignore =
        unpack_record_and_set_generated_fields(table->record[0], m_next_row);
    //  m_thd_ndb->m_pushed_reads++;
    if (likely(!ignore)) {
      return NdbQuery::NextResult_gotRow;
    }
  }
  if (likely(result == NdbQuery::NextResult_scanComplete)) {
    assert(m_next_row == nullptr);
    DBUG_PRINT("info", ("No more records"));
    //  m_thd_ndb->m_pushed_reads++;
    return result;
  }
  DBUG_PRINT("info", ("Error from 'nextResult()'"));
  return ndb_err(m_thd_ndb->trans);
}

/**
  Get the first record from an indexed table access being a child
  operation in a pushed join. Fetch will be from prefetched
  cached records which are materialized into the bound buffer
  areas as result of this call.
*/

int ha_ndbcluster::index_read_pushed(uchar *buf, const uchar *key,
                                     key_part_map keypart_map) {
  DBUG_TRACE;

  // Handler might have decided to not execute the pushed joins which has been
  // prepared In this case we do an unpushed index_read based on 'Plain old'
  // NdbOperations
  if (unlikely(!check_is_pushed())) {
    int res = index_read_map(buf, key, keypart_map, HA_READ_KEY_EXACT);
    return res;
  }

  assert(m_pushed_join_operation > PUSHED_ROOT);  // Child of a pushed join
  assert(m_active_query == nullptr);

  // Might need to re-establish first result row (wrt. its parents which may
  // have been navigated)
  NdbQuery::NextResultOutcome result = m_pushed_operation->firstResult();

  // Result from pushed operation will be referred by 'm_next_row' if non-NULL
  if (result == NdbQuery::NextResult_gotRow) {
    assert(m_next_row != nullptr);
    const int ignore = unpack_record_and_set_generated_fields(buf, m_next_row);
    m_thd_ndb->m_pushed_reads++;

    // Pushed join results are Ref-compared using the correlation key, not
    // the specified key (unless where it is not push-executed after all).
    // Check that we still returned a row matching the specified key.
    assert(key_cmp_if_same(
               table, key, active_index,
               calculate_key_len(table, active_index, keypart_map)) == 0);

    if (unlikely(ignore)) {
      return index_next_pushed(buf);
    }
    return 0;
  }
  assert(result != NdbQuery::NextResult_gotRow);
  DBUG_PRINT("info", ("No record found"));
  return HA_ERR_END_OF_FILE;
}

/**
  Get the next record from an indexes table access being a child
  operation in a pushed join. Fetch will be from prefetched
  cached records which are materialized into the bound buffer
  areas as result of this call.
*/
int ha_ndbcluster::index_next_pushed(uchar *buf) {
  DBUG_TRACE;

  // Handler might have decided to not execute the pushed joins which has been
  // prepared In this case we do an unpushed index_read based on 'Plain old'
  // NdbOperations
  if (unlikely(!check_is_pushed())) {
    int res = index_next(buf);
    return res;
  }

  assert(m_pushed_join_operation > PUSHED_ROOT);  // Child of a pushed join
  assert(m_active_query == nullptr);

  int res = fetch_next_pushed();
  if (res == NdbQuery::NextResult_gotRow) {
    assert(pushed_cond == nullptr ||
           const_cast<Item *>(pushed_cond)->val_int());
    return 0;
  } else if (res == NdbQuery::NextResult_scanComplete) {
    return HA_ERR_END_OF_FILE;
  }
  return ndb_err(m_thd_ndb->trans);
}

/**
  Get the next record of a started scan. Try to fetch
  it locally from NdbApi cached records if possible,
  otherwise ask NDB for more.

  @note
    If this is a update/delete make sure to not contact
    NDB before any pending ops have been sent to NDB.
*/

inline int ha_ndbcluster::next_result(uchar *buf) {
  int res;
  DBUG_TRACE;

  if (m_active_cursor) {
    while ((res = fetch_next(m_active_cursor)) == 0) {
      DBUG_PRINT("info", ("One more record found"));

      const int ignore = unpack_record(buf, m_next_row);
      if (likely(!ignore)) {
        assert(pushed_cond == nullptr ||
               const_cast<Item *>(pushed_cond)->val_int());
        return 0;  // Found a row
      }
    }
    // No rows found, or error
    if (res == 1) {
      // No more records
      DBUG_PRINT("info", ("No more records"));

      if (m_thd_ndb->sql_command() == SQLCOM_ALTER_TABLE) {
        // Detected end of scan for copying ALTER TABLE. Check commit_count of
        // the scanned (source) table in order to detect that no concurrent
        // changes has occurred.
        DEBUG_SYNC(table->in_use, "ndb.before_commit_count_check");

        if (int error =
                copying_alter.check_saved_commit_count(m_thd_ndb, m_table)) {
          return error;
        }
        DEBUG_SYNC(table->in_use, "ndb.after_commit_count_check");
      }

      return HA_ERR_END_OF_FILE;
    }
    return ndb_err(m_thd_ndb->trans);
  } else if (m_active_query) {
    res = fetch_next_pushed();
    if (res == NdbQuery::NextResult_gotRow) {
      assert(pushed_cond == nullptr ||
             const_cast<Item *>(pushed_cond)->val_int());
      return 0;  // Found a row
    } else if (res == NdbQuery::NextResult_scanComplete) {
      return HA_ERR_END_OF_FILE;
    }
    return ndb_err(m_thd_ndb->trans);
  }
  return HA_ERR_END_OF_FILE;
}

int ha_ndbcluster::log_exclusive_read(const NdbRecord *key_rec,
                                      const uchar *key, uchar *buf,
                                      Uint32 *ppartition_id) const {
  DBUG_TRACE;
  NdbOperation::OperationOptions opts;
  opts.optionsPresent = NdbOperation::OperationOptions::OO_ABORTOPTION |
                        NdbOperation::OperationOptions::OO_ANYVALUE;

  /* If the key does not exist, that is ok */
  opts.abortOption = NdbOperation::AO_IgnoreError;

  /*
     Mark the AnyValue as a read operation, so that the update
     is processed
  */
  opts.anyValue = 0;
  ndbcluster_anyvalue_set_read_op(opts.anyValue);

  if (ppartition_id != nullptr) {
    assert(m_user_defined_partitioning);
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_PARTITION_ID;
    opts.partitionId = *ppartition_id;
  }

  const NdbOperation *markingOp = m_thd_ndb->trans->updateTuple(
      key_rec, (const char *)key, m_ndb_record, (char *)buf, empty_mask, &opts,
      opts.size());
  if (!markingOp) {
    char msg[FN_REFLEN];
    snprintf(
        msg, sizeof(msg),
        "Error logging exclusive reads, failed creating markingOp, %u, %s\n",
        m_thd_ndb->trans->getNdbError().code,
        m_thd_ndb->trans->getNdbError().message);
    push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                        ER_EXCEPTIONS_WRITE_ERROR,
                        ER_THD(current_thd, ER_EXCEPTIONS_WRITE_ERROR), msg);
    /*
      By returning -1 the caller (pk_unique_index_read_key) will return
      NULL and error on transaction object will be returned.
    */
    return -1;
  }

  return 0;
}

int ha_ndbcluster::scan_log_exclusive_read(NdbScanOperation *cursor,
                                           NdbTransaction *trans) const {
  DBUG_TRACE;
  NdbOperation::OperationOptions opts;
  opts.optionsPresent = NdbOperation::OperationOptions::OO_ANYVALUE;

  /*
     Mark the AnyValue as a read operation, so that the update
     is processed
  */
  opts.anyValue = 0;
  ndbcluster_anyvalue_set_read_op(opts.anyValue);

  const NdbOperation *markingOp =
      cursor->updateCurrentTuple(trans, m_ndb_record, dummy_row, empty_mask,
                                 &opts, sizeof(NdbOperation::OperationOptions));
  if (markingOp == nullptr) {
    char msg[FN_REFLEN];
    snprintf(msg, sizeof(msg),
             "Error logging exclusive reads during scan, failed creating "
             "markingOp, %u, %s\n",
             m_thd_ndb->trans->getNdbError().code,
             m_thd_ndb->trans->getNdbError().message);
    push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                        ER_EXCEPTIONS_WRITE_ERROR,
                        ER_THD(current_thd, ER_EXCEPTIONS_WRITE_ERROR), msg);
    return -1;
  }

  return 0;
}

/**
  Do a primary key or unique key index read operation.
  The key value is taken from a buffer in mysqld key format.
*/
const NdbOperation *ha_ndbcluster::pk_unique_index_read_key(
    uint idx, const uchar *key, uchar *buf, NdbOperation::LockMode lm,
    Uint32 *ppartition_id, uchar *row_side_buffer) {
  DBUG_TRACE;
  const NdbOperation *op;
  const NdbRecord *key_rec;
  NdbOperation::OperationOptions options;
  NdbOperation::OperationOptions *poptions = nullptr;
  options.optionsPresent = 0;
  NdbOperation::GetValueSpec gets[2];

  assert(m_thd_ndb->trans);

  DBUG_PRINT("info", ("pk_unique_index_read_key of table %s",
                      table->s->table_name.str));

  if (idx != MAX_KEY)
    key_rec = m_index[idx].ndb_unique_record_key;
  else
    key_rec = m_ndb_hidden_key_record;

  /* Initialize the null bitmap, setting unused null bits to 1. */
  memset(buf, 0xff, table->s->null_bytes);

  if (table_share->primary_key == MAX_KEY) {
    get_hidden_fields_keyop(&options, gets);
    poptions = &options;
  }

  if (ppartition_id != nullptr) {
    assert(m_user_defined_partitioning);
    options.optionsPresent |= NdbOperation::OperationOptions::OO_PARTITION_ID;
    options.partitionId = *ppartition_id;
  }

  if (m_row_side_buffer_size) {
    options.optionsPresent |=
        NdbOperation::OperationOptions::OO_ROW_SIDE_BUFFER;
    options.rowSideBuffer = row_side_buffer;
    options.rowSideBufferSize = m_row_side_buffer_size;
  }

  if (options.optionsPresent) poptions = &options;
  /*
    We prepared a ScanFilter. However it turns out that we will
    do a primary/unique key readTuple which does not use ScanFilter (yet)
    We set up the handler to evaluate the condition itself
  */
  m_cond.set_condition(pushed_cond);

  get_read_set(false, idx);
  op = m_thd_ndb->trans->readTuple(
      key_rec, (const char *)key, m_ndb_record, (char *)buf, lm,
      m_table_map->get_column_mask(table->read_set), poptions,
      sizeof(NdbOperation::OperationOptions));

  if (uses_blob_value(table->read_set) &&
      get_blob_values(op, buf, table->read_set) != 0)
    return nullptr;

  /* Perform 'empty update' to mark the read in the binlog, iff required */
  /*
   * Lock_mode = exclusive
   * Index = primary or unique (always true inside this method)
   * Index is not the hidden primary key
   * Session_state = marking_exclusive_reads
   * THEN
   * issue updateTuple with AnyValue explicitly set
   */
  if ((lm == NdbOperation::LM_Exclusive) && idx != MAX_KEY &&
      THDVAR(current_thd, log_exclusive_reads)) {
    if (log_exclusive_read(key_rec, key, buf, ppartition_id) != 0)
      return nullptr;
  }

  return op;
}

static bool is_shrinked_varchar(const Field *field) {
  if (field->real_type() == MYSQL_TYPE_VARCHAR) {
    if (field->get_length_bytes() == 1) return true;
  }

  return false;
}

int ha_ndbcluster::pk_unique_index_read_key_pushed(uint idx, const uchar *key) {
  DBUG_TRACE;
  assert(m_thd_ndb->trans);
  assert(idx < MAX_KEY);

  if (m_active_query) {
    m_active_query->close(false);
    m_active_query = nullptr;
  }

  KEY *key_def = &table->key_info[idx];
  KEY_PART_INFO *key_part;

  uint i;
  Uint32 offset = 0;
  NdbQueryParamValue paramValues[ndb_pushed_join::MAX_KEY_PART];
  assert(key_def->user_defined_key_parts <= ndb_pushed_join::MAX_KEY_PART);

  uint map[ndb_pushed_join::MAX_KEY_PART];
  m_index[idx].fill_column_map(key_def, map);

  // Bind key values defining root of pushed join
  for (i = 0, key_part = key_def->key_part; i < key_def->user_defined_key_parts;
       i++, key_part++) {
    bool shrinkVarChar = is_shrinked_varchar(key_part->field);

    if (key_part->null_bit)  // Column is nullable
    {
      assert(idx != table_share->primary_key);  // PK can't be nullable
      assert(*(key + offset) == 0);  // Null values not allowed in key
                                     // Value is imm. after NULL indicator
      paramValues[map[i]] = NdbQueryParamValue(key + offset + 1, shrinkVarChar);
    } else  // Non-nullable column
    {
      paramValues[map[i]] = NdbQueryParamValue(key + offset, shrinkVarChar);
    }
    offset += key_part->store_length;
  }

  const int ret =
      create_pushed_join(paramValues, key_def->user_defined_key_parts);
  return ret;
}

/** Count number of columns in key part. */
static uint count_key_columns(const KEY *key_info, const key_range *key) {
  KEY_PART_INFO *first_key_part = key_info->key_part;
  KEY_PART_INFO *key_part_end =
      first_key_part + key_info->user_defined_key_parts;
  KEY_PART_INFO *key_part;
  uint length = 0;
  for (key_part = first_key_part; key_part < key_part_end; key_part++) {
    if (length >= key->length) break;
    length += key_part->store_length;
  }
  return (uint)(key_part - first_key_part);
}

/* Helper method to compute NDB index bounds. Note: does not set range_no. */
/* Stats queries may differ so add "from" 0:normal 1:RIR 2:RPK. */
void compute_index_bounds(NdbIndexScanOperation::IndexBound &bound,
                          const KEY *key_info, const key_range *start_key,
                          const key_range *end_key, int from) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("from: %d", from));

#ifndef NDEBUG
  DBUG_PRINT("info", ("key parts: %u length: %u",
                      key_info->user_defined_key_parts, key_info->key_length));
  {
    for (uint j = 0; j <= 1; j++) {
      const key_range *kr = (j == 0 ? start_key : end_key);
      if (kr) {
        DBUG_PRINT("info", ("key range %u: length: %u map: %lx flag: %d", j,
                            kr->length, kr->keypart_map, kr->flag));
        DBUG_DUMP("key", kr->key, kr->length);
      } else {
        DBUG_PRINT("info", ("key range %u: none", j));
      }
    }
  }
#endif

  if (start_key) {
    bound.low_key = (const char *)start_key->key;
    bound.low_key_count = count_key_columns(key_info, start_key);
    bound.low_inclusive = start_key->flag != HA_READ_AFTER_KEY &&
                          start_key->flag != HA_READ_BEFORE_KEY;
  } else {
    bound.low_key = nullptr;
    bound.low_key_count = 0;
  }

  /* RIR query for x >= 1 inexplicably passes HA_READ_KEY_EXACT. */
  if (start_key &&
      (start_key->flag == HA_READ_KEY_EXACT ||
       start_key->flag == HA_READ_PREFIX_LAST) &&
      from != 1) {
    bound.high_key = bound.low_key;
    bound.high_key_count = bound.low_key_count;
    bound.high_inclusive = true;
  } else if (end_key) {
    bound.high_key = (const char *)end_key->key;
    bound.high_key_count = count_key_columns(key_info, end_key);
    /*
      For some reason, 'where b >= 1 and b <= 3' uses HA_READ_AFTER_KEY for
      the end_key.
      So HA_READ_AFTER_KEY in end_key sets high_inclusive, even though in
      start_key it does not set low_inclusive.
    */
    bound.high_inclusive = end_key->flag != HA_READ_BEFORE_KEY;
    if (end_key->flag == HA_READ_KEY_EXACT ||
        end_key->flag == HA_READ_PREFIX_LAST) {
      bound.low_key = bound.high_key;
      bound.low_key_count = bound.high_key_count;
      bound.low_inclusive = true;
    }
  } else {
    bound.high_key = nullptr;
    bound.high_key_count = 0;
  }
  DBUG_PRINT(
      "info",
      ("start_flag=%d end_flag=%d"
       " lo_keys=%d lo_incl=%d hi_keys=%d hi_incl=%d",
       start_key ? start_key->flag : 0, end_key ? end_key->flag : 0,
       bound.low_key_count, bound.low_key_count ? bound.low_inclusive : 0,
       bound.high_key_count, bound.high_key_count ? bound.high_inclusive : 0));
}

/**
  Start ordered index scan in NDB
*/

int ha_ndbcluster::ordered_index_scan(const key_range *start_key,
                                      const key_range *end_key, bool sorted,
                                      bool descending, uchar *buf,
                                      part_id_range *part_spec) {
  NdbTransaction *trans;
  NdbIndexScanOperation *op;
  int error;

  DBUG_TRACE;
  DBUG_PRINT("enter",
             ("index: %u, sorted: %d, descending: %d read_set=0x%x",
              active_index, sorted, descending, table->read_set->bitmap[0]));
  DBUG_PRINT("enter",
             ("Starting new ordered scan on %s", table_share->table_name.str));

  if (unlikely(!(trans = get_transaction(error)))) {
    return error;
  }

  if ((error = close_scan())) return error;

  const NdbOperation::LockMode lm = get_ndb_lock_mode(m_lock.type);

  const NdbRecord *key_rec = m_index[active_index].ndb_record_key;
  const NdbRecord *row_rec = m_ndb_record;

  NdbIndexScanOperation::IndexBound bound;
  NdbIndexScanOperation::IndexBound *pbound = nullptr;
  if (start_key != nullptr || end_key != nullptr) {
    /*
       Compute bounds info, reversing range boundaries
       if descending
     */
    compute_index_bounds(bound, table->key_info + active_index,
                         (descending ? end_key : start_key),
                         (descending ? start_key : end_key), 0);
    bound.range_no = 0;
    pbound = &bound;
  }

  if (check_if_pushable(NdbQueryOperationDef::OrderedIndexScan, active_index)) {
    const int error = create_pushed_join();
    if (unlikely(error)) return error;

    NdbQuery *const query = m_active_query;
    if (sorted &&
        query->getQueryOperation((uint)PUSHED_ROOT)
            ->setOrdering(descending
                              ? NdbQueryOptions::ScanOrdering_descending
                              : NdbQueryOptions::ScanOrdering_ascending)) {
      ERR_RETURN(query->getNdbError());
    }

    if (pbound && query->setBound(key_rec, pbound) != 0)
      ERR_RETURN(query->getNdbError());

    m_thd_ndb->m_scan_count++;

    bool prunable = false;
    if (unlikely(query->isPrunable(prunable) != 0))
      ERR_RETURN(query->getNdbError());
    if (prunable) m_thd_ndb->m_pruned_scan_count++;

    // Can't have BLOB in pushed joins (yet)
    assert(!uses_blob_value(table->read_set));
  } else {
    NdbScanOperation::ScanOptions options;
    options.optionsPresent = NdbScanOperation::ScanOptions::SO_SCANFLAGS;
    options.scan_flags = 0;

    NdbOperation::GetValueSpec gets[2];
    if (table_share->primary_key == MAX_KEY)
      get_hidden_fields_scan(&options, gets);

    if (lm == NdbOperation::LM_Read)
      options.scan_flags |= NdbScanOperation::SF_KeyInfo;
    if (sorted) options.scan_flags |= NdbScanOperation::SF_OrderByFull;
    if (descending) options.scan_flags |= NdbScanOperation::SF_Descending;

    /* Partition pruning */
    if (m_use_partition_pruning && m_user_defined_partitioning &&
        part_spec != nullptr && part_spec->start_part == part_spec->end_part) {
      /* Explicitly set partition id when pruning User-defined partitioned scan
       */
      options.partitionId = part_spec->start_part;
      options.optionsPresent |= NdbScanOperation::ScanOptions::SO_PARTITION_ID;
    }

    NdbInterpretedCode code(m_table);
    generate_scan_filter(&code, &options);

    get_read_set(true, active_index);
    if (!(op = trans->scanIndex(key_rec, row_rec, lm,
                                m_table_map->get_column_mask(table->read_set),
                                pbound, &options,
                                sizeof(NdbScanOperation::ScanOptions))))
      ERR_RETURN(trans->getNdbError());

    DBUG_PRINT("info",
               ("Is scan pruned to 1 partition? : %u", op->getPruned()));
    m_thd_ndb->m_scan_count++;
    m_thd_ndb->m_pruned_scan_count += (op->getPruned() ? 1 : 0);

    if (uses_blob_value(table->read_set) &&
        get_blob_values(op, nullptr, table->read_set) != 0)
      ERR_RETURN(op->getNdbError());

    m_active_cursor = op;
  }

  if (sorted) {
    m_thd_ndb->m_sorted_scan_count++;
  }

  if (execute_no_commit(m_thd_ndb, trans, m_ignore_no_key) != 0)
    return ndb_err(trans);

  return next_result(buf);
}

static int guess_scan_flags(NdbOperation::LockMode lm, Ndb_table_map *table_map,
                            const NDBTAB *tab, const MY_BITMAP *readset) {
  int flags = 0;
  flags |= (lm == NdbOperation::LM_Read) ? NdbScanOperation::SF_KeyInfo : 0;
  if (tab->checkColumns(nullptr, 0) & 2) {
    const Uint32 *colmap = (const Uint32 *)table_map->get_column_mask(readset);
    int ret = tab->checkColumns(colmap, no_bytes_in_map(readset));

    if (ret & 2) {  // If disk columns...use disk scan
      flags |= NdbScanOperation::SF_DiskScan;
    } else if ((ret & 4) == 0 && (lm == NdbOperation::LM_Exclusive)) {
      // If no mem column is set and exclusive...guess disk scan
      flags |= NdbScanOperation::SF_DiskScan;
    }
  }
  return flags;
}

/*
  Start full table scan in NDB or unique index scan
 */

int ha_ndbcluster::full_table_scan(const KEY *key_info,
                                   const key_range *start_key,
                                   const key_range *end_key, uchar *buf) {
  THD *thd = table->in_use;
  int error;
  NdbTransaction *trans = m_thd_ndb->trans;
  part_id_range part_spec;
  bool use_set_part_id = false;
  NdbOperation::GetValueSpec gets[2];

  DBUG_TRACE;
  DBUG_PRINT("enter", ("Starting new scan on %s", table_share->table_name.str));

  if (m_use_partition_pruning && m_user_defined_partitioning) {
    assert(m_pushed_join_operation != PUSHED_ROOT);
    part_spec.start_part = 0;
    part_spec.end_part = m_part_info->get_tot_partitions() - 1;
    prune_partition_set(table, &part_spec);
    DBUG_PRINT("info", ("part_spec.start_part: %u  part_spec.end_part: %u",
                        part_spec.start_part, part_spec.end_part));
    /*
      If partition pruning has found no partition in set
      we can return HA_ERR_END_OF_FILE
    */
    if (part_spec.start_part > part_spec.end_part) {
      return HA_ERR_END_OF_FILE;
    }

    if (part_spec.start_part == part_spec.end_part) {
      /*
       * Only one partition is required to scan, if sorted is required
       * don't need it anymore since output from one ordered partitioned
       * index is always sorted.
       *
       * Note : This table scan pruning currently only occurs for
       * UserDefined partitioned tables.
       * It could be extended to occur for natively partitioned tables if
       * the Partitioning layer can make a key (e.g. start or end key)
       * available so that we can determine the correct pruning in the
       * NDBAPI layer.
       */
      use_set_part_id = true;
      if (!trans)
        if (unlikely(!(
                trans = get_transaction_part_id(part_spec.start_part, error))))
          return error;
    }
  }
  if (!trans)
    if (unlikely(!(trans = start_transaction(error)))) return error;

  /*
    If the scan is part of an ALTER TABLE we need exclusive locks on rows
    to block parallel updates from other connections to Ndb.
   */
  const NdbOperation::LockMode lm = (thd_sql_command(thd) == SQLCOM_ALTER_TABLE)
                                        ? NdbOperation::LM_Exclusive
                                        : get_ndb_lock_mode(m_lock.type);
  NdbScanOperation::ScanOptions options;
  options.optionsPresent = (NdbScanOperation::ScanOptions::SO_SCANFLAGS |
                            NdbScanOperation::ScanOptions::SO_PARALLEL);
  options.scan_flags =
      guess_scan_flags(lm, m_table_map, m_table, table->read_set);
  options.parallel = DEFAULT_PARALLELISM;
  DBUG_EXECUTE_IF("ndb_disk_scan", {
    if (!(options.scan_flags & NdbScanOperation::SF_DiskScan))
      return ER_INTERNAL_ERROR;
  });

  if (use_set_part_id) {
    assert(m_user_defined_partitioning);
    options.optionsPresent |= NdbScanOperation::ScanOptions::SO_PARTITION_ID;
    options.partitionId = part_spec.start_part;
  };

  if (table_share->primary_key == MAX_KEY)
    get_hidden_fields_scan(&options, gets);

  if (check_if_pushable(NdbQueryOperationDef::TableScan)) {
    const int error = create_pushed_join();
    if (unlikely(error)) return error;

    m_thd_ndb->m_scan_count++;
    // Can't have BLOB in pushed joins (yet)
    assert(!uses_blob_value(table->read_set));
  } else {
    NdbScanOperation *op;
    NdbInterpretedCode code(m_table);

    if (!key_info) {
      generate_scan_filter(&code, &options);
    } else {
      /* Unique index scan in NDB (full table scan with scan filter) */
      DBUG_PRINT("info", ("Starting unique index scan"));
      if (generate_scan_filter_with_key(&code, &options, key_info, start_key,
                                        end_key))
        ERR_RETURN(code.getNdbError());
    }

    get_read_set(true, MAX_KEY);
    if (!(op = trans->scanTable(
              m_ndb_record, lm, m_table_map->get_column_mask(table->read_set),
              &options, sizeof(NdbScanOperation::ScanOptions))))
      ERR_RETURN(trans->getNdbError());

    m_thd_ndb->m_scan_count++;
    m_thd_ndb->m_pruned_scan_count += (op->getPruned() ? 1 : 0);

    assert(m_active_cursor == nullptr);
    m_active_cursor = op;

    if (uses_blob_value(table->read_set) &&
        get_blob_values(op, nullptr, table->read_set) != 0)
      ERR_RETURN(op->getNdbError());
  }  // if (check_if_pushable(NdbQueryOperationDef::TableScan))

  if (execute_no_commit(m_thd_ndb, trans, m_ignore_no_key) != 0)
    return ndb_err(trans);
  DBUG_PRINT("exit", ("Scan started successfully"));
  return next_result(buf);
}  // ha_ndbcluster::full_table_scan()

int ha_ndbcluster::set_auto_inc(Ndb *ndb, Field *field) {
  DBUG_TRACE;
  bool read_bit = bitmap_is_set(table->read_set, field->field_index());
  bitmap_set_bit(table->read_set, field->field_index());
  Uint64 next_val = (Uint64)field->val_int() + 1;
  if (!read_bit) bitmap_clear_bit(table->read_set, field->field_index());
  return set_auto_inc_val(ndb, next_val);
}

inline int ha_ndbcluster::set_auto_inc_val(Ndb *ndb, Uint64 value) const {
  DBUG_TRACE;
  DBUG_PRINT("info", ("Trying to set auto increment value to %llu", value));
  {
    NDB_SHARE::Tuple_id_range_guard g(m_share);

    if (ndb->checkUpdateAutoIncrementValue(g.range, value)) {
      if (ndb->setAutoIncrementValue(m_table, g.range, value, true) == -1) {
        ERR_RETURN(ndb->getNdbError());
      }
    }
  }
  return 0;
}

void ha_ndbcluster::get_read_set(bool use_cursor, uint idx [[maybe_unused]]) {
  const bool is_delete = table->in_use->lex->sql_command == SQLCOM_DELETE ||
                         table->in_use->lex->sql_command == SQLCOM_DELETE_MULTI;

  const bool is_update = table->in_use->lex->sql_command == SQLCOM_UPDATE ||
                         table->in_use->lex->sql_command == SQLCOM_UPDATE_MULTI;

  /**
   * Any fields referred from an unpushed condition is not guaranteed to
   * be included in the read_set requested by server. Thus, ha_ndbcluster
   * has to make sure they are read.
   */
  m_cond.add_read_set(table);

#ifndef NDEBUG
  /**
   * In DEBUG build we also need to include all fields referred
   * from the assert:
   *
   *  assert(pushed_cond == nullptr || ((Item*)pushed_cond)->val_int())
   */
  m_cond.add_read_set(table, pushed_cond);
#endif

  if (!is_delete && !is_update) {
    return;
  }

  assert(use_cursor || idx == MAX_KEY || idx == table_share->primary_key ||
         table->key_info[idx].flags & HA_NOSAME);

  /**
   * It is questionable that we in some cases seems to
   * do a read even if 'm_read_before_write_removal_used'.
   * The usage pattern for this seems to be update/delete
   * cursors which establish a 'current of' position before
   * a delete- / updateCurrentTuple().
   * Anyway, as 'm_read_before_write_removal_used' we don't
   * have to add more columns to 'read_set'.
   *
   * FUTURE: Investigate if we could have completely
   * cleared the 'read_set'.
   *
   */
  if (m_read_before_write_removal_used) {
    return;
  }

  /**
   * If (part of) a primary key is updated, it is executed
   * as a delete+reinsert. In order to avoid extra read-round trips
   * to fetch missing columns required by reinsert:
   * Ensure all columns not being modified (in write_set)
   * are read prior to ::ndb_pk_update_row().
   * All PK columns are also required by ::ndb_delete_row()
   */
  if (bitmap_is_overlapping(table->write_set, m_pk_bitmap_p)) {
    assert(table_share->primary_key != MAX_KEY);
    bitmap_set_all(&m_bitmap);
    bitmap_subtract(&m_bitmap, table->write_set);
    bitmap_union(table->read_set, &m_bitmap);
    bitmap_union(table->read_set, m_pk_bitmap_p);
  }

  /**
   * Determine whether we have to read PK columns in
   * addition to those columns already present in read_set.
   * NOTE: As checked above, It is a precondition that
   *       a read is required as part of delete/update
   *       (!m_read_before_write_removal_used)
   *
   * PK columns are required when:
   *  1) This is a primary/unique keyop.
   *     (i.e. not a positioned update/delete which
   *      maintain a 'current of' position.)
   *
   * In addition, when a 'current of' position is available:
   *  2) When deleting a row containing BLOBs PK is required
   *     to delete BLOB stored in separate fragments.
   *  3) When updating BLOB columns PK is required to delete
   *     old BLOB + insert new BLOB contents
   */
  else if (!use_cursor ||                              // 1)
           (is_delete && table_share->blob_fields) ||  // 2)
           uses_blob_value(table->write_set))          // 3)
  {
    bitmap_union(table->read_set, m_pk_bitmap_p);
  }

  /**
   * If update/delete use partition pruning, we need
   * to read the column values which being part of the
   * partition spec as they are used by
   * ::get_parts_for_update() / ::get_parts_for_delete()
   * Part. columns are always part of PK, so we only
   * have to do this if pk_bitmap wasn't added yet,
   */
  else if (m_use_partition_pruning)  // && m_user_defined_partitioning)
  {
    assert(bitmap_is_subset(&m_part_info->full_part_field_set, m_pk_bitmap_p));
    bitmap_union(table->read_set, &m_part_info->full_part_field_set);
  }

  /**
   * Update might cause PK or Unique key violation.
   * Error reporting need values from the offending
   * unique columns to have been read:
   *
   * NOTE: This is NOT required for the correctness
   *       of the update operation itself. Maybe we
   *       should consider other strategies, like
   *       deferring reading of the column values
   *       until formatting the error message.
   */
  if (is_update && m_has_unique_index) {
    for (uint i = 0; i < table_share->keys; i++) {
      if ((table->key_info[i].flags & HA_NOSAME) &&
          bitmap_is_overlapping(table->write_set, m_key_fields[i])) {
        bitmap_union(table->read_set, m_key_fields[i]);
      }
    }
  }
}

Uint32 ha_ndbcluster::setup_get_hidden_fields(
    NdbOperation::GetValueSpec gets[2]) {
  Uint32 num_gets = 0;
  /*
    We need to read the hidden primary key, and possibly the FRAGMENT
    pseudo-column.
  */
  gets[num_gets].column = get_hidden_key_column();
  gets[num_gets].appStorage = &m_ref;
  num_gets++;
  if (m_user_defined_partitioning) {
    /* Need to read partition id to support ORDER BY columns. */
    gets[num_gets].column = NdbDictionary::Column::FRAGMENT;
    gets[num_gets].appStorage = &m_part_id;
    num_gets++;
  }
  return num_gets;
}

void ha_ndbcluster::get_hidden_fields_keyop(
    NdbOperation::OperationOptions *options,
    NdbOperation::GetValueSpec gets[2]) {
  Uint32 num_gets = setup_get_hidden_fields(gets);
  options->optionsPresent |= NdbOperation::OperationOptions::OO_GETVALUE;
  options->extraGetValues = gets;
  options->numExtraGetValues = num_gets;
}

void ha_ndbcluster::get_hidden_fields_scan(
    NdbScanOperation::ScanOptions *options,
    NdbOperation::GetValueSpec gets[2]) {
  Uint32 num_gets = setup_get_hidden_fields(gets);
  options->optionsPresent |= NdbScanOperation::ScanOptions::SO_GETVALUE;
  options->extraGetValues = gets;
  options->numExtraGetValues = num_gets;
}

static inline void eventSetAnyValue(Thd_ndb *thd_ndb,
                                    NdbOperation::OperationOptions *options) {
  options->anyValue = 0;
  if (thd_ndb->get_applier()) {
    /*
      Applier thread is applying a replicated event.
      Set the server_id to the value received from the log which may be a
      composite of server_id and other data according to the server_id_bits
      option. In future it may be useful to support *not* mapping composite
      AnyValues to/from Binlogged server-ids
    */
    options->optionsPresent |= NdbOperation::OperationOptions::OO_ANYVALUE;
    options->anyValue = thd_unmasked_server_id(thd_ndb->get_thd());

    /*
      Ignore TRANS_NO_LOGGING for applier thread. For other threads it's used to
      indicate log-replica-updates option. This is instead handled in the
      injector thread, by looking explicitly at "opt_log_replica_updates".
    */
  } else {
    if (thd_ndb->check_trans_option(Thd_ndb::TRANS_NO_LOGGING)) {
      options->optionsPresent |= NdbOperation::OperationOptions::OO_ANYVALUE;
      ndbcluster_anyvalue_set_nologging(options->anyValue);
    }
  }
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("ndb_set_reflect_anyvalue", true, false)) {
    fprintf(stderr, "Ndb forcing reflect AnyValue\n");
    options->optionsPresent |= NdbOperation::OperationOptions::OO_ANYVALUE;
    ndbcluster_anyvalue_set_reflect_op(options->anyValue);
  }
  if (DBUG_EVALUATE_IF("ndb_set_refresh_anyvalue", true, false)) {
    fprintf(stderr, "Ndb forcing refresh AnyValue\n");
    options->optionsPresent |= NdbOperation::OperationOptions::OO_ANYVALUE;
    ndbcluster_anyvalue_set_refresh_op(options->anyValue);
  }

  /*
    MySQLD will set the user-portion of AnyValue (if any) to all 1s
    This tests code filtering ServerIds on the value of server-id-bits.
  */
  const char *p = getenv("NDB_TEST_ANYVALUE_USERDATA");
  if (p != nullptr && *p != 0 && *p != '0' && *p != 'n' && *p != 'N') {
    options->optionsPresent |= NdbOperation::OperationOptions::OO_ANYVALUE;
    dbug_ndbcluster_anyvalue_set_userbits(options->anyValue);
  }
#endif
}

/**
   prepare_conflict_detection

   This method is called during operation definition by the slave,
   when writing to a table with conflict detection defined.

   It is responsible for defining and adding any operation filtering
   required, and for saving any operation definition state required
   for post-execute analysis.

   For transactional detection, this method may determine that the
   operation being defined should not be executed, and conflict
   handling should occur immediately.  In this case, conflict_handled
   is set to true.
*/
int ha_ndbcluster::prepare_conflict_detection(
    enum_conflicting_op_type op_type, const NdbRecord *key_rec,
    const NdbRecord *data_rec, const uchar *old_data, const uchar *new_data,
    const MY_BITMAP *write_set, NdbTransaction *trans, NdbInterpretedCode *code,
    NdbOperation::OperationOptions *options, bool &conflict_handled,
    bool &avoid_ndbapi_write) {
  DBUG_TRACE;

  conflict_handled = false;

  if (unlikely(m_share->is_apply_status_table())) {
    // The ndb_apply_status table should not have any conflict detection
    return 0;
  }

  Ndb_applier *const applier = m_thd_ndb->get_applier();
  assert(applier);

  /*
     Check transaction id first, as in transactional conflict detection,
     the transaction id is what eventually dictates whether an operation
     is applied or not.

     Not that this applies even if the current operation's table does not
     have a conflict function defined - if a transaction spans a 'transactional
     conflict detection' table and a non transactional table, the
     non-transactional table's data will also be reverted.
  */
  Uint64 transaction_id = Ndb_binlog_extra_row_info::InvalidTransactionId;
  bool op_is_marked_as_read = false;
  bool op_is_marked_as_reflected = false;
  // Only used for sanity check and debug printout
  bool op_is_marked_as_refresh [[maybe_unused]] = false;

  THD *thd = table->in_use;
  if (thd->binlog_row_event_extra_data) {
    Ndb_binlog_extra_row_info extra_row_info;
    if (extra_row_info.loadFromBuffer(thd->binlog_row_event_extra_data) != 0) {
      ndb_log_warning(
          "Replica: Malformed event received on table %s "
          "cannot parse. Stopping SQL thread.",
          m_share->key_string());
      return ER_REPLICA_CORRUPT_EVENT;
    }

    if (extra_row_info.getFlags() &
        Ndb_binlog_extra_row_info::NDB_ERIF_TRANSID) {
      transaction_id = extra_row_info.getTransactionId();
    }

    if (extra_row_info.getFlags() &
        Ndb_binlog_extra_row_info::NDB_ERIF_CFT_FLAGS) {
      const Uint16 conflict_flags = extra_row_info.getConflictFlags();
      DBUG_PRINT("info", ("conflict flags : %x\n", conflict_flags));

      if (conflict_flags & NDB_ERIF_CFT_REFLECT_OP) {
        op_is_marked_as_reflected = true;
        applier->increment_reflect_op_prepare_count();
      }

      if (conflict_flags & NDB_ERIF_CFT_REFRESH_OP) {
        op_is_marked_as_refresh = true;
        applier->increment_refresh_op_count();
      }

      if (conflict_flags & NDB_ERIF_CFT_READ_OP) {
        op_is_marked_as_read = true;
      }

      /* Sanity - 1 flag at a time at most */
      assert(!(op_is_marked_as_reflected && op_is_marked_as_refresh));
      assert(!(op_is_marked_as_read &&
               (op_is_marked_as_reflected || op_is_marked_as_refresh)));
    }
  }

  const st_conflict_fn_def *conflict_fn =
      (m_share->m_cfn_share ? m_share->m_cfn_share->m_conflict_fn : nullptr);

  bool pass_mode = false;
  if (conflict_fn) {
    /* Check Slave Conflict Role Variable setting */
    if (conflict_fn->flags & CF_USE_ROLE_VAR) {
      switch (opt_ndb_slave_conflict_role) {
        case SCR_NONE: {
          ndb_log_warning(
              "Replica: Conflict function %s defined on "
              "table %s requires ndb_applier_conflict_role variable "
              "to be set. Stopping SQL thread.",
              conflict_fn->name, m_share->key_string());
          return ER_REPLICA_CONFIGURATION;
        }
        case SCR_PASS: {
          pass_mode = true;
        }
        default:
          /* PRIMARY, SECONDARY */
          break;
      }
    }
  }

  {
    bool handle_conflict_now = false;
    const uchar *row_data = (op_type == WRITE_ROW ? new_data : old_data);
    int res = applier->atPrepareConflictDetection(
        m_table, key_rec, row_data, transaction_id, handle_conflict_now);
    if (res) {
      return res;
    }

    if (handle_conflict_now) {
      DBUG_PRINT("info", ("Conflict handling for row occurring now"));
      NdbError noRealConflictError;
      /*
       * If the user operation was a read and we receive an update
       * log event due to an AnyValue update, then the conflicting operation
       * should be reported as a read.
       */
      enum_conflicting_op_type conflicting_op =
          (op_type == UPDATE_ROW && op_is_marked_as_read) ? READ_ROW : op_type;
      /*
         Directly handle the conflict here - e.g refresh/ write to
         exceptions table etc.
      */
      res = handle_row_conflict(applier, m_share->m_cfn_share,
                                m_share->table_name, "Transaction", key_rec,
                                data_rec, old_data, new_data, conflicting_op,
                                TRANS_IN_CONFLICT, noRealConflictError, trans,
                                write_set, transaction_id);
      if (unlikely(res)) {
        return res;
      }

      applier->set_flag(Ndb_applier::OPS_DEFINED);

      /*
        Indicate that there (may be) some more operations to
        execute before committing
      */
      m_thd_ndb->m_unsent_bytes += 12;
      conflict_handled = true;
      return 0;
    }
  }

  if (conflict_fn == nullptr || pass_mode) {
    /* No conflict function definition required */
    return 0;
  }

  /**
   * By default conflict algorithms use the 'natural' NdbApi ops
   * (insert/update/delete) which can detect presence anomalies,
   * as opposed to NdbApi write which ignores them.
   * However in some cases, we want to use NdbApi write to apply
   * events received on tables with conflict detection defined
   * (e.g. when we want to forcibly align a row with a refresh op).
   */
  avoid_ndbapi_write = true;

  if (unlikely((conflict_fn->flags & CF_TRANSACTIONAL) &&
               (transaction_id ==
                Ndb_binlog_extra_row_info::InvalidTransactionId))) {
    ndb_log_warning(
        "Replica: Transactional conflict detection defined on "
        "table %s, but events received without transaction ids.  "
        "Check --ndb-log-transaction-id setting on "
        "upstream Cluster.",
        m_share->key_string());
    /* This is a user error, but we want them to notice, so treat seriously */
    return ER_REPLICA_CORRUPT_EVENT;
  }

  bool prepare_interpreted_program = false;
  if (op_type != WRITE_ROW) {
    prepare_interpreted_program = true;
  } else if (conflict_fn->flags & CF_USE_INTERP_WRITE) {
    prepare_interpreted_program = true;
    avoid_ndbapi_write = false;
  }

  if (conflict_fn->flags & CF_REFLECT_SEC_OPS) {
    /* This conflict function reflects secondary ops at the Primary */

    if (opt_ndb_slave_conflict_role == SCR_PRIMARY) {
      /**
       * Here we mark the applied operations to indicate that they
       * should be reflected back to the SECONDARY cluster.
       * This is required so that :
       *   1.  They are given local Binlog Event source serverids
       *       and so will pass through to the storage engine layer
       *       on the SECONDARY.
       *       (Normally they would be filtered in the Slave IO thread
       *        as having returned-to-source)
       *
       *   2.  They can be tagged as reflected so that the SECONDARY
       *       can handle them differently
       *       (They are force-applied)
       */
      DBUG_PRINT("info", ("Setting AnyValue to reflect secondary op"));

      options->optionsPresent |= NdbOperation::OperationOptions::OO_ANYVALUE;
      ndbcluster_anyvalue_set_reflect_op(options->anyValue);
    } else if (opt_ndb_slave_conflict_role == SCR_SECONDARY) {
      /**
       * On the Secondary, we receive reflected operations which
       * we want to attempt to apply under certain conditions.
       * This is done to recover from situations where
       * both PRIMARY and SECONDARY have performed concurrent
       * DELETEs.
       *
       * For non reflected operations we want to apply Inserts and
       * Updates using write_tuple() to get an idempotent effect
       */
      if (op_is_marked_as_reflected) {
        /**
         * Apply operations using their 'natural' operation types
         * with interpreted programs attached where appropriate.
         * Natural operation types used so that we become aware
         * of any 'presence' issues (row does/not exist).
         */
        DBUG_PRINT("info", ("Reflected operation"));
      } else {
        /**
         * Either a normal primary sourced change, or a refresh
         * operation.
         * In both cases we want to apply the operation idempotently,
         * and there's no need for an interpreted program.
         * e.g.
         *   WRITE_ROW  -> NdbApi write_row
         *   UPDATE_ROW -> NdbApi write_row
         *   DELETE_ROW -> NdbApi delete_row
         *
         * NdbApi write_row does not fail.
         * NdbApi delete_row will complain if the row does not exist
         * but this will be ignored
         */
        DBUG_PRINT("info", ("Allowing use of NdbApi write_row "
                            "for non reflected op (%u)",
                            op_is_marked_as_refresh));
        prepare_interpreted_program = false;
        avoid_ndbapi_write = false;
      }
    }
  }

  /*
     Prepare interpreted code for operation according to algorithm used
  */
  if (prepare_interpreted_program) {
    const int res = conflict_fn->prep_func(m_share->m_cfn_share, op_type,
                                           m_ndb_record, old_data, new_data,
                                           table->read_set,   // Before image
                                           table->write_set,  // After image
                                           code, applier->get_max_rep_epoch());

    if (res == 0) {
      if (code->getWordsUsed() > 0) {
        /* Attach conflict detecting filter program to operation */
        options->optionsPresent |=
            NdbOperation::OperationOptions::OO_INTERPRETED;
        options->interpretedCode = code;
      }
    } else {
      ndb_log_warning(
          "Replica: Binlog event on table %s missing "
          "info necessary for conflict detection.  "
          "Check binlog format options on upstream cluster.",
          m_share->key_string());
      return ER_REPLICA_CORRUPT_EVENT;
    }
  }

  applier->set_flag(Ndb_applier::OPS_DEFINED);

  /* Now save data for potential insert to exceptions table... */
  Ndb_exceptions_data ex_data;
  ex_data.share = m_share;
  ex_data.key_rec = key_rec;
  ex_data.data_rec = data_rec;
  ex_data.op_type = op_type;
  ex_data.reflected_operation = op_is_marked_as_reflected;
  ex_data.trans_id = transaction_id;

  // Save the row data for possible conflict resolution after execute()
  if (old_data) {
    ex_data.old_row =
        m_thd_ndb->copy_to_batch_mem(old_data, table_share->stored_rec_length);
    if (ex_data.old_row == nullptr) {
      return HA_ERR_OUT_OF_MEM;
    }
  }
  if (new_data) {
    ex_data.new_row =
        m_thd_ndb->copy_to_batch_mem(new_data, table_share->stored_rec_length);
    if (ex_data.new_row == nullptr) {
      return HA_ERR_OUT_OF_MEM;
    }
  }

  ex_data.bitmap_buf = nullptr;
  ex_data.write_set = nullptr;
  if (table->write_set) {
    /* Copy table write set */
    // NOTE! Could copy only data here and create bitmap if there is a conflict
    ex_data.bitmap_buf =
        (my_bitmap_map *)m_thd_ndb->get_buffer(table->s->column_bitmap_size);
    if (ex_data.bitmap_buf == nullptr) {
      return HA_ERR_OUT_OF_MEM;
    }
    ex_data.write_set = (MY_BITMAP *)m_thd_ndb->get_buffer(sizeof(MY_BITMAP));
    if (ex_data.write_set == nullptr) {
      return HA_ERR_OUT_OF_MEM;
    }
    bitmap_init(ex_data.write_set, ex_data.bitmap_buf,
                table->write_set->n_bits);
    bitmap_copy(ex_data.write_set, table->write_set);
  }

  // Save the control structure for possible conflict detection after execute()
  void *ex_data_buffer =
      m_thd_ndb->copy_to_batch_mem(&ex_data, sizeof(ex_data));
  if (ex_data_buffer == nullptr) {
    return HA_ERR_OUT_OF_MEM;
  }

  /* Store pointer to the copied exceptions data in operations 'customdata' */
  options->optionsPresent |= NdbOperation::OperationOptions::OO_CUSTOMDATA;
  options->customData = ex_data_buffer;

  return 0;
}

/**
   handle_conflict_op_error

   This method is called when an error is detected after executing an
   operation with conflict detection active.

   If the operation error is related to conflict detection, handling
   starts.

   Handling involves incrementing the relevant counter, and optionally
   refreshing the row and inserting an entry into the exceptions table
*/

static int handle_conflict_op_error(Ndb_applier *const applier,
                                    NdbTransaction *trans, const NdbError &err,
                                    const NdbOperation *op) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("ndb error: %d", err.code));

  if (err.code == ERROR_CONFLICT_FN_VIOLATION ||
      err.code == ERROR_OP_AFTER_REFRESH_OP ||
      err.classification == NdbError::ConstraintViolation ||
      err.classification == NdbError::NoDataFound) {
    DBUG_PRINT("info", ("err.code = %s, err.classification = %s",
                        ((err.code == ERROR_CONFLICT_FN_VIOLATION)
                             ? "error_conflict_fn_violation"
                             : ((err.code == ERROR_OP_AFTER_REFRESH_OP)
                                    ? "error_op_after_refresh_op"
                                    : "?")),
                        ((err.classification == NdbError::ConstraintViolation)
                             ? "ConstraintViolation"
                             : ((err.classification == NdbError::NoDataFound)
                                    ? "NoDataFound"
                                    : "?"))));

    enum_conflict_cause conflict_cause;

    /* Map cause onto our conflict description type */
    if (err.code == ERROR_CONFLICT_FN_VIOLATION ||
        err.code == ERROR_OP_AFTER_REFRESH_OP) {
      DBUG_PRINT("info", ("ROW_IN_CONFLICT"));
      conflict_cause = ROW_IN_CONFLICT;
    } else if (err.classification == NdbError::ConstraintViolation) {
      DBUG_PRINT("info", ("ROW_ALREADY_EXISTS"));
      conflict_cause = ROW_ALREADY_EXISTS;
    } else {
      assert(err.classification == NdbError::NoDataFound);
      DBUG_PRINT("info", ("ROW_DOES_NOT_EXIST"));
      conflict_cause = ROW_DOES_NOT_EXIST;
    }

    /* Get exceptions data from operation */
    const void *buffer = op->getCustomData();
    assert(buffer);
    Ndb_exceptions_data ex_data;
    memcpy(&ex_data, buffer, sizeof(ex_data));
    NDB_SHARE *share = ex_data.share;
    NDB_CONFLICT_FN_SHARE *cfn_share = share ? share->m_cfn_share : nullptr;

    const NdbRecord *key_rec = ex_data.key_rec;
    const NdbRecord *data_rec = ex_data.data_rec;
    const uchar *old_row = ex_data.old_row;
    const uchar *new_row = ex_data.new_row;
#ifndef NDEBUG
    const uchar *row =
        (ex_data.op_type == DELETE_ROW) ? ex_data.old_row : ex_data.new_row;
#endif
    enum_conflicting_op_type causing_op_type = ex_data.op_type;
    const MY_BITMAP *write_set = ex_data.write_set;

    DBUG_PRINT("info", ("Conflict causing op type : %u", causing_op_type));

    if (causing_op_type == REFRESH_ROW) {
      /*
         The failing op was a refresh row, require that it
         failed due to being a duplicate (e.g. a refresh
         occurring on a refreshed row)
       */
      if (err.code == ERROR_OP_AFTER_REFRESH_OP) {
        DBUG_PRINT("info", ("Operation after refresh - ignoring"));
        return 0;
      } else {
        DBUG_PRINT("info", ("Refresh op hit real error %u", err.code));
        /* Unexpected error, normal handling*/
        return err.code;
      }
    }

    if (ex_data.reflected_operation) {
      DBUG_PRINT("info", ("Reflected operation error : %u.", err.code));

      /**
       * Expected cases are :
       *   Insert : Row already exists :      Don't care - discard
       *              Secondary has this row, or a future version
       *
       *   Update : Row does not exist :      Don't care - discard
       *              Secondary has deleted this row later.
       *
       *            Conflict
       *            (Row written here last) : Don't care - discard
       *              Secondary has this row, or a future version
       *
       *   Delete : Row does not exist :      Don't care - discard
       *              Secondary has deleted this row later.
       *
       *            Conflict
       *            (Row written here last) : Don't care - discard
       *              Secondary has a future version of this row
       *
       *   Presence and authorship conflicts are used to determine
       *   whether to apply a reflecte operation.
       *   The presence checks avoid divergence and the authorship
       *   checks avoid all actions being applied in delayed
       *   duplicate.
       */
      assert(err.code == ERROR_CONFLICT_FN_VIOLATION ||
             err.classification == NdbError::ConstraintViolation ||
             err.classification == NdbError::NoDataFound);

      applier->increment_reflect_op_discard_count();
      return 0;
    }

    {
      /**
       * For asymmetric algorithms that use the ROLE variable to
       * determine their role, we check whether we are on the
       * SECONDARY cluster.
       * This is far as we want to process conflicts on the
       * SECONDARY.
       */
      bool secondary = cfn_share && cfn_share->m_conflict_fn &&
                       (cfn_share->m_conflict_fn->flags & CF_USE_ROLE_VAR) &&
                       (opt_ndb_slave_conflict_role == SCR_SECONDARY);

      if (secondary) {
        DBUG_PRINT("info", ("Conflict detected, on secondary - ignore"));
        return 0;
      }
    }

    assert(share != nullptr && row != nullptr);
    bool table_has_trans_conflict_detection =
        cfn_share && cfn_share->m_conflict_fn &&
        (cfn_share->m_conflict_fn->flags & CF_TRANSACTIONAL);

    if (table_has_trans_conflict_detection) {
      /* Mark this transaction as in-conflict.
       * For Delete-NoSuchRow (aka Delete-Delete) conflicts, we
       * do not always mark the transaction as in-conflict, as
       *  i) Row based algorithms cannot do so safely w.r.t batching
       * ii) NDB$EPOCH_TRANS cannot avoid divergence in any case,
       *     and so chooses to ignore such conflicts
       * So only NDB$EPOCH_TRANS2 (controlled by the CF_DEL_DEL_CFT
       * flag will mark the transaction as in-conflict due to a
       * delete of a non-existent row.
       */
      bool is_del_del_cft = ((causing_op_type == DELETE_ROW) &&
                             (conflict_cause == ROW_DOES_NOT_EXIST));
      bool fn_treats_del_del_as_cft =
          (cfn_share->m_conflict_fn->flags & CF_DEL_DEL_CFT);

      if (!is_del_del_cft || fn_treats_del_del_as_cft) {
        /* Perform special transactional conflict-detected handling */
        const int res = applier->atTransConflictDetected(ex_data.trans_id);
        if (res) {
          return res;
        }
      }
    }

    if (cfn_share) {
      /* Now handle the conflict on this row */
      enum_conflict_fn_type cft = cfn_share->m_conflict_fn->type;
      applier->increment_violation_count(cft);

      int res = handle_row_conflict(
          applier, cfn_share, share->table_name, "Row", key_rec, data_rec,
          old_row, new_row, causing_op_type, conflict_cause, err, trans,
          write_set,
          /*
            ORIG_TRANSID not available for
            non-transactional conflict detection.
          */
          Ndb_binlog_extra_row_info::InvalidTransactionId);

      return res;
    } else {
      DBUG_PRINT("info", ("missing cfn_share"));
      return 0;  // TODO : Correct?
    }
  } else {
    /* Non conflict related error */
    DBUG_PRINT("info", ("err.code == %u", err.code));
    return err.code;
  }

  return 0;  // Reachable?
}

int ha_ndbcluster::write_row(uchar *record) {
  DBUG_TRACE;

  Ndb_applier *const applier = m_thd_ndb->get_applier();
  if (applier && m_share->is_apply_status_table()) {
    // Applier is writing to ndb_apply_status table

    // Extract server_id and epoch from the written row
    assert(record == table->record[0]);
    const Uint32 row_server_id = table->field[0]->val_int();
    const Uint64 row_epoch = table->field[1]->val_int();

    bool skip_write = false;
    const int result =
        applier->atApplyStatusWrite(row_server_id, row_epoch, skip_write);
    if (result != 0) {
      // Stop applier
      return result;
    }

    if (skip_write) {
      // The applier has handled this write by deferring it until commit time
      return 0;
    }
  }

  return ndb_write_row(record, false, false);
}

/**
  Insert one record into NDB
*/
int ha_ndbcluster::ndb_write_row(uchar *record, bool primary_key_update,
                                 bool batched_update) {
  bool has_auto_increment;
  const NdbOperation *op;
  THD *thd = table->in_use;
  Thd_ndb *thd_ndb = m_thd_ndb;
  NdbTransaction *trans;
  uint32 part_id = 0;
  int error = 0;
  Uint64 auto_value;
  longlong func_value = 0;
  const Uint32 authorValue = 1;
  NdbOperation::SetValueSpec sets[3];
  Uint32 num_sets = 0;
  DBUG_TRACE;

  has_auto_increment = (table->next_number_field && record == table->record[0]);

  if (has_auto_increment && table_share->primary_key != MAX_KEY) {
    /*
     * Increase any auto_incremented primary key
     */
    m_skip_auto_increment = false;
    if ((error = update_auto_increment())) return error;
    m_skip_auto_increment = (insert_id_for_cur_row == 0 ||
                             thd->auto_inc_intervals_forced.nb_elements());
  }

  /*
   * If IGNORE the ignore constraint violations on primary and unique keys
   */
  if (!m_use_write && m_ignore_dup_key) {
    /*
      compare if expression with that in start_bulk_insert()
      start_bulk_insert will set parameters to ensure that each
      write_row is committed individually
    */
    const int peek_res = peek_indexed_rows(record, NDB_INSERT);

    if (!peek_res) {
      error = HA_ERR_FOUND_DUPP_KEY;
    } else if (peek_res != HA_ERR_KEY_NOT_FOUND) {
      error = peek_res;
    }
    if (error) {
      if ((has_auto_increment) && (m_skip_auto_increment)) {
        int ret_val;
        if ((ret_val =
                 set_auto_inc(m_thd_ndb->ndb, table->next_number_field))) {
          return ret_val;
        }
      }
      m_skip_auto_increment = true;
      return error;
    }
  }

  bool uses_blobs = uses_blob_value(table->write_set);

  const NdbRecord *key_rec;
  const uchar *key_row;
  if (table_share->primary_key == MAX_KEY) {
    /* Table has hidden primary key. */
    Ndb *ndb = m_thd_ndb->ndb;
    uint retries = NDB_AUTO_INCREMENT_RETRIES;
    for (;;) {
      NDB_SHARE::Tuple_id_range_guard g(m_share);
      if (ndb->getAutoIncrementValue(m_table, g.range, auto_value, 1000) ==
          -1) {
        if (--retries && !thd_killed(thd) &&
            ndb->getNdbError().status == NdbError::TemporaryError) {
          ndb_trans_retry_sleep();
          continue;
        }
        ERR_RETURN(ndb->getNdbError());
      }
      break;
    }
    sets[num_sets].column = get_hidden_key_column();
    sets[num_sets].value = &auto_value;
    num_sets++;
    key_rec = m_ndb_hidden_key_record;
    key_row = (const uchar *)&auto_value;
  } else {
    key_rec = m_index[table_share->primary_key].ndb_unique_record_row;
    key_row = record;
  }

  trans = thd_ndb->trans;
  if (m_user_defined_partitioning) {
    assert(m_use_partition_pruning);
    my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
    error = m_part_info->get_partition_id(m_part_info, &part_id, &func_value);
    dbug_tmp_restore_column_map(table->read_set, old_map);
    if (unlikely(error)) {
      m_part_info->err_value = func_value;
      return error;
    }
    {
      /*
        We need to set the value of the partition function value in
        NDB since the NDB kernel doesn't have easy access to the function
        to calculate the value.
      */
      if (func_value >= INT_MAX32) func_value = INT_MAX32;
      sets[num_sets].column = get_partition_id_column();
      sets[num_sets].value = &func_value;
      num_sets++;
    }
    if (!trans)
      if (unlikely(!(trans = start_transaction_part_id(part_id, error))))
        return error;
  } else if (!trans) {
    if (unlikely(!(trans = start_transaction_row(key_rec, key_row, error))))
      return error;
  }
  assert(trans);

  ha_statistic_increment(&System_status_var::ha_write_count);

  /*
     Setup OperationOptions
   */
  NdbOperation::OperationOptions options;
  NdbOperation::OperationOptions *poptions = nullptr;
  options.optionsPresent = 0;

  eventSetAnyValue(m_thd_ndb, &options);
  const bool need_flush =
      thd_ndb->add_row_check_if_batch_full(m_bytes_per_write);

  if (thd_ndb->get_applier() && m_table->getExtraRowAuthorBits()) {
    /* Set author to indicate slave updated last */
    sets[num_sets].column = NdbDictionary::Column::ROW_AUTHOR;
    sets[num_sets].value = &authorValue;
    num_sets++;
  }

  if (m_user_defined_partitioning) {
    options.optionsPresent |= NdbOperation::OperationOptions::OO_PARTITION_ID;
    options.partitionId = part_id;
  }
  if (num_sets) {
    options.optionsPresent |= NdbOperation::OperationOptions::OO_SETVALUE;
    options.extraSetValues = sets;
    options.numExtraSetValues = num_sets;
  }
  if (thd_ndb->get_applier() || THDVAR(thd, deferred_constraints)) {
    options.optionsPresent |=
        NdbOperation::OperationOptions::OO_DEFERRED_CONSTAINTS;
  }

  if (thd_test_options(thd, OPTION_NO_FOREIGN_KEY_CHECKS)) {
    DBUG_PRINT("info", ("Disabling foreign keys"));
    options.optionsPresent |= NdbOperation::OperationOptions::OO_DISABLE_FK;
  }

  if (options.optionsPresent != 0) poptions = &options;

  const Uint32 bitmapSz = (NDB_MAX_ATTRIBUTES_IN_TABLE + 31) / 32;
  uint32 tmpBitmapSpace[bitmapSz];
  MY_BITMAP tmpBitmap;
  MY_BITMAP *user_cols_written_bitmap;
  bool avoidNdbApiWriteOp = false; /* ndb_write_row defaults to write */
  Uint32 buffer[MAX_CONFLICT_INTERPRETED_PROG_SIZE];
  NdbInterpretedCode code(m_table, buffer, sizeof(buffer) / sizeof(buffer[0]));

  /* Conflict resolution in applier */
  const Ndb_applier *const applier = m_thd_ndb->get_applier();
  if (applier) {
    bool conflict_handled = false;
    if (unlikely((error = prepare_conflict_detection(
                      WRITE_ROW, key_rec, m_ndb_record, nullptr, /* old_data */
                      record,                                    /* new_data */
                      table->write_set, trans, &code,            /* code */
                      &options, conflict_handled, avoidNdbApiWriteOp))))
      return error;

    if (unlikely(conflict_handled)) {
      /* No need to continue with operation definition */
      /* TODO : Ensure batch execution */
      return 0;
    }
  };

  if (m_use_write && !avoidNdbApiWriteOp) {
    uchar *mask;

    if (applying_binlog(thd)) {
      /*
        Use write_set when applying binlog to avoid trampling
        unchanged columns
      */
      user_cols_written_bitmap = table->write_set;
      mask = m_table_map->get_column_mask(user_cols_written_bitmap);
    } else {
      /* Ignore write_set for REPLACE command */
      user_cols_written_bitmap = nullptr;
      mask = nullptr;
    }

    op = trans->writeTuple(key_rec, (const char *)key_row, m_ndb_record,
                           (char *)record, mask, poptions,
                           sizeof(NdbOperation::OperationOptions));
  } else {
    uchar *mask;

    /* Check whether Ndb table definition includes any default values. */
    if (m_table->hasDefaultValues()) {
      DBUG_PRINT("info", ("Not sending values for native defaulted columns"));

      /*
        If Ndb is unaware of the table's defaults, we must provide all column
        values to the insert. This is done using a NULL column mask. If Ndb is
        aware of the table's defaults, we only need to provide the columns
        explicitly mentioned in the write set, plus any extra columns required
        due to bug#41616. plus the primary key columns required due to
        bug#42238.
      */
      /*
        The following code for setting user_cols_written_bitmap
        should be removed after BUG#41616 and Bug#42238 are fixed
      */
      /* Copy table write set so that we can add to it */
      user_cols_written_bitmap = &tmpBitmap;
      bitmap_init(user_cols_written_bitmap, tmpBitmapSpace,
                  table->write_set->n_bits);
      bitmap_copy(user_cols_written_bitmap, table->write_set);

      for (uint i = 0; i < table->s->fields; i++) {
        Field *field = table->field[i];
        DBUG_PRINT("info", ("Field#%u, (%u), Type : %u "
                            "NO_DEFAULT_VALUE_FLAG : %u PRI_KEY_FLAG : %u",
                            i, field->field_index(), field->real_type(),
                            field->is_flag_set(NO_DEFAULT_VALUE_FLAG),
                            field->is_flag_set(PRI_KEY_FLAG)));
        if (field->is_flag_set(NO_DEFAULT_VALUE_FLAG) ||  // bug 41616
            field->is_flag_set(PRI_KEY_FLAG) ||           // bug 42238
            !type_supports_default_value(field->real_type())) {
          bitmap_set_bit(user_cols_written_bitmap, field->field_index());
        }
      }
      /* Finally, translate the whole bitmap from MySQL field numbers
         to NDB column numbers */
      mask = m_table_map->get_column_mask(user_cols_written_bitmap);
    } else {
      /* No defaults in kernel, provide all columns ourselves */
      DBUG_PRINT("info", ("No native defaults, sending all values"));
      user_cols_written_bitmap = nullptr;
      mask = nullptr;
    }

    /* Using insert, we write all non default columns */
    op = trans->insertTuple(key_rec, (const char *)key_row, m_ndb_record,
                            (char *)record,
                            mask,  // Default value should be masked
                            poptions, sizeof(NdbOperation::OperationOptions));
  }
  if (!(op)) ERR_RETURN(trans->getNdbError());

  /**
   * Batching
   *
   * iff :
   *   Batching allowed (bulk insert, update, thd_allow())
   *   Don't need to flush batch
   *   Not doing pk updates
   */
  const bool bulk_insert = (m_rows_to_insert > 1);
  const bool will_batch =
      !need_flush && (bulk_insert || batched_update || thd_allow_batch(thd)) &&
      !primary_key_update;

  uint blob_count = 0;
  if (table_share->blob_fields > 0) {
    my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
    /* Set Blob values for all columns updated by the operation */
    int res =
        set_blob_values(op, record - table->record[0], user_cols_written_bitmap,
                        &blob_count, will_batch);
    dbug_tmp_restore_column_map(table->read_set, old_map);
    if (res != 0) return res;
  }

  /*
    Execute operation
  */
  m_trans_table_stats->update_uncommitted_rows(1);
  if (will_batch) {
    if (uses_blobs) {
      m_thd_ndb->m_unsent_bytes += 12;
      m_thd_ndb->m_unsent_blob_ops = true;
    }
  } else {
    const int res = flush_bulk_insert();
    if (res != 0) {
      m_skip_auto_increment = true;
      return res;
    }
  }
  if ((has_auto_increment) && (m_skip_auto_increment)) {
    int ret_val;
    if ((ret_val = set_auto_inc(m_thd_ndb->ndb, table->next_number_field))) {
      return ret_val;
    }
  }
  m_skip_auto_increment = true;

  DBUG_PRINT("exit", ("ok"));
  return 0;
}

/* Compare if an update changes the primary key in a row. */
int ha_ndbcluster::primary_key_cmp(const uchar *old_row, const uchar *new_row) {
  uint keynr = table_share->primary_key;
  KEY_PART_INFO *key_part = table->key_info[keynr].key_part;
  KEY_PART_INFO *end = key_part + table->key_info[keynr].user_defined_key_parts;

  for (; key_part != end; key_part++) {
    if (!bitmap_is_set(table->write_set, key_part->fieldnr - 1)) continue;

    /* The primary key does not allow NULLs. */
    assert(!key_part->null_bit);

    if (key_part->key_part_flag & (HA_BLOB_PART | HA_VAR_LENGTH_PART)) {
      if (key_part->field->cmp_binary((old_row + key_part->offset),
                                      (new_row + key_part->offset),
                                      (ulong)key_part->length))
        return 1;
    } else {
      if (memcmp(old_row + key_part->offset, new_row + key_part->offset,
                 key_part->length))
        return 1;
    }
  }
  return 0;
}

static Ndb_exceptions_data StaticRefreshExceptionsData = {
    nullptr, nullptr, nullptr,     nullptr, nullptr,
    nullptr, nullptr, REFRESH_ROW, false,   0};

static int handle_row_conflict(
    Ndb_applier *const applier, NDB_CONFLICT_FN_SHARE *cfn_share,
    const char *table_name, const char *handling_type, const NdbRecord *key_rec,
    const NdbRecord *data_rec, const uchar *old_row, const uchar *new_row,
    enum_conflicting_op_type op_type, enum_conflict_cause conflict_cause,
    const NdbError &conflict_error, NdbTransaction *conflict_trans,
    const MY_BITMAP *write_set, Uint64 transaction_id) {
  DBUG_TRACE;

  const uchar *row = (op_type == DELETE_ROW) ? old_row : new_row;
  /*
     We will refresh the row if the conflict function requires
     it, or if we are handling a transactional conflict.
  */
  bool refresh_row = (conflict_cause == TRANS_IN_CONFLICT) ||
                     (cfn_share && (cfn_share->m_flags & CFF_REFRESH_ROWS));

  if (refresh_row) {
    /* A conflict has been detected between an applied replicated operation
     * and the data in the DB.
     * The attempt to change the local DB will have been rejected.
     * We now take steps to generate a refresh Binlog event so that
     * other clusters will be re-aligned.
     */
    DBUG_PRINT("info",
               ("Conflict on table %s.  Operation type : %s, "
                "conflict cause :%s, conflict error : %u : %s",
                table_name,
                ((op_type == WRITE_ROW)    ? "WRITE_ROW"
                 : (op_type == UPDATE_ROW) ? "UPDATE_ROW"
                                           : "DELETE_ROW"),
                ((conflict_cause == ROW_ALREADY_EXISTS)   ? "ROW_ALREADY_EXISTS"
                 : (conflict_cause == ROW_DOES_NOT_EXIST) ? "ROW_DOES_NOT_EXIST"
                                                          : "ROW_IN_CONFLICT"),
                conflict_error.code, conflict_error.message));

    assert(key_rec != nullptr);
    assert(row != nullptr);

    do {
      /* When the slave splits an epoch into batches, a conflict row detected
       * and refreshed in an early batch can be written to by operations in
       * a later batch.  As the operations will not have applied, and the
       * row has already been refreshed, we need not attempt to refresh
       * it again
       */
      if (conflict_cause == ROW_IN_CONFLICT &&
          conflict_error.code == ERROR_OP_AFTER_REFRESH_OP) {
        /* Attempt to apply an operation after the row was refreshed
         * Ignore the error
         */
        DBUG_PRINT("info", ("Operation after refresh error - ignoring"));
        break;
      }

      /**
       * Delete - NoSuchRow conflicts (aka Delete-Delete conflicts)
       *
       * Row based algorithms + batching :
       * When a delete operation finds that the row does not exist, it indicates
       * a DELETE vs DELETE conflict.  If we refresh the row then we can get
       * non deterministic behaviour depending on slave batching as follows :
       *   Row is deleted
       *
       *     Case 1
       *       Slave applied DELETE, INSERT in 1 batch
       *
       *         After first batch, the row is present (due to INSERT), it is
       *         refreshed.
       *
       *     Case 2
       *       Slave applied DELETE in 1 batch, INSERT in 2nd batch
       *
       *         After first batch, the row is not present, it is refreshed
       *         INSERT is then rejected.
       *
       * The problem of not being able to 'record' a DELETE vs DELETE conflict
       * is known.  We attempt at least to give consistent behaviour for
       * DELETE vs DELETE conflicts by :
       *   NOT refreshing a row when a DELETE vs DELETE conflict is detected
       * This should map all batching scenarios onto Case1.
       *
       * Transactional algorithms
       *
       * For transactional algorithms, there are multiple passes over the
       * epoch transaction.  Earlier passes 'mark' in-conflict transactions
       * so that any row changes to in-conflict rows are automatically
       * in-conflict.  Therefore the batching problem above is avoided.
       *
       * NDB$EPOCH_TRANS chooses to ignore DELETE-DELETE conflicts entirely
       * and so skips refreshing rows with only DELETE-DELETE conflicts.
       * NDB$EPOCH2_TRANS does not ignore them, and so refreshes them.
       * This behaviour is controlled by the algorthm's CF_DEL_DEL_CFT
       * flag at conflict detection time.
       *
       * For the final pass of the transactional algorithms, every conflict
       * is a TRANS_IN_CONFLICT error here, so no need to adjust behaviour.
       *
       */
      if ((op_type == DELETE_ROW) && (conflict_cause == ROW_DOES_NOT_EXIST)) {
        applier->increment_delete_delete_count();
        DBUG_PRINT("info", ("Delete vs Delete detected, NOT refreshing"));
        break;
      }

      /*
        We give the refresh operation some 'exceptions data', so that
        it can be identified as part of conflict resolution when
        handling operation errors.
        Specifically we need to be able to handle duplicate row
        refreshes.
        As there is no unique exceptions data, we use a singleton.

        We also need to 'force' the ANYVALUE of the row to 0 to
        indicate that the refresh is locally-sourced.
        Otherwise we can 'pickup' the ANYVALUE of a previous
        update to the row.
        If some previous update in this transaction came from a
        Slave, then using its ANYVALUE can result in that Slave
        ignoring this correction.
      */
      NdbOperation::OperationOptions options;
      options.optionsPresent = NdbOperation::OperationOptions::OO_CUSTOMDATA |
                               NdbOperation::OperationOptions::OO_ANYVALUE;
      options.customData = &StaticRefreshExceptionsData;
      options.anyValue = 0;

      /* Use AnyValue to indicate that this is a refreshTuple op */
      ndbcluster_anyvalue_set_refresh_op(options.anyValue);

      /* Create a refresh to operation to realign other clusters */
      // TODO Do we ever get non-PK key?
      //      Keyless table?
      //      Unique index
      const NdbOperation *refresh_op = conflict_trans->refreshTuple(
          key_rec, (const char *)row, &options, sizeof(options));
      if (!refresh_op) {
        NdbError err = conflict_trans->getNdbError();

        if (err.status == NdbError::TemporaryError) {
          /* Slave will roll back and retry entire transaction. */
          ERR_RETURN(err);
        } else {
          char msg[FN_REFLEN];

          /* We cannot refresh a row which has Blobs, as we do not support
           * Blob refresh yet.
           * Rows implicated by a transactional conflict function may have
           * Blobs.
           * We will generate an error in this case
           */
          const int NDBAPI_ERR_REFRESH_ON_BLOB_TABLE = 4343;
          if (err.code == NDBAPI_ERR_REFRESH_ON_BLOB_TABLE) {
            // Generate legacy error message instead of using
            // the error code and message returned from NdbApi
            snprintf(msg, sizeof(msg),
                     "%s conflict handling on table %s failed as table "
                     "has Blobs which cannot be refreshed.",
                     handling_type, table_name);

            push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                                ER_EXCEPTIONS_WRITE_ERROR,
                                ER_THD(current_thd, ER_EXCEPTIONS_WRITE_ERROR),
                                msg);

            return ER_EXCEPTIONS_WRITE_ERROR;
          }

          snprintf(msg, sizeof(msg),
                   "Row conflict handling "
                   "on table %s hit Ndb error %d '%s'",
                   table_name, err.code, err.message);
          push_warning_printf(
              current_thd, Sql_condition::SL_WARNING, ER_EXCEPTIONS_WRITE_ERROR,
              ER_THD(current_thd, ER_EXCEPTIONS_WRITE_ERROR), msg);
          /* Slave will stop replication. */
          return ER_EXCEPTIONS_WRITE_ERROR;
        }
      }
    } while (0);  // End of 'refresh' block
  }

  DBUG_PRINT(
      "info",
      ("Table %s does%s have an exceptions table", table_name,
       (cfn_share && cfn_share->m_ex_tab_writer.hasTable()) ? "" : " not"));
  if (cfn_share && cfn_share->m_ex_tab_writer.hasTable()) {
    NdbError err;
    const auto current_state = applier->get_current_epoch_state();
    if (cfn_share->m_ex_tab_writer.writeRow(
            conflict_trans, key_rec, data_rec, current_state.own_server_id,
            current_state.source_server_id, current_state.epoch_value, old_row,
            new_row, op_type, conflict_cause, transaction_id, write_set,
            err) != 0) {
      if (err.code != 0) {
        if (err.status == NdbError::TemporaryError) {
          /* Slave will roll back and retry entire transaction. */
          ERR_RETURN(err);
        } else {
          char msg[FN_REFLEN];
          snprintf(msg, sizeof(msg),
                   "%s conflict handling "
                   "on table %s hit Ndb error %d '%s'",
                   handling_type, table_name, err.code, err.message);
          push_warning_printf(
              current_thd, Sql_condition::SL_WARNING, ER_EXCEPTIONS_WRITE_ERROR,
              ER_THD(current_thd, ER_EXCEPTIONS_WRITE_ERROR), msg);
          /* Slave will stop replication. */
          return ER_EXCEPTIONS_WRITE_ERROR;
        }
      }
    }
  } /* if (cfn_share->m_ex_tab != NULL) */

  return 0;
}

/**
  Update one record in NDB using primary key.
*/

bool ha_ndbcluster::start_bulk_update() {
  DBUG_TRACE;
  if (!m_use_write && m_ignore_dup_key) {
    DBUG_PRINT("info", ("Batching turned off as duplicate key is "
                        "ignored by using peek_row"));
    return true;
  }
  return false;
}

int ha_ndbcluster::bulk_update_row(const uchar *old_data, uchar *new_data,
                                   uint *dup_key_found) {
  DBUG_TRACE;
  *dup_key_found = 0;
  return ndb_update_row(old_data, new_data, 1);
}

int ha_ndbcluster::exec_bulk_update(uint *dup_key_found) {
  NdbTransaction *trans = m_thd_ndb->trans;
  DBUG_TRACE;
  *dup_key_found = 0;

  /* If a fatal error is encountered during an update op, the error
   * is saved and exec continues. So exec_bulk_update may be called
   * even when init functions fail. Check for error conditions like
   * an uninit'ed transaction.
   */
  if (unlikely(!m_thd_ndb->trans)) {
    DBUG_PRINT("exit", ("Transaction was not started"));
    int error = 0;
    ERR_SET(m_thd_ndb->ndb->getNdbError(), error);
    return error;
  }

  // m_handler must be NULL or point to _this_ handler instance
  assert(m_thd_ndb->m_handler == nullptr || m_thd_ndb->m_handler == this);

  /*
   * Normal bulk update execution, driven by mysql_update() in sql_update.cc
   * - read_record calls start_transaction and inits m_thd_ndb->trans.
   * - ha_bulk_update calls ha_ndbcluster::bulk_update_row().
   * - ha_ndbcluster::bulk_update_row calls ha_ndbcluster::ndb_update_row().
   *   with flag is_bulk_update = 1.
   * - ndb_update_row sets up update, sets various flags and options,
   *   but does not execute_nocommit() because of batched exec.
   * - after read_record processes all rows, exec_bulk_update checks for
   *   rbwr and does an execute_commit() if rbwr enabled. If rbwr is
   *   enabled, exec_bulk_update does an execute_nocommit().
   * - if rbwr not enabled, execute_commit() done in ndbcluster_commit().
   */
  if (m_thd_ndb->m_handler && m_read_before_write_removal_possible) {
    /*
      This is an autocommit involving only one table and rbwr is on

      Commit the autocommit transaction early(before the usual place
      in ndbcluster_commit) in order to:
      1) save one round trip, "no-commit+commit" converted to "commit"
      2) return the correct number of updated and affected rows
         to the update loop(which will ask handler in rbwr mode)
    */
    DBUG_PRINT("info", ("committing auto-commit+rbwr early"));
    uint ignore_count = 0;
    const int ignore_error = 1;
    if (execute_commit(m_thd_ndb, trans, m_thd_ndb->m_force_send, ignore_error,
                       &ignore_count) != 0) {
      m_thd_ndb->trans_tables.reset_stats();
      return ndb_err(trans);
    }
    THD *thd = table->in_use;
    if (!applying_binlog(thd)) {
      DBUG_PRINT("info", ("ignore_count: %u", ignore_count));
      assert(m_rows_updated >= ignore_count);
      m_rows_updated -= ignore_count;
    }
    return 0;
  }

  if (m_thd_ndb->m_unsent_bytes == 0) {
    DBUG_PRINT("exit", ("skip execute - no unsent bytes"));
    return 0;
  }

  if (thd_allow_batch(table->in_use)) {
    /*
      Turned on by @@transaction_allow_batching=ON
      or implicitly by slave exec thread
    */
    DBUG_PRINT("exit", ("skip execute - transaction_allow_batching is ON"));
    return 0;
  }

  if (m_thd_ndb->m_handler && !m_thd_ndb->m_unsent_blob_ops) {
    // Execute at commit time(in 'ndbcluster_commit') to save a round trip
    DBUG_PRINT("exit", ("skip execute - simple autocommit"));
    return 0;
  }

  uint ignore_count = 0;
  if (execute_no_commit(m_thd_ndb, trans,
                        m_ignore_no_key || m_read_before_write_removal_used,
                        &ignore_count) != 0) {
    m_thd_ndb->trans_tables.reset_stats();
    return ndb_err(trans);
  }
  THD *thd = table->in_use;
  if (!applying_binlog(thd)) {
    assert(m_rows_updated >= ignore_count);
    m_rows_updated -= ignore_count;
  }
  return 0;
}

void ha_ndbcluster::end_bulk_update() { DBUG_TRACE; }

int ha_ndbcluster::update_row(const uchar *old_data, uchar *new_data) {
  return ndb_update_row(old_data, new_data, 0);
}

void ha_ndbcluster::setup_key_ref_for_ndb_record(const NdbRecord **key_rec,
                                                 const uchar **key_row,
                                                 const uchar *record,
                                                 bool use_active_index) {
  DBUG_TRACE;
  if (use_active_index) {
    /* Use unique key to access table */
    DBUG_PRINT("info", ("Using unique index (%u)", active_index));
    assert((table->key_info[active_index].flags & HA_NOSAME));
    /* Can't use key if we didn't read it first */
    assert(bitmap_is_subset(m_key_fields[active_index], table->read_set));
    *key_rec = m_index[active_index].ndb_unique_record_row;
    *key_row = record;
  } else if (table_share->primary_key != MAX_KEY) {
    /* Use primary key to access table */
    DBUG_PRINT("info", ("Using primary key"));
    /* Can't use pk if we didn't read it first */
    assert(bitmap_is_subset(m_pk_bitmap_p, table->read_set));
    *key_rec = m_index[table_share->primary_key].ndb_unique_record_row;
    *key_row = record;
  } else {
    /* Use hidden primary key previously read into m_ref. */
    DBUG_PRINT("info", ("Using hidden primary key (%llu)", m_ref));
    /* Can't use hidden pk if we didn't read it first */
    assert(bitmap_is_subset(m_pk_bitmap_p, table->read_set));
    assert(m_read_before_write_removal_used == false);
    *key_rec = m_ndb_hidden_key_record;
    *key_row = (const uchar *)(&m_ref);
  }
}

/*
  Update one record in NDB using primary key
*/

int ha_ndbcluster::ndb_update_row(const uchar *old_data, uchar *new_data,
                                  int is_bulk_update) {
  THD *thd = table->in_use;
  Thd_ndb *thd_ndb = m_thd_ndb;
  NdbScanOperation *cursor = m_active_cursor;
  const NdbOperation *op;
  uint32 old_part_id = ~uint32(0), new_part_id = ~uint32(0);
  int error = 0;
  longlong func_value = 0;
  Uint32 func_value_uint32;
  bool have_pk = (table_share->primary_key != MAX_KEY);
  const bool pk_update =
      (!m_read_before_write_removal_possible && have_pk &&
       bitmap_is_overlapping(table->write_set, m_pk_bitmap_p) &&
       primary_key_cmp(old_data, new_data));
  bool batch_allowed =
      !m_update_cannot_batch && (is_bulk_update || thd_allow_batch(thd));
  NdbOperation::SetValueSpec sets[2];
  Uint32 num_sets = 0;

  DBUG_TRACE;

  /* Start a transaction now if none available
   * (Manual Binlog application...)
   */
  /* TODO : Consider hinting */
  if (unlikely((!m_thd_ndb->trans) && !get_transaction(error))) {
    return error;
  }

  NdbTransaction *trans = m_thd_ndb->trans;
  assert(trans);

  /*
   * If IGNORE the ignore constraint violations on primary and unique keys,
   * but check that it is not part of INSERT ... ON DUPLICATE KEY UPDATE
   */
  if (m_ignore_dup_key && (thd->lex->sql_command == SQLCOM_UPDATE ||
                           thd->lex->sql_command == SQLCOM_UPDATE_MULTI)) {
    const NDB_WRITE_OP write_op = pk_update ? NDB_PK_UPDATE : NDB_UPDATE;
    const int peek_res = peek_indexed_rows(new_data, write_op);

    if (!peek_res) {
      return HA_ERR_FOUND_DUPP_KEY;
    }
    if (peek_res != HA_ERR_KEY_NOT_FOUND) return peek_res;
  }

  ha_statistic_increment(&System_status_var::ha_update_count);

  bool skip_partition_for_unique_index = false;
  if (m_use_partition_pruning) {
    if (!cursor && m_read_before_write_removal_used) {
      const NDB_INDEX_TYPE type = get_index_type(active_index);
      /*
        Ndb unique indexes are global so when
        m_read_before_write_removal_used is active
        the unique index can be used directly for update
        without finding the partitions
      */
      if (type == UNIQUE_INDEX || type == UNIQUE_ORDERED_INDEX) {
        skip_partition_for_unique_index = true;
        goto skip_partition_pruning;
      }
    }
    if ((error = get_parts_for_update(old_data, new_data, table->record[0],
                                      m_part_info, &old_part_id, &new_part_id,
                                      &func_value))) {
      m_part_info->err_value = func_value;
      return error;
    }
    DBUG_PRINT("info",
               ("old_part_id: %u  new_part_id: %u", old_part_id, new_part_id));
  skip_partition_pruning:
    (void)0;
  }

  /*
   * Check for update of primary key or partition change
   * for special handling
   */
  if (pk_update || old_part_id != new_part_id) {
    return ndb_pk_update_row(old_data, new_data);
  }
  /*
    If we are updating a unique key with auto_increment
    then we need to update the auto_increment counter
   */
  if (table->found_next_number_field &&
      bitmap_is_set(table->write_set,
                    table->found_next_number_field->field_index()) &&
      (error = set_auto_inc(m_thd_ndb->ndb, table->found_next_number_field))) {
    return error;
  }
  /*
    Set only non-primary-key attributes.
    We already checked that any primary key attribute in write_set has no
    real changes.
  */
  bitmap_copy(&m_bitmap, table->write_set);
  bitmap_subtract(&m_bitmap, m_pk_bitmap_p);
  uchar *mask = m_table_map->get_column_mask(&m_bitmap);
  assert(!pk_update);

  NdbOperation::OperationOptions *poptions = nullptr;
  NdbOperation::OperationOptions options;
  options.optionsPresent = 0;

  /* Need to set the value of any user-defined partitioning function.
     (except for when using unique index)
  */
  if (m_user_defined_partitioning && !skip_partition_for_unique_index) {
    if (func_value >= INT_MAX32)
      func_value_uint32 = INT_MAX32;
    else
      func_value_uint32 = (uint32)func_value;
    sets[num_sets].column = get_partition_id_column();
    sets[num_sets].value = &func_value_uint32;
    num_sets++;

    if (!cursor) {
      options.optionsPresent |= NdbOperation::OperationOptions::OO_PARTITION_ID;
      options.partitionId = new_part_id;
    }
  }

  eventSetAnyValue(m_thd_ndb, &options);

  const bool need_flush =
      thd_ndb->add_row_check_if_batch_full(m_bytes_per_write);

  const Uint32 authorValue = 1;
  if (thd_ndb->get_applier() && m_table->getExtraRowAuthorBits()) {
    /* Set author to indicate slave updated last */
    sets[num_sets].column = NdbDictionary::Column::ROW_AUTHOR;
    sets[num_sets].value = &authorValue;
    num_sets++;
  }

  if (num_sets) {
    options.optionsPresent |= NdbOperation::OperationOptions::OO_SETVALUE;
    options.extraSetValues = sets;
    options.numExtraSetValues = num_sets;
  }

  if (thd_ndb->get_applier() || THDVAR(thd, deferred_constraints)) {
    options.optionsPresent |=
        NdbOperation::OperationOptions::OO_DEFERRED_CONSTAINTS;
  }

  if (thd_test_options(thd, OPTION_NO_FOREIGN_KEY_CHECKS)) {
    DBUG_PRINT("info", ("Disabling foreign keys"));
    options.optionsPresent |= NdbOperation::OperationOptions::OO_DISABLE_FK;
  }

  if (cursor) {
    /*
      We are scanning records and want to update the record
      that was just found, call updateCurrentTuple on the cursor
      to take over the lock to a new update operation
      And thus setting the primary key of the record from
      the active record in cursor
    */
    DBUG_PRINT("info", ("Calling updateTuple on cursor, write_set=0x%x",
                        table->write_set->bitmap[0]));

    if (options.optionsPresent != 0) poptions = &options;

    if (!(op = cursor->updateCurrentTuple(
              trans, m_ndb_record, (const char *)new_data, mask, poptions,
              sizeof(NdbOperation::OperationOptions))))
      ERR_RETURN(trans->getNdbError());

    m_lock_tuple = false;
    thd_ndb->m_unsent_bytes += 12;
  } else {
    const NdbRecord *key_rec;
    const uchar *key_row;
    setup_key_ref_for_ndb_record(&key_rec, &key_row, new_data,
                                 m_read_before_write_removal_used);

    bool avoidNdbApiWriteOp = true; /* Default update op for ndb_update_row */
    Uint32 buffer[MAX_CONFLICT_INTERPRETED_PROG_SIZE];
    NdbInterpretedCode code(m_table, buffer,
                            sizeof(buffer) / sizeof(buffer[0]));

    /* Conflict resolution in Applier */
    const Ndb_applier *const applier = m_thd_ndb->get_applier();
    if (applier) {
      bool conflict_handled = false;
      /* Conflict resolution in slave thread. */
      DBUG_PRINT("info", ("Slave thread, preparing conflict resolution for "
                          "update with mask : %x",
                          *((Uint32 *)mask)));

      if (unlikely((error = prepare_conflict_detection(
                        UPDATE_ROW, key_rec, m_ndb_record, old_data, new_data,
                        table->write_set, trans, &code, &options,
                        conflict_handled, avoidNdbApiWriteOp))))
        return error;

      if (unlikely(conflict_handled)) {
        /* No need to continue with operation definition */
        /* TODO : Ensure batch execution */
        return 0;
      }
    }

    if (options.optionsPresent != 0) poptions = &options;

    if (likely(avoidNdbApiWriteOp)) {
      if (!(op =
                trans->updateTuple(key_rec, (const char *)key_row, m_ndb_record,
                                   (const char *)new_data, mask, poptions,
                                   sizeof(NdbOperation::OperationOptions))))
        ERR_RETURN(trans->getNdbError());
    } else {
      DBUG_PRINT("info", ("Update op using writeTuple"));
      if (!(op = trans->writeTuple(key_rec, (const char *)key_row, m_ndb_record,
                                   (const char *)new_data, mask, poptions,
                                   sizeof(NdbOperation::OperationOptions))))
        ERR_RETURN(trans->getNdbError());
    }
  }

  uint blob_count = 0;
  if (uses_blob_value(table->write_set)) {
    int row_offset = (int)(new_data - table->record[0]);
    int res = set_blob_values(op, row_offset, table->write_set, &blob_count,
                              (batch_allowed && !need_flush));
    if (res != 0) return res;
  }
  uint ignore_count = 0;
  /*
    Batch update operation if we are doing a scan for update, unless
    there exist UPDATE AFTER triggers
  */
  if (m_update_cannot_batch || !(cursor || (batch_allowed && have_pk)) ||
      need_flush) {
    if (execute_no_commit(m_thd_ndb, trans,
                          m_ignore_no_key || m_read_before_write_removal_used,
                          &ignore_count) != 0) {
      m_thd_ndb->trans_tables.reset_stats();
      return ndb_err(trans);
    }
  } else if (blob_count > 0)
    m_thd_ndb->m_unsent_blob_ops = true;

  m_rows_updated++;

  if (!applying_binlog(thd)) {
    assert(m_rows_updated >= ignore_count);
    m_rows_updated -= ignore_count;
  }

  return 0;
}

/*
  handler delete interface
*/

int ha_ndbcluster::delete_row(const uchar *record) {
  return ndb_delete_row(record, false);
}

bool ha_ndbcluster::start_bulk_delete() {
  DBUG_TRACE;
  m_is_bulk_delete = true;
  return 0;  // Bulk delete used by handler
}

int ha_ndbcluster::end_bulk_delete() {
  NdbTransaction *trans = m_thd_ndb->trans;
  DBUG_TRACE;
  assert(m_is_bulk_delete);  // Don't allow end() without start()
  m_is_bulk_delete = false;

  // m_handler must be NULL or point to _this_ handler instance
  assert(m_thd_ndb->m_handler == nullptr || m_thd_ndb->m_handler == this);

  if (unlikely(trans == nullptr)) {
    /* Problem with late starting transaction, do nothing here */
    return 0;
  }

  if (m_thd_ndb->m_handler && m_read_before_write_removal_possible) {
    /*
      This is an autocommit involving only one table and rbwr is on

      Commit the autocommit transaction early(before the usual place
      in ndbcluster_commit) in order to:
      1) save one round trip, "no-commit+commit" converted to "commit"
      2) return the correct number of updated and affected rows
         to the delete loop(which will ask handler in rbwr mode)
    */
    DBUG_PRINT("info", ("committing auto-commit+rbwr early"));
    uint ignore_count = 0;
    const int ignore_error = 1;
    if (execute_commit(m_thd_ndb, trans, m_thd_ndb->m_force_send, ignore_error,
                       &ignore_count) != 0) {
      m_thd_ndb->trans_tables.reset_stats();
      m_rows_deleted = 0;
      return ndb_err(trans);
    }
    THD *thd = table->in_use;
    if (!applying_binlog(thd)) {
      DBUG_PRINT("info", ("ignore_count: %u", ignore_count));
      assert(m_rows_deleted >= ignore_count);
      m_rows_deleted -= ignore_count;
    }
    return 0;
  }

  if (m_thd_ndb->m_unsent_bytes == 0) {
    DBUG_PRINT("exit", ("skip execute - no unsent bytes"));
    return 0;
  }

  if (thd_allow_batch(table->in_use)) {
    /*
      Turned on by @@transaction_allow_batching=ON
      or implicitly by slave exec thread
    */
    DBUG_PRINT("exit", ("skip execute - transaction_allow_batching is ON"));
    return 0;
  }

  if (m_thd_ndb->m_handler) {
    // Execute at commit time(in 'ndbcluster_commit') to save a round trip
    DBUG_PRINT("exit", ("skip execute - simple autocommit"));
    return 0;
  }

  uint ignore_count = 0;
  if (execute_no_commit(m_thd_ndb, trans,
                        m_ignore_no_key || m_read_before_write_removal_used,
                        &ignore_count) != 0) {
    m_thd_ndb->trans_tables.reset_stats();
    return ndb_err(trans);
  }

  THD *thd = table->in_use;
  if (!applying_binlog(thd)) {
    assert(m_rows_deleted >= ignore_count);
    m_rows_deleted -= ignore_count;
    m_trans_table_stats->update_uncommitted_rows(ignore_count);
  }
  return 0;
}

/**
  Delete one record from NDB, using primary key .
*/

int ha_ndbcluster::ndb_delete_row(const uchar *record,
                                  bool primary_key_update) {
  THD *thd = table->in_use;
  Thd_ndb *thd_ndb = m_thd_ndb;
  NdbScanOperation *cursor = m_active_cursor;
  uint32 part_id = ~uint32(0);
  int error = 0;
  bool allow_batch =
      !m_delete_cannot_batch && (m_is_bulk_delete || thd_allow_batch(thd));

  DBUG_TRACE;

  /* Start a transaction now if none available
   * (Manual Binlog application...)
   */
  /* TODO : Consider hinting */
  if (unlikely((!m_thd_ndb->trans) && !get_transaction(error))) {
    return error;
  }

  NdbTransaction *trans = m_thd_ndb->trans;
  assert(trans);

  ha_statistic_increment(&System_status_var::ha_delete_count);

  bool skip_partition_for_unique_index = false;
  if (m_use_partition_pruning) {
    if (!cursor && m_read_before_write_removal_used) {
      const NDB_INDEX_TYPE type = get_index_type(active_index);
      /*
        Ndb unique indexes are global so when
        m_read_before_write_removal_used is active
        the unique index can be used directly for deleting
        without finding the partitions
      */
      if (type == UNIQUE_INDEX || type == UNIQUE_ORDERED_INDEX) {
        skip_partition_for_unique_index = true;
        goto skip_partition_pruning;
      }
    }
    if ((error = get_part_for_delete(record, table->record[0], m_part_info,
                                     &part_id))) {
      return error;
    }
  skip_partition_pruning:
    (void)0;
  }

  NdbOperation::OperationOptions options;
  NdbOperation::OperationOptions *poptions = nullptr;
  options.optionsPresent = 0;

  eventSetAnyValue(m_thd_ndb, &options);

  // Approximate number of bytes that need to be sent to NDB when deleting a row
  // of this table
  const uint delete_size = 12 + (m_bytes_per_write >> 2);
  const bool need_flush = thd_ndb->add_row_check_if_batch_full(delete_size);

  if (thd_ndb->get_applier() || THDVAR(thd, deferred_constraints)) {
    options.optionsPresent |=
        NdbOperation::OperationOptions::OO_DEFERRED_CONSTAINTS;
  }

  if (thd_test_options(thd, OPTION_NO_FOREIGN_KEY_CHECKS)) {
    DBUG_PRINT("info", ("Disabling foreign keys"));
    options.optionsPresent |= NdbOperation::OperationOptions::OO_DISABLE_FK;
  }

  if (cursor) {
    if (options.optionsPresent != 0) poptions = &options;

    /*
      We are scanning records and want to delete the record
      that was just found, call deleteTuple on the cursor
      to take over the lock to a new delete operation
      And thus setting the primary key of the record from
      the active record in cursor
    */
    DBUG_PRINT("info", ("Calling deleteTuple on cursor"));
    if (cursor->deleteCurrentTuple(
            trans, m_ndb_record,
            nullptr,  // result_row
            nullptr,  // result_mask
            poptions, sizeof(NdbOperation::OperationOptions)) == nullptr) {
      ERR_RETURN(trans->getNdbError());
    }
    m_lock_tuple = false;
    thd_ndb->m_unsent_bytes += 12;

    m_trans_table_stats->update_uncommitted_rows(-1);
    m_rows_deleted++;

    if (!(primary_key_update || m_delete_cannot_batch)) {
      thd_ndb->m_unsent_blob_ops |= ndb_table_has_blobs(m_table);
      // If deleting from cursor, NoCommit will be handled in next_result
      return 0;
    }
  } else {
    const NdbRecord *key_rec;
    const uchar *key_row;

    if (m_user_defined_partitioning && !skip_partition_for_unique_index) {
      options.optionsPresent |= NdbOperation::OperationOptions::OO_PARTITION_ID;
      options.partitionId = part_id;
    }

    setup_key_ref_for_ndb_record(&key_rec, &key_row, record,
                                 m_read_before_write_removal_used);

    Uint32 buffer[MAX_CONFLICT_INTERPRETED_PROG_SIZE];
    NdbInterpretedCode code(m_table, buffer,
                            sizeof(buffer) / sizeof(buffer[0]));
    /* Conflict resolution in Applier */
    const Ndb_applier *const applier = m_thd_ndb->get_applier();
    if (applier) {
      bool conflict_handled = false;
      bool dummy_delete_does_not_care = false;

      /* Conflict resolution in slave thread. */
      if (unlikely(
              (error = prepare_conflict_detection(
                   DELETE_ROW, key_rec, m_ndb_record, key_row, /* old_data */
                   nullptr,                                    /* new_data */
                   table->write_set, trans, &code, &options, conflict_handled,
                   dummy_delete_does_not_care))))
        return error;

      if (unlikely(conflict_handled)) {
        /* No need to continue with operation definition */
        /* TODO : Ensure batch execution */
        return 0;
      }
    }

    if (options.optionsPresent != 0) poptions = &options;

    if (trans->deleteTuple(key_rec, (const char *)key_row, m_ndb_record,
                           nullptr,  // row
                           nullptr,  // mask
                           poptions,
                           sizeof(NdbOperation::OperationOptions)) == nullptr) {
      ERR_RETURN(trans->getNdbError());
    }

    m_trans_table_stats->update_uncommitted_rows(-1);
    m_rows_deleted++;

    /*
      Check if we can batch the delete.

      We don't batch deletes as part of primary key updates.
      We do not batch deletes on tables with no primary key. For such tables,
      replication uses full table scan to locate the row to delete. The
      problem is the following scenario when deleting 2 (or more) rows:

       1. Table scan to locate the first row.
       2. Delete the row, batched so no execute.
       3. Table scan to locate the second row is executed, along with the
          batched delete operation from step 2.
       4. The first row is returned from nextResult() (not deleted yet).
       5. The kernel deletes the row (operation from step 2).
       6. lockCurrentTuple() is called on the row returned in step 4. However,
          as that row is now deleted, the operation fails and the transaction
          is aborted.
       7. The delete of the second tuple now fails, as the transaction has
          been aborted.
    */

    if (allow_batch && table_share->primary_key != MAX_KEY &&
        !primary_key_update && !need_flush) {
      return 0;
    }
  }

  // Execute delete operation
  uint ignore_count = 0;
  if (execute_no_commit(m_thd_ndb, trans,
                        m_ignore_no_key || m_read_before_write_removal_used,
                        &ignore_count) != 0) {
    m_thd_ndb->trans_tables.reset_stats();
    return ndb_err(trans);
  }
  if (!primary_key_update) {
    if (!applying_binlog(thd)) {
      assert(m_rows_deleted >= ignore_count);
      m_rows_deleted -= ignore_count;
      m_trans_table_stats->update_uncommitted_rows(ignore_count);
    }
  }
  return 0;
}

/**
  Unpack a record returned from a scan.
  We copy field-for-field to
   1. Avoid unnecessary copying for sparse rows.
   2. Properly initialize not used null bits.
  Note that we do not unpack all returned rows; some primary/unique key
  operations can read directly into the destination row.
*/
int ha_ndbcluster::unpack_record(uchar *dst_row, const uchar *src_row) {
  DBUG_TRACE;
  assert(src_row != nullptr);

  ptrdiff_t dst_offset = dst_row - table->record[0];
  ptrdiff_t src_offset = src_row - table->record[0];

  // Set the NULL flags for all fields
  memset(dst_row, 0xff, table->s->null_bytes);

  uchar *blob_ptr = m_blobs_buffer.get_ptr(0);

  for (uint i = 0; i < table_share->fields; i++) {
    if (!bitmap_is_set(table->read_set, i)) continue;

    Field *field = table->field[i];
    if (!field->stored_in_db) continue;

    // Handle Field_blob (BLOB, JSON, GEOMETRY)
    if (field->is_flag_set(BLOB_FLAG) &&
        !(m_row_side_buffer && bitmap_is_set(&m_in_row_side_buffer, i))) {
      Field_blob *field_blob = (Field_blob *)field;
      NdbBlob *ndb_blob = m_value[i].blob;
      /* unpack_record *only* called for scan result processing
       * *while* the scan is open and the Blob is active.
       * Verify Blob state to be certain.
       * Accessing PK/UK op Blobs after execute() is unsafe
       */
      assert(ndb_blob != nullptr);
      assert(ndb_blob->getState() == NdbBlob::Active);
      int isNull;
      ndbcluster::ndbrequire(ndb_blob->getNull(isNull) == 0);
      Uint64 len64 = 0;
      field_blob->move_field_offset(dst_offset);
      if (!isNull) {
        ndbcluster::ndbrequire(ndb_blob->getLength(len64) == 0);
        ndbcluster::ndbrequire(len64 <= (Uint64)0xffffffff);

        if (len64 > field_blob->max_data_length()) {
          len64 = calc_ndb_blob_len(ndb_blob->getColumn()->getCharset(),
                                    blob_ptr, field_blob->max_data_length());

          // push a warning
          push_warning_printf(
              table->in_use, Sql_condition::SL_WARNING, WARN_DATA_TRUNCATED,
              "Truncated value from TEXT field \'%s\'", field_blob->field_name);
        }
        field->set_notnull();
      }
      /* Need not set_null(), as we initialized null bits to 1 above. */
      field_blob->set_ptr((uint32)len64, blob_ptr);
      field_blob->move_field_offset(-dst_offset);
      blob_ptr += (len64 + 7) & ~((Uint64)7);
      continue;
    }

    // Handle Field_bit
    // Store value in destination even if NULL (i.e. 0)
    if (field->type() == MYSQL_TYPE_BIT) {
      Field_bit *field_bit = down_cast<Field_bit *>(field);
      field->move_field_offset(src_offset);
      longlong value = field_bit->val_int();
      field->move_field_offset(dst_offset - src_offset);
      if (field->is_real_null(src_offset)) {
        // This sets the uneven highbits, located after the null bit
        // in the Field_bit ptr, to 0
        value = 0;
        // Make sure destination null flag is correct
        field->set_null(dst_offset);
      } else {
        field->set_notnull(dst_offset);
      }
      // Field_bit in DBUG requires the bit set in write_set for store().
      my_bitmap_map *old_map =
          dbug_tmp_use_all_columns(table, table->write_set);
      ndbcluster::ndbrequire(field_bit->store(value, true) == 0);
      dbug_tmp_restore_column_map(table->write_set, old_map);
      field->move_field_offset(-dst_offset);
      continue;
    }

    // A normal field (not blob or bit type).
    if (field->is_real_null(src_offset)) {
      // Field is NULL and the null flags are already set
      continue;
    }
    const uint32 actual_length = field_used_length(field, src_offset);
    field->set_notnull(dst_offset);
    memcpy(field->field_ptr() + dst_offset, field->field_ptr() + src_offset,
           actual_length);
  }

  if (unlikely(!m_cond.check_condition())) {
    return HA_ERR_KEY_NOT_FOUND;  // False condition
  }
  assert(pushed_cond == nullptr || const_cast<Item *>(pushed_cond)->val_int());
  return 0;
}

int ha_ndbcluster::unpack_record_and_set_generated_fields(
    uchar *dst_row, const uchar *src_row) {
  const int res = unpack_record(dst_row, src_row);
  if (res == 0 && Ndb_table_map::has_virtual_gcol(table)) {
    update_generated_read_fields(dst_row, table);
  }
  return res;
}

/**
  Get the default value of the field from default_values of the table.
*/
static void get_default_value(void *def_val, Field *field) {
  assert(field != nullptr);
  assert(field->stored_in_db);

  ptrdiff_t src_offset = field->table->default_values_offset();

  {
    if (bitmap_is_set(field->table->read_set, field->field_index())) {
      if (field->type() == MYSQL_TYPE_BIT) {
        Field_bit *field_bit = static_cast<Field_bit *>(field);
        if (!field->is_real_null(src_offset)) {
          field->move_field_offset(src_offset);
          longlong value = field_bit->val_int();
          /* Map to NdbApi format - two Uint32s */
          Uint32 out[2];
          out[0] = 0;
          out[1] = 0;
          for (int b = 0; b < 64; b++) {
            out[b >> 5] |= (value & 1) << (b & 31);

            value = value >> 1;
          }
          memcpy(def_val, out, sizeof(longlong));
          field->move_field_offset(-src_offset);
        }
      } else if (field->is_flag_set(BLOB_FLAG) &&
                 field->type() != MYSQL_TYPE_VECTOR) {
        assert(false);
      } else {
        field->move_field_offset(src_offset);
        /* Normal field (not blob or bit type). */
        if (!field->is_null()) {
          /* Only copy actually used bytes of varstrings. */
          uint32 actual_length = field_used_length(field);
          uchar *src_ptr = field->field_ptr();
          field->set_notnull();
          memcpy(def_val, src_ptr, actual_length);
        }
        field->move_field_offset(-src_offset);
        /* No action needed for a NULL field. */
      }
    }
  }
}

int fail_index_offline(TABLE *t, int index) {
  KEY *key_info = t->key_info + index;
  push_warning_printf(
      t->in_use, Sql_condition::SL_WARNING, ER_NOT_KEYFILE,
      "Index %s is not available in NDB. Use \"ALTER TABLE %s ALTER INDEX %s "
      "INVISIBLE\" to prevent MySQL from attempting to access it, or use "
      "\"ndb_restore --rebuild-indexes\" to rebuild it.",
      key_info->name, t->s->table_name.str, key_info->name);
  return HA_ERR_CRASHED;
}

int ha_ndbcluster::index_init(uint index, bool sorted) {
  DBUG_TRACE;
  DBUG_PRINT("enter", ("index: %u  sorted: %d", index, sorted));
  if (index < MAX_KEY && m_index[index].type == UNDEFINED_INDEX)
    return fail_index_offline(table, index);

  if (m_thd_ndb->get_applier()) {
    if (table_share->primary_key == MAX_KEY &&  // hidden pk
        m_thd_ndb->m_unsent_bytes) {
      // Applier starting read from table with hidden pk when there are already
      // defined operations that need to be prepared in order to "read your own
      // writes" as well as handle errors uniformly.
      DBUG_PRINT("info", ("Prepare already defined operations before read"));
      constexpr bool IGNORE_NO_KEY = true;
      if (execute_no_commit(m_thd_ndb, m_thd_ndb->trans, IGNORE_NO_KEY) != 0) {
        m_thd_ndb->trans_tables.reset_stats();
        return ndb_err(m_thd_ndb->trans);
      }
    }
  }

  active_index = index;
  m_sorted = sorted;
  /*
    Locks are are explicitly released in scan
    unless m_lock.type == TL_READ_HIGH_PRIORITY
    and no sub-sequent call to unlock_row()
  */
  m_lock_tuple = false;

  if (table_share->primary_key == MAX_KEY && m_use_partition_pruning) {
    bitmap_union(table->read_set, &m_part_info->full_part_field_set);
  }

  return 0;
}

int ha_ndbcluster::index_end() {
  DBUG_TRACE;
  return close_scan();
}

/**
  Check if key contains null.
*/
static int check_null_in_key(const KEY *key_info, const uchar *key,
                             uint key_len) {
  KEY_PART_INFO *curr_part, *end_part;
  const uchar *end_ptr = key + key_len;
  curr_part = key_info->key_part;
  end_part = curr_part + key_info->user_defined_key_parts;

  for (; curr_part != end_part && key < end_ptr; curr_part++) {
    if (curr_part->null_bit && *key) return 1;

    key += curr_part->store_length;
  }
  return 0;
}

int ha_ndbcluster::index_read(uchar *buf, const uchar *key, uint key_len,
                              enum ha_rkey_function find_flag) {
  key_range start_key, end_key, *end_key_p = nullptr;
  bool descending = false;
  DBUG_TRACE;
  DBUG_PRINT("enter", ("active_index: %u, key_len: %u, find_flag: %d",
                       active_index, key_len, find_flag));

  start_key.key = key;
  start_key.length = key_len;
  start_key.flag = find_flag;
  switch (find_flag) {
    case HA_READ_KEY_EXACT:
      /**
       * Specify as a closed EQ_RANGE.
       * Setting HA_READ_AFTER_KEY seems odd, but this is according
       * to MySQL convention, see opt_range.cc.
       */
      end_key.key = key;
      end_key.length = key_len;
      end_key.flag = HA_READ_AFTER_KEY;
      end_key_p = &end_key;
      break;
    case HA_READ_KEY_OR_PREV:
    case HA_READ_BEFORE_KEY:
    case HA_READ_PREFIX_LAST:
    case HA_READ_PREFIX_LAST_OR_PREV:
      descending = true;
      break;
    default:
      break;
  }
  const int error =
      read_range_first_to_buf(&start_key, end_key_p, descending, m_sorted, buf);
  return error;
}

int ha_ndbcluster::index_next(uchar *buf) {
  DBUG_TRACE;
  ha_statistic_increment(&System_status_var::ha_read_next_count);
  const int error = next_result(buf);
  return error;
}

int ha_ndbcluster::index_prev(uchar *buf) {
  DBUG_TRACE;
  ha_statistic_increment(&System_status_var::ha_read_prev_count);
  const int error = next_result(buf);
  return error;
}

int ha_ndbcluster::index_first(uchar *buf) {
  DBUG_TRACE;
  if (!m_index[active_index].index)
    return fail_index_offline(table, active_index);
  ha_statistic_increment(&System_status_var::ha_read_first_count);
  // Start the ordered index scan and fetch the first row

  // Only HA_READ_ORDER indexes get called by index_first
  const int error =
      ordered_index_scan(nullptr, nullptr, m_sorted, false, buf, nullptr);
  return error;
}

int ha_ndbcluster::index_last(uchar *buf) {
  DBUG_TRACE;
  if (!m_index[active_index].index)
    return fail_index_offline(table, active_index);
  ha_statistic_increment(&System_status_var::ha_read_last_count);
  const int error =
      ordered_index_scan(nullptr, nullptr, m_sorted, true, buf, nullptr);
  return error;
}

int ha_ndbcluster::index_next_same(uchar *buf,
                                   const uchar *key [[maybe_unused]],
                                   uint length [[maybe_unused]]) {
  DBUG_TRACE;
  ha_statistic_increment(&System_status_var::ha_read_next_count);
  const int error = next_result(buf);
  return error;
}

int ha_ndbcluster::index_read_last(uchar *buf, const uchar *key, uint key_len) {
  DBUG_TRACE;
  return index_read(buf, key, key_len, HA_READ_PREFIX_LAST);
}

int ha_ndbcluster::read_range_first_to_buf(const key_range *start_key,
                                           const key_range *end_key, bool desc,
                                           bool sorted, uchar *buf) {
  part_id_range part_spec;
  const NDB_INDEX_TYPE type = get_index_type(active_index);
  const KEY *key_info = table->key_info + active_index;
  int error;
  DBUG_TRACE;
  DBUG_PRINT("info", ("desc: %d, sorted: %d", desc, sorted));

  if (unlikely((error = close_scan()))) return error;

  if (m_use_partition_pruning) {
    assert(m_pushed_join_operation != PUSHED_ROOT);
    get_partition_set(table, buf, active_index, start_key, &part_spec);
    DBUG_PRINT("info", ("part_spec.start_part: %u  part_spec.end_part: %u",
                        part_spec.start_part, part_spec.end_part));
    /*
      If partition pruning has found no partition in set
      we can return HA_ERR_END_OF_FILE
      If partition pruning has found exactly one partition in set
      we can optimize scan to run towards that partition only.
    */
    if (part_spec.start_part > part_spec.end_part) {
      return HA_ERR_END_OF_FILE;
    }

    if (part_spec.start_part == part_spec.end_part) {
      /*
        Only one partition is required to scan, if sorted is required we
        don't need it any more since output from one ordered partitioned
        index is always sorted.
      */
      sorted = false;
      if (unlikely(!get_transaction_part_id(part_spec.start_part, error))) {
        return error;
      }
    }
  }

  switch (type) {
    case PRIMARY_KEY_ORDERED_INDEX:
    case PRIMARY_KEY_INDEX:
      if (start_key && start_key->length == key_info->key_length &&
          start_key->flag == HA_READ_KEY_EXACT) {
        if (!m_thd_ndb->trans)
          if (unlikely(
                  !start_transaction_key(active_index, start_key->key, error)))
            return error;
        DBUG_DUMP("key", start_key->key, start_key->length);
        error = pk_read(
            start_key->key, buf,
            (m_use_partition_pruning) ? &(part_spec.start_part) : nullptr);
        return error == HA_ERR_KEY_NOT_FOUND ? HA_ERR_END_OF_FILE : error;
      }
      break;
    case UNIQUE_ORDERED_INDEX:
    case UNIQUE_INDEX:
      if (start_key && start_key->length == key_info->key_length &&
          start_key->flag == HA_READ_KEY_EXACT &&
          !check_null_in_key(key_info, start_key->key, start_key->length)) {
        if (!m_thd_ndb->trans)
          if (unlikely(
                  !start_transaction_key(active_index, start_key->key, error)))
            return error;
        DBUG_DUMP("key", start_key->key, start_key->length);
        error = unique_index_read(start_key->key, buf);
        return error == HA_ERR_KEY_NOT_FOUND ? HA_ERR_END_OF_FILE : error;
      } else if (type == UNIQUE_INDEX)
        return full_table_scan(key_info, start_key, end_key, buf);
      break;
    default:
      break;
  }
  if (!m_use_partition_pruning && !m_thd_ndb->trans) {
    get_partition_set(table, buf, active_index, start_key, &part_spec);
    if (part_spec.start_part == part_spec.end_part)
      if (unlikely(!start_transaction_part_id(part_spec.start_part, error)))
        return error;
  }
  // Start the ordered index scan and fetch the first row
  return ordered_index_scan(start_key, end_key, sorted, desc, buf,
                            (m_use_partition_pruning) ? &part_spec : nullptr);
}

int ha_ndbcluster::read_range_first(const key_range *start_key,
                                    const key_range *end_key,
                                    bool /* eq_range */, bool sorted) {
  uchar *buf = table->record[0];
  DBUG_TRACE;
  return read_range_first_to_buf(start_key, end_key, false, sorted, buf);
}

int ha_ndbcluster::read_range_next() {
  DBUG_TRACE;
  return next_result(table->record[0]);
}

int ha_ndbcluster::Copying_alter::save_commit_count(
    Thd_ndb *thd_ndb, const NdbDictionary::Table *ndbtab) {
  NdbError ndb_err;
  Uint64 commit_count;
  if (ndb_get_table_commit_count(thd_ndb->ndb, ndbtab, ndb_err,
                                 &commit_count)) {
    return ndb_to_mysql_error(&ndb_err);
  }

  DBUG_PRINT("info", ("Saving commit count: %llu", commit_count));
  m_saved_commit_count = commit_count;
  return 0;
}

// Check that commit count have not changed since it was saved
int ha_ndbcluster::Copying_alter::check_saved_commit_count(
    Thd_ndb *thd_ndb, const NdbDictionary::Table *ndbtab) const {
  NdbError ndb_err;
  Uint64 commit_count;
  if (ndb_get_table_commit_count(thd_ndb->ndb, ndbtab, ndb_err,
                                 &commit_count)) {
    return ndb_to_mysql_error(&ndb_err);
  }

  DBUG_PRINT("info", ("Comparing commit count: %llu with saved value: %llu",
                      commit_count, m_saved_commit_count));
  if (commit_count != m_saved_commit_count) {
    my_printf_error(
        ER_TABLE_DEF_CHANGED,
        "Detected change to data in source table during copying ALTER "
        "TABLE. Alter aborted to avoid inconsistency.",
        MYF(0));
    return HA_ERR_GENERIC;  // Does not set a new error
  }
  return 0;
}

int ha_ndbcluster::rnd_init(bool) {
  DBUG_TRACE;

  if (int error = close_scan()) {
    return error;
  }

  if (int error = index_init(table_share->primary_key, 0)) {
    return error;
  }

  if (m_thd_ndb->sql_command() == SQLCOM_ALTER_TABLE) {
    // Detected start of scan for copying ALTER TABLE. Save commit count of the
    // scanned (source) table.
    if (int error = copying_alter.save_commit_count(m_thd_ndb, m_table)) {
      return error;
    }
  }

  return 0;
}

int ha_ndbcluster::close_scan() {
  DBUG_TRACE;

  if (m_active_query) {
    m_active_query->close(m_thd_ndb->m_force_send);
    m_active_query = nullptr;
  }

  m_cond.cond_close();

  NdbScanOperation *cursor = m_active_cursor;
  if (!cursor) {
    cursor = m_multi_cursor;
    if (!cursor) return 0;
  }

  int error;
  NdbTransaction *trans = m_thd_ndb->trans;
  if ((error = scan_handle_lock_tuple(cursor, trans)) != 0) return error;

  if (m_thd_ndb->m_unsent_bytes) {
    /*
      Take over any pending transactions to the
      deleting/updating transaction before closing the scan
    */
    DBUG_PRINT("info", ("thd_ndb->m_unsent_bytes: %ld",
                        (long)m_thd_ndb->m_unsent_bytes));
    if (execute_no_commit(m_thd_ndb, trans, m_ignore_no_key) != 0) {
      m_thd_ndb->trans_tables.reset_stats();
      return ndb_err(trans);
    }
  }

  cursor->close(m_thd_ndb->m_force_send, true);
  m_active_cursor = nullptr;
  m_multi_cursor = nullptr;
  return 0;
}

int ha_ndbcluster::rnd_end() {
  DBUG_TRACE;
  return close_scan();
}

int ha_ndbcluster::rnd_next(uchar *buf) {
  DBUG_TRACE;
  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);

  int error;
  if (m_active_cursor || m_active_query)
    error = next_result(buf);
  else
    error = full_table_scan(nullptr, nullptr, nullptr, buf);

  return error;
}

/**
  An "interesting" record has been found and it's pk
  retrieved by calling position. Now it's time to read
  the record from db once again.
*/

int ha_ndbcluster::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_TRACE;
  ha_statistic_increment(&System_status_var::ha_read_rnd_count);
  // The primary key for the record is stored in pos
  // Perform a pk_read using primary key "index"
  {
    part_id_range part_spec;
    uint key_length = ref_length;
    if (m_user_defined_partitioning) {
      if (table_share->primary_key == MAX_KEY) {
        /*
          The partition id has been fetched from ndb
          and has been stored directly after the hidden key
        */
        DBUG_DUMP("key+part", pos, key_length);
        key_length = ref_length - sizeof(m_part_id);
        part_spec.start_part = part_spec.end_part =
            *(uint32 *)(pos + key_length);
      } else {
        key_range key_spec;
        KEY *key_info = table->key_info + table_share->primary_key;
        key_spec.key = pos;
        key_spec.length = key_length;
        key_spec.flag = HA_READ_KEY_EXACT;
        get_full_part_id_from_key(table, buf, key_info, &key_spec, &part_spec);
        assert(part_spec.start_part == part_spec.end_part);
      }
      DBUG_PRINT("info", ("partition id %u", part_spec.start_part));
    }
    DBUG_DUMP("key", pos, key_length);
    int res = pk_read(
        pos, buf,
        (m_user_defined_partitioning) ? &(part_spec.start_part) : nullptr);
    if (res == HA_ERR_KEY_NOT_FOUND) {
      /**
       * When using rnd_pos
       *   server first retrieves a set of records (typically scans them)
       *   and store a unique identifier (for ndb this is the primary key)
       *   and later retrieves the record again using rnd_pos and the
       *   saved primary key. For ndb, since we only support committed read
       *   the record could have been deleted in between the "save" and
       *   the rnd_pos.
       *   Therefore we return HA_ERR_RECORD_DELETED in this case rather than
       *   HA_ERR_KEY_NOT_FOUND (which will cause statement to be aborted)
       *
       */
      res = HA_ERR_RECORD_DELETED;
    }
    return res;
  }
}

/**
  Store the primary key of this record in ref
  variable, so that the row can be retrieved again later
  using "reference" in rnd_pos.
*/

void ha_ndbcluster::position(const uchar *record) {
  KEY *key_info;
  KEY_PART_INFO *key_part;
  KEY_PART_INFO *end;
  uchar *buff;
  uint key_length;

  DBUG_TRACE;

  if (table_share->primary_key != MAX_KEY) {
    key_length = ref_length;
    key_info = table->key_info + table_share->primary_key;
    key_part = key_info->key_part;
    end = key_part + key_info->user_defined_key_parts;
    buff = ref;

    for (; key_part != end; key_part++) {
      if (key_part->null_bit) {
        /* Store 0 if the key part is a NULL part */
        if (record[key_part->null_offset] & key_part->null_bit) {
          *buff++ = 1;
          continue;
        }
        *buff++ = 0;
      }

      size_t len = key_part->length;
      const uchar *ptr = record + key_part->offset;
      Field *field = key_part->field;
      if (field->type() == MYSQL_TYPE_VARCHAR) {
        size_t var_length;
        if (field->get_length_bytes() == 1) {
          /**
           * Keys always use 2 bytes length
           */
          buff[0] = ptr[0];
          buff[1] = 0;
          var_length = ptr[0];
          assert(var_length <= len);
          memcpy(buff + 2, ptr + 1, var_length);
        } else {
          var_length = ptr[0] + (ptr[1] * 256);
          assert(var_length <= len);
          memcpy(buff, ptr, var_length + 2);
        }
        /**
          We have to zero-pad any unused VARCHAR buffer so that MySQL is
          able to use simple memcmp to compare two instances of the same
          unique key value to determine if they are equal.
          MySQL does this to compare contents of two 'ref' values.
          (Duplicate weedout algorithm is one such case.)
        */
        memset(buff + 2 + var_length, 0, len - var_length);
        len += 2;
      } else {
        memcpy(buff, ptr, len);
      }
      buff += len;
    }
  } else {
    // No primary key, get hidden key
    DBUG_PRINT("info", ("Getting hidden key"));
    // If table has user defined partition save the partition id as well
    if (m_user_defined_partitioning) {
      DBUG_PRINT("info", ("Saving partition id %u", m_part_id));
      key_length = ref_length - sizeof(m_part_id);
      memcpy(ref + key_length, (void *)&m_part_id, sizeof(m_part_id));
    } else
      key_length = ref_length;
#ifndef NDEBUG
    constexpr uint NDB_HIDDEN_PRIMARY_KEY_LENGTH = 8;
    const int hidden_no = Ndb_table_map::num_stored_fields(table);
    const NDBCOL *hidden_col = m_table->getColumn(hidden_no);
    assert(hidden_col->getPrimaryKey() && hidden_col->getAutoIncrement() &&
           key_length == NDB_HIDDEN_PRIMARY_KEY_LENGTH);
#endif
    memcpy(ref, &m_ref, key_length);
  }
#ifndef NDEBUG
  if (table_share->primary_key == MAX_KEY && m_user_defined_partitioning)
    DBUG_DUMP("key+part", ref, key_length + sizeof(m_part_id));
#endif
  DBUG_DUMP("ref", ref, key_length);
}

int ha_ndbcluster::cmp_ref(const uchar *ref1, const uchar *ref2) const {
  DBUG_TRACE;

  if (table_share->primary_key != MAX_KEY) {
    KEY *key_info = table->key_info + table_share->primary_key;
    KEY_PART_INFO *key_part = key_info->key_part;
    KEY_PART_INFO *end = key_part + key_info->user_defined_key_parts;

    for (; key_part != end; key_part++) {
      // NOTE: No need to check for null since PK is not-null

      Field *field = key_part->field;
      int result = field->key_cmp(ref1, ref2);
      if (result) {
        return result;
      }

      if (field->type() == MYSQL_TYPE_VARCHAR) {
        ref1 += 2;
        ref2 += 2;
      }

      ref1 += key_part->length;
      ref2 += key_part->length;
    }
    return 0;
  } else {
    return memcmp(ref1, ref2, ref_length);
  }
}

int ha_ndbcluster::info(uint flag) {
  THD *thd = table->in_use;
  DBUG_TRACE;
  DBUG_PRINT("enter", ("flag: %d", flag));

  if (flag & HA_STATUS_POS) DBUG_PRINT("info", ("HA_STATUS_POS"));
  if (flag & HA_STATUS_TIME) DBUG_PRINT("info", ("HA_STATUS_TIME"));
  if (flag & HA_STATUS_CONST) {
    /*
      Set size required by a single record in the MRR 'HANDLER_BUFFER'.
      MRR buffer has both a fixed and a variable sized part.
      Size is calculated assuming max size of the variable part.

      See comments for multi_range_fixed_size() and
      multi_range_max_entry() regarding how the MRR buffer is organized.
    */
    stats.mrr_length_per_rec =
        multi_range_fixed_size(1) +
        multi_range_max_entry(PRIMARY_KEY_INDEX, m_mrr_reclength);
  }
  if (flag & HA_STATUS_VARIABLE) {
    DBUG_PRINT("info", ("HA_STATUS_VARIABLE"));

    if (!thd) {
      thd = current_thd;
    }

    if (!m_trans_table_stats) {
      if (check_ndb_connection(thd)) return HA_ERR_NO_CONNECTION;
    }

    /*
      May need to update local copy of statistics in
      'm_trans_table_stats', either directly from datanodes,
      or from NDB_SHARE cached copy (mutex protected), if:
       1) 'ndb_use_exact_count' has been set (by config or user).
       2) HA_STATUS_NO_LOCK -> read from NDB_SHARE cached copy.
       3) Local copy is invalid.
    */
    const bool exact_count = THDVAR(thd, use_exact_count);
    DBUG_PRINT("info", ("exact_count: %d", exact_count));

    const bool no_lock_flag = flag & HA_STATUS_NO_LOCK;
    DBUG_PRINT("info", ("no_lock: %d", no_lock_flag));

    if (exact_count ||                     // 1)
        !no_lock_flag ||                   // 2)
        m_trans_table_stats == nullptr ||  // 3) no trans stats registered
        m_trans_table_stats->invalid())    // 3)
    {
      const int result = update_stats(thd, exact_count || !no_lock_flag);
      if (result) {
        return result;
      }
    } else {
      // Use transaction table stats, these stats are only used by this thread
      // so no locks are required. Just double check that the stats have been
      // updated previously.
      assert(!m_trans_table_stats->invalid());

      // Update handler::stats with rows in table plus rows changed by trans
      // This is doing almost the same thing as in update_stats()
      // i.e the number of records in active transaction plus number of
      // uncommitted are assigned to stats.records
      stats.records = m_trans_table_stats->table_rows +
                      m_trans_table_stats->uncommitted_rows;
      DBUG_PRINT("table_stats",
                 ("records updated from trans stats: %llu ", stats.records));
    }

    const int sql_command = thd_sql_command(thd);
    if (sql_command == SQLCOM_SHOW_TABLE_STATUS ||
        sql_command == SQLCOM_SHOW_KEYS) {
      DBUG_PRINT("table_stats",
                 ("Special case for showing actual number of records: %llu",
                  stats.records));
    } else {
      // Adjust `stats.records` to never be < 2 since optimizer interprets the
      // values 0 and 1 as EXACT.
      // NOTE! It looks like the above statement is correct only when
      // HA_STATS_RECORDS_IS_EXACT is returned from table_flags(), something
      // which ndbcluster does not.
      if (stats.records < 2) {
        DBUG_PRINT("table_stats", ("adjust records %llu -> 2", stats.records));
        stats.records = 2;
      }
    }
    set_rec_per_key(thd);
  }
  if (flag & HA_STATUS_ERRKEY) {
    DBUG_PRINT("info", ("HA_STATUS_ERRKEY dupkey=%u", m_dupkey));
    errkey = m_dupkey;
  }
  if (flag & HA_STATUS_AUTO) {
    DBUG_PRINT("info", ("HA_STATUS_AUTO"));
    if (m_table && table->found_next_number_field) {
      if (!thd) thd = current_thd;
      if (check_ndb_connection(thd)) return HA_ERR_NO_CONNECTION;
      Ndb *ndb = get_thd_ndb(thd)->ndb;
      NDB_SHARE::Tuple_id_range_guard g(m_share);

      Uint64 auto_increment_value64;
      if (ndb->readAutoIncrementValue(m_table, g.range,
                                      auto_increment_value64) == -1) {
        const NdbError err = ndb->getNdbError();
        ndb_log_error("Error %d in readAutoIncrementValue(): %s", err.code,
                      err.message);
        stats.auto_increment_value = ~(ulonglong)0;
      } else
        stats.auto_increment_value = (ulonglong)auto_increment_value64;
    }
  }

  return 0;
}

/**
   @brief Return statistics for given partition

   @param[out] stat_info    The place where to return updated statistics
   @param[out] checksum     The place where to return checksum (if any)
   @param part_id           Id of the partition to return statistics for
 */
void ha_ndbcluster::get_dynamic_partition_info(ha_statistics *stat_info,
                                               ha_checksum *checksum,
                                               uint part_id) {
  DBUG_TRACE;
  DBUG_PRINT("enter", ("part_id: %d", part_id));

  THD *thd = current_thd;
  if (check_ndb_connection(thd)) {
    my_error(HA_ERR_NO_CONNECTION, MYF(0));
    return;
  }
  Thd_ndb *thd_ndb = get_thd_ndb(thd);

  // Checksum not supported, set it to 0
  *checksum = 0;

  // Read fresh stats from NDB for given partition (one roundtrip)
  NdbError ndb_error;
  Ndb_table_stats part_stats;
  if (ndb_get_table_statistics(thd, thd_ndb->ndb, m_table, &part_stats,
                               ndb_error, part_id)) {
    if (ndb_error.classification == NdbError::SchemaError) {
      // Updating stats for table failed due to a schema error. Mark the NDB
      // table def as invalid, this will cause also all index defs to be
      // invalidate on close
      m_table->setStatusInvalid();
    }
    ndb_to_mysql_error(&ndb_error);  // Called to push any NDB error as warning

    // Nothing else to do, caller has initialized stat_info to zero
    DBUG_PRINT("error", ("Failed to update stats"));
    return;
  }

  // Copy partition stats into callers stats buffer
  stat_info->records = part_stats.row_count;
  stat_info->mean_rec_length = part_stats.row_size;
  stat_info->data_file_length = part_stats.fragment_memory;
  stat_info->delete_length = part_stats.fragment_extent_free_space;
  stat_info->max_data_file_length = part_stats.fragment_extent_space;
}

int ha_ndbcluster::extra(enum ha_extra_function operation) {
  DBUG_TRACE;
  switch (operation) {
    case HA_EXTRA_IGNORE_DUP_KEY: /* Dup keys don't rollback everything*/
      DBUG_PRINT("info", ("HA_EXTRA_IGNORE_DUP_KEY"));
      DBUG_PRINT("info", ("Ignoring duplicate key"));
      m_ignore_dup_key = true;
      break;
    case HA_EXTRA_NO_IGNORE_DUP_KEY:
      DBUG_PRINT("info", ("HA_EXTRA_NO_IGNORE_DUP_KEY"));
      m_ignore_dup_key = false;
      break;
    case HA_EXTRA_IGNORE_NO_KEY:
      DBUG_PRINT("info", ("HA_EXTRA_IGNORE_NO_KEY"));
      DBUG_PRINT("info", ("Turning on AO_IgnoreError at Commit/NoCommit"));
      m_ignore_no_key = true;
      break;
    case HA_EXTRA_NO_IGNORE_NO_KEY:
      DBUG_PRINT("info", ("HA_EXTRA_NO_IGNORE_NO_KEY"));
      DBUG_PRINT("info", ("Turning on AO_IgnoreError at Commit/NoCommit"));
      m_ignore_no_key = false;
      break;
    case HA_EXTRA_WRITE_CAN_REPLACE:
      DBUG_PRINT("info", ("HA_EXTRA_WRITE_CAN_REPLACE"));
      if (!m_has_unique_index ||
          /*
             Always set if slave, quick fix for bug 27378
             or if manual binlog application, for bug 46662
          */
          applying_binlog(current_thd)) {
        DBUG_PRINT("info", ("Turning ON use of write instead of insert"));
        m_use_write = true;
      }
      break;
    case HA_EXTRA_WRITE_CANNOT_REPLACE:
      DBUG_PRINT("info", ("HA_EXTRA_WRITE_CANNOT_REPLACE"));
      DBUG_PRINT("info", ("Turning OFF use of write instead of insert"));
      m_use_write = false;
      break;
    case HA_EXTRA_DELETE_CANNOT_BATCH:
      DBUG_PRINT("info", ("HA_EXTRA_DELETE_CANNOT_BATCH"));
      m_delete_cannot_batch = true;
      break;
    case HA_EXTRA_UPDATE_CANNOT_BATCH:
      DBUG_PRINT("info", ("HA_EXTRA_UPDATE_CANNOT_BATCH"));
      m_update_cannot_batch = true;
      break;
    // We don't implement 'KEYREAD'. However, KEYREAD also implies
    // DISABLE_JOINPUSH.
    case HA_EXTRA_KEYREAD:
      DBUG_PRINT("info", ("HA_EXTRA_KEYREAD"));
      m_disable_pushed_join = true;
      break;
    case HA_EXTRA_NO_KEYREAD:
      DBUG_PRINT("info", ("HA_EXTRA_NO_KEYREAD"));
      m_disable_pushed_join = false;
      break;
    case HA_EXTRA_BEGIN_ALTER_COPY:
      // Start of copy into intermediate table during copying alter, turn
      // off transactions when writing into the intermediate table in order to
      // avoid exhausting NDB transaction resources, this is safe as it would
      // be dropped anyway if there is a failure during the alter
      DBUG_PRINT("info", ("HA_EXTRA_BEGIN_ALTER_COPY"));
      m_thd_ndb->set_trans_option(Thd_ndb::TRANS_TRANSACTIONS_OFF);
      break;
    case HA_EXTRA_END_ALTER_COPY:
      // End of copy into intermediate table during copying alter.
      // Nothing to do, the transactions will automatically be enabled
      // again for subsequent statement
      DBUG_PRINT("info", ("HA_EXTRA_END_ALTER_COPY"));
      break;
    default:
      break;
  }

  return 0;
}

bool ha_ndbcluster::start_read_removal() {
  THD *thd = table->in_use;
  DBUG_TRACE;

  if (uses_blob_value(table->write_set)) {
    DBUG_PRINT("exit", ("No! Blob field in write_set"));
    return false;
  }

  if (thd->lex->sql_command == SQLCOM_DELETE && table_share->blob_fields) {
    DBUG_PRINT("exit", ("No! DELETE from table with blob(s)"));
    return false;
  }

  if (table_share->primary_key == MAX_KEY) {
    DBUG_PRINT("exit", ("No! Table with hidden key"));
    return false;
  }

  if (bitmap_is_overlapping(table->write_set, m_pk_bitmap_p)) {
    DBUG_PRINT("exit", ("No! Updating primary key"));
    return false;
  }

  if (m_has_unique_index) {
    for (uint i = 0; i < table_share->keys; i++) {
      const KEY *key = table->key_info + i;
      if ((key->flags & HA_NOSAME) &&
          bitmap_is_overlapping(table->write_set, m_key_fields[i])) {
        DBUG_PRINT("exit", ("No! Unique key %d is updated", i));
        return false;
      }
    }
  }
  m_read_before_write_removal_possible = true;
  DBUG_PRINT("exit", ("Yes, rbwr is possible!"));
  return true;
}

ha_rows ha_ndbcluster::end_read_removal(void) {
  DBUG_TRACE;
  assert(m_read_before_write_removal_possible);
  DBUG_PRINT("info",
             ("updated: %llu, deleted: %llu", m_rows_updated, m_rows_deleted));
  return m_rows_updated + m_rows_deleted;
}

int ha_ndbcluster::reset() {
  DBUG_TRACE;
  m_cond.cond_clear();

  assert(m_active_query == nullptr);
  if (m_pushed_join_operation == PUSHED_ROOT)  // Root of pushed query
  {
    delete m_pushed_join_member;  // Also delete QueryDef
  }
  m_pushed_join_member = nullptr;
  m_pushed_join_operation = -1;
  m_disable_pushed_join = false;

  /* reset flags set by extra calls */
  m_read_before_write_removal_possible = false;
  m_read_before_write_removal_used = false;
  m_rows_updated = m_rows_deleted = 0;
  m_ignore_dup_key = false;
  m_use_write = false;
  m_ignore_no_key = false;
  m_rows_to_insert = (ha_rows)1;
  m_delete_cannot_batch = false;
  m_update_cannot_batch = false;

  assert(m_is_bulk_delete == false);
  m_is_bulk_delete = false;
  return 0;
}

int ha_ndbcluster::flush_bulk_insert(bool allow_batch) {
  NdbTransaction *trans = m_thd_ndb->trans;
  DBUG_TRACE;
  assert(trans);

  if (m_thd_ndb->check_trans_option(Thd_ndb::TRANS_TRANSACTIONS_OFF)) {
    /*
      signal that transaction will be broken up and hence cannot
      be rolled back
    */
    THD *thd = table->in_use;
    thd->get_transaction()->mark_modified_non_trans_table(
        Transaction_ctx::SESSION);
    thd->get_transaction()->mark_modified_non_trans_table(
        Transaction_ctx::STMT);
    if (execute_commit(m_thd_ndb, trans, m_thd_ndb->m_force_send,
                       m_ignore_no_key) != 0) {
      m_thd_ndb->trans_tables.reset_stats();
      return ndb_err(trans);
    }
    if (trans->restart() != 0) {
      assert(0);
      return -1;
    }
    return 0;
  }

  if (!allow_batch &&
      execute_no_commit(m_thd_ndb, trans, m_ignore_no_key) != 0) {
    m_thd_ndb->trans_tables.reset_stats();
    return ndb_err(trans);
  }

  return 0;
}

/**
  Start of an insert, remember number of rows to be inserted, it will
  be used in write_row and get_autoincrement to send an optimal number
  of rows in each roundtrip to the server.

  @param
   rows     number of rows to insert, 0 if unknown
*/

void ha_ndbcluster::start_bulk_insert(ha_rows rows) {
  DBUG_TRACE;
  DBUG_PRINT("enter", ("rows: %d", (int)rows));

  if (!m_use_write && m_ignore_dup_key) {
    /*
      compare if expression with that in write_row
      we have a situation where peek_indexed_rows() will be called
      so we cannot batch
    */
    DBUG_PRINT("info", ("Batching turned off as duplicate key is "
                        "ignored by using peek_row"));
    m_rows_to_insert = 1;
    return;
  }
  if (rows == (ha_rows)0) {
    /* We don't know how many will be inserted, guess */
    m_rows_to_insert = (m_autoincrement_prefetch > DEFAULT_AUTO_PREFETCH)
                           ? m_autoincrement_prefetch
                           : DEFAULT_AUTO_PREFETCH;
    m_autoincrement_prefetch = m_rows_to_insert;
  } else {
    m_rows_to_insert = rows;
    if (m_autoincrement_prefetch < m_rows_to_insert)
      m_autoincrement_prefetch = m_rows_to_insert;
  }
}

/**
  End of an insert.
*/
int ha_ndbcluster::end_bulk_insert() {
  int error = 0;

  DBUG_TRACE;
  // Check if last inserts need to be flushed

  THD *thd = table->in_use;
  Thd_ndb *thd_ndb = m_thd_ndb;

  if (!thd_allow_batch(thd) && thd_ndb->m_unsent_bytes) {
    const bool allow_batch = (thd_ndb->m_handler != nullptr);
    error = flush_bulk_insert(allow_batch);
    if (error != 0) {
      // The requirement to calling set_my_errno() here is
      // not according to the handler interface specification
      // However there it is still code in Sql_cmd_load_table::execute_inner()
      // which checks 'my_errno' after end_bulk_insert has reported failure
      // The call to set_my_errno() can be removed from here when
      // Bug #26126535 	MYSQL_LOAD DOES NOT CHECK RETURN VALUES
      // FROM HANDLER BULK INSERT FUNCTIONS has been fixed upstream
      set_my_errno(error);
    }
  }

  m_rows_to_insert = 1;
  return error;
}

/**
  How many seeks it will take to read through the table.

  This is to be comparable to the number returned by records_in_range so
  that we can decide if we should scan the table or use keys.
*/
double ha_ndbcluster::scan_time() {
  DBUG_TRACE;
  const double res = rows2double(stats.records * 1000);
  DBUG_PRINT("exit", ("table: %s value: %f", table_share->table_name.str, res));
  return res;
}

/**
  read_time() need to differentiate between single row type lookups,
  and accesses where an ordered index need to be scanned.
  The later will need to scan all fragments, which might be
  significantly more expensive - imagine a deployment with hundreds
  of partitions.
 */
double ha_ndbcluster::read_time(uint index, uint ranges, ha_rows rows) {
  DBUG_TRACE;
  assert(rows > 0);
  assert(ranges > 0);
  assert(rows >= ranges);

  const NDB_INDEX_TYPE index_type =
      (index < MAX_KEY)    ? get_index_type(index)
      : (index == MAX_KEY) ? PRIMARY_KEY_INDEX  // Hidden primary key
                           : UNDEFINED_INDEX;   // -> worst index

  // fanout_factor is intended to compensate for the amount
  // of roundtrips between API <-> data node and between data nodes
  // themself by the different index type. As an initial guess
  // we assume a single full roundtrip for each 'range'.
  double fanout_factor;

  /**
   * Note that for now we use the default handler cost estimate
   * 'rows2double(ranges + rows)' as the baseline - Even if it
   * might have some obvious flaws. For now it is more important
   * to get the relative cost between PK/UQ and order index scan
   * more correct. It is also a matter of not changing too many
   * existing MTR tests. (and customer queries as well!)
   *
   * We also estimate the same cost for a request roundtrip as
   * for returning a row. Thus the baseline cost 'ranges + rows'
   */
  if (index_type == PRIMARY_KEY_INDEX) {
    assert(index == table->s->primary_key);
    // Need a full roundtrip for each row
    fanout_factor = 1.0 * rows2double(rows);
  } else if (index_type == UNIQUE_INDEX) {
    // Need to lookup first on UQ, then on PK, + lock/unlock
    fanout_factor = 2.0 * rows2double(rows);

  } else if (rows > ranges || index_type == ORDERED_INDEX ||
             index_type == UNDEFINED_INDEX) {
    // Assume || need a range scan

    // TODO: - Handler call need a parameter specifying whether
    //         key was fully specified or not (-> scan or lookup)
    //       - The range scan could be pruned -> lower cost, or
    //       - The scan need to be 'ordered' -> higher cost.
    //       - Returning multiple rows pr range has a lower
    //         pr. row cost?
    const uint fragments_to_scan =
        m_table->getFullyReplicated() ? 1 : m_table->getPartitionCount();

    // The range scan does one API -> TC request, which scale out the
    // requests to all fragments. Assume a somewhat (*0.5) lower cost
    // for these requests, as they are not full roundtrips back to the API
    fanout_factor = (double)ranges * (1.0 + ((double)fragments_to_scan * 0.5));

  } else {
    assert(rows == ranges);

    // Assume a set of PK/UQ single row lookups.
    // We assume the hash key is used for a direct lookup
    if (index_type == PRIMARY_KEY_ORDERED_INDEX) {
      assert(index == table->s->primary_key);
      fanout_factor = (double)ranges * 1.0;
    } else {
      assert(index_type == UNIQUE_ORDERED_INDEX);
      // Unique key access has a higher cost than PK. Need to first
      // lookup in index, then use that to lookup the row + lock & unlock
      fanout_factor = (double)ranges * 2.0;  // Assume twice as many roundtrips
    }
  }
  return fanout_factor + rows2double(rows);
}

/**
 * Estimate the cost for reading the specified number of rows,
 * using 'index'. Note that there is no such thing as a 'page'-read
 * in ha_ndbcluster. Unfortunately, the optimizer does some
 * assumptions about an underlying page based storage engine,
 * which explains the name.
 *
 * In the NDB implementation we simply ignore the 'page', and
 * calculate it as any other read_cost()
 */
double ha_ndbcluster::page_read_cost(uint index, double rows) {
  DBUG_TRACE;
  return read_cost(index, 1, rows).total_cost();
}

/**
 * Estimate the upper cost for reading rows in a seek-and-read fashion.
 * Calculation is based on the worst index we can find for this table, such
 * that any other better way of reading the rows will be preferred.
 *
 * Note that worst_seek will be compared against page_read_cost().
 * Thus, it need to calculate the cost using comparable 'metrics'.
 */
double ha_ndbcluster::worst_seek_times(double reads) {
  // Specifying the 'UNDEFINED_INDEX' is a special case in read_time(),
  // where the cost for the most expensive/worst index will be calculated.
  const uint undefined_index = MAX_KEY + 1;
  return page_read_cost(undefined_index, std::max(reads, 1.0));
}

/*
  Convert MySQL table locks into locks supported by Ndb Cluster.
  Note that MySQL Cluster does currently not support distributed
  table locks, so to be safe one should set cluster in Single
  User Mode, before relying on table locks when updating tables
  from several MySQL servers
*/

THR_LOCK_DATA **ha_ndbcluster::store_lock(THD *thd, THR_LOCK_DATA **to,
