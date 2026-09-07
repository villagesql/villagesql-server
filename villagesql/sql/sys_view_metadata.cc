/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */
#include "villagesql/sql/sys_view_metadata.h"

#include <array>
#include <cctype>
#include <cstring>
#include <string>

#include "my_sys.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dd_schema.h"
#include "sql/dd/types/abstract_table.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/view.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/thd_raii.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/systable/helpers.h"

// Declared the same way sql/dd/impl/upgrade/server.cc does for
// fix_sys_schema().
extern const char *mysql_sys_schema[];

namespace villagesql {

namespace {

constexpr const char *kSysSchemaName = "sys";

struct AffectedSysView {
  const char *view_name;
  // A column whose nullability reveals whether this view's metadata was
  // rewritten: any column the body renders through an Item_str_func, i.e. a
  // COLLATE or CONCAT expression. Nullable means vanilla, NOT NULL means
  // rewritten.
  const char *sentinel_column;
};

// TODO(villagesql-rebase): upstream may add, rename or drop a sys view that
// reads one of the overridden INFORMATION_SCHEMA views, in which case this list
// needs updating. Docs/merging/post-merge-checks.md has the procedure, under
// "The sys view metadata repair".
//
// This is a membership set, not an execution order: the replay follows
// mysql_sys_schema[] order, which lists every view after the ones it reads.
// It has to be this order as recreating a view implicitly recreates the
// metadata of whatever reads it.
//
// Every entry carries its own sentinel so the detection and the repair cannot
// drift apart.
constexpr auto kAffectedSysViews = std::to_array<AffectedSysView>({
    {"schema_object_overview", "object_type"},
    {"x$schema_flattened_keys", "index_name"},
    {"schema_auto_increment_columns", "column_name"},
    {"schema_redundant_indexes", "redundant_index_name"},
});

// True when query is the CREATE OR REPLACE VIEW statement for view_name.
bool statement_creates_view(const char *query, const char *view_name) {
  const std::string needle = std::string("VIEW ") + view_name;
  const char *hit = strstr(query, needle.c_str());
  if (hit == nullptr) return false;

  // hit[needle.length()] is the character just after the match, which must
  // not be alphanumeric or '_' or '$' to be a whole-identifier match.
  // hit[needle.length()] is safe - it's null character in the worst case.
  const unsigned char after = static_cast<unsigned char>(hit[needle.length()]);
  return !(std::isalnum(after) || after == '_' || after == '$');
}

// True when every sentinel column is still nullable, meaning nothing needs
// repairing.
bool sys_view_metadata_is_vanilla(THD *thd) {
  for (const AffectedSysView &affected : kAffectedSysViews) {
    const dd::Abstract_table *table = nullptr;
    if (thd->dd_client()->acquire(kSysSchemaName, affected.view_name, &table)) {
      LogVSQL(WARNING_LEVEL,
              "Could not read sys.%s from the data dictionary; refreshing sys "
              "view metadata anyway",
              affected.view_name);
      return false;
    }

    const dd::View *view = dynamic_cast<const dd::View *>(table);
    if (view == nullptr) {
      // TODO(villagesql-rebase): upstream renamed or dropped this sys view.
      // Update kAffectedSysViews to match scripts/sys_schema/.
      LogVSQL(WARNING_LEVEL,
              "sys.%s is not a view; refreshing sys view metadata anyway",
              affected.view_name);
      return false;
    }

    const dd::Column *column = nullptr;
    for (const dd::Column *candidate : view->columns()) {
      if (my_strcasecmp(system_charset_info, candidate->name().c_str(),
                        affected.sentinel_column) == 0) {
        column = candidate;
        break;
      }
    }
    if (column == nullptr) {
      // TODO(villagesql-rebase): upstream renamed or dropped this column.
      LogVSQL(WARNING_LEVEL,
              "sys.%s has no column %s; refreshing sys view metadata anyway",
              affected.view_name, affected.sentinel_column);
      return false;
    }

    if (!column->is_nullable()) return false;
  }

  return true;
}

// SET NAMES utf8mb4 is recorded as the view's character_set_client and
// collation_connection, and it also fixes the collation of the string literals
// in bodies like CONCAT('ALTER TABLE `', ...).
class Sys_schema_ddl_context {
 public:
  explicit Sys_schema_ddl_context(THD *thd)
      : m_thd(thd),
        m_saved_client_cs(thd->variables.character_set_client),
        m_saved_connection_cl(thd->variables.collation_connection),
        m_saved_db(thd->db()) {
    const CHARSET_INFO *client_cs = nullptr;
    const CHARSET_INFO *connection_cl = nullptr;
    if (resolve_charset("utf8mb4", system_charset_info, &client_cs) ||
        resolve_collation("utf8mb4_0900_ai_ci", system_charset_info,
                          &connection_cl)) {
      LogVSQL(ERROR_LEVEL, "Could not resolve the utf8mb4 charset");
      m_error = true;
      return;
    }

    m_thd->variables.character_set_client = client_cs;
    m_thd->variables.collation_connection = connection_cl;
    m_thd->update_charset();
    m_thd->reset_db({kSysSchemaName, strlen(kSysSchemaName)});
  }

  ~Sys_schema_ddl_context() {
    m_thd->reset_db(m_saved_db);
    m_thd->variables.character_set_client = m_saved_client_cs;
    m_thd->variables.collation_connection = m_saved_connection_cl;
    m_thd->update_charset();
  }

  Sys_schema_ddl_context(const Sys_schema_ddl_context &) = delete;
  Sys_schema_ddl_context &operator=(const Sys_schema_ddl_context &) = delete;

  bool error() const { return m_error; }

 private:
  THD *m_thd;
  const CHARSET_INFO *m_saved_client_cs;
  const CHARSET_INFO *m_saved_connection_cl;
  LEX_CSTRING m_saved_db;
  bool m_error{false};
};

}  // namespace

// Restores the stored column metadata of the sys views listed in
// kAffectedSysViews, by replaying their CREATE OR REPLACE VIEW statements out
// of the generated mysql_sys_schema[] array.
//
// Must run on a bootstrap thread. run_bootstrap_thread() sets the server
// default sql_mode (strict_mode).
// Every failure path below logs and returns without repairing the rest. The
// metadata is display only, so a partial or skipped repair is not worth failing
// startup over.
void refresh_sys_view_metadata(THD *thd) {
  bool sys_schema_exists = false;
  if (dd::schema_exists(thd, kSysSchemaName, &sys_schema_exists)) {
    LogVSQL(ERROR_LEVEL,
            "Could not determine whether the sys schema exists; leaving sys "
            "view metadata untouched");
    return;
  }
  if (!sys_schema_exists) return;

  if (sys_view_metadata_is_vanilla(thd)) return;

  const Disable_binlog_guard binlog_guard(thd);
  const Disable_sql_log_bin_guard sql_log_bin_guard(thd);
  const Sys_schema_ddl_context ddl_context(thd);
  if (ddl_context.error()) return;

  std::array<size_t, kAffectedSysViews.size()> replayed{};
  for (const char **query = &mysql_sys_schema[0]; *query != nullptr; query++) {
    for (size_t i = 0; i < kAffectedSysViews.size(); i++) {
      if (!statement_creates_view(*query, kAffectedSysViews[i].view_name))
        continue;
      replayed[i]++;
      // A CREATE OR REPLACE VIEW implicitly commits, which is why this runs
      // between two statements rather than mid-transaction. Disabling
      // autocommit does not suppress that: SQLCOM_CREATE_VIEW carries
      // CF_AUTO_COMMIT_TRANS.
      // execute_statement() has already logged the failure.
      if (villagesql::execute_statement(thd, *query)) return;
      break;
    }
  }

  for (size_t i = 0; i < kAffectedSysViews.size(); i++) {
    if (replayed[i] == 1) continue;
    // TODO(villagesql-rebase): upstream renamed, split or dropped this sys
    // view. Update kAffectedSysViews to match scripts/sys_schema/.
    LogVSQL(WARNING_LEVEL,
            "Expected exactly one CREATE VIEW statement for sys.%s in the sys "
            "schema script, found %zu; sys view metadata left as it is",
            kAffectedSysViews[i].view_name, replayed[i]);
    return;
  }

  LogVSQL(INFORMATION_LEVEL, "Refreshed stored metadata of %zu sys views",
          kAffectedSysViews.size());
}

}  // namespace villagesql
