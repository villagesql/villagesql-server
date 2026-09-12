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
#include <string_view>

#include "m_string.h"
#include "my_sys.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dd_schema.h"
#include "sql/dd/types/abstract_table.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/view.h"
#include "sql/dd_table_share.h"
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
// Tests in villagesql/information_schema back that up. sys_view_metadata
// catches a rename or drop. sys_view_metadata_dependents catches an addition,
// because it derives the dependent set from VIEW_TABLE_USAGE instead of a fixed
// list.
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

// Returns query advanced past leading blank and "--" comment lines, so the
// result points at the statement's first real token. Needed because
// mysql_sys_schema keeps each statement's comment block -- GPL header included
// -- in the same mysql_sys_schema[] element as the statement, so an element
// begins
// "-- Copyright (c) ..." rather than at its CREATE.
//
// Only "--" and blank lines are skipped. MySQL also accepts "#" and "/* */",
// but no statement opens with either. Returned string_view will not be a
// valid SQL statement.
// Returning an empty view means "nothing left that could be a statement".
std::string_view skip_leading_dash_comments(std::string_view query) {
  for (;;) {
    const size_t token = query.find_first_not_of(" \t\n\r");
    if (token == std::string_view::npos) return {};
    query.remove_prefix(token);

    if (!query.starts_with("--")) return query;

    const size_t eol = query.find('\n');
    if (eol == std::string_view::npos) return {};
    query.remove_prefix(eol + 1);
  }
}

size_t find_case_insensitive(std::string_view haystack,
                             std::string_view needle) {
  if (needle.size() > haystack.size()) return std::string_view::npos;

  const size_t last_start = haystack.size() - needle.size();
  for (size_t i = 0; i <= last_start; i++) {
    if (native_strncasecmp(haystack.data() + i, needle.data(), needle.size()) ==
        0)
      return i;
  }
  return std::string_view::npos;
}

// True when query is the CREATE OR REPLACE VIEW statement for view_name.
//
// The statement must *be* a CREATE OR REPLACE, not merely contain one: sample
// SQL quoted in a comment block would satisfy containment, and
// procedures/statement_performance_analyzer.sql already documents a
// "mysql> CREATE OR REPLACE VIEW mydb.my_statements AS" example.
//
// The name is still searched for rather than expected adjacently, because the
// sys views are written as
//
//   CREATE OR REPLACE
//     ALGORITHM = TEMPTABLE
//     DEFINER = 'mysql.sys'@'localhost'
//     SQL SECURITY INVOKER
//   VIEW x$schema_flattened_keys (
//
// so "CREATE OR REPLACE VIEW" may not appear contiguously. Matching is case
// insensitive and byte-wise; sys view names are ASCII.
bool statement_creates_view(std::string_view query,
                            std::string_view view_name) {
  static constexpr std::string_view kCreateOrReplace = "CREATE OR REPLACE";

  const std::string_view stmt = skip_leading_dash_comments(query);
  if (stmt.size() < kCreateOrReplace.size() ||
      native_strncasecmp(stmt.data(), kCreateOrReplace.data(),
                         kCreateOrReplace.size()) != 0)
    return false;

  const std::string needle = std::string("VIEW ").append(view_name);
  const size_t pos = find_case_insensitive(stmt, needle);
  if (pos == std::string_view::npos) return false;

  // The character just after the name must not be alphanumeric or
  // '_' or '$' to be a whole-identifier match.
  const size_t after_pos = pos + needle.size();
  if (after_pos == stmt.size()) return true;
  const unsigned char after = static_cast<unsigned char>(stmt[after_pos]);
  return !(std::isalnum(after) || after == '_' || after == '$');
}

// Text of the error a failed data dictionary read raised, for logging before
// clear_dd_error() discards it.
const char *dd_error_text(THD *thd) {
  return thd->is_error() ? thd->get_stmt_da()->message_text()
                         : "no error reported";
}

// Consumes the error a failed data dictionary read left on the THD.
void clear_dd_error(THD *thd) { thd->clear_error(); }

// True when the sentinel columns no longer hold the metadata a vanilla server
// records, so replaying is worth doing.
//
// There are three outcomes underneath, and two of them return false. Vanilla
// metadata needs no replay. An unreadable dictionary also returns false, but
// for the opposite reason -- we learned nothing, so there is no basis to act
// on -- and it logs and clears the error before returning.
bool sys_view_metadata_needs_refresh(THD *thd) {
  for (const AffectedSysView &affected : kAffectedSysViews) {
    const dd::Abstract_table *table = nullptr;
    // A missing view is not an error and does not land here: acquire() reports
    // that as success with a null table. Reaching this branch means the
    // dictionary itself could not be read.
    if (thd->dd_client()->acquire(kSysSchemaName, affected.view_name, &table)) {
      LogVSQL(ERROR_LEVEL,
              "Could not read sys.%s from the data dictionary: %s; leaving sys "
              "view metadata as it is until the next restart",
              affected.view_name, dd_error_text(thd));
      clear_dd_error(thd);
      return false;
    }

    const dd::View *view = dynamic_cast<const dd::View *>(table);
    if (view == nullptr) {
      // TODO(villagesql-rebase): upstream renamed or dropped this sys view.
      // Update kAffectedSysViews to match scripts/sys_schema/.
      LogVSQL(WARNING_LEVEL,
              "sys.%s is not a view; refreshing sys view metadata anyway",
              affected.view_name);
      return true;
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
      return true;
    }

    if (!column->is_nullable()) return true;
  }

  return false;
}

class Sys_schema_db_context {
 public:
  explicit Sys_schema_db_context(THD *thd) : m_thd(thd), m_saved_db(thd->db()) {
    m_thd->reset_db({kSysSchemaName, strlen(kSysSchemaName)});
  }

  ~Sys_schema_db_context() { m_thd->reset_db(m_saved_db); }

  Sys_schema_db_context(const Sys_schema_db_context &) = delete;
  Sys_schema_db_context &operator=(const Sys_schema_db_context &) = delete;

 private:
  THD *m_thd;
  LEX_CSTRING m_saved_db;
};

// Recreates a view under the character set and collation it already carries.
class View_charset_context {
 public:
  View_charset_context(THD *thd, const CHARSET_INFO *client_cs,
                       const CHARSET_INFO *connection_cl)
      : m_thd(thd),
        m_saved_client_cs(thd->variables.character_set_client),
        m_saved_connection_cl(thd->variables.collation_connection) {
    m_thd->variables.character_set_client = client_cs;
    m_thd->variables.collation_connection = connection_cl;
    m_thd->update_charset();
  }

  ~View_charset_context() {
    m_thd->variables.character_set_client = m_saved_client_cs;
    m_thd->variables.collation_connection = m_saved_connection_cl;
    m_thd->update_charset();
  }

  View_charset_context(const View_charset_context &) = delete;
  View_charset_context &operator=(const View_charset_context &) = delete;

 private:
  THD *m_thd;
  const CHARSET_INFO *m_saved_client_cs;
  const CHARSET_INFO *m_saved_connection_cl;
};

// Reads the character set and collation sys.view_name currently records, the
// pair a CREATE OR REPLACE has to reproduce. False means they could not be
// determined, and the caller leaves that view alone rather than guessing: a
// wrong character set would be written into the view silently, with nothing in
// the column metadata to show for it.
bool view_ddl_charset(THD *thd, const char *view_name,
                      const CHARSET_INFO **client_cs,
                      const CHARSET_INFO **connection_cl) {
  const dd::Abstract_table *table = nullptr;
  if (thd->dd_client()->acquire(kSysSchemaName, view_name, &table)) {
    LogVSQL(ERROR_LEVEL, "Could not read sys.%s from the data dictionary: %s",
            view_name, dd_error_text(thd));
    clear_dd_error(thd);
    return false;
  }

  const dd::View *view = dynamic_cast<const dd::View *>(table);
  if (view == nullptr) return false;

  *client_cs = dd_get_mysql_charset(view->client_collation_id());
  *connection_cl = dd_get_mysql_charset(view->connection_collation_id());
  return *client_cs != nullptr && *connection_cl != nullptr;
}

}  // namespace

// Restores the stored column metadata of the sys views listed in
// kAffectedSysViews, by replaying their CREATE OR REPLACE VIEW statements out
// of the generated mysql_sys_schema[] array.
//
// Must run on a bootstrap thread. run_bootstrap_thread() sets the server
// default sql_mode (strict_mode).
// Every failure path below logs and returns without repairing the rest. The
// metadata is display only, so a partial or skipped repair is not worth failing
// startup over, and nothing records that the attempt was made: the next startup
// inspects the dictionary again and repairs then if it can.
void refresh_sys_view_metadata(THD *thd) {
  bool sys_schema_exists = false;
  if (dd::schema_exists(thd, kSysSchemaName, &sys_schema_exists)) {
    LogVSQL(
        ERROR_LEVEL,
        "Could not determine whether the sys schema exists: %s; leaving sys "
        "view metadata as it is until the next restart",
        dd_error_text(thd));
    clear_dd_error(thd);
    return;
  }
  if (!sys_schema_exists) return;

  // A false here can mean the metadata is fine or that the dictionary could not
  // be read, which has logged and cleared its own error. Nothing is persisted
  // either way, so the next startup re-inspects and repairs if it can.
  if (!sys_view_metadata_needs_refresh(thd)) {
    assert(false == thd->is_error());
    // sys_view_metadata_needs_refresh should've cleared the error if it failed.
    // Just to be safe, log and clear the error here as well.
    if (thd->is_error()) {
      LogVSQL(
          ERROR_LEVEL,
          "Could not determine whether sys view metadata needs refresh: %s; "
          "leaving sys view metadata as it is until the next restart",
          dd_error_text(thd));
      clear_dd_error(thd);
    }
    return;
  }

  const Disable_binlog_guard binlog_guard(thd);
  const Disable_sql_log_bin_guard sql_log_bin_guard(thd);
  const Sys_schema_db_context db_context(thd);

  std::array<size_t, kAffectedSysViews.size()> replayed{};
  for (const char **query = &mysql_sys_schema[0]; *query != nullptr; query++) {
    for (size_t i = 0; i < kAffectedSysViews.size(); i++) {
      const char *view_name = kAffectedSysViews[i].view_name;
      if (!statement_creates_view(*query, view_name)) continue;
      // Counted as found even when the replay below is skipped: this covers
      // what the sys schema script contains
      replayed[i]++;

      const CHARSET_INFO *client_cs = nullptr;
      const CHARSET_INFO *connection_cl = nullptr;
      if (!view_ddl_charset(thd, view_name, &client_cs, &connection_cl)) {
        LogVSQL(WARNING_LEVEL,
                "Could not read the character set sys.%s was created with; "
                "leaving that view as it is",
                view_name);
        break;
      }
      const View_charset_context charset_context(thd, client_cs, connection_cl);

      // A CREATE OR REPLACE VIEW implicitly commits, which is why this runs
      // between two statements rather than mid-transaction. Disabling
      // autocommit does not suppress that: SQLCOM_CREATE_VIEW carries
      // CF_AUTO_COMMIT_TRANS.
      // So nothing may be pending here, or the implicit commit would commit it
      // on behalf of whoever opened it. Asserted every iteration rather than
      // once before the loop, because that same implicit commit is what has to
      // leave the state clean for the next view.
      assert(thd->get_transaction()->is_empty(Transaction_ctx::STMT));
      assert(thd->get_transaction()->is_empty(Transaction_ctx::SESSION));

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
