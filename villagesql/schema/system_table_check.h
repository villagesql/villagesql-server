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

#ifndef VILLAGESQL_SCHEMA_SYSTEM_TABLE_CHECK_H
#define VILLAGESQL_SCHEMA_SYSTEM_TABLE_CHECK_H

#include "mysql/components/services/log_builtins.h"
#include "mysql/my_loglevel.h"
#include "mysqld_error.h"
#include "sql/table.h"  // Table_check_intact

class THD;

namespace villagesql {

/**
  Class to check if VillageSQL system table is intact.

  Similar to System_table_intact but for VillageSQL-specific tables
  in the villagesql schema.
*/
class Village_system_table_intact : public Table_check_intact {
 public:
  explicit Village_system_table_intact(THD *thd,
                                       enum loglevel log_level = ERROR_LEVEL)
      : m_thd(thd), m_log_level(log_level) {
    has_keys = true;  // VillageSQL system tables should have proper keys
  }

  THD *thd() { return m_thd; }

  // Base-class check() plus a collation check. The base class verifies the
  // charset name only; identifier lookups depend on the exact binary
  // collation, so a wrong-collation table must be caught at startup.
  bool check_with_collation(THD *thd, TABLE *table,
                            const TABLE_FIELD_DEF *table_def) {
    if (check(thd, table, table_def)) return true;
    for (Field **field = table->field; *field; field++) {
      if (!(*field)->has_charset()) continue;
      const CHARSET_INFO *cs = (*field)->charset();
      if (cs != &my_charset_utf8mb4_bin) {
        report_error(0,
                     "Incorrect collation for column %s.%s.%s: found %s, "
                     "expected %s",
                     table->s->db.str, table->s->table_name.str,
                     (*field)->field_name, cs->m_coll_name,
                     my_charset_utf8mb4_bin.m_coll_name);
        return true;
      }
    }
    return false;
  }

 protected:
  void report_error(uint code, const char *fmt, ...) override
      MY_ATTRIBUTE((format(printf, 3, 4))) {
    va_list args;
    va_start(args, fmt);

    if (code == 0) {
      // Generic table check failure
      LogEvent()
          .prio(WARNING_LEVEL)
          .errcode(ER_SERVER_TABLE_CHECK_FAILED)
          .subsys(LOG_SUBSYSTEM_TAG)
          .source_file(MY_BASENAME)
          .messagev(fmt, args);
    } else if (code == ER_CANNOT_LOAD_FROM_TABLE_V2) {
      // Special handling for "cannot load from table" errors
      char *db_name, *table_name;
      db_name = va_arg(args, char *);
      table_name = va_arg(args, char *);
      my_error(code, MYF(0), db_name, table_name);
      LogErr(m_log_level, ER_SERVER_CANNOT_LOAD_FROM_TABLE_V2, db_name,
             table_name);
    } else {
      // Report to client
      my_printv_error(code, ER_THD_NONCONST(m_thd, code), MYF(0), args);
      va_end(args);

      // Map to server error codes for logging
      uint log_code = code;
      if (code == ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE_V2)
        log_code = ER_SERVER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE_V2;
      else if (code == ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2)
        log_code = ER_SERVER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2;
      else
        log_code = ER_SERVER_TABLE_CHECK_FAILED;

      // Log with VillageSQL prefix for clarity
      va_start(args, fmt);
      LogEvent()
          .prio(m_log_level)
          .errcode(log_code)
          .subsys(LOG_SUBSYSTEM_TAG)
          .source_file(MY_BASENAME)
          .message("VillageSQL: ")
          .messagev(fmt, args);
    }

    va_end(args);
  }

 private:
  THD *m_thd;
  enum loglevel m_log_level;
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SYSTEM_TABLE_CHECK_H
