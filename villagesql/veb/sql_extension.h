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

#ifndef VILLAGESQL_VEB_SQL_EXTENSION_H_
#define VILLAGESQL_VEB_SQL_EXTENSION_H_

#include "include/lex_string.h"
#include "include/my_sqlcommand.h"
#include "my_io.h"
#include "sql/sql_cmd.h"

class THD;

// Global variables for VEB directory configuration
// These are set during server startup based on --veb-dir command line option
// or the default VEBDIR compile-time constant
extern char *opt_veb_dir_ptr;
extern char opt_veb_dir[FN_REFLEN];

// This class implements both the install path and the version-update path
// for extensions. The two paths share an Sql_cmd because they share most of
// the setup work (VEB resolution, MDL acquisition, victionary access); the
// dispatch flags below select which path runs.
//
// TODO(villagesql-general): evaluate giving `ALTER EXTENSION` its own
// `enum_sql_command` value. Today both INSTALL and ALTER report as
// `SQLCOM_INSTALL_EXTENSION` in the slow query log, audit log, and
// performance_schema statement events; they should be distinguishable.
class Sql_cmd_install_extension : public Sql_cmd {
 public:
  // version: requested VEB-manifest version (m_version.str == nullptr if no
  // VERSION clause was given on the install path). When set, install opens
  // {name}-{version}.veb and fails unless the manifest version matches.
  // When omitted, install picks the only versioned VEB present, or
  // {name}.veb if it exists.
  //
  // update_version: true when the statement updates an already-installed
  // extension to a different version. Dispatched to execute_update_version,
  // which currently rejects with a "not yet supported" error.
  //
  // at_restart: when update_version is true, indicates the version change
  // should be deferred until the next server restart rather than applied
  // live. Today this is always true when update_version is true; the flag
  // is kept separate so a future live-update path reuses
  // execute_update_version with the flag unset.
  explicit Sql_cmd_install_extension(const LEX_CSTRING &name,
                                     const LEX_CSTRING &version,
                                     bool update_version = false,
                                     bool at_restart = false)
      : m_name(name),
        m_version(version),
        m_update_version(update_version),
        m_at_restart(at_restart) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_INSTALL_EXTENSION;
  }

  // Install a new extension.
  // @param thd  Thread context
  // @returns false if success, true otherwise
  bool execute(THD *thd) override;

 private:
  bool execute_install(THD *thd);
  bool execute_update_version(THD *thd);

  LEX_CSTRING m_name;
  LEX_CSTRING m_version;
  bool m_update_version;
  bool m_at_restart;
};

// This class implements the UNINSTALL EXTENSION statement.
class Sql_cmd_uninstall_extension : public Sql_cmd {
 public:
  // @param name     Extension name to uninstall.
  // @param version  Expected installed version (m_version.str == nullptr if no
  //                 VERSION clause was specified). When set, uninstall fails
  //                 unless the installed version matches exactly.
  Sql_cmd_uninstall_extension(const LEX_CSTRING &name,
                              const LEX_CSTRING &version)
      : m_name(name), m_version(version) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_UNINSTALL_EXTENSION;
  }

  // Uninstall an extension.
  // @param thd  Thread context
  // @returns false if success, true otherwise
  bool execute(THD *thd) override;

 private:
  LEX_CSTRING m_name;
  LEX_CSTRING m_version;
};

#endif  // VILLAGESQL_VEB_SQL_EXTENSION_H_
