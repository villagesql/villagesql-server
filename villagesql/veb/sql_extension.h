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

// This class implements the INSTALL EXTENSION statement.
class Sql_cmd_install_extension : public Sql_cmd {
 public:
  // version: asserted VEB-manifest version (m_version.str == nullptr if no
  // VERSION clause). When set, install fails unless the manifest matches.
  Sql_cmd_install_extension(const LEX_CSTRING &name, const LEX_CSTRING &version)
      : m_name(name), m_version(version) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_INSTALL_EXTENSION;
  }

  // Install a new extension.
  // @param thd  Thread context
  // @returns false if success, true otherwise
  bool execute(THD *thd) override;

 private:
  LEX_CSTRING m_name;
  LEX_CSTRING m_version;
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
