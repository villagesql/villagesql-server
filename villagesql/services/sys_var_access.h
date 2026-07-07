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

#ifndef VILLAGESQL_SERVICES_SYS_VAR_ACCESS_H
#define VILLAGESQL_SERVICES_SYS_VAR_ACCESS_H

#include <cstdint>
#include <string_view>

namespace villagesql::services {

// Server-internal descriptor for a single system variable. Used by
// register_one_sys_var; has no dependency on the extension-facing ABI.
// Currently supports bool variables only.
struct SysVarDesc {
  const char *name;
  const char *comment;
  bool *value_ptr;
  bool def_val;
  // Called after the server writes a new value. May be null.
  // var_name is the variable name without the extension prefix.
  void (*on_change)(const char *var_name, bool new_val);
};

// Register a single bool system variable for the given extension.
// Returns false on success, true on error.
bool register_one_sys_var(std::string_view extension_name,
                          const SysVarDesc &desc);

// Unregister a single system variable previously registered with
// register_one_sys_var.
void unregister_one_sys_var(std::string_view extension_name,
                            const char *var_name);

// Read the global value of a system variable.
//
// component_name: extension name, or "mysql_server" for built-in variables.
// name:           variable name without the component prefix.
// val:            on success, set to a newly allocated buffer holding the value
//                 as a null-terminated string; caller must free with free().
// val_len:        on success, set to the string length (excluding null).
//
// Returns false on success, true on error.
bool get_variable(const char *component_name, const char *name, void **val,
                  uint64_t *val_len);

// Set the value of a system variable.
//
// component_name: extension name, or "mysql_server" for built-in variables.
// name:           variable name without the component prefix.
// scope:          nullptr / "GLOBAL"       → update running value (not
// persisted)
//                 "PERSIST"               → update running value AND persist
//                 "PERSIST_ONLY"          → persist only; running value
//                 unchanged
// val:            new value as a null-terminated string (e.g. "42", "hello").
//
// Returns false on success, true on error.
bool set_variable(const char *component_name, const char *name,
                  const char *scope, const char *val);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_SYS_VAR_ACCESS_H
