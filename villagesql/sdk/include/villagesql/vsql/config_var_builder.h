// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_VSQL_CONFIG_VAR_BUILDER_H
#define VILLAGESQL_VSQL_CONFIG_VAR_BUILDER_H

// Config Variable Builder - Declare extension configuration variables
//
// Usage (in extension registration):
//
//   make_extension("myext", "1.0")
//     .config_var(make_config_var_int("threshold_ms",
//                                     "Slow query threshold in ms",
//                                     &g_threshold_ms, 1000, 0, 3600000))
//     .config_var(make_config_var_str("log_file",
//                                     "Path to log file",
//                                     &g_log_file, "/tmp/myext.log"))
//
// Variables are accessible in SQL as:
//   SELECT @@global.myext.threshold_ms
//   SET GLOBAL myext.threshold_ms = 500;

#include <villagesql/abi/types.h>

namespace villagesql {
namespace config_var_builder {

// Wraps a single vef_config_var_desc_t by value so the builder can store it
// in a compile-time tuple.
struct ConfigVarDescriptor {
  vef_config_var_desc_t desc;
};

inline ConfigVarDescriptor make_config_var_bool(const char *name,
                                                const char *comment,
                                                bool *value_ptr, bool def_val) {
  ConfigVarDescriptor d{};
  d.desc.protocol = VEF_PROTOCOL_2;
  d.desc.name = name;
  d.desc.comment = comment;
  d.desc.type = VEF_VAR_BOOL;
  d.desc.boolean.value_ptr = value_ptr;
  d.desc.boolean.def_val = def_val;
  return d;
}

inline ConfigVarDescriptor make_config_var_int(
    const char *name, const char *comment, long long *value_ptr,
    long long def_val, long long min_val, long long max_val) {
  ConfigVarDescriptor d{};
  d.desc.protocol = VEF_PROTOCOL_2;
  d.desc.name = name;
  d.desc.comment = comment;
  d.desc.type = VEF_VAR_INT;
  d.desc.integer.value_ptr = value_ptr;
  d.desc.integer.def_val = def_val;
  d.desc.integer.min_val = min_val;
  d.desc.integer.max_val = max_val;
  return d;
}

inline ConfigVarDescriptor make_config_var_double(
    const char *name, const char *comment, double *value_ptr, double def_val,
    double min_val, double max_val) {
  ConfigVarDescriptor d{};
  d.desc.protocol = VEF_PROTOCOL_2;
  d.desc.name = name;
  d.desc.comment = comment;
  d.desc.type = VEF_VAR_DOUBLE;
  d.desc.dbl.value_ptr = value_ptr;
  d.desc.dbl.def_val = def_val;
  d.desc.dbl.min_val = min_val;
  d.desc.dbl.max_val = max_val;
  return d;
}

inline ConfigVarDescriptor make_config_var_str(const char *name,
                                               const char *comment,
                                               char **value_ptr,
                                               const char *def_val) {
  ConfigVarDescriptor d{};
  d.desc.protocol = VEF_PROTOCOL_2;
  d.desc.name = name;
  d.desc.comment = comment;
  d.desc.type = VEF_VAR_STR;
  d.desc.str.value_ptr = value_ptr;
  d.desc.str.def_val = def_val;
  return d;
}

}  // namespace config_var_builder

namespace sys_var {

// Extension-local storage for the function pointers injected by the server
// via vef_register_arg_t. Set once during vef_register() by
// vef_register_impl() in extension_builder.h.
inline vef_get_variable_fn g_get_variable = nullptr;
inline vef_set_variable_fn g_set_variable = nullptr;

// Wrappers callable as free functions from extension code.
inline bool get(const char *component_name, const char *name, void **val,
                size_t *val_len) {
  if (g_get_variable == nullptr) return true;
  return g_get_variable(component_name, name, val, val_len);
}

inline bool set(const char *component_name, const char *name, const char *scope,
                const char *val) {
  if (g_set_variable == nullptr) return true;
  return g_set_variable(component_name, name, scope, val);
}

}  // namespace sys_var

}  // namespace villagesql

#endif  // VILLAGESQL_VSQL_CONFIG_VAR_BUILDER_H
