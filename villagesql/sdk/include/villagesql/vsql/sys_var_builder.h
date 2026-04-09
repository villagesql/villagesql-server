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

#ifndef VILLAGESQL_VSQL_SYS_VAR_BUILDER_H
#define VILLAGESQL_VSQL_SYS_VAR_BUILDER_H

// System Variable Builder - Declare extension system variables
//
// Usage (in extension registration):
//
//   make_extension("myext", "1.0")
//     .sys_var(make_sys_var_int("threshold_ms",
//                               "Slow query threshold in ms",
//                               &g_threshold_ms, 1000, 0, 3600000))
//     .sys_var(make_sys_var_str("log_file",
//                               "Path to log file",
//                               &g_log_file, "/tmp/myext.log"))
//
// Variables are accessible in SQL as:
//   SELECT @@global.myext.threshold_ms
//   SET GLOBAL myext.threshold_ms = 500;

#include <cstdio>

#include <villagesql/abi/types.h>

namespace villagesql {
namespace sys_var_builder {

// Wraps a single vef_sys_var_desc_t by value so the builder can store it
// in a compile-time tuple.
struct SysVarDescriptor {
  vef_sys_var_desc_t desc;
};

constexpr SysVarDescriptor make_sys_var_bool(const char *name,
                                             const char *comment,
                                             bool *value_ptr, bool def_val) {
  return SysVarDescriptor{.desc = {.protocol = VEF_PROTOCOL_2,
                                   .name = name,
                                   .comment = comment,
                                   .type = VEF_VAR_BOOL,
                                   .boolean = {.value_ptr = value_ptr,
                                               .def_val = def_val}}};
}

constexpr SysVarDescriptor make_sys_var_int(const char *name,
                                            const char *comment,
                                            long long *value_ptr,
                                            long long def_val,
                                            long long min_val,
                                            long long max_val) {
  return SysVarDescriptor{.desc = {.protocol = VEF_PROTOCOL_2,
                                   .name = name,
                                   .comment = comment,
                                   .type = VEF_VAR_INT,
                                   .integer = {.value_ptr = value_ptr,
                                               .def_val = def_val,
                                               .min_val = min_val,
                                               .max_val = max_val}}};
}

constexpr SysVarDescriptor make_sys_var_double(const char *name,
                                               const char *comment,
                                               double *value_ptr,
                                               double def_val, double min_val,
                                               double max_val) {
  return SysVarDescriptor{.desc = {.protocol = VEF_PROTOCOL_2,
                                   .name = name,
                                   .comment = comment,
                                   .type = VEF_VAR_DOUBLE,
                                   .dbl = {.value_ptr = value_ptr,
                                           .def_val = def_val,
                                           .min_val = min_val,
                                           .max_val = max_val}}};
}

constexpr SysVarDescriptor make_sys_var_str(const char *name,
                                            const char *comment,
                                            char **value_ptr,
                                            const char *def_val) {
  return SysVarDescriptor{.desc = {.protocol = VEF_PROTOCOL_2,
                                   .name = name,
                                   .comment = comment,
                                   .type = VEF_VAR_STR,
                                   .str = {.value_ptr = value_ptr,
                                           .def_val = def_val}}};
}

}  // namespace sys_var_builder

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

inline bool set(const char *component_name, const char *name, const char *scope,
                long long val) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", val);
  return set(component_name, name, scope, buf);
}

}  // namespace sys_var

}  // namespace villagesql

#endif  // VILLAGESQL_VSQL_SYS_VAR_BUILDER_H
