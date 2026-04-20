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

#ifndef VILLAGESQL_VSQL_STATUS_VAR_BUILDER_H
#define VILLAGESQL_VSQL_STATUS_VAR_BUILDER_H

// Status Variable Builder - Declare extension status variables
//
// Status variables are read-only counters and gauges exposed via SHOW STATUS.
// Unlike system variables they cannot be SET by the user; the extension writes
// to the backing storage and the server reads it at query time.
//
// Usage (in extension registration):
//
//   static long long g_requests_total = 0;
//   static long long g_errors_total   = 0;
//
//   make_extension("myext", "1.0")
//     .status_var(make_status_var_int("requests_total", &g_requests_total))
//     .status_var(make_status_var_int("errors_total",   &g_errors_total))
//
// Variables are visible as:
//   SHOW GLOBAL STATUS LIKE 'myext%';

#include <villagesql/abi/types.h>

namespace villagesql {
namespace status_var_builder {

// Wraps a single vef_status_var_desc_t by value so the builder can store it
// in a compile-time tuple.
struct StatusVarDescriptor {
  vef_status_var_desc_t desc;
};

constexpr StatusVarDescriptor make_status_var_int(const char *name,
                                                  long long *value_ptr) {
  StatusVarDescriptor d{};
  d.desc.protocol = VEF_PROTOCOL_2;
  d.desc.name = name;
  d.desc.type = VEF_STATUS_VAR_INT;
  d.desc.integer_ptr = value_ptr;
  return d;
}

constexpr StatusVarDescriptor make_status_var_double(const char *name,
                                                     double *value_ptr) {
  StatusVarDescriptor d{};
  d.desc.protocol = VEF_PROTOCOL_2;
  d.desc.name = name;
  d.desc.type = VEF_STATUS_VAR_DOUBLE;
  d.desc.double_ptr = value_ptr;
  return d;
}

}  // namespace status_var_builder
}  // namespace villagesql

#endif  // VILLAGESQL_VSQL_STATUS_VAR_BUILDER_H
