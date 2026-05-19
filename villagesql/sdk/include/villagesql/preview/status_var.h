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

#ifndef VILLAGESQL_PREVIEW_STATUS_VAR_H
#define VILLAGESQL_PREVIEW_STATUS_VAR_H

#include <array>
#include <cstdint>

#include <villagesql/abi/preview/status_var.h>
#include <villagesql/abi/types.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_status_var {

// Type tag for StatusVarDescriptor — matches vef_status_var_type_t values.
enum Type {
  INT = VEF_STATUS_VAR_INT,
  DOUBLE = VEF_STATUS_VAR_DOUBLE,
};

// Aggregate descriptor for a single status variable. Use the factory
// functions make_int() / make_double() to construct:
//
//   sv::make_int   ("hits",    &g_hits)
//   sv::make_double("latency", &g_latency)
struct StatusVarDescriptor {
  Type type;
  const char *name;
  union {
    long long *integer_ptr;
    double *double_ptr;
  };
};

// StatusVarCapability<N> holds N status variable descriptors and the pointer
// array the server reads during on_populate. Use make_capability() to
// construct one without specifying N explicitly.
//
// Usage:
//
//   static long long g_hits = 0;
//   static long long g_misses = 0;
//
//   namespace sv = vsql::preview_status_var;
//
//   static auto g_status_vars = sv::make_capability({
//       sv::make_int("hits",   &g_hits),
//       sv::make_int("misses", &g_misses)});
//
//   VEF_GENERATE_ENTRY_POINTS(
//       make_extension()
//           .func(...)
//           .with(g_status_vars));
//
// The server populates the capability during registration and reads
// `descriptor_list` in the on_populate callback to register the variables
// with MySQL.
template <size_t N>
class StatusVarCapability
    : public ::vsql::detail::CapabilityBase<StatusVarCapability<N>> {
 public:
  explicit StatusVarCapability(StatusVarDescriptor const (&descs)[N]) {
    for (size_t i = 0; i < N; ++i) {
      descs_[i].name = descs[i].name;
      descs_[i].type = static_cast<vef_status_var_type_t>(descs[i].type);
      switch (descs[i].type) {
        case INT:
          descs_[i].integer_ptr = descs[i].integer_ptr;
          break;
        case DOUBLE:
          descs_[i].double_ptr = descs[i].double_ptr;
          break;
      }
      ptrs_[i] = &descs_[i];
    }
    descriptor_list.vars = ptrs_.data();
    descriptor_list.var_count = static_cast<uint32_t>(N);
  }

  // Read by the server's on_populate callback. Public so CapabilityTraits
  // can return its address as extension_data.
  vef_status_var_descriptor_list_t descriptor_list{};

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;

  const vef_preview_status_var_t *abi_ = nullptr;

  std::array<vef_status_var_desc_t, N> descs_;
  std::array<const vef_status_var_desc_t *, N> ptrs_;
};

// Type-specific factory functions for StatusVarDescriptor. Use these inside
// make_capability():
//
//   namespace sv = vsql::preview_status_var;
//   auto g_sv = sv::make_capability({
//       sv::make_int   ("hits",    &g_hits),
//       sv::make_double("latency", &g_latency),
//   });
inline StatusVarDescriptor make_int(const char *name, long long *value_ptr) {
  StatusVarDescriptor d;
  d.type = INT;
  d.name = name;
  d.integer_ptr = value_ptr;
  return d;
}

inline StatusVarDescriptor make_double(const char *name, double *value_ptr) {
  StatusVarDescriptor d;
  d.type = DOUBLE;
  d.name = name;
  d.double_ptr = value_ptr;
  return d;
}

// Factory: deduces N from the array size.
//
//   namespace sv = vsql::preview_status_var;
//   auto g_sv = sv::make_capability({
//       sv::make_int   ("hits",    &g_hits),
//       sv::make_double("latency", &g_latency),
//   });
template <size_t N>
auto make_capability(StatusVarDescriptor const (&descs)[N]) {
  return StatusVarCapability<N>(descs);
}

}  // namespace vsql::preview_status_var

#include <villagesql/preview/status_var_register.h>

#endif  // VILLAGESQL_PREVIEW_STATUS_VAR_H
