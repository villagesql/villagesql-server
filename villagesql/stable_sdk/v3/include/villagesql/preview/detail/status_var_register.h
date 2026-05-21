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

#ifndef VILLAGESQL_PREVIEW_DETAIL_STATUS_VAR_REGISTER_H
#define VILLAGESQL_PREVIEW_DETAIL_STATUS_VAR_REGISTER_H

#include <villagesql/abi/preview/status_var.h>
#include <villagesql/detail/capability_traits.h>
#include <villagesql/preview/status_var.h>

namespace vsql::detail {

template <size_t N>
struct CapabilityTraits<::vsql::preview_status_var::StatusVarCapability<N>> {
  static constexpr const char *kName = VEF_PREVIEW_STATUS_VAR_NAME;
  static constexpr const char *kCppTypeName =
      "vsql::preview_status_var::StatusVarCapability";
  using CapabilityConfigType = vef_status_var_descriptor_list_t;
  static constexpr const char *kVtableHash = "ver-1";
  static constexpr const char *kCapabilityConfigHash = "ver-1";

  static constexpr void *vtable_destination(
      ::vsql::preview_status_var::StatusVarCapability<N> *p) noexcept {
    return static_cast<void *>(&p->abi_);
  }

  // Returns a pointer to the descriptor list so the server's on_populate
  // callback can reach the variable descriptors. The server receives this
  // as capability_config in the populate context.
  static constexpr const void *capability_config(
      ::vsql::preview_status_var::StatusVarCapability<N> *p) noexcept {
    return static_cast<const void *>(&p->descriptor_list);
  }
};

}  // namespace vsql::detail

#endif  // VILLAGESQL_PREVIEW_DETAIL_STATUS_VAR_REGISTER_H
