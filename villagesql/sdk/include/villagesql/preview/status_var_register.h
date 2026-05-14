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

#ifndef VILLAGESQL_PREVIEW_STATUS_VAR_REGISTER_H
#define VILLAGESQL_PREVIEW_STATUS_VAR_REGISTER_H

#include <villagesql/abi/preview/status_var.h>
#include <villagesql/vsql/capability_traits.h>

namespace vsql::detail {

template <size_t N>
struct CapabilityTraits<::vsql::preview_status_var::StatusVarCapability<N>> {
  static constexpr const char *kName = VEF_PREVIEW_STATUS_VAR_NAME;
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_STATUS_VAR_ABI_VERSION;
  using AbiType = vef_preview_status_var_t;
  using DescriptorType = vef_status_var_descriptor_list_t;

  static constexpr void *vtable_destination(
      ::vsql::preview_status_var::StatusVarCapability<N> *p) noexcept {
    return static_cast<void *>(&p->abi);
  }

  // Returns a pointer to the descriptor list so the server's on_populate
  // callback can reach the variable descriptors. The server receives this
  // as extension_data in the CapabilityValue callbacks.
  static constexpr const void *extension_data(
      ::vsql::preview_status_var::StatusVarCapability<N> *p) noexcept {
    return static_cast<const void *>(&p->descriptor_list);
  }
};

}  // namespace vsql::detail

#endif  // VILLAGESQL_PREVIEW_STATUS_VAR_REGISTER_H
