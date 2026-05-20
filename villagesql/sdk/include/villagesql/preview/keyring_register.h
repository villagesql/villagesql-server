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
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_PREVIEW_KEYRING_REGISTER_H
#define VILLAGESQL_PREVIEW_KEYRING_REGISTER_H

#include <villagesql/abi/preview/keyring.h>
#include <villagesql/detail/capability_traits.h>
#include <villagesql/preview/keyring.h>

namespace vsql::detail {

template <>
struct CapabilityTraits<::vsql::preview_keyring::KeyringCapability> {
  static constexpr const char *kName = VEF_PREVIEW_KEYRING_NAME;
  static constexpr const char *kCppTypeName =
      "vsql::preview_keyring::KeyringCapability";
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_KEYRING_ABI_VERSION;
  using AbiType = vef_preview_keyring_t;

  static constexpr void *vtable_destination(
      ::vsql::preview_keyring::KeyringCapability *p) noexcept {
    return static_cast<void *>(&p->abi_);
  }
};

}  // namespace vsql::detail

#endif  // VILLAGESQL_PREVIEW_KEYRING_REGISTER_H
