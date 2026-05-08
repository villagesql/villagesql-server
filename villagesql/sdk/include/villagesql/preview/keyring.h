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

#ifndef VILLAGESQL_PREVIEW_KEYRING_H
#define VILLAGESQL_PREVIEW_KEYRING_H

#include <string>
#include <string_view>
#include <type_traits>

#include <villagesql/abi/preview/keyring.h>
#include <villagesql/detail/capability_hash.h>
#include <villagesql/vsql/extension_builder.h>

namespace vsql::preview::keyring {

// C++ wrapper around vef_preview_keyring_t.
//
// Usage:
//   static auto g_keyring = vsql::preview::keyring::make_capability();
//
//   // In extension code:
//   auto result = g_keyring.read("my-key", "", value);
//
// Register with:
//   make_extension().with<preview_keyring<g_keyring>>()
class Capability {
 public:
  static constexpr const char *kName = VEF_PREVIEW_KEYRING_NAME;
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_KEYRING_ABI_VERSION;

  // Read a secret from the MySQL keyring component into value.
  //   auth_id may be empty to read internal keys.
  //   Returns VEF_KEYRING_OK on success, VEF_KEYRING_NOT_FOUND if the key does
  //   not exist, VEF_KEYRING_UNAVAILABLE if no keyring component is installed,
  //   or VEF_KEYRING_ERROR on other failures.
  vef_keyring_result_t read(std::string_view data_id, std::string_view auth_id,
                            std::string &value) const {
    if (!available()) return VEF_KEYRING_UNAVAILABLE;
    value.resize(4096);
    size_t out_len = 0;
    vef_keyring_result_t result =
        abi_->read(data_id.data(), auth_id.empty() ? nullptr : auth_id.data(),
                   reinterpret_cast<unsigned char *>(value.data()),
                   value.size(), &out_len);
    if (result == VEF_KEYRING_OK) value.resize(out_len);
    return result;
  }

  // Write a secret to the MySQL keyring component.
  //   auth_id may be empty to store as an internal key.
  //   Returns VEF_KEYRING_OK on success, VEF_KEYRING_UNAVAILABLE if no keyring
  //   component is installed, or VEF_KEYRING_ERROR on other failures.
  vef_keyring_result_t write(std::string_view data_id, std::string_view auth_id,
                             std::string_view data) const {
    if (!available()) return VEF_KEYRING_UNAVAILABLE;
    return abi_->write(
        data_id.data(), auth_id.empty() ? nullptr : auth_id.data(),
        reinterpret_cast<const unsigned char *>(data.data()), data.size());
  }

  bool available() const { return version() > 0; }

  // Returns the server-side capability ABI version, or 0 if unavailable.
  // Compare against VEF_PREVIEW_KEYRING_ABI_VERSION to check what the current
  // SDK was compiled against.
  uint32_t version() const { return abi_ != nullptr ? abi_->version : 0; }

  // Public so that cap_receive() can store the server vtable pointer here.
  // Do not access directly — use read(), write(), and available() instead.
  const vef_preview_keyring_t *abi_ = nullptr;
};

inline Capability make_capability() { return Capability{}; }

}  // namespace vsql::preview::keyring

namespace vsql::preview {

// Traits type for registering the keyring capability via
// .with<preview_keyring<cap>>. Only available when this header is included.
template <auto &cap>
struct preview_keyring {
  template <typename Inner>
  static constexpr auto bind(Inner builder) {
    using Cap = keyring::Capability;
    return builder.required_capability(
        {Cap::kName, &::vsql::cap_receive<Cap, &cap>,
         ::villagesql::detail::abi_type_hash<
             std::remove_cv_t<std::remove_pointer_t<decltype(cap.abi_)>>>(),
         Cap::kAbiVersion});
  }
};

}  // namespace vsql::preview

#endif  // VILLAGESQL_PREVIEW_KEYRING_H
