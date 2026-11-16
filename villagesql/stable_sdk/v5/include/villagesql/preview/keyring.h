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

// =============================================================================
// PREVIEW CAPABILITY — UNSTABLE API
// =============================================================================
// This header is part of the VEF preview surface. Its API and ABI may change
// or be removed without notice. See villagesql/preview/README.md for details.
// =============================================================================

#ifndef VILLAGESQL_PREVIEW_KEYRING_H
#define VILLAGESQL_PREVIEW_KEYRING_H

#include <string>
#include <string_view>

#include <villagesql/abi/preview/keyring.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_keyring {

// Declare a KeyringCapability by value in your extension and pass it to
// .with(). VEF populates the capability during registration.
class KeyringCapability
    : public ::vsql::detail::CapabilityBase<KeyringCapability> {
 public:
  enum class Status {
    OK = VEF_KEYRING_OK,
    NOT_FOUND = VEF_KEYRING_NOT_FOUND,
    UNAVAILABLE = VEF_KEYRING_UNAVAILABLE,
    ERROR = VEF_KEYRING_ERROR,
  };

  // Outcome of a read(). On Status::OK, value contains the secret.
  // On any other Status, value is empty.
  struct ReadResult {
    Status status;
    std::string value;
  };

  // Read a secret from the keyring. auth_id defaults to empty (internal
  // keys, not accessible via SQL).
  [[nodiscard]] ReadResult read(std::string_view data_id,
                                std::string_view auth_id = {}) const;

  // Write a secret to the keyring. auth_id may be empty to store as an
  // internal key.
  [[nodiscard]] Status write(std::string_view data_id, std::string_view auth_id,
                             std::string_view data) const;

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;

  const vef_preview_keyring_t *abi_ = nullptr;
};

}  // namespace vsql::preview_keyring

#include <villagesql/preview/detail/keyring_register.h>
#include <villagesql/preview/keyring_impl.h>

#endif  // VILLAGESQL_PREVIEW_KEYRING_H
