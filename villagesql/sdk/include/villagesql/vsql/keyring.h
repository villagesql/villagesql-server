// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#ifndef VILLAGESQL_VSQL_KEYRING_H_
#define VILLAGESQL_VSQL_KEYRING_H_

#include <string>
#include <string_view>

#include <villagesql/abi/types.h>

namespace villagesql {
namespace keyring {

// Extension-local storage for the keyring function pointers injected by the
// server via vef_register_arg_t. Set once during vef_register() by
// vef_register_impl() in extension_builder.h.
inline vef_read_keyring_fn g_read_keyring = nullptr;
inline vef_write_keyring_fn g_write_keyring = nullptr;

// Read a secret from the MySQL keyring component into value.
//   auth_id may be empty to read internal keys.
//   Returns VEF_KEYRING_OK on success, VEF_KEYRING_NOT_FOUND if the key does
//   not exist, VEF_KEYRING_UNAVAILABLE if no keyring component is installed,
//   or VEF_KEYRING_ERROR on other failures.
inline vef_keyring_result_t read(std::string_view data_id,
                                 std::string_view auth_id, std::string &value) {
  if (g_read_keyring == nullptr) return VEF_KEYRING_UNAVAILABLE;
  value.resize(4096);
  size_t out_len = 0;
  vef_keyring_result_t result = g_read_keyring(
      data_id.data(), auth_id.empty() ? nullptr : auth_id.data(),
      reinterpret_cast<unsigned char *>(value.data()), value.size(), &out_len);
  if (result == VEF_KEYRING_OK) value.resize(out_len);
  return result;
}

// Write a secret to the MySQL keyring component.
//   auth_id may be empty to store as an internal key.
//   Returns VEF_KEYRING_OK on success, VEF_KEYRING_UNAVAILABLE if no keyring
//   component is installed, or VEF_KEYRING_ERROR on other failures.
inline vef_keyring_result_t write(std::string_view data_id,
                                  std::string_view auth_id,
                                  std::string_view data) {
  if (g_write_keyring == nullptr) return VEF_KEYRING_UNAVAILABLE;
  return g_write_keyring(
      data_id.data(), auth_id.empty() ? nullptr : auth_id.data(),
      reinterpret_cast<const unsigned char *>(data.data()), data.size());
}

}  // namespace keyring
}  // namespace villagesql

#endif  // VILLAGESQL_VSQL_KEYRING_H_
