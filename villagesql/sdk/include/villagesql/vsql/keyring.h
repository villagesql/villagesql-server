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

#include <villagesql/abi/types.h>

namespace villagesql {
namespace keyring {

// Extension-local storage for the keyring function pointers injected by the
// server via vef_register_arg_t. Set once during vef_register() by
// vef_register_impl() in extension_builder.h.
inline vef_read_keyring_fn g_read_keyring = nullptr;
inline vef_write_keyring_fn g_write_keyring = nullptr;

// Read a secret from the MySQL keyring component.
//   data_id:  identifier for the secret.
//   auth_id:  owner of the secret, or NULL for internal keys.
//   buf:      caller-provided buffer to receive the secret bytes.
//   buf_len:  size of buf in bytes.
//   out_len:  set to the actual number of bytes written on success.
//   Returns false on success, true on error (including key not found).
inline bool read(const char *data_id, const char *auth_id, unsigned char *buf,
                 size_t buf_len, size_t *out_len) {
  if (g_read_keyring == nullptr) return true;
  return g_read_keyring(data_id, auth_id, buf, buf_len, out_len);
}

// Write a secret to the MySQL keyring component.
//   data_id:   identifier for the secret.
//   auth_id:   owner of the secret, or NULL for internal keys.
//   data:      secret bytes to store.
//   data_len:  length of data in bytes.
//   Returns false on success, true on error.
inline bool write(const char *data_id, const char *auth_id,
                  const unsigned char *data, size_t data_len) {
  if (g_write_keyring == nullptr) return true;
  return g_write_keyring(data_id, auth_id, data, data_len);
}

}  // namespace keyring
}  // namespace villagesql

#endif  // VILLAGESQL_VSQL_KEYRING_H_
