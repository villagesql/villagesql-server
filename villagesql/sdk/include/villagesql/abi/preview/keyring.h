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
// VEF PREVIEW ABI HEADER — UNSTABLE BINARY INTERFACE
// =============================================================================
// This header is both:
//   - an ABI header — extension authors should use the C++ API in
//     <villagesql/vsql.h>, not these raw types. See villagesql/abi/README.md.
//   - a preview capability — API and ABI may change or be removed without
//     notice. See villagesql/preview/README.md.
// =============================================================================

#ifndef VILLAGESQL_ABI_PREVIEW_KEYRING_H
#define VILLAGESQL_ABI_PREVIEW_KEYRING_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::preview::keyring"
//
// Provides access to the MySQL keyring component. Extensions that require
// this capability can read and write secrets via the vtable functions below.
//
// Capability name: VEF_PREVIEW_KEYRING_NAME

#define VEF_PREVIEW_KEYRING_NAME "vsql::preview::keyring"

// Capability ABI version compiled into this SDK snapshot.
// Extensions can compare this against abi_->version at runtime to determine
// which fields the server supports.
#define VEF_PREVIEW_KEYRING_ABI_VERSION 1

typedef enum {
  VEF_KEYRING_OK = 0,
  VEF_KEYRING_NOT_FOUND = 1,    // key does not exist
  VEF_KEYRING_UNAVAILABLE = 2,  // no keyring component is installed
  VEF_KEYRING_ERROR = 3,        // other error
} vef_keyring_result_t;

// Read a secret from the MySQL keyring component.
//   data_id:  identifier for the secret.
//   auth_id:  owner of the secret, or NULL for internal keys.
//   buf:      caller-provided buffer to receive the secret bytes.
//   buf_len:  size of buf in bytes.
//   out_len:  set to the actual number of bytes written on success.
typedef vef_keyring_result_t (*vef_read_keyring_fn)(const char *data_id,
                                                    const char *auth_id,
                                                    unsigned char *buf,
                                                    size_t buf_len,
                                                    size_t *out_len);

// Write a secret to the MySQL keyring component.
//   data_id:   identifier for the secret.
//   auth_id:   owner of the secret, or NULL for internal keys.
//   data:      secret bytes to store.
//   data_len:  length of data in bytes.
typedef vef_keyring_result_t (*vef_write_keyring_fn)(const char *data_id,
                                                     const char *auth_id,
                                                     const unsigned char *data,
                                                     size_t data_len);

typedef struct {
  // Capability ABI version. Always the first field in every capability vtable.
  // Extensions must check this before accessing fields added in later versions.
  uint32_t version;

  // version >= 1
  vef_read_keyring_fn read;
  vef_write_keyring_fn write;
} vef_preview_keyring_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_KEYRING_H
