// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// VillageSQL extension demonstrating keyring access via vef_register_arg_t.
//
// Exposes a single VDF:
//   keyring_read(data_id VARCHAR, auth_id VARCHAR) RETURNS VARCHAR
//
// Reads a secret from the MySQL keyring component. Returns the secret as a
// string, or NULL if the key does not exist or the keyring is unavailable.
// auth_id may be NULL to read internal keys (not accessible via SQL).
//
// Prerequisites: a keyring component must be installed, e.g.:
//   INSTALL COMPONENT 'file://component_keyring_file';

#include <villagesql/vsql.h>

// Buffer size for keyring secrets. Secrets larger than this will not be read.
static const size_t kMaxSecretLen = 4096;

// keyring_read(data_id, auth_id) - reads a secret from the keyring.
// Returns the secret as a string, or NULL if not found or keyring unavailable.
// Pass NULL for auth_id to read internal keys.
void keyring_read_impl(vef_context_t * /*ctx*/, vef_invalue_t *data_id_arg,
                       vef_invalue_t *auth_id_arg, vef_vdf_result_t *out) {
  if (data_id_arg->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  std::string_view auth_id = auth_id_arg->is_null ? "" : auth_id_arg->str_value;

  std::string value;
  vef_keyring_result_t kr =
      villagesql::keyring::read(data_id_arg->str_value, auth_id, value);
  if (kr == VEF_KEYRING_UNAVAILABLE) {
    out->type = VEF_RESULT_ERROR;
    snprintf(out->error_msg, VEF_MAX_ERROR_LEN,
             "No keyring component is installed");
    return;
  }
  if (kr != VEF_KEYRING_OK) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  out->actual_len = value.size();
  memcpy(out->str_buf, value.data(), value.size());
}

// keyring_store(data_id, auth_id, value) - stores a secret in the keyring.
// Returns 0 on success, 1 on error.
// Pass NULL for auth_id to store as an internal key.
void keyring_store_impl(vef_context_t * /*ctx*/, vef_invalue_t *data_id_arg,
                        vef_invalue_t *auth_id_arg, vef_invalue_t *value_arg,
                        vef_vdf_result_t *out) {
  out->int_value = 1;
  if (data_id_arg->is_null || value_arg->is_null) return;

  std::string_view auth_id = auth_id_arg->is_null ? "" : auth_id_arg->str_value;

  vef_keyring_result_t kr = villagesql::keyring::write(
      data_id_arg->str_value, auth_id,
      std::string_view(value_arg->str_value, value_arg->str_len));
  if (kr == VEF_KEYRING_UNAVAILABLE) {
    out->type = VEF_RESULT_ERROR;
    snprintf(out->error_msg, VEF_MAX_ERROR_LEN,
             "No keyring component is installed");
    return;
  }
  if (kr == VEF_KEYRING_OK) out->int_value = 0;
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension("vsql_keyring_reader", "0.0.1")
        .func(make_func<&keyring_read_impl>("keyring_read")
                  .returns(STRING)
                  .buffer_size(kMaxSecretLen)
                  .param(STRING)
                  .param(STRING)
                  .build())
        .func(make_func<&keyring_store_impl>("keyring_store")
                  .returns(INT)
                  .param(STRING)
                  .param(STRING)
                  .param(STRING)
                  .build()))
