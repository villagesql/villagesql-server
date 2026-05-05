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

using namespace vsql;

// keyring_read(data_id, auth_id) - reads a secret from the keyring.
// Returns the secret as a string, or NULL if not found.
// Pass NULL for auth_id to read internal keys.
void keyring_read(StringArg data_id, StringArg auth_id, StringResult out) {
  if (data_id.is_null()) {
    out.set_null();
    return;
  }

  std::string value;
  vef_keyring_result_t kr = vsql::keyring::read(
      data_id.value(), auth_id.is_null() ? "" : auth_id.value(), value);
  if (kr == VEF_KEYRING_UNAVAILABLE) {
    out.error("No keyring component is installed");
    return;
  }
  if (kr != VEF_KEYRING_OK) {
    out.set_null();
    return;
  }

  auto buf = out.buffer();
  size_t len = std::min(value.size(), buf.size());
  memcpy(buf.data(), value.data(), len);
  out.set_length(len);
}

// keyring_store(data_id, auth_id, value) - stores a secret in the keyring.
// Returns 0 on success, 1 on error.
// Pass NULL for auth_id to store as an internal key.
void keyring_store(StringArg data_id, StringArg auth_id, StringArg value,
                   IntResult out) {
  if (data_id.is_null() || value.is_null()) {
    out.set(1);
    return;
  }

  vef_keyring_result_t kr = vsql::keyring::write(
      data_id.value(), auth_id.is_null() ? "" : auth_id.value(), value.value());
  if (kr == VEF_KEYRING_UNAVAILABLE) {
    out.error("No keyring component is installed");
    return;
  }
  out.set(kr == VEF_KEYRING_OK ? 0 : 1);
}

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .func(make_func<&keyring_read>("keyring_read")
                                        .returns(STRING)
                                        .param(STRING)
                                        .param(STRING)
                                        .build())
                              .func(make_func<&keyring_store>("keyring_store")
                                        .returns(INT)
                                        .param(STRING)
                                        .param(STRING)
                                        .param(STRING)
                                        .build()))
