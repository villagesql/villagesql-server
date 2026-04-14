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

#ifndef VILLAGESQL_SERVICES_KEYRING_H_
#define VILLAGESQL_SERVICES_KEYRING_H_

#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {
namespace services {

// Implementations of the keyring access functions passed to extensions
// via vef_register_arg_t.
vef_keyring_result_t read_keyring(const char *data_id, const char *auth_id,
                                   unsigned char *buf, size_t buf_len,
                                   size_t *out_len);
vef_keyring_result_t write_keyring(const char *data_id, const char *auth_id,
                                    const unsigned char *data, size_t data_len);

}  // namespace services
}  // namespace villagesql

#endif  // VILLAGESQL_SERVICES_KEYRING_H_
