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

// Returns a vef_context_t with function pointers initialized for the given
// protocol. For protocol >= VEF_PROTOCOL_2, read_keyring is wired up.
vef_context_t make_vef_context(vef_protocol_t protocol);

}  // namespace services
}  // namespace villagesql

#endif  // VILLAGESQL_SERVICES_KEYRING_H_
