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

#include "villagesql/services/preview/keyring.h"

#include "villagesql/services/keyring.h"

namespace villagesql::services {

namespace {

vef_preview_keyring_t g_keyring_vtable{villagesql::services::read_keyring,
                                       villagesql::services::write_keyring};

}  // namespace

vef_preview_keyring_t *preview_keyring_vtable() { return &g_keyring_vtable; }

}  // namespace villagesql::services
