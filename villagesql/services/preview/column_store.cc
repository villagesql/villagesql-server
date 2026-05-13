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

#include "villagesql/services/preview/column_store.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"

namespace villagesql::services {

namespace {

vef_preview_column_store_t g_column_store_vtable{VEF_COLUMN_STORE_INTF_VERSION};

}  // namespace

vef_preview_column_store_t *preview_column_store_vtable() {
  return &g_column_store_vtable;
}

}  // namespace villagesql::services
