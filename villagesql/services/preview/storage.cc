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

#include "villagesql/services/preview/storage.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"

namespace villagesql::services {

namespace {

vef_preview_storage_t g_storage_vtable{
    vef_storage_mtr_start,         vef_storage_mtr_commit,
    vef_storage_segment_create,    vef_storage_segment_drop,
    vef_storage_page_load,         vef_storage_page_allocate_and_load,
    vef_storage_page_latch,        vef_storage_page_release,
    vef_storage_page_get_size,     vef_storage_page_write_integer,
    vef_storage_page_write_string,
};

}  // namespace

vef_preview_storage_t *preview_storage_vtable() { return &g_storage_vtable; }

}  // namespace villagesql::services
