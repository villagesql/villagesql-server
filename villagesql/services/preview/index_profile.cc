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

#include "villagesql/services/preview/index_profile.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

namespace villagesql::services {

namespace {

vef_preview_index_profile_t g_index_profile_vtable{
    VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION};

}  // namespace

vef_preview_index_profile_t *preview_index_profile_vtable() {
  return &g_index_profile_vtable;
}

}  // namespace villagesql::services
