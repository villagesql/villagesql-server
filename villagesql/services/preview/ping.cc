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

#include "villagesql/services/preview/ping.h"

#include <atomic>

namespace villagesql::services {

namespace {

std::atomic<uint64_t> g_ping_counter{0};
uint64_t vsql_ping() { return ++g_ping_counter; }
vef_preview_ping_t g_ping_vtable{&vsql_ping};

}  // namespace

vef_preview_ping_t *preview_ping_vtable() { return &g_ping_vtable; }

}  // namespace villagesql::services
