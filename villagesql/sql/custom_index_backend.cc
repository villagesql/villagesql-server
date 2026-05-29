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

#include "villagesql/sql/custom_index_backend.h"

#include <mutex>
#include <vector>

namespace villagesql {

namespace {

std::mutex g_backend_registry_mu;
std::vector<CustomIndexBackend *> g_backend_registry;

}  // namespace

void register_custom_index_backend(CustomIndexBackend *backend) {
  if (backend == nullptr) return;
  std::lock_guard<std::mutex> guard(g_backend_registry_mu);
  g_backend_registry.push_back(backend);
}

CustomIndexBackend *find_custom_index_backend(
    const vef_type_index_intf_t &intf) {
  std::lock_guard<std::mutex> guard(g_backend_registry_mu);
  for (CustomIndexBackend *backend : g_backend_registry) {
    if (backend->matches(intf)) return backend;
  }
  return nullptr;
}

void for_each_custom_index_backend(void (*fn)(CustomIndexBackend *, void *),
                                   void *user) {
  if (fn == nullptr) return;
  std::lock_guard<std::mutex> guard(g_backend_registry_mu);
  for (CustomIndexBackend *backend : g_backend_registry) fn(backend, user);
}

}  // namespace villagesql
