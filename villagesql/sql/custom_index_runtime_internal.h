/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_RUNTIME_INTERNAL_H_
#define VILLAGESQL_SQL_CUSTOM_INDEX_RUNTIME_INTERNAL_H_

// Internal contract shared between the custom-index runtime translation units
// (custom_index_runtime.cc, custom_index_knn_scan.cc). Not part of the public
// runtime interface in custom_index_runtime.h; only the runtime's own
// translation units include it.
//
// The runtime no longer keeps a cache of loaded custom indexes. Both the scan
// and insert paths obtain the loaded index instance (its vef_index_ctx_t +
// storage context) from the storage engine via handler::get_custom_index_handle
// (-> dict_index_t::custom_index); the engine owns that instance's lifetime.

#include <cstdint>
#include <string>
#include <vector>

#include "villagesql/sdk/include/villagesql/abi/preview/index.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"

class THD;
class handler;

namespace villagesql {

// Resolve the committed column names of a custom index, ordered by key
// position. Defined in custom_index_runtime.cc.
bool get_custom_index_columns(THD *thd, uint64_t index_id,
                              std::vector<std::string> *out);

// Fetch the loaded custom-index instance the storage engine holds for key
// `key_idx` (the intf vtable + index/storage contexts it loaded at table-open),
// via handler::get_custom_index_handle. Used by both the insert and scan paths
// so they operate on the same engine-owned instance. Returns false and fills
// the out-params on success; true if the key is not a loaded custom index.
// Defined in custom_index_runtime.cc.
bool get_loaded_custom_index(handler *file, uint key_idx,
                             const vef_type_index_intf_t **intf,
                             vef_index_ctx_t **ctx,
                             vef_storage_ctx_t **storage);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_RUNTIME_INTERNAL_H_
