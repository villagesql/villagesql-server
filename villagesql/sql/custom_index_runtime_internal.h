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

// Internal contract for the custom-index runtime (custom_index_runtime.cc).
// Not part of the public runtime interface in custom_index_runtime.h.

#include <cstdint>
#include <string>
#include <vector>

class THD;

namespace villagesql {

// Resolve the committed column names of a custom index, ordered by key
// position. Defined in custom_index_runtime.cc.
bool get_custom_index_columns(THD *thd, uint64_t index_id,
                              std::vector<std::string> *out);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_RUNTIME_INTERNAL_H_
