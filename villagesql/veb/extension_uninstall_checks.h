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

#ifndef VILLAGESQL_VEB_EXTENSION_UNINSTALL_CHECKS_H_
#define VILLAGESQL_VEB_EXTENSION_UNINSTALL_CHECKS_H_

#include <vector>

#include "villagesql/schema/systable/custom_index_columns.h"
#include "villagesql/schema/systable/custom_indexes.h"
#include "villagesql/schema/systable/extensions.h"

namespace villagesql {

// Returns true and emits villagesql_error if any IndexEntry in `all_indexes`
// belongs to `ext_entry` (matched on extension_name + extension_version), or
// if any IndexColumnEntry in `all_index_columns` references a profile from
// `ext_entry` (matched on profile_extension_name + profile_extension_version).
// RESTRICT semantics: the caller aborts uninstall when this returns true.
// No system-table mutations occur.
bool check_for_indexes_of_extension(
    const ExtensionEntry &ext_entry,
    const std::vector<const IndexEntry *> &all_indexes,
    const std::vector<const IndexColumnEntry *> &all_index_columns);

}  // namespace villagesql

#endif  // VILLAGESQL_VEB_EXTENSION_UNINSTALL_CHECKS_H_
