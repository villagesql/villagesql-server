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

#ifndef VILLAGESQL_VEB_VEB_REGISTER_TYPE_H_
#define VILLAGESQL_VEB_VEB_REGISTER_TYPE_H_

#include <optional>
#include <string>

#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {
namespace veb {

// Build a TypeDescriptor from a protocol-1 type descriptor.
// Only reads protocol-1 fields (direct function pointers).
// Returns nullopt on error.
std::optional<TypeDescriptor> build_type_descriptor_v1(
    const vef_type_desc_t *td, const std::string &type_name,
    const std::string &extension_name, const std::string &extension_version);

// Build a TypeDescriptor from a protocol-3 type descriptor.
// Reads all fields including VDF names and intrinsic_default.
// Storage is wired in separately after all types are loaded.
// Returns nullopt on error.
std::optional<TypeDescriptor> build_type_descriptor_v3(
    const vef_type_desc_t *td, const vef_registration_t &reg,
    const std::string &type_name, const std::string &extension_name,
    const std::string &extension_version);

}  // namespace veb
}  // namespace villagesql

#endif  // VILLAGESQL_VEB_VEB_REGISTER_TYPE_H_
