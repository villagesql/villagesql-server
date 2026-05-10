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

// Parses a raw VEF ExtensionRegistration (ABI structs from a loaded .so)
// into a ValidatedRegistration containing typed C++ descriptors.
//
// parse_extension_registration() has no side effects and no dependency on
// the victionary or THD, making it independently testable.

#ifndef VILLAGESQL_VEB_VALIDATE_H_
#define VILLAGESQL_VEB_VALIDATE_H_

#include <optional>
#include <string>
#include <vector>

#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {
namespace veb {

struct ExtensionRegistration;

// The result of parsing a raw ExtensionRegistration.
// Contains parsed, ready-to-register C++ descriptors for types and funcs.
struct ValidatedRegistration {
  std::vector<TypeDescriptor> types;
  std::vector<FuncDescriptor> funcs;
};

// Parses the raw ABI registration into C++ descriptors, including wiring
// column storage interfaces into any types that register them.
// No THD, no VictionaryClient — purely operates on the ExtensionRegistration.
// Returns nullopt on error; error_out is set to a descriptive message.
std::optional<ValidatedRegistration> parse_extension_registration(
    const ExtensionRegistration &ext_reg, const std::string &extension_name,
    const std::string &extension_version, std::string &error_out);

}  // namespace veb
}  // namespace villagesql

#endif  // VILLAGESQL_VEB_VALIDATE_H_
