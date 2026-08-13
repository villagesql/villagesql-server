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

#include "villagesql/schema/descriptor/extension_descriptor.h"

#include "villagesql/schema/identifier_names.h"

namespace villagesql {

ExtensionDescriptorKey::ExtensionDescriptorKey(std::string extension_name,
                                               std::string extension_version)
    : extension_name_(std::move(extension_name)),
      extension_version_(std::move(extension_version)),
      normalized_key_(canonical_extension_name(extension_name_) + "." +
                      canonical_extension_name(extension_version_)) {}

}  // namespace villagesql
