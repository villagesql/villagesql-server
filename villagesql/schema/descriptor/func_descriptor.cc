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

#include "villagesql/schema/descriptor/func_descriptor.h"

#include "villagesql/schema/identifier_names.h"

namespace villagesql {

FuncKeyPrefix::FuncKeyPrefix(std::string function_name)
    : function_name_(std::move(function_name)),
      normalized_prefix_(canonical_extension_name(function_name_) + ".") {}

FuncKeyPrefix::FuncKeyPrefix(std::string function_name,
                             std::string extension_name)
    : function_name_(std::move(function_name)),
      extension_name_(std::move(extension_name)),
      normalized_prefix_(
          canonical_extension_name(function_name_) + "." +
          (extension_name_.empty()
               ? ""
               : canonical_extension_name(extension_name_) + ".")) {}

FuncKey::FuncKey(std::string function_name, std::string extension_name)
    : function_name_(std::move(function_name)),
      extension_name_(std::move(extension_name)),
      normalized_key_(canonical_extension_name(function_name_) + "." +
                      canonical_extension_name(extension_name_)) {}

FuncDescriptor::FuncDescriptor(FuncKey key, std::string extension_version,
                               const vef_func_desc_t *func_desc,
                               vef_protocol_t protocol, Item_result return_type)
    : key_(std::move(key)),
      extension_version_(std::move(extension_version)),
      func_desc_(func_desc),
      protocol_(protocol),
      return_type_(return_type) {}

}  // namespace villagesql
