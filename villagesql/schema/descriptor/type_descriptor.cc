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

#include "villagesql/schema/descriptor/type_descriptor.h"

#include "villagesql/schema/systable/helpers.h"

namespace villagesql {

TypeDescriptorKey::TypeDescriptorKey(std::string type_name,
                                     std::string extension_name,
                                     std::string extension_version)
    : type_name_(std::move(type_name)),
      extension_name_(std::move(extension_name)),
      extension_version_(std::move(extension_version)),
      normalized_key_(normalize_type_name(type_name_) + "." +
                      normalize_extension_name(extension_name_) + "." +
                      normalize_extension_name(extension_version_)) {}

TypeDescriptor::TypeDescriptor(TypeDescriptorKey key, unsigned char impl_type,
                               int64_t persisted_len,
                               int64_t max_unpersisted_len, EncodeOp encode,
                               DecodeOp decode, CompareOp compare,
                               std::optional<HashOp> hash,
                               std::optional<IntToParamsOp> int_to_params,
                               std::optional<ResolveParamsOp> resolve_params)
    : key_(std::move(key)),
      implementation_type_(impl_type),
      persisted_length_(persisted_len),
      max_decode_buffer_length_(max_unpersisted_len),
      encode_op_(std::move(encode)),
      decode_op_(std::move(decode)),
      compare_op_(std::move(compare)),
      hash_op_(std::move(hash)),
      int_to_params_op_(std::move(int_to_params)),
      resolve_params_op_(std::move(resolve_params)) {}

}  // namespace villagesql
