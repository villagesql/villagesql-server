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

// Protocol-1 type registration: builds a TypeDescriptor from a
// vef_type_desc_t that uses direct function pointers.
//
// Only protocol-1 fields are read from td.  Protocol-2 fields
// (VDF names, intrinsic_default_str, storage_intf) are ignored.

#include "villagesql/veb/veb_register_type.h"

#include <optional>
#include <string>

#include "sql/field.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/types/type_function.h"

namespace villagesql {
namespace veb {

std::optional<TypeDescriptor> build_type_descriptor_v1(
    const vef_type_desc_t *td, const std::string &type_name,
    const std::string &extension_name, const std::string &extension_version) {
  // 1. Resolve encode / decode / compare (required).
  EncodeFunction encode_fn(td->encode_func);
  DecodeFunction decode_fn(td->decode_func);
  CompareFunction compare_fn(td->compare_func);

  // 2. Resolve hash (optional).
  std::optional<HashFunction> hash_fn;
  if (td->hash_func != nullptr) hash_fn.emplace(td->hash_func);

  // 3. Build TypeDescriptor.
  // V1 ABI doesn't carry max_persisted_length; pass 0 (no inference path for
  // v1 types — constant-string inference is a v2-only capability).
  TypeDescriptor descriptor(
      TypeDescriptorKey(type_name, extension_name, extension_version),
      VEF_PROTOCOL_1, MYSQL_TYPE_VARCHAR, td->persisted_length,
      td->max_decode_buffer_length, /*max_persisted_length=*/0,
      std::move(encode_fn), std::move(decode_fn), std::move(compare_fn),
      std::move(hash_fn));

  // 5. No intrinsic default for protocol-1; encode("") is the fallback.

  return descriptor;
}

}  // namespace veb
}  // namespace villagesql
