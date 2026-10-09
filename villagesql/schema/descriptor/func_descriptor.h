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

// FuncDescriptor: In-memory descriptor for VillageSQL Defined Functions (VDFs).
// FuncDescriptor is built programmatically from extension .so files and holds
// pointers to the function descriptors from the extension registration.

#ifndef VILLAGESQL_SCHEMA_DESCRIPTOR_FUNC_DESCRIPTOR_H_
#define VILLAGESQL_SCHEMA_DESCRIPTOR_FUNC_DESCRIPTOR_H_

#include <string>
#include <utility>

#include "mysql/udf_registration_types.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {

template <typename EntryType>
struct TableTraits;

// Prefix key for querying FuncDescriptors.
// Format: "normalized_func_name." or
// "normalized_func_name.normalized_ext_name." Function name comes first to
// enable prefix queries for unqualified lookups.
struct FuncKeyPrefix {
 public:
  // Query by function name only (for future unqualified lookups)
  explicit FuncKeyPrefix(std::string function_name);

  // Query by function name + extension name
  FuncKeyPrefix(std::string function_name, std::string extension_name);

  const std::string &str() const { return normalized_prefix_; }

  const std::string &function_name() const { return function_name_; }
  const std::string &extension_name() const { return extension_name_; }

 private:
  std::string function_name_;
  std::string extension_name_;
  std::string normalized_prefix_;
};

// Key for FuncDescriptor entries in the VictionaryClient map.
// Format: "normalized_func_name.normalized_extension_name"
// Function name comes first to enable prefix queries for unqualified lookups.
// Note: extension_version is stored in FuncDescriptor, not in the key, because
// lookups happen by function+extension name without version.
struct FuncKey {
 public:
  FuncKey() = default;

  FuncKey(std::string function_name, std::string extension_name);

  const std::string &str() const { return normalized_key_; }

  const std::string &function_name() const { return function_name_; }
  const std::string &extension_name() const { return extension_name_; }

  bool operator<(const FuncKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const FuncKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string function_name_;
  std::string extension_name_;
  std::string normalized_key_;
};

// FuncDescriptor: In-memory descriptor for a VillageSQL Defined Function.
// Built programmatically from extension registration, not from table rows.
// Holds a pointer to the function descriptor from the extension's registration.
class FuncDescriptor {
 public:
  using key_type = FuncKey;
  using key_prefix_type = FuncKeyPrefix;

  // Default constructor - creates an empty/invalid descriptor
  // Required for use with SystemTableMap's PendingOperation
  FuncDescriptor() = default;

  // Construct with key only (useful for testing)
  explicit FuncDescriptor(FuncKey key) : key_(std::move(key)) {}

  // Full constructor with all fields
  FuncDescriptor(FuncKey key, std::string extension_version,
                 const vef_func_desc_t *func_desc, vef_protocol_t protocol,
                 Item_result return_type);

  // Disable copy (descriptors should not be copied)
  FuncDescriptor(const FuncDescriptor &) = delete;
  FuncDescriptor &operator=(const FuncDescriptor &) = delete;

  // Enable move (needed for SystemTableMap storage)
  FuncDescriptor(FuncDescriptor &&) = default;
  FuncDescriptor &operator=(FuncDescriptor &&) = default;

  ~FuncDescriptor() = default;

  // Key accessor (required by SystemTableMap)
  const FuncKey &key() const { return key_; }

  // Accessors for key components (delegate to key)
  const std::string &function_name() const { return key_.function_name(); }
  const std::string &extension_name() const { return key_.extension_name(); }
  const std::string &extension_version() const { return extension_version_; }

  // Function-specific accessors
  const vef_func_desc_t *func_desc() const { return func_desc_; }
  vef_protocol_t protocol() const { return protocol_; }
  Item_result return_type() const { return return_type_; }

  // False when a type in this extension claims this VDF as its intrinsic
  // default. Such a VDF is invoked only by TypeContext, which supplies the type
  // parameters it needs; a direct SQL call has none to give, so there is no
  // value it could correctly return.
  bool is_callable_from_sql() const { return callable_from_sql_; }
  void set_not_callable_from_sql() { callable_from_sql_ = false; }

 private:
  FuncKey key_;

  // Extension version (stored separately from key since lookups don't use it)
  std::string extension_version_;

  // Pointer to the function descriptor from the extension's registration.
  // This pointer is valid as long as the extension is loaded.
  const vef_func_desc_t *func_desc_{nullptr};

  // Protocol version for this function
  vef_protocol_t protocol_{VEF_PROTOCOL_1};

  // MySQL result type for the return value
  Item_result return_type_{STRING_RESULT};

  // Set by parse_extension_registration() for a VDF a type claims as its
  // intrinsic default. Defaults to callable so a function no type claims keeps
  // the behaviour it has always had.
  bool callable_from_sql_{true};
};

// TableTraits specialization for FuncDescriptor.
// Empty because FuncDescriptor doesn't have table-backed operations.
template <>
struct TableTraits<FuncDescriptor> {};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_FUNC_DESCRIPTOR_H_
