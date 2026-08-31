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

#ifndef VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_SP_PARAMS_H_
#define VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_SP_PARAMS_H_

#include <string>

#include "villagesql/schema/identifier_names.h"

// Forward declarations
struct TABLE;

namespace villagesql {

// Forward declarations
class TypeContext;
template <typename EntryType>
struct TableTraits;

// Sentinel param name used to store the custom return type of a stored function
// in custom_sp_params. Must not collide with valid SQL identifier names.
inline constexpr const char *SP_RETURNS_SENTINEL = "@returns";

// Prefix key for querying all params of a stored procedure/function.
// Format: "normalized_db.normalized_sp_name."
struct SpParamKeyPrefix {
 public:
  SpParamKeyPrefix(std::string db_name, std::string sp_name)
      : db_(std::move(db_name)),
        sp_name_(std::move(sp_name)),
        normalized_prefix_(
            join_key_components({canonical_database_name(db_),
                                 canonical_table_name(sp_name_)}) +
            ".") {}

  // TODO(villagesql-production): Add a db-only constructor (no sp_name) to
  // support bulk deletion of all sp params for a given database, needed for
  // DROP DATABASE cleanup.
  const std::string &str() const { return normalized_prefix_; }

  const std::string &db() const { return db_; }
  const std::string &sp_name() const { return sp_name_; }

 private:
  std::string db_;
  std::string sp_name_;
  std::string normalized_prefix_;
};

// Key for custom_sp_params table entries.
// Format: "normalized_db.normalized_sp_name.normalized_param_name"
struct SpParamKey {
 public:
  SpParamKey() = default;

  SpParamKey(std::string db_name, std::string sp_name, std::string param_name)
      : db_(std::move(db_name)),
        sp_name_(std::move(sp_name)),
        param_(std::move(param_name)),
        normalized_key_(join_key_components({canonical_database_name(db_),
                                             canonical_table_name(sp_name_),
                                             canonical_column_name(param_)})) {}

  const std::string &str() const { return normalized_key_; }

  const std::string &db() const { return db_; }
  const std::string &sp_name() const { return sp_name_; }
  const std::string &param() const { return param_; }

  bool operator<(const SpParamKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const SpParamKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string db_;
  std::string sp_name_;
  std::string param_;
  std::string normalized_key_;
};

// Entry for custom_sp_params system table.
// Represents a stored procedure/function parameter that uses a custom type.
struct SpParamEntry {
 public:
  using key_type = SpParamKey;
  using key_prefix_type = SpParamKeyPrefix;

  // Non-key data (public)
  std::string extension_name;
  std::string extension_version;
  std::string type_name;
  std::string type_parameters = "{}";  // JSON-serialized TypeParameters

  // Full constructor with all fields
  SpParamEntry(SpParamKey key, std::string ext_name, std::string ext_version,
               std::string t_name, std::string type_params = "{}")
      : extension_name(std::move(ext_name)),
        extension_version(std::move(ext_version)),
        type_name(std::move(t_name)),
        type_parameters(std::move(type_params)),
        key_(std::move(key)) {}

  // Construct with key only (for delete operations and testing)
  explicit SpParamEntry(SpParamKey key) : key_(std::move(key)) {}

  SpParamEntry() = default;

  const SpParamKey &key() const { return key_; }

  const std::string &db_name() const { return key_.db(); }
  const std::string &sp_name() const { return key_.sp_name(); }
  const std::string &param_name() const { return key_.param(); }

 protected:
  void set_key(SpParamKey key) { key_ = std::move(key); }
  friend struct TableTraits<SpParamEntry>;

 private:
  SpParamKey key_;
};

// TableTraits specialization for SpParamEntry
template <>
struct TableTraits<SpParamEntry> {
  static bool read_from_table(TABLE &table, SpParamEntry &entry);

  static bool write_to_table(TABLE &table, const SpParamEntry &entry);

  static bool update_in_table(TABLE &table, const SpParamEntry &entry,
                              const SpParamKey &old_key);

  static bool delete_from_table(TABLE &table, const SpParamEntry &entry);
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_SP_PARAMS_H_
