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

#ifndef VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_INDEX_COLUMNS_H_
#define VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_INDEX_COLUMNS_H_

#include <cstdint>
#include <string>

#include "villagesql/schema/systable/helpers.h"

struct TABLE;

namespace villagesql {

template <typename EntryType>
struct TableTraits;

// Prefix key for querying all column bindings for a given index_id.
// Format: "index_id_string."  (e.g. "42.")
// ASCII ordering ensures "42." < "42.N" < "42/" so prefix scan is exact.
struct IndexColumnKeyPrefix {
 public:
  explicit IndexColumnKeyPrefix(uint64_t index_id)
      : prefix_(std::to_string(index_id) + ".") {}

  const std::string &str() const { return prefix_; }

 private:
  std::string prefix_;
};

// Key for custom_index_columns table entries.
// Format: "index_id_string.key_position_string"  (e.g. "42.0")
struct IndexColumnKey {
 public:
  IndexColumnKey() = default;

  IndexColumnKey(uint64_t index_id, uint32_t key_position)
      : index_id_(index_id),
        key_position_(key_position),
        key_str_(std::to_string(index_id) + "." +
                 std::to_string(key_position)) {}

  const std::string &str() const { return key_str_; }

  uint64_t index_id() const { return index_id_; }
  uint32_t key_position() const { return key_position_; }

  bool operator<(const IndexColumnKey &other) const {
    return key_str_ < other.key_str_;
  }
  bool operator==(const IndexColumnKey &other) const {
    return key_str_ == other.key_str_;
  }

 private:
  uint64_t index_id_{0};
  uint32_t key_position_{0};
  std::string key_str_;
};

// Entry for custom_index_columns system table.
// Stores per-column profile bindings for a VillageSQL custom index.
struct IndexColumnEntry {
 public:
  using key_type = IndexColumnKey;
  using key_prefix_type = IndexColumnKeyPrefix;

  // Non-key fields (public)
  std::string column_name;
  std::string profile_extension_name;
  std::string profile_extension_version;
  std::string profile_name;

  IndexColumnEntry() = default;

  explicit IndexColumnEntry(IndexColumnKey key) : key_(std::move(key)) {}

  IndexColumnEntry(IndexColumnKey key, std::string col_name,
                   std::string prof_ext_name, std::string prof_ext_version,
                   std::string prof_name)
      : column_name(std::move(col_name)),
        profile_extension_name(std::move(prof_ext_name)),
        profile_extension_version(std::move(prof_ext_version)),
        profile_name(std::move(prof_name)),
        key_(std::move(key)) {}

  const IndexColumnKey &key() const { return key_; }
  uint64_t index_id() const { return key_.index_id(); }
  uint32_t key_position() const { return key_.key_position(); }

 protected:
  void set_key(IndexColumnKey key) { key_ = std::move(key); }
  friend struct TableTraits<IndexColumnEntry>;

 private:
  IndexColumnKey key_;
};

// TableTraits specialization for IndexColumnEntry.
// delete_from_table and update_in_table use the PRIMARY KEY (index_id,
// key_position) = key_info[0].
template <>
struct TableTraits<IndexColumnEntry> {
  static bool read_from_table(TABLE &table, IndexColumnEntry &entry);
  static bool write_to_table(TABLE &table, const IndexColumnEntry &entry);
  static bool update_in_table(TABLE &table, const IndexColumnEntry &entry,
                              const std::string &old_key);
  static bool delete_from_table(TABLE &table, const IndexColumnEntry &entry);
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_INDEX_COLUMNS_H_
