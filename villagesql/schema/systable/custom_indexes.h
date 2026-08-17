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

#ifndef VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_INDEXES_H_
#define VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_INDEXES_H_

#include <cstdint>
#include <string>

#include "sql/key_spec.h"
#include "villagesql/schema/identifier_names.h"

struct TABLE;

namespace villagesql {

template <typename EntryType>
struct TableTraits;

// Prefix key for querying all custom indexes for a db.table.
// Format: "normalized_db.normalized_table."
struct IndexKeyPrefix {
 public:
  // If table_name is empty, produces "db." prefix for all tables in that db.
  IndexKeyPrefix(std::string db_name, std::string table_name)
      : db_(std::move(db_name)),
        table_(std::move(table_name)),
        normalized_prefix_(
            join_key_components({canonical_database_name(db_)}) + "." +
            (table_.empty()
                 ? ""
                 : join_key_components({canonical_table_name(table_)}) + ".")) {
  }

  const std::string &str() const { return normalized_prefix_; }
  const std::string &db() const { return db_; }
  const std::string &table() const { return table_; }

 private:
  std::string db_;
  std::string table_;
  std::string normalized_prefix_;
};

// Key for custom_indexes table entries (natural key).
// Format: "normalized_db.normalized_table.normalized_index_name"
struct IndexKey {
 public:
  IndexKey() = default;

  IndexKey(std::string db_name, std::string table_name, std::string index_name)
      : db_(std::move(db_name)),
        table_(std::move(table_name)),
        index_(std::move(index_name)),
        normalized_key_(join_key_components({canonical_database_name(db_),
                                             canonical_table_name(table_),
                                             canonical_index_name(index_)})) {}

  const std::string &str() const { return normalized_key_; }

  const std::string &db() const { return db_; }
  const std::string &table() const { return table_; }
  const std::string &index_name() const { return index_; }

  bool operator<(const IndexKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const IndexKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string db_;
  std::string table_;
  std::string index_;
  std::string normalized_key_;
};

// Entry for custom_indexes system table.
// The natural in-memory key is "db.table.index_name". index_id is the physical
// surrogate primary key, assigned by VictionaryClient::allocate_index_id()
// before MarkForInsertion so it is known at staging time.
struct IndexEntry {
 public:
  using key_type = IndexKey;
  using key_prefix_type = IndexKeyPrefix;

  // Surrogate primary key in the physical table.
  uint64_t index_id{0};

  // Non-key data (public)
  std::string extension_name;
  std::string extension_version;
  std::string index_type_name;
  std::string index_type_parameters = "{}";  // JSON-serialized parameters

  IndexEntry() = default;

  explicit IndexEntry(IndexKey key) : key_(std::move(key)) {}

  IndexEntry(IndexKey key, uint64_t id, std::string ext_name,
             std::string ext_version, std::string type_name,
             std::string type_params = "{}")
      : index_id(id),
        extension_name(std::move(ext_name)),
        extension_version(std::move(ext_version)),
        index_type_name(std::move(type_name)),
        index_type_parameters(std::move(type_params)),
        key_(std::move(key)) {}

  const IndexKey &key() const { return key_; }

  const std::string &db_name() const { return key_.db(); }
  const std::string &table_name() const { return key_.table(); }
  const std::string &index_name() const { return key_.index_name(); }

 protected:
  void set_key(IndexKey key) { key_ = std::move(key); }
  friend struct TableTraits<IndexEntry>;

 private:
  IndexKey key_;
};

// TableTraits specialization for IndexEntry.
// delete_from_table and update_in_table use the UNIQUE KEY idx_natural
// (db_name, table_name, index_name) = key_info[1] for lookups, since the
// natural key is what callers have; index_id is the physical PK but is not
// known for DELETE-by-key temp entries.
template <>
struct TableTraits<IndexEntry> {
  static bool read_from_table(TABLE &table, IndexEntry &entry);
  static bool write_to_table(TABLE &table, const IndexEntry &entry);
  static bool update_in_table(TABLE &table, const IndexEntry &entry,
                              const IndexKey &old_key);
  static bool delete_from_table(TABLE &table, const IndexEntry &entry);
};

// Serialize a WITH-clause parameter list to a JSON object string.
// Mirrors TypeParameters::to_json() for the index parameter pipeline.
// Numeric params (is_string=false) are emitted unquoted; string/identifier
// params are emitted quoted with backslash and double-quote escaping.
// An empty list produces "{}".
std::string params_to_json(const Mem_root_array<IndexWithParam> &params);

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_INDEXES_H_
