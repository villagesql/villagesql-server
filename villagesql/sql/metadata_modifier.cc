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

#include "villagesql/sql/metadata_modifier.h"

#include <unordered_set>
#include <utility>

#include "sql/create_field.h"
#include "sql/field.h"
#include "sql/field_common_properties.h"
#include "sql/handler.h"
#include "sql/key_spec.h"
#include "sql/mdl.h"
#include "sql/sp_head.h"
#include "sql/sp_pcontext.h"
#include "sql/sql_alter.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_list.h"
#include "sql/sql_udf.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/identifier_names.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/systable/custom_sp_params.h"
#include "villagesql/schema/systable/extensions.h"
#include "villagesql/schema/util.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/sql/custom_vdf.h"
#include "villagesql/sql/func_lookup.h"
#include "villagesql/types/util.h"

namespace villagesql {

Metadata_modifier::AlterGuard::~AlterGuard() {
  ClearAlterCustomFields(thd_);
  if (armed_) rollback(thd_);
}

static constexpr const char *error_uninitialized_name =
    "Uninitialized DB name or Table Name found while verifying custom columns";
static constexpr const char *error_uninitialized_victionary =
    "Victionary Client not initialized";
static constexpr const char *error_uninitialized_session =
    "Uninitialized Session: NULL THD";

bool Metadata_modifier::ensure_engine_is_innodb(const handlerton *hton,
                                                const char *operation) {
  assert(hton);
  if (hton && hton->db_type != DB_TYPE_INNODB) {
    villagesql_error(
        "Custom types are only supported with InnoDB storage engine. "
        "Cannot %s with %s storage engine.",
        MYF(0), operation, ha_resolve_storage_engine_name(hton));
    return true;
  }
  return false;
}

bool Metadata_modifier::add_columns(THD *thd [[maybe_unused]],
                                    Table_name db_table,
                                    const List<Create_field> &create_fields) {
  if (should_assert_if_null(db_table.first) ||
      should_assert_if_null(db_table.second)) {
    villagesql_error(error_uninitialized_name, MYF(0));
    return true;
  }

  const char *db_name = db_table.first;
  const char *table_name = db_table.second;

  // Skip special databases
  if (is_system_schema(db_name)) {
    return false;
  }

  // We must already have locked the table to make sure that that the
  // table columns are not altered.
  assert(thd->mdl_context.owns_equal_or_stronger_lock(MDL_key::TABLE, db_name,
                                                      table_name, MDL_SHARED));

  // Iterate through all columns in the create list.
  for (const Create_field &create_field : create_fields) {
    if (!create_field.custom_type_context) {
      continue;  // Not a custom type column.
    }
    // Skip duplicate column entries. Refer to prepare_create_field()
    // For CREATE ... SELECT statement, field redefinition is possible.
    bool duplicate = false;
    for (const ColumnEntry &entry : to_add_) {
      if (column_names_equal(create_field.field_name, entry.column_name()) &&
          database_names_equal(db_name, entry.db_name()) &&
          table_names_equal(table_name, entry.table_name())) {
        duplicate = true;
        break;
      }
    }
    if (duplicate) {
      // Skip duplicate column entries.
      continue;
    }

    // This column uses a custom type - create entry.
    const TypeContext *type_context = create_field.custom_type_context;
    to_add_.emplace_back(
        ColumnKey(db_name, table_name, create_field.field_name),
        type_context->extension_name(), type_context->extension_version(),
        type_context->type_name(), type_context->parameters().to_json());
  }

  return false;
}

// Resolves a name to exactly one registered descriptor via prefix lookup.
// Returns nullptr and sets a villagesql_error on 0 or >1 matches.
// REQUIRES: Caller must hold vclient read lock.
template <typename EntryType, typename Map, typename Prefix>
static const EntryType *resolve_unique_descriptor(const Map &map,
                                                  const Prefix &prefix,
                                                  const char *entity_type,
                                                  const char *name,
                                                  const char *qualify_hint) {
  auto matches = map.get_prefix_committed(prefix);
  if (matches.empty()) {
    villagesql_error("Unknown %s '%s'", MYF(0), entity_type, name);
    return nullptr;
  }
  if (matches.size() > 1) {
    villagesql_error("%s '%s' is ambiguous; qualify as %s", MYF(0), entity_type,
                     name, qualify_hint);
    return nullptr;
  }
  return matches[0];
}

// Finds the default profile registered for (col_type, index_type) pair and
// sets out_* on success. Returns true and sets a villagesql_error if the column
// type cannot be determined or no default profile is registered for the pair.
// REQUIRES: Caller must hold vclient read lock.
// TODO(villagesql): string parameters here (and canonical_type_name /
// canonical_extension_name) should use std::string_view; requires updating the
// downstream helpers consistently.
static bool find_default_profile(VictionaryClient &vclient,
                                 const Alter_info *alter_info, const char *db,
                                 const char *table_name, const char *field_name,
                                 const std::string &index_type_name,
                                 const std::string &index_type_ext_name,
                                 std::string &out_profile_name,
                                 std::string &out_prof_ext_name,
                                 std::string &out_prof_ext_version) {
  std::string col_type_name;
  std::string col_ext_name;
  // Common case: column already exists in the table.
  const ColumnEntry *ce =
      vclient.columns().get_committed(ColumnKey(db, table_name, field_name));
  if (ce) {
    // Guard against DROP + re-ADD of the same column in one ALTER TABLE; the
    // existing entry is stale in that case so fall through to create_list.
    bool being_dropped = false;
    for (const Alter_drop *drop : alter_info->drop_list) {
      if (drop->type == Alter_drop::COLUMN &&
          column_names_equal(drop->name, field_name)) {
        being_dropped = true;
        break;
      }
    }
    if (!being_dropped) {
      col_type_name = ce->type_name;
      col_ext_name = ce->extension_name;
      assert(!col_ext_name.empty());
    }
  }
  // Column is new (ADD COLUMN in this statement).
  if (col_type_name.empty()) {
    // TODO(villagesql-indexing): Built-in types have no custom_type_context;
    // support custom indexes on built-in types by also matching fields without
    // a custom_type_context using the field's SQL type name.
    for (const Create_field &field : alter_info->create_list) {
      if (field.custom_type_context &&
          column_names_equal(field.field_name, field_name)) {
        col_type_name = field.custom_type_context->type_name();
        col_ext_name = field.custom_type_context->extension_name();
        assert(!col_ext_name.empty());
        break;
      }
    }
  }
  if (col_type_name.empty()) {
    villagesql_error(
        "No default index profile found: column '%s' is not a custom type",
        MYF(0), field_name);
    return true;
  }

  // TODO(villagesql-performance): This is a linear scan over all registered
  // profiles. A secondary map in VictionaryClient keyed by (type_name,
  // index_type_name) would make it faster. Currently justified by small profile
  // counts and DDL-only call site.
  const std::string norm_col = canonical_type_name(col_type_name);
  const std::string norm_col_ext = canonical_extension_name(col_ext_name);
  const std::string norm_idx = canonical_type_name(index_type_name);
  const std::string norm_index_type_ext =
      canonical_extension_name(index_type_ext_name);
  for (const IndexProfileDescriptor *pd :
       vclient.index_profile_descriptors().get_all_committed()) {
    if (!pd->default_for_type()) continue;
    if (canonical_type_name(pd->type_name()) != norm_col) continue;
    assert(!pd->type_ref().extension_name().empty());
    if (canonical_extension_name(pd->type_ref().extension_name()) !=
        norm_col_ext)
      continue;
    if (canonical_type_name(pd->index_type_name()) != norm_idx) continue;
    assert(!norm_index_type_ext.empty());
    assert(!pd->index_type_ref().extension_name().empty());
    if (canonical_extension_name(pd->index_type_ref().extension_name()) !=
        norm_index_type_ext)
      continue;
    out_profile_name = pd->profile_name();
    out_prof_ext_name = pd->extension_name();
    out_prof_ext_version = pd->extension_version();
    return false;
  }
  villagesql_error(
      "No default index profile found for type '%s' (extension '%s') with"
      " index type '%s' (extension '%s')",
      MYF(0), col_type_name.c_str(), col_ext_name.c_str(),
      index_type_name.c_str(), index_type_ext_name.c_str());
  return true;
}

bool Metadata_modifier::add_indexes(THD *thd [[maybe_unused]], const char *db,
                                    const char *table_name,
                                    const Alter_info *alter_info) {
  if (is_system_schema(db)) return false;

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return false;

  for (const Key_spec *key : alter_info->key_list) {
    if (!key->key_create_info.custom_index_type.str) continue;

    const KEY_CREATE_INFO &kci = key->key_create_info;

    std::string ext_name = kci.custom_index_extension.str
                               ? std::string(kci.custom_index_extension.str,
                                             kci.custom_index_extension.length)
                               : "";
    const std::string type_name(kci.custom_index_type.str,
                                kci.custom_index_type.length);
    const std::string index_name(key->name.str, key->name.length);
    const std::string params_json =
        (kci.custom_index_params && !kci.custom_index_params->empty())
            ? params_to_json(*kci.custom_index_params)
            : "{}";
    std::string ext_version;

    {
      auto guard = vclient.get_read_lock();

      const auto *desc = resolve_unique_descriptor<IndexTypeDescriptor>(
          vclient.index_type_descriptors(),
          IndexTypeDescriptorKeyPrefix(type_name, ext_name),
          "custom index type", type_name.c_str(), "'extension.type_name'");
      if (!desc) return true;
      ext_name = desc->extension_name();
      ext_version = desc->extension_version();

      const uint64_t index_id = vclient.allocate_index_id();
      to_add_indexes_.emplace_back(IndexKey(db, table_name, index_name),
                                   index_id, ext_name, ext_version, type_name,
                                   params_json);

      uint32_t key_pos = 0;
      for (const Key_part_spec *kp : key->columns) {
        // Custom index not supported on expression.
        assert(kp->get_field_name());

        std::string profile_name, prof_ext_name, prof_ext_version;
        if (kp->has_index_profile()) {
          LEX_CSTRING prof = kp->get_index_profile();
          profile_name = std::string(prof.str, prof.length);
          const std::string prof_extension =
              kp->has_index_profile_extension()
                  ? std::string(kp->get_index_profile_extension().str,
                                kp->get_index_profile_extension().length)
                  : "";
          const auto *prof_desc =
              resolve_unique_descriptor<IndexProfileDescriptor>(
                  vclient.index_profile_descriptors(),
                  IndexProfileDescriptorKeyPrefix(profile_name, prof_extension),
                  "index profile", profile_name.c_str(),
                  "'extension.profile_name'");
          if (!prof_desc) return true;

          // Validate that the column's type matches the profile's expected
          // type.
          std::string col_type_name;
          std::string col_ext_name;
          const char *field_name = kp->get_field_name();
          const ColumnEntry *ce = vclient.columns().get_committed(
              ColumnKey(db, table_name, field_name));
          if (ce) {
            bool being_dropped = false;
            for (const Alter_drop *drop : alter_info->drop_list) {
              if (drop->type == Alter_drop::COLUMN &&
                  column_names_equal(drop->name, field_name)) {
                being_dropped = true;
                break;
              }
            }
            if (!being_dropped) {
              col_type_name = ce->type_name;
              col_ext_name = ce->extension_name;
            }
          }
          if (col_type_name.empty()) {
            for (const Create_field &field : alter_info->create_list) {
              if (field.custom_type_context &&
                  column_names_equal(field.field_name, field_name)) {
                col_type_name = field.custom_type_context->type_name();
                col_ext_name = field.custom_type_context->extension_name();
                break;
              }
            }
          }
          if (col_type_name.empty() ||
              canonical_type_name(col_type_name) !=
                  canonical_type_name(prof_desc->type_name()) ||
              canonical_extension_name(col_ext_name) !=
                  canonical_extension_name(
                      prof_desc->type_ref().extension_name())) {
            villagesql_error(
                "Column '%s' is not of type '%s' (extension '%s') required"
                " by index profile '%s'",
                MYF(0), field_name, prof_desc->type_name().c_str(),
                prof_desc->type_ref().extension_name().c_str(),
                profile_name.c_str());
            return true;
          }
          // TODO(villagesql-indexing): Validate that the requested sort
          // direction (kp->is_ascending()) is supported by
          // prof_desc->ordering(). Return an error if the profile's ordering
          // bitmask does not include the requested direction (e.g., an
          // unordered profile with ordering == VEF_INDEX_ORDERING_NONE used
          // with ASC/DESC).
          prof_ext_name = prof_desc->extension_name();
          prof_ext_version = prof_desc->extension_version();

        } else {
          if (find_default_profile(vclient, alter_info, db, table_name,
                                   kp->get_field_name(), type_name, ext_name,
                                   profile_name, prof_ext_name,
                                   prof_ext_version))
            return true;
        }

        to_add_index_columns_.emplace_back(IndexColumnKey(index_id, key_pos),
                                           std::string(kp->get_field_name()),
                                           prof_ext_name, prof_ext_version,
                                           profile_name);
        ++key_pos;
      }
    }
  }
  return false;
}

bool Metadata_modifier::remove_columns(THD *thd [[maybe_unused]],
                                       Table_name db_table) {
  if (should_assert_if_null(db_table.first) ||
      should_assert_if_null(db_table.second)) {
    villagesql_error(error_uninitialized_name, MYF(0));
    return true;
  }

  const char *db_name = db_table.first;
  const char *table_name = db_table.second;

  // Skip special databases
  if (is_system_schema(db_name)) {
    return false;
  }

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    return false;
  }
  // We must already have locked the table to make sure that that the
  // table columns are not altered.
  assert(thd->mdl_context.owns_equal_or_stronger_lock(MDL_key::TABLE, db_name,
                                                      table_name, MDL_SHARED));

  // Get all custom columns for this table.
  std::vector<const ColumnEntry *> custom_columns;
  {
    auto guard = vclient.get_read_lock();
    custom_columns = vclient.GetCustomColumnsForTable(db_name, table_name);
  }

  if (custom_columns.empty()) {
    return false;  // No custom columns to delete.
  }

  for (const ColumnEntry *entry : custom_columns) {
    if (!entry) continue;

    to_remove_.emplace_back(entry->key());
  }

  return false;
}

bool Metadata_modifier::rename_columns_table(THD *thd [[maybe_unused]],
                                             Table_name old_name,
                                             Table_name new_name) {
  if (should_assert_if_null(old_name.first) ||
      should_assert_if_null(old_name.second) ||
      should_assert_if_null(new_name.first) ||
      should_assert_if_null(new_name.second)) {
    villagesql_error(error_uninitialized_name, MYF(0));
    return true;
  }

  const char *old_db = old_name.first;
  const char *old_table = old_name.second;
  const char *new_db = new_name.first;
  const char *new_table = new_name.second;

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    return false;
  }
  // We must already have locked the table to make sure that that the
  // table columns are not altered.
  assert(thd->mdl_context.owns_equal_or_stronger_lock(MDL_key::TABLE, old_db,
                                                      old_table, MDL_SHARED));
  assert(thd->mdl_context.owns_equal_or_stronger_lock(MDL_key::TABLE, new_db,
                                                      new_table, MDL_SHARED));

  auto guard = vclient.get_read_lock();
  auto custom_columns = vclient.GetCustomColumnsForTable(old_db, old_table);

  if (custom_columns.empty()) {
    return false;  // No custom columns to rename.
  }

  for (const ColumnEntry *old_col : custom_columns) {
    if (!old_col) continue;

    ColumnEntry new_entry(ColumnKey(new_db, new_table, old_col->column_name()),
                          old_col->extension_name, old_col->extension_version,
                          old_col->type_name, old_col->type_parameters);

    to_rename_.emplace_back(new_entry, old_col->key());
  }

  return false;
}

bool Metadata_modifier::alter_columns(THD *thd [[maybe_unused]],
                                      Table_ref *table_ref,
                                      const Alter_info *alter_info) {
  if (!alter_info) {
    return false;  // No alter info, nothing to do
  }

  TABLE *table = table_ref->table;
  Table_name db_table = {table_ref->db, table_ref->table_name};
  if (should_assert_if_null(db_table.first) ||
      should_assert_if_null(db_table.second)) {
    villagesql_error(error_uninitialized_name, MYF(0));
    return true;
  }
  const char *db_name = db_table.first;
  const char *table_name = db_table.second;

  // Skip special databases
  if (is_system_schema(db_name)) {
    return false;
  }

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    return false;
  }

  // 0. Handle ALTER TABLE RENAME - update all custom columns with new table
  // name
  // This must be done before acquiring the read lock since
  // rename_columns_table() acquires its own lock
  if ((alter_info->flags & Alter_info::ALTER_RENAME) &&
      alter_info->new_table_name.str) {
    const char *new_db =
        alter_info->new_db_name.str ? alter_info->new_db_name.str : db_name;
    const char *new_table = alter_info->new_table_name.str;

    Table_name new_name = {new_db, new_table};

    if (rename_columns_table(thd, db_table, new_name)) {
      return true;
    }
  }

  // We must already have locked the table to make sure that that the
  // table columns are not altered.
  assert(thd->mdl_context.owns_equal_or_stronger_lock(MDL_key::TABLE, db_name,
                                                      table_name, MDL_SHARED));
  auto guard = vclient.get_read_lock();

  // Get all current custom columns for this table
  auto custom_columns = vclient.GetCustomColumnsForTable(db_name, table_name);

  // Build a set of custom column names for fast lookup
  std::unordered_set<std::string> custom_column_names;
  for (const ColumnEntry *col : custom_columns) {
    custom_column_names.insert(canonical_column_name(col->column_name()));
  }
  existing_custom_count_ = custom_column_names.size();

  // Deletions must carry the stored entry's key: the disk probe uses its
  // as-entered components, and the statement may spell the name with
  // different case.
  auto find_custom_column = [&custom_columns](const char *name) {
    for (const ColumnEntry *col : custom_columns) {
      if (column_names_equal(col->column_name(), name)) return col;
    }
    return static_cast<const ColumnEntry *>(nullptr);
  };

  // 1. Handle DROP COLUMN - delete from custom_columns if custom type
  for (const Alter_drop *drop : alter_info->drop_list) {
    if (drop->type == Alter_drop::COLUMN) {
      const ColumnEntry *entry = find_custom_column(drop->name);
      if (entry) {
        to_remove_.emplace_back(entry->key());
      }
    }
  }

  // 2. Handle RENAME COLUMN - update custom_columns entry
  for (const Alter_column *alter : alter_info->alter_list) {
    if (alter->change_type() == Alter_column::Type::RENAME_COLUMN) {
      if (custom_column_names.count(canonical_column_name(alter->name))) {
        // Find the existing entry to get full metadata
        const ColumnEntry *old_entry_ptr = nullptr;
        for (const ColumnEntry *col : custom_columns) {
          if (canonical_column_name(col->column_name()) ==
              canonical_column_name(alter->name)) {
            old_entry_ptr = col;
            break;
          }
        }

        if (old_entry_ptr) {
          // Create new entry with renamed column
          ColumnEntry new_entry(
              ColumnKey(db_name, table_name, alter->m_new_name),
              old_entry_ptr->extension_name, old_entry_ptr->extension_version,
              old_entry_ptr->type_name, old_entry_ptr->type_parameters);

          to_rename_.emplace_back(new_entry, old_entry_ptr->key());
        }
      }
    }
  }

  // 3. Validate type conversion compatibility for MODIFY/CHANGE COLUMN.
  if (table) {
    for (const Create_field &field : alter_info->create_list) {
      if (!field.change) continue;
      bool is_custom_type = (field.custom_type_context != nullptr);
      bool was_custom_type =
          custom_column_names.count(canonical_column_name(field.change));

      if (!was_custom_type && is_custom_type) {
        // Non-custom → custom: not allowed. Use explicit conversion functions.
        villagesql_error("Cannot convert column '%s' to custom type '%s'",
                         MYF(0), field.change,
                         field.custom_type_context->type_name().c_str());
        return true;
      } else if (was_custom_type && !is_custom_type) {
        // Custom → non-custom: only string destination types allowed.
        if (!is_string_type(field.sql_type)) {
          villagesql_error(
              "Cannot convert custom type column '%s' to non-string type",
              MYF(0), field.change);
          return true;
        }
      } else if (was_custom_type && is_custom_type) {
        // Custom → custom: types must be compatible.
        Field *old_field = find_field_in_table_sef(table, field.change);
        assert(old_field);
        if (!old_field->get_type_context()->is_compatible_with(
                *field.custom_type_context)) {
          villagesql_error(
              "Cannot convert between incompatible custom types '%s' and '%s'",
              MYF(0), old_field->get_type_context()->qualified_name().c_str(),
              field.custom_type_context->qualified_name().c_str());
          return true;
        }
      }
    }
  }

  // 4. Handle ADD/MODIFY/CHANGE COLUMN from create_list
  for (const Create_field &field : alter_info->create_list) {
    bool is_custom_type = (field.custom_type_context != nullptr);
    bool was_custom_type =
        field.change &&
        custom_column_names.count(canonical_column_name(field.change));

    if (field.change) {
      // This is MODIFY COLUMN or CHANGE COLUMN
      if (was_custom_type && !is_custom_type) {
        // Changing FROM custom TO non-custom - delete entry
        to_remove_.emplace_back(find_custom_column(field.change)->key());
      } else if (!was_custom_type && is_custom_type) {
        // Changing FROM non-custom TO custom - insert entry
        to_add_.emplace_back(ColumnKey(db_name, table_name, field.field_name),
                             field.custom_type_context->extension_name(),
                             field.custom_type_context->extension_version(),
                             field.custom_type_context->type_name(),
                             field.custom_type_context->parameters().to_json());
      } else if (was_custom_type && is_custom_type) {
        // Changing FROM custom TO custom - use delete-then-insert pattern
        // Note: Apply removals before additions
        // See Metadata_modifier::mark_victionary_modifications()
        to_remove_.emplace_back(find_custom_column(field.change)->key());

        to_add_.emplace_back(ColumnKey(db_name, table_name, field.field_name),
                             field.custom_type_context->extension_name(),
                             field.custom_type_context->extension_version(),
                             field.custom_type_context->type_name(),
                             field.custom_type_context->parameters().to_json());
      }
    } else if (is_custom_type) {
      // This is ADD COLUMN with custom type - insert entry
      to_add_.emplace_back(ColumnKey(db_name, table_name, field.field_name),
                           field.custom_type_context->extension_name(),
                           field.custom_type_context->extension_version(),
                           field.custom_type_context->type_name(),
                           field.custom_type_context->parameters().to_json());
    }
  }
  return false;
}

bool Metadata_modifier::remove_indexes(THD *thd [[maybe_unused]],
                                       const char *db, const char *table_name,
                                       const Alter_info *alter_info) {
  if (is_system_schema(db)) return false;

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return false;

  // Collect normalized names of keys being dropped. MySQL passes the
  // user-typed name verbatim in Alter_drop::name.
  std::unordered_set<std::string> dropping;
  for (const Alter_drop *drop : alter_info->drop_list) {
    if (drop->type == Alter_drop::KEY)
      dropping.insert(canonical_index_name(drop->name));
  }
  if (dropping.empty()) return false;

  auto guard = vclient.get_read_lock();
  for (const IndexEntry *entry :
       vclient.GetCustomIndexesForTable(db, table_name)) {
    if (!entry || !dropping.count(canonical_index_name(entry->index_name())))
      continue;

    // Queue child column rows for deletion before the parent index row.
    for (const IndexColumnEntry *col :
         vclient.GetColumnsForIndex(entry->index_id)) {
      if (!col) continue;
      assert(!col->profile_extension_name.empty());
      to_remove_index_columns_.emplace_back(
          col->key(), col->column_name, col->profile_extension_name,
          col->profile_extension_version, col->profile_name);
    }
    assert(!entry->extension_name.empty());
    to_remove_indexes_.emplace_back(
        entry->key(), entry->index_id, entry->extension_name,
        entry->extension_version, entry->index_type_name,
        entry->index_type_parameters);
  }
  return false;
}

bool Metadata_modifier::remove_all_indexes(THD *thd [[maybe_unused]],
                                           const char *db,
                                           const char *table_name) {
  if (is_system_schema(db)) return false;

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return false;

  auto guard = vclient.get_read_lock();
  for (const IndexEntry *entry :
       vclient.GetCustomIndexesForTable(db, table_name)) {
    if (!entry) continue;

    for (const IndexColumnEntry *col :
         vclient.GetColumnsForIndex(entry->index_id)) {
      if (!col) continue;
      assert(!col->profile_extension_name.empty());
      to_remove_index_columns_.emplace_back(
          col->key(), col->column_name, col->profile_extension_name,
          col->profile_extension_version, col->profile_name);
    }
    assert(!entry->extension_name.empty());
    to_remove_indexes_.emplace_back(
        entry->key(), entry->index_id, entry->extension_name,
        entry->extension_version, entry->index_type_name,
        entry->index_type_parameters);
  }
  return false;
}

bool Metadata_modifier::lock_extensions_shared(THD *thd) {
  if (should_assert_if_null(thd)) {
    villagesql_error(error_uninitialized_session, MYF(0));
    return true;
  }

  MDL_request_list mdl_requests;
  std::unordered_set<std::string> seen_extensions;

  // Lambda to add MDL request for an extension (if not already added)
  auto add_mdl_request = [&](const std::string &ext_name) -> bool {
    if (ext_name.empty() ||
        seen_extensions.count(canonical_extension_name(ext_name))) {
      return false;  // Already processed or empty
    }

    ExtensionKey ext_key(ext_name);
    const std::string &normalized_name = ext_key.str();

    MDL_request *new_request = new (thd->mem_root) MDL_request;
    if (should_assert_if_null(new_request)) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(MDL_request));
      return true;
    }

    MDL_REQUEST_INIT(new_request, MDL_key::EXTENSION, "",
                     normalized_name.c_str(), MDL_SHARED, MDL_STATEMENT);
    mdl_requests.push_front(new_request);
    seen_extensions.insert(canonical_extension_name(ext_name));
    return false;
  };

  for (const ColumnEntry &entry : to_add_) {
    if (add_mdl_request(entry.extension_name)) {
      return true;
    }
  }

  for (const ColumnEntry &entry : to_remove_) {
    if (add_mdl_request(entry.extension_name)) {
      return true;
    }
  }

  for (const auto &[new_entry, old_key] : to_rename_) {
    if (add_mdl_request(new_entry.extension_name)) {
      return true;
    }
  }

  for (const Croutine_entry &routine : to_call_) {
    if (add_mdl_request(routine.extension_name)) {
      return true;
    }
  }

  for (const IndexEntry &entry : to_add_indexes_) {
    assert(!entry.extension_name.empty());
    if (add_mdl_request(entry.extension_name)) {
      return true;
    }
  }

  for (const IndexColumnEntry &entry : to_add_index_columns_) {
    assert(!entry.profile_extension_name.empty());
    if (add_mdl_request(entry.profile_extension_name)) {
      return true;
    }
  }

  for (const IndexEntry &entry : to_remove_indexes_) {
    assert(!entry.extension_name.empty());
    if (add_mdl_request(entry.extension_name)) {
      return true;
    }
  }

  for (const IndexColumnEntry &entry : to_remove_index_columns_) {
    assert(!entry.profile_extension_name.empty());
    if (add_mdl_request(entry.profile_extension_name)) {
      return true;
    }
  }

  if (mdl_requests.is_empty()) {
    return false;
  }

  if (thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout)) {
    return true;
  }
  return false;
}

bool Metadata_modifier::validate_entries() {
  auto &vclient = VictionaryClient::instance();
  if (should_assert_if_false(vclient.is_initialized())) {
    villagesql_error(error_uninitialized_victionary, MYF(0));
    return true;
  }

  auto guard = vclient.get_read_lock();

  // Validate a single entry
  auto validate_entry = [&](const ColumnEntry &entry) -> bool {
    if (entry.extension_name.empty() || entry.type_name.empty()) {
      return false;  // Nothing to validate
    }

    // Construct TypeDescriptorKey and look up the type descriptor
    // Types are now registered directly from extensions as TypeDescriptors
    TypeDescriptorKey type_descriptor_key(entry.type_name, entry.extension_name,
                                          entry.extension_version);
    const TypeDescriptor *type_descriptor =
        vclient.type_descriptors().get_committed(type_descriptor_key);

    if (!type_descriptor) {
      // Type not found
      villagesql_error(
          "Custom type '%s' from extension '%s' version '%s' not found", MYF(0),
          entry.type_name.c_str(), entry.extension_name.c_str(),
          entry.extension_version.c_str());
      return true;
    }

    // Verify extension name and version match
    if (type_descriptor->extension_name() != entry.extension_name ||
        type_descriptor->extension_version() != entry.extension_version) {
      villagesql_error(
          "Extension Name: %s Version: %s is not found for custom type '%s'",
          MYF(0), entry.extension_name.c_str(), entry.extension_version.c_str(),
          entry.type_name.c_str());
      return true;
    }
    return false;
  };

  // Validate column entries to be added.
  for (const ColumnEntry &entry : to_add_) {
    if (validate_entry(entry)) {
      return true;
    }
  }

  // Validate column entries to be removed.
  for (const ColumnEntry &entry : to_remove_) {
    if (validate_entry(entry)) {
      return true;
    }
  }

  // Validate column entries to rename to.
  for (const auto &[new_entry, old_key] : to_rename_) {
    if (validate_entry(new_entry)) {
      return true;
    }
  }

  // Validate custom function extensions in to_call_.
  for (const Croutine_entry &routine : to_call_) {
    assert(!routine.extension_name.empty());

    // Check if extension exists in victionary
    ExtensionKey ext_key(routine.extension_name);
    const ExtensionEntry *ext_entry =
        vclient.extensions().get_committed(ext_key);

    if (!ext_entry) {
      villagesql_error("Extension '%s' not found for custom function '%s'",
                       MYF(0), routine.extension_name.c_str(),
                       routine.function_name.c_str());
      return true;
    }

    // Verify that the function exists in VictionaryClient. Use the _locked
    // variant: validate_entries already holds the read lock.
    if (!func_exists_locked(routine.extension_name, routine.function_name)) {
      villagesql_error("Custom function '%s' not found in extension '%s'",
                       MYF(0), routine.function_name.c_str(),
                       routine.extension_name.c_str());
      return true;
    }
  }

  for (const IndexEntry &entry : to_add_indexes_) {
    assert(!entry.extension_name.empty());
    IndexTypeDescriptorKeyPrefix pfx(entry.index_type_name,
                                     entry.extension_name);
    if (!vclient.index_type_descriptors().has_prefix_committed(pfx)) {
      villagesql_error("Custom index type '%s' from extension '%s' not found",
                       MYF(0), entry.index_type_name.c_str(),
                       entry.extension_name.c_str());
      return true;
    }
  }

  for (const IndexColumnEntry &entry : to_add_index_columns_) {
    assert(!entry.profile_name.empty());
    assert(!entry.profile_extension_name.empty());
    IndexProfileDescriptorKeyPrefix ppfx(entry.profile_name,
                                         entry.profile_extension_name);
    if (!vclient.index_profile_descriptors().has_prefix_committed(ppfx)) {
      villagesql_error("Index profile '%s' from extension '%s' not found",
                       MYF(0), entry.profile_name.c_str(),
                       entry.profile_extension_name.c_str());
      return true;
    }
  }

  return false;
}

bool Metadata_modifier::mark_victionary_modifications(THD *thd,
                                                      bool &marked_column,
                                                      bool &marked_index) {
  auto &vclient = VictionaryClient::instance();
  if (should_assert_if_false(vclient.is_initialized())) {
    villagesql_error(error_uninitialized_victionary, MYF(0));
    return true;
  }

  if (should_assert_if_null(thd)) {
    villagesql_error(error_uninitialized_session, MYF(0));
    return true;
  }

  marked_column = false;
  marked_index = false;
  auto guard = vclient.get_write_lock();

  // 1. Process removals.
  for (const ColumnEntry &entry : to_remove_) {
    if (vclient.columns().MarkForDeletion(*thd, entry.key())) {
      return true;  // Error marking for deletion
    }
    marked_column = true;
  }
  to_remove_.clear();

  // 2. Process renames (updates with old key).
  for (auto &[new_entry, old_key] : to_rename_) {
    if (vclient.columns().MarkForUpdate(*thd, std::move(new_entry),
                                        std::move(old_key))) {
      return true;  // Error marking for update
    }
    marked_column = true;
  }
  to_rename_.clear();

  // 3. Process additions.
  for (ColumnEntry &entry : to_add_) {
    if (vclient.columns().MarkForInsertion(*thd, std::move(entry))) {
      return true;  // Error marking for insertion
    }
    marked_column = true;
  }
  to_add_.clear();

  // 4. Process custom index column removals (child before parent).
  for (const IndexColumnEntry &entry : to_remove_index_columns_) {
    if (vclient.custom_index_columns().MarkForDeletion(*thd, entry.key())) {
      return true;
    }
    marked_index = true;
  }
  to_remove_index_columns_.clear();

  // 5. Process custom index removals.
  for (const IndexEntry &entry : to_remove_indexes_) {
    if (vclient.custom_indexes().MarkForDeletion(*thd, entry.key())) {
      return true;
    }
    marked_index = true;
  }
  to_remove_indexes_.clear();

  // 6. Process custom index additions. custom_indexes must be staged before
  // custom_index_columns so the parent row exists when child rows reference it.
  for (IndexEntry &entry : to_add_indexes_) {
    if (vclient.custom_indexes().MarkForInsertion(*thd, std::move(entry))) {
      return true;
    }
    marked_index = true;
  }
  to_add_indexes_.clear();

  // 7. Process custom index column additions.
  for (IndexColumnEntry &entry : to_add_index_columns_) {
    if (vclient.custom_index_columns().MarkForInsertion(*thd,
                                                        std::move(entry))) {
      return true;
    }
    marked_index = true;
  }
  to_add_index_columns_.clear();

  return false;
}

bool Metadata_modifier::add_system_tables(THD *thd, bool marked_column,
                                          bool marked_index) {
  if (should_assert_if_null(thd)) {
    villagesql_error(error_uninitialized_session, MYF(0));
    return true;
  }

  // Open only the system tables needed for the staged modifications. All
  // tables are added to the query list and opened in one call so they
  // participate in the same lock scope. If the calling DDL does not lock
  // query tables, these tables are locked explicitly before writing in
  // Metadata_modifier::store().
  auto add_table = [&](const char *table_name) {
    Table_ref *tref =
        new (thd->mem_root) Table_ref(SchemaManager::VILLAGESQL_SCHEMA_NAME,
                                      table_name, TL_WRITE, MDL_SHARED_WRITE);
    thd->lex->add_to_query_tables(tref);
    return tref;
  };

  Table_ref *first = nullptr;
  if (marked_column) {
    first = add_table(SchemaManager::COLUMNS_TABLE_NAME);
  }
  if (marked_index) {
    Table_ref *idx = add_table(SchemaManager::INDEXES_TABLE_NAME);
    if (!first) first = idx;
    (void)add_table(SchemaManager::INDEX_COLUMNS_TABLE_NAME);
  }

  if (!first) return false;

  DML_prelocking_strategy strategy;
  uint counter = 0;
  if (open_tables(thd, &first, &counter, MYF(0), &strategy)) {
    villagesql_error("Cannot open VillageSQL system tables", MYF(0));
    return true;
  }
  system_tables_opened_ = counter;
  return false;
}

bool Metadata_modifier::lock_and_apply(THD *thd) {
  // Early return if no modifications are pending.
  if (!has_entries()) {
    return false;
  }

  // DEBUG_SYNC point for testing MDL synchronization with UNINSTALL EXTENSION
  DEBUG_SYNC_C("metadata_before_extension_lock");

  // 1. Acquire MDL_SHARED locks on all referenced extensions.
  if (lock_extensions_shared(thd)) {
    return true;
  }

  // DEBUG_SYNC point after acquiring extension locks
  DEBUG_SYNC_C("metadata_after_extension_lock");

  // 2. Validate that all referenced types exist with matching extension info.
  if (validate_entries()) {
    return true;
  }

  // 3. Mark all modifications in Victionary Client.
  bool marked_column = false;
  bool marked_index = false;
  if (mark_victionary_modifications(thd, marked_column, marked_index)) {
    return true;
  }

  // 4. Add VillageSQL system tables to THD's query list.
  if (add_system_tables(thd, marked_column, marked_index)) {
    return true;
  }

  return false;
}

bool Metadata_modifier::process_create(THD *thd,
                                       const HA_CREATE_INFO *create_info,
                                       const Alter_info *alter_info,
                                       const char *db, const char *table_name) {
  // Tmp tables are handled separately.
  if (!create_info || !alter_info ||
      (create_info->options & HA_LEX_CREATE_TMP_TABLE)) {
    return false;
  }

  Metadata_modifier custom_modifier;
  Table_name db_table = {db, table_name};

  if (custom_modifier.add_columns(thd, db_table, alter_info->create_list)) {
    return true;
  }

  if (custom_modifier.add_indexes(thd, db, table_name, alter_info)) {
    return true;
  }

  // Check storage engine if custom columns or indexes were added.
  if (custom_modifier.has_entries() &&
      ensure_engine_is_innodb(create_info->db_type, "create table")) {
    return true;
  }

  if (custom_modifier.lock_and_apply(thd)) {
    return true;
  }

  return false;
}

bool Metadata_modifier::process_alter(THD *thd,
                                      const HA_CREATE_INFO *create_info,
                                      Table_ref *table_list,
                                      const Alter_info *alter_info,
                                      uint *tables_opened) {
  // Tmp tables are handles separately.
  if (!table_list || !table_list->table ||
      table_list->table->s->tmp_table != NO_TMP_TABLE) {
    return false;
  }

#ifndef NDEBUG
  const uint tables_before = count_global_tables(table_list);
#endif

  Metadata_modifier custom_modifier;

  if (custom_modifier.alter_columns(thd, table_list, alter_info)) {
    return true;
  }

  const char *db = table_list->db;
  const char *table_name = table_list->table_name;
  if (custom_modifier.add_indexes(thd, db, table_name, alter_info)) {
    return true;
  }

  if (custom_modifier.remove_indexes(thd, db, table_name, alter_info)) {
    return true;
  }

  // Reject any ALTER that combines custom column changes with an engine change.
  // Non-atomic DDL (when either engine is non-InnoDB) causes an internal commit
  // that clears uncommitted victionary entries before they can be written.
  // The user must perform the engine change and custom column changes
  // separately.
  // TODO(villagesql): Support combining engine change with custom column
  // add/drop in a single ALTER TABLE statement.
  const bool current_has_custom = custom_modifier.existing_custom_count_ > 0;
  const bool adding_custom = !custom_modifier.to_add_.empty();
  const bool removing_custom = !custom_modifier.to_remove_.empty();
  const bool adding_custom_index = !custom_modifier.to_add_indexes_.empty();
  const bool removing_custom_index =
      !custom_modifier.to_remove_indexes_.empty();
  const handlerton *current_engine = table_list->table->s->db_type();
  const handlerton *target_engine = create_info->db_type;
  const bool engine_changing =
      (target_engine->db_type != current_engine->db_type);
  if (engine_changing && (removing_custom || adding_custom ||
                          adding_custom_index || removing_custom_index)) {
    villagesql_error(
        "Cannot combine storage engine change with custom type columns.",
        MYF(0));
    return true;
  }
  // The target engine must be InnoDB when custom columns are involved.
  if ((current_has_custom || adding_custom) &&
      ensure_engine_is_innodb(target_engine, "alter table")) {
    return true;
  }

  if (custom_modifier.lock_and_apply(thd)) {
    return true;
  }

  // Keep the caller's tables_opened count in sync with the system tables
  // add_system_tables() appended to the query-tables list. The debug assertion
  // verifies this internal count matches the actual growth of the next_global
  // chain that lock_tables() will walk.
  if (tables_opened != nullptr)
    *tables_opened += custom_modifier.system_tables_opened_;
  assert(count_global_tables(table_list) - tables_before ==
         custom_modifier.system_tables_opened_);

  return false;
}

bool Metadata_modifier::process_rename(THD *thd, Table_ref *table_list) {
  if (!table_list) {
    return false;
  }

  Metadata_modifier custom_modifier;
  Table_ref *old_ref, *new_ref;

  for (old_ref = table_list; old_ref; old_ref = new_ref->next_local) {
    if (!(new_ref = old_ref->next_local)) {
      assert(false);  // Missing new table.
      break;
    }

    Table_name old_table = {old_ref->db, old_ref->table_name};
    Table_name new_table = {new_ref->db, new_ref->table_name};

    if (custom_modifier.rename_columns_table(thd, old_table, new_table)) {
      return true;
    }
  }

  if (custom_modifier.lock_and_apply(thd)) {
    return true;
  }

  return false;
}

bool Metadata_modifier::store(THD *thd) {
  // We may not have victionary initialized yet during bootstrap.
  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    return false;
  }
  if (should_assert_if_null(thd)) {
    villagesql_error(error_uninitialized_session, MYF(0));
    return true;
  }
  // Check if anything is marked for write across all VillageSQL system tables.
  bool has_columns = false;
  bool has_custom_indexes = false;
  {
    auto guard = vclient.get_read_lock();
    has_columns = vclient.columns().has_uncommitted(thd);
    has_custom_indexes = vclient.custom_indexes().has_uncommitted(thd);
    if (!has_columns && !has_custom_indexes) {
      assert(!vclient.custom_index_columns().has_uncommitted(thd));
      return false;
    }
  }
  if (!thd->lex->is_query_tables_locked()) {
    // The tables were already opened via add_system_table(). Find each one in
    // query_tables and lock them together.
    auto find_table = [&](const char *tname) -> Table_ref * {
      for (Table_ref *tl = thd->lex->query_tables; tl; tl = tl->next_global) {
        if (is_villagesql_system_table(tl->db, tl->table_name, tname))
          return tl;
      }
      return nullptr;
    };

    Table_ref *lock_first = nullptr;
    Table_ref **lock_next = &lock_first;
    uint lock_count = 0;

    auto chain = [&](const char *tname) -> bool {
      Table_ref *tl = find_table(tname);
      if (!tl || should_assert_if_null(tl->table)) {
        villagesql_error("VillageSQL system table '%s' not found or not opened",
                         MYF(0), tname);
        return true;
      }
      tl->next_local = nullptr;
      *lock_next = tl;
      lock_next = &tl->next_local;
      ++lock_count;
      return false;
    };

    if (has_columns && chain(SchemaManager::COLUMNS_TABLE_NAME)) return true;
    if (has_custom_indexes) {
      if (chain(SchemaManager::INDEXES_TABLE_NAME)) return true;
      if (chain(SchemaManager::INDEX_COLUMNS_TABLE_NAME)) return true;
    }

    if (lock_tables(thd, lock_first, lock_count, MYSQL_LOCK_IGNORE_TIMEOUT)) {
      villagesql_error("Cannot lock VillageSQL system tables", MYF(0));
      return true;
    }
  }
  if (vclient.write_all_uncommitted_entries(thd)) {
    villagesql_error("Cannot write to VillageSQL system tables", MYF(0));
    return true;
  }
  return false;
}

bool Metadata_modifier::commit(THD *thd) {
  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    return false;
  }
  vclient.commit_all_tables(thd);
  return false;
}

void Metadata_modifier::rollback(THD *thd) {
  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    return;
  }
  vclient.rollback_all_tables(thd);
}

bool Metadata_modifier::lock_extension_exclusive(
    THD *thd, const std::string &extension_name, enum_mdl_duration duration) {
  ExtensionKey ext_key(extension_name);
  const std::string &normalized_name = ext_key.str();

  MDL_request mdl_request;
  MDL_REQUEST_INIT(&mdl_request, MDL_key::EXTENSION, "",
                   normalized_name.c_str(), MDL_EXCLUSIVE, duration);

  if (thd->mdl_context.acquire_lock(&mdl_request,
                                    thd->variables.lock_wait_timeout)) {
    return true;
  }

  return false;
}

bool Metadata_modifier::add_functions(
    SQL_I_List<Croutine_hash_entry> &croutines_list) {
  // Iterate through the custom routines list and add to to_call_
  Croutine_hash_entry *routine = croutines_list.first;
  while (routine) {
    assert(routine->m_extension_name.length > 0);
    assert(routine->m_function_name.length > 0);

    to_call_.emplace_back(std::string(routine->m_extension_name.str,
                                      routine->m_extension_name.length),
                          std::string(routine->m_function_name.str,
                                      routine->m_function_name.length));
    routine = routine->next;
  }
  return false;
}

bool Metadata_modifier::process_calls(
    THD *thd, SQL_I_List<Croutine_hash_entry> &croutines_list) {
  Metadata_modifier modifier;
  if (modifier.add_functions(croutines_list) || modifier.lock_and_apply(thd)) {
    return true;
  }
  return false;
}

bool PersistCustomSpParams(THD *thd, sp_head *sp) {
  sp_pcontext *root_ctx = sp->get_root_parsing_context();
  if (!root_ctx) return false;

  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return false;

  List<Create_field> field_def_lst;
  root_ctx->retrieve_field_definitions(&field_def_lst);

  bool any_custom = false;
  {
    auto guard = vclient.get_write_lock();

    // Persist custom-typed params and DECLARE vars.
    List_iterator_fast<Create_field> it(field_def_lst);
    Create_field *cdef;
    while ((cdef = it++)) {
      const TypeContext *tc = cdef->custom_type_context;
      if (!tc) continue;
      SpParamEntry entry(
          SpParamKey(sp->m_db.str, sp->m_name.str, cdef->field_name),
          tc->extension_name(), tc->extension_version(), tc->type_name(),
          tc->parameters().to_json());
      if (vclient.sp_params().MarkForInsertion(*thd, std::move(entry)))
        return true;
      any_custom = true;
    }

    // Persist the function return type using the sentinel name.
    if (sp->m_type == enum_sp_type::FUNCTION && sp->m_return_type_context_ref) {
      const TypeContext &tc = *sp->m_return_type_context_ref;
      SpParamEntry entry(
          SpParamKey(sp->m_db.str, sp->m_name.str, SP_RETURNS_SENTINEL),
          tc.extension_name(), tc.extension_version(), tc.type_name(),
          tc.parameters().to_json());
      if (vclient.sp_params().MarkForInsertion(*thd, std::move(entry)))
        return true;
      any_custom = true;
    }

    // Verify that only sp_params has uncommitted entries for this THD. This
    // matters because write_all_uncommitted_entries() only opens
    // custom_sp_params — if another table had pending entries it would fail.
    assert(!vclient.columns().has_uncommitted(thd));
    assert(!vclient.properties().has_uncommitted(thd));
    assert(!vclient.extensions().has_uncommitted(thd));
  }
  if (!any_custom) return false;

  // Only open custom_sp_params. write_all_uncommitted_entries() checks each
  // table's uncommitted map for this THD before touching any open TABLE*, so
  // other system tables (columns, properties, extensions) are skipped safely
  // because no entries were marked for them on this code path.
  // TODO(villagesql-general) Evaluate change the API in the future to allow
  // committing only from a subset of tables and asserting the others have
  // nothing pending.
  Table_ref sp_params_table(SchemaManager::VILLAGESQL_SCHEMA_NAME,
                            SchemaManager::SP_PARAMS_TABLE_NAME, TL_WRITE,
                            MDL_SHARED_WRITE);
  if (open_and_lock_tables(thd, &sp_params_table, MYSQL_LOCK_IGNORE_TIMEOUT))
    return true;
  return vclient.write_all_uncommitted_entries(thd);
}

bool DeleteCustomSpParams(THD *thd, const sp_name *name) {
  auto &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return false;

  SpParamKeyPrefix prefix(name->m_db.str, name->m_name.str);
  std::vector<SpParamKey> keys_to_delete;
  {
    auto guard = vclient.get_read_lock();
    auto entries = vclient.sp_params().get_prefix_committed(prefix);
    for (const SpParamEntry *entry : entries) {
      if (entry) keys_to_delete.push_back(entry->key());
    }
  }
  if (keys_to_delete.empty()) return false;

  {
    auto guard = vclient.get_write_lock();
    for (const SpParamKey &key : keys_to_delete) {
      if (vclient.sp_params().MarkForDeletion(*thd, key)) return true;
    }
  }

  Table_ref sp_params_table(SchemaManager::VILLAGESQL_SCHEMA_NAME,
                            SchemaManager::SP_PARAMS_TABLE_NAME, TL_WRITE,
                            MDL_SHARED_WRITE);
  if (open_and_lock_tables(thd, &sp_params_table, MYSQL_LOCK_IGNORE_TIMEOUT))
    return true;
  return vclient.write_all_uncommitted_entries(thd);
}

}  // namespace villagesql
