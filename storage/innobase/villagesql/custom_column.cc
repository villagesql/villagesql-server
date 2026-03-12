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
#include "custom_column.h"

#include "sql/current_thd.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/types/table.h"
#include "sql/field.h"
#include "sql/table.h"
#include "storage/innobase/include/dict0dd.h"
#include "storage/innobase/include/dict0dict.h"
#include "storage/innobase/include/dict0mem.h"
#include "storage/innobase/include/ha_prototypes.h"
#include "storage/innobase/include/mem0mem.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/types/util.h"

namespace villagesql {
namespace innodb {

Custom_column::Info Custom_column::get_from_field(const dict_field_t *field) {
  bool ascending = true;
  Custom_column *custom_column = nullptr;

  if (field) {
    ascending = field->is_ascending;
    custom_column = field->col ? field->col->custom_column : nullptr;
  }
  return {custom_column, ascending};
}

Custom_column::Info Custom_column::get_from_position(const dict_index_t *index,
                                                     size_t position) {
  if (dict_index_is_ibuf(index)) return {nullptr, true};
  return get_from_field(index->get_field(position));
}

int Custom_column::compare(const unsigned char *data1, size_t len1,
                           const unsigned char *data2, size_t len2) const {
  return type_context_->compare_op().invoke(data1, len1, data2, len2);
}

void Custom_column::load(dict_table_t *table, dict_col_t *col,
                         const Field *sql_field, const dd::Column *) {
  ut_ad(!col->custom_column);

  if (!sql_field->has_type_context()) return;

  auto tc = AcquireTypeContextClientManaged(sql_field->get_type_context());
  if (!tc) return;

  void *mem = mem_heap_alloc(table->heap, sizeof(Custom_column));
  col->custom_column = new (mem) Custom_column(std::move(tc));
}

void Custom_column::free_all(dict_table_t *table) {
  for (ulint i = 0; i < table->n_def; i++) {
    dict_col_t *col = &table->cols[i];
    if (col->custom_column) {
      col->custom_column->~Custom_column();
      col->custom_column = nullptr;
    }
  }
}

void Custom_column::load_all(dict_table_t *table) {
  auto log_warning = [table](const char *mesg) {
    LogVSQL(WARNING_LEVEL,
            "Cannot load custom columns for table '%s': %s during recovery",
            table->name.m_name, mesg);
  };

  // Extension system must be initialized to load custom columns.
  if (!VictionaryClient::instance().is_initialized()) {
    log_warning("extension system not initialized");
    ut_ad(false);
    return;
  }

  THD *thd = current_thd;
  if (!thd) {
    log_warning("THD unavailable");
    ut_ad(false);
    return;
  }
  dd::cache::Dictionary_client *dc = dd::get_dd_client(thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(dc);

  // Get schema and table name from table.
  dd::String_type schema;
  dd::String_type tablename;
  if (dc->get_table_name_by_se_private_id(handler_name, table->id, &schema,
                                          &tablename)) {
    log_warning("failed to get table name by ID");
    ut_ad(false);
    return;
  }

  // Check if partitioned table.
  if (schema.empty()) {
    if (dc->get_table_name_by_partition_se_private_id(handler_name, table->id,
                                                      &schema, &tablename) ||
        schema.empty()) {
      log_warning("failed to get partition table name");
      ut_ad(false);
      return;
    }
  }

  // Acquire the DD table object.
  const dd::Table *dd_table;
  if (dc->acquire(schema, tablename, &dd_table) || dd_table == nullptr) {
    log_warning("failed to acquire DD table");
    ut_ad(false);
    return;
  }

  // Build TABLE_SHARE object.
  TABLE_SHARE ts;
  TABLE td;
  int error = acquire_uncached_table(thd, dc, dd_table, nullptr, &ts, &td);
  if (error != 0) {
    log_warning("failed to build TABLE_SHARE");
    ut_ad(false);
    return;
  }

  // Check and load all custom column descriptors.
  uint col_index = 0;
  for (uint i = 0; i < ts.fields; i++) {
    Field *field = td.field[i];

    // Skip virtual columns.
    if (field->is_virtual_gcol()) {
      ut_ad(table->n_v_def > 0);
      continue;
    }
    ut_ad(dict_table_mysql_pos_to_innodb(table, field->field_index()) ==
          col_index);
    ut_a(col_index < table->get_n_user_cols());

    dict_col_t *col = &table->cols[col_index];
    const dd::Column *dd_col = dd_find_column(dd_table, field->field_name);

    // Load custom column information if this field has a custom type.
    Custom_column::load(table, col, field, dd_col);
    ++col_index;
  }

  // Clean up the TABLE_SHARE and TABLE.
  release_uncached_table(&ts, &td);
}

}  // namespace innodb
}  // namespace villagesql
