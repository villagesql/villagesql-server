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

#ifndef STORAGE_INNOBASE_VILLAGESQL_CUSTOM_COLUMN_H_
#define STORAGE_INNOBASE_VILLAGESQL_CUSTOM_COLUMN_H_

#include <memory>
#include <utility>
#include "villagesql/types/type_op.h"

// Forward declarations
struct dict_table_t;
struct dict_col_t;

struct dict_index_t;
struct dict_field_t;

class Field;

namespace dd {
class Column;
}  // namespace dd

namespace villagesql {

class TypeContext;

namespace innodb {

class Custom_column {
 public:
  using Info = std::pair<Custom_column *, bool>;

  Custom_column(CompareOp compare_op,
                std::shared_ptr<const TypeContext> type_context)
      : compare_op_(std::move(compare_op)),
        type_context_(std::move(type_context)) {}

  // Compare two values using the registered compare implementation.
  int compare(const unsigned char *data1, size_t len1,
              const unsigned char *data2, size_t len2) const;

  // Get custom column descriptor and ascending flag from index position.
  static Info get_from_position(const dict_index_t *index, size_t position);

  // Get custom column descriptor and ascending flag from index field.
  static Info get_from_field(const dict_field_t *field);

  // Get the TypeContext for this custom column.
  const TypeContext *type_context() const { return type_context_.get(); }

  // Load innodb column's(dict_col_t) custom column descriptor.
  static void load(dict_table_t *table, dict_col_t *col, const Field *sql_field,
                   const dd::Column *dd_col);

  // Free all custom column descriptors for a table.
  // Must be called before the table's heap is freed (from
  // dict_mem_table_free).
  static void free_all(dict_table_t *table);

  // load_all() is called during crash recovery when InnoDB
  // resurrects tables from the data dictionary cache. Specifically:
  //
  // 1. During recovery, InnoDB loads table definitions from disk into the dict
  //    cache (dictionary cache) to prepare for undo log application.
  // 2. At this early stage, the VillageSQL extension system has not yet been
  //    initialized because extension loading happens later in the server
  //    startup sequence.
  // 3. Tables with the DICT_TF2_RESURRECT_PREPARED flag are resurrected in the
  //    dict cache but lack custom column metadata.
  // 4. After the extension system initializes, load_all() is called from
  //    innobase_dict_cache_reset_tables_and_tablespaces() to populate custom
  //    column comparison functions for these resurrected tables. This ensures
  //    resurrected tables are fully initialized before normal use.
  static void load_all(dict_table_t *table);

 private:
  CompareOp compare_op_;
  std::shared_ptr<const TypeContext> type_context_;
};
}  // namespace innodb
}  // namespace villagesql

#endif  // STORAGE_INNOBASE_VILLAGESQL_CUSTOM_COLUMN_H_
