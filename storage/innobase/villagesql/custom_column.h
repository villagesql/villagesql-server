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

#include "db0err.h"
#include "mem0mem.h"
#include "trx0types.h"

#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"
#include "villagesql/types/storage.h"

// Forward declarations
struct btr_pcur_t;
struct dict_table_t;
struct dict_col_t;
struct dict_index_t;
struct dict_field_t;
struct dfield_t;
struct dtuple_t;
struct upd_t;
struct TABLE;

class Flush_observer;

class Alter_inplace_info;
class Field;

namespace dd {
class Column;
}  // namespace dd

namespace villagesql {

class TypeContext;

namespace innodb {

// Arena allocator context and callback provided to extensions during
// storage create and load operations.
struct Arena {
  using Type = vef_storage_arena_t;
  using Func = vef_storage_arena_func_t;

  static constexpr uint32_t MIN_ALIGNMENT = VEF_STORAGE_MIN_ALLOCATOR_ALIGNMENT;
};

class Custom_column {
 public:
  using Ref = vef_storage_col_ref_t;
  using Data = vef_storage_col_data_t;
  using TrxRef = vef_storage_trx_ref_t;
  using StorageRef = vef_storage_ref_t;
  using StorageCtx = vef_storage_ctx_t;
  using StorageIntf = villagesql::StorageInterface;
  using Info = std::pair<Custom_column *, bool>;

  static constexpr Ref EMPTY_REF = VEF_STORAGE_EMPTY_COLUMN_REF;
  static constexpr uint32_t ERROR_MSG_SIZE = 512;

  explicit Custom_column(std::shared_ptr<const TypeContext> type_context)
      : type_context_(std::move(type_context)) {}

  // Returns the storage interface function table, or nullopt if the column
  // storage is not managed by an extension.
  const std::optional<StorageIntf> &storage_interface() const;

  // Returns the storage context, or nullptr if not yet initialized.
  StorageCtx *storage_ctx() const { return storage_ctx_; }

  // Sets the storage context.
  void set_storage_ctx(StorageCtx *ctx) { storage_ctx_ = ctx; }

  // Returns true if the column storage is managed by an extension.
  bool stored_by_extension() const { return storage_interface().has_value(); }

  // Returns true if ha_alter_info adds or drops a column with extended storage.
  static bool alter_add_drop_with_extended_storage(
      const Alter_inplace_info *ha_alter_info, const TABLE *table);

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

  // Creates the extended column storage for a table.
  // @param table The table for which to create storage.
  // @param trx_id The transaction ID creating the storage.
  // @return DB_SUCCESS or an error code.
  static dberr_t create(const dict_table_t *table, trx_id_t trx_id);

  // Drops the extended column storage for a table.
  // @param table The table from which to drop storage.
  // @param trx_id The transaction ID dropping the storage.
  // @return DB_SUCCESS or an error code.
  static dberr_t drop(const dict_table_t *table, trx_id_t trx_id);

  // Persists the extended column storage reference to the data dictionary.
  // Typically called during DDL operations to persist storage information for
  // columns stored by extensions.
  // @param col The column whose storage reference to save.
  // @param dd_col The data dictionary column object to update.
  static void save_ref(dict_col_t *col, dd::Column *dd_col);

  // Inserts extended column data for a newly inserted row.
  // @param table The table being inserted into.
  // @param trx_id The transaction ID of the insert.
  // @param index_entry The clustered index entry of the new row.
  // @param offsets Offsets for the index entry.
  // @param heap Memory heap.
  // @return DB_SUCCESS or an error code.
  static dberr_t insert(dict_table_t *table, trx_id_t trx_id,
                        const dtuple_t *index_entry, ulint *offsets,
                        mem_heap_t **heap);

  // Inserts extended column data into the column store before bulk-load record
  // conversion. Updates each extended field in the tuple with the REF in-place.
  // Writes column-store pages with MTR_LOG_NO_REDO; durability comes from the
  // bulk-load flush observer (force-flushed before commit) plus redo-logged
  // page allocation, mirroring Page_load::init in btr0load.cc.
  // @param observer Bulk-load flush observer; must be non-null.
  // @return DB_SUCCESS or an error code.
  static dberr_t insert_direct(dict_table_t *table, trx_id_t trx_id,
                               dtuple_t *tuple, Flush_observer *observer);

  // Updates extended column data for an updated row.
  // Marks the old data as deleted and inserts the new data.
  // @param table The table being updated.
  // @param trx_id The transaction ID of the update.
  // @param pcur Cursor pointing to the clustered index record.
  // @param upd The update vector.
  // @param offsets Offsets for the record.
  // @param heap Memory heap.
  // @return DB_SUCCESS or an error code.
  static dberr_t update(const dict_table_t *table, trx_id_t trx_id,
                        btr_pcur_t *pcur, upd_t *upd, ulint *offsets,
                        mem_heap_t **heap);

  // Marks or unmarks extended column data as deleted.
  // Used during delete operations (mark) and rollbacks (unmark).
  // @param table The table.
  // @param trx_id The transaction ID.
  // @param upd The update vector (if only some columns are affected), or
  // nullptr.
  // @param row_entry The row entry containing the extended column references.
  // @param del_mark True to mark as deleted, false to unmark.
  // @return DB_SUCCESS or an error code.
  static dberr_t mark_delete(const dict_table_t *table, trx_id_t trx_id,
                             const upd_t *upd, const dtuple_t *row_entry,
                             bool del_mark);

  // Purges extended columns for a record specified by a cursor.
  // Used during purge when a full row_entry is not readily available.
  // @param table The table.
  // @param pcur Cursor pointing to the record.
  // @return DB_SUCCESS or an error code.
  static dberr_t purge_at_pcur(const dict_table_t *table, btr_pcur_t *pcur);

  // Rolls back inserted extended columns for a given row.
  // Used when rolling back an entire row insertion.
  // @param table The table.
  // @param row_entry The row being rolled back.
  // @param pcur Cursor pointing to the record.
  // @return DB_SUCCESS or an error code.
  static dberr_t rollback_inserted(const dict_table_t *table,
                                   const dtuple_t *row_entry, btr_pcur_t *pcur);

  // Rolls back updated extended columns for a given row.
  // Only the columns present in the upd_t struct are affected.
  // @param table The table.
  // @param type undo record type e.g. TRX_UNDO_UPD_EXIST_REC
  // @param upd The update vector containing the fields to roll back.
  // @param undo_trx_id transaction ID of the record after rollback
  // @param undo_row row after rollback
  // @param cur_row updated row before rollback.
  // @param pcur Cursor pointing to the record.
  // @return DB_SUCCESS or an error code.
  static dberr_t rollback_updated(const dict_table_t *table, ulint type,
                                  const upd_t *upd, trx_id_t undo_trx_id,
                                  const dtuple_t *undo_row,
                                  const dtuple_t *cur_row, btr_pcur_t *pcur);

  // Purges the old versions of updated extended columns.
  // Called by the purge system to physically remove data no longer visible to
  // any active transaction.
  // @param table The table.
  // @param trx_id The transaction ID that created the new version.
  // @param upd The update vector containing the old data references.
  // @return DB_SUCCESS or an error code.
  static dberr_t purge_updated(const dict_table_t *table, trx_id_t trx_id,
                               const upd_t *upd);

  // Fetches the data for a single extended column.
  // @param table The table.
  // @param col The column definition.
  // @param dest Destination buffer to store the fetched data.
  // @param dest_len Length of the destination buffer.
  // @param src Source buffer containing the extended column reference.
  // @param src_len Length of the source buffer.
  // @return DB_SUCCESS or an error code.
  static dberr_t fetch(const dict_table_t *table, const dict_col_t *col,
                       byte *dest, ulint dest_len, const byte *src,
                       ulint src_len);

  // Allocates memory and fetches the data for a single extended column.
  // @param table The table.
  // @param col The column definition.
  // @param[in,out] data On input, a pointer to the reference; on output, a
  // pointer to the fetched data.
  // @param[in,out] data_len On input, the length of the reference; on output,
  // the length of the fetched data.
  // @param heap Memory heap to allocate from.
  // @return DB_SUCCESS or an error code.
  static dberr_t allocate_fetch(const dict_table_t *table,
                                const dict_col_t *col, const byte *&data,
                                ulint &data_len, mem_heap_t *heap);

  // Materialize extended storage columns during DDL bulk operations.
  // @param new_index  Destination clustered index.
  // @param old_index  Source clustered index; nullptr if not applicable.
  // @param col_map    Mapping from old column numbers to new ones; nullptr if
  //                   the old and new tables are identical.
  // @param n_fields   Number of fields in the tuple.
  // @param fields     Array of dfield_t representing the tuple.
  // @param heap       Memory heap used to allocate fetched data.
  // @return DB_SUCCESS on success; otherwise, an error code.
  static dberr_t fetch_for_bulk_ddl(const dict_index_t *new_index,
                                    const dict_index_t *old_index,
                                    const ulint *col_map, size_t n_fields,
                                    dfield_t *fields, mem_heap_t *heap);

 private:
  std::shared_ptr<const TypeContext> type_context_;
  StorageCtx *storage_ctx_ = nullptr;
};

}  // namespace innodb
}  // namespace villagesql

#endif  // STORAGE_INNOBASE_VILLAGESQL_CUSTOM_COLUMN_H_
