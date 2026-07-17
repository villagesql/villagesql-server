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

#include <unordered_set>

#include "sql/create_field.h"
#include "sql/current_thd.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/table.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/table.h"
#include "storage/innobase/include/btr0pcur.h"
#include "storage/innobase/include/dict0dd.h"
#include "storage/innobase/include/dict0dict.h"
#include "storage/innobase/include/dict0mem.h"
#include "storage/innobase/include/ha_prototypes.h"
#include "storage/innobase/include/log0chkp.h"
#include "storage/innobase/include/mem0mem.h"
#include "storage/innobase/include/mtr0mtr.h"
#include "storage/innobase/include/rem0types.h"
#include "storage/innobase/include/row0log.h"
#include "storage/innobase/include/row0upd.h"
#include "storage/innobase/include/trx0rec.h"
#include "storage/innobase/include/univ.i"
#include "storage/innobase/rem/rec.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/types/util.h"

namespace villagesql {
namespace innodb {

const std::optional<Custom_column::StorageIntf> &
Custom_column::storage_interface() const {
  return type_context_->storage_intf();
}

bool Custom_column::alter_add_drop_with_extended_storage(
    const Alter_inplace_info *ha_alter_info, const TABLE *table) {
  auto has_extended = [](const TypeContext *tc) {
    return tc != nullptr && tc->storage_intf().has_value();
  };

  // Build set of old Field* objects being kept; also check any added column.
  std::unordered_set<const Field *> kept;
  for (const Create_field &f : ha_alter_info->alter_info->create_list) {
    if (f.field != nullptr)
      kept.insert(f.field);
    else if (has_extended(f.custom_type_context))
      return true;  // ADD of extended-storage column
  }

  // Any old field absent from kept is being dropped.
  for (uint i = 0; table->field[i]; i++) {
    const Field *f = table->field[i];
    if (!kept.count(f) && has_extended(f->get_type_context())) return true;
  }
  return false;
}

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
                         const Field *sql_field, const dd::Column *dd_col) {
  ut_ad(!col->custom_column);

  if (!sql_field->has_type_context()) return;

  auto tc = AcquireTypeContextClientManaged(sql_field->get_type_context());
  if (!tc) return;

  void *mem = mem_heap_alloc(table->heap, sizeof(Custom_column));
  col->custom_column = new (mem) Custom_column(std::move(tc));

  auto &custom_column = col->custom_column;

  if (!custom_column->stored_by_extension()) return;

  table->has_extended_storage = true;

  // Check DD for an existing storage reference.
  const char *store_key = dd_column_key_strings[DD_EXTENDED_STORAGE_REF];
  if (dd_col == nullptr || !dd_col->se_private_data().exists(store_key)) {
    return;
  }

  StorageRef storage_ref;
  dd_col->se_private_data().get(store_key, &storage_ref);

  auto arena_alloc = [](Arena::Type *ctx, uint32_t sz) -> void * {
    return mem_heap_zalloc(reinterpret_cast<mem_heap_t *>(ctx), sz);
  };
  const auto &intf = col->custom_column->storage_interface();
  char error_msg[Custom_column::ERROR_MSG_SIZE] = {};

  ut_ad(intf);
  bool err = !intf || intf->load(storage_ref,
                                 reinterpret_cast<Arena::Type *>(table->heap),
                                 arena_alloc, &custom_column->storage_ctx_,
                                 error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (err) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Error loading extended column store: " << error_msg;
    col->custom_column->set_storage_ctx(nullptr);
  } else {
    custom_column->storage_ctx_->ref = storage_ref;
  }
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

static void log_extended_error(const char *operation, const dict_table_t *table,
                               const dict_index_t *index, uint32_t field_num) {
  if (table == nullptr) {
    ut_a(index);
    dict_field_t *ind_field = index->get_field(field_num);
    dict_col_t *col = ind_field->col;

    table = index->table;
    field_num = col->ind;
  }

  std::string schema_name;
  std::string table_name;

  table->get_table_name(schema_name, table_name);

  ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
      << "InnoDB: Failed custom column store operation: " << operation
      << ", schema: " << schema_name << ", table: " << table_name
      << ", column: " << table->get_col_name(field_num);
}

void Custom_column::save_ref(dict_col_t *col, dd::Column *dd_col) {
  if (dd_col == nullptr || col == nullptr || !col->stored_by_extn()) {
    return;
  }
  const char *store_key = dd_column_key_strings[DD_EXTENDED_STORAGE_REF];

  auto &custom_column = col->custom_column;
  if (!custom_column->storage_ctx()) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: dd_write: Uninitialized custom column store";
    return;
  }
  dd_col->se_private_data().set(store_key, custom_column->storage_ctx_->ref);
}

static dberr_t create_column_storage(const dict_col_t *col, mem_heap_t *heap,
                                     space_id_t space, trx_id_t trx_id) {
  // Ensure VillageSQL allocator alignment matches InnoDB memory alignment
  static_assert(Arena::MIN_ALIGNMENT <= UNIV_MEM_ALIGNMENT,
                "Arena::MIN_ALIGNMENT exceeds UNIV_MEM_ALIGNMENT");

  if (!col->stored_by_extn()) {
    return DB_SUCCESS;
  }

  auto arena_alloc = [](Arena::Type *ctx, uint32_t sz) -> void * {
    return mem_heap_zalloc(reinterpret_cast<mem_heap_t *>(ctx), sz);
  };

  auto &custom_column = col->custom_column;
  const auto &intf = custom_column->storage_interface();

  Custom_column::StorageCtx *ctx = nullptr;
  char error_msg[Custom_column::ERROR_MSG_SIZE] = {};

  ut_ad(intf);
  bool failed =
      !intf || intf->create(space, trx_id, col->len,
                            reinterpret_cast<Arena::Type *>(heap), arena_alloc,
                            &ctx, error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Error creating custom column store: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }
  custom_column->set_storage_ctx(ctx);
  return DB_SUCCESS;
}

dberr_t Custom_column::create(const dict_table_t *table, trx_id_t trx_id) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }
  for (uint i = 0; i < table->n_def; i++) {
    const dict_col_t *col = table->get_col(i);
    dberr_t err = create_column_storage(col, table->heap, table->space, trx_id);

    if (err != DB_SUCCESS) {
      log_extended_error("Create", table, nullptr, i);
      return err;
    }
  }
  return DB_SUCCESS;
}

static dberr_t drop_column_storage(const dict_col_t *col, trx_id_t trx_id) {
  if (!col->stored_by_extn()) {
    return DB_SUCCESS;
  }

  auto &custom_column = col->custom_column;
  if (!custom_column->storage_ctx()) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Drop: Uninitialized custom column store";
    return DB_VILLAGESQL_ERROR;
  }
  const auto &intf = custom_column->storage_interface();
  char error_msg[Custom_column::ERROR_MSG_SIZE] = {};

  ut_ad(intf);
  bool failed = !intf || intf->drop(custom_column->storage_ctx(), trx_id,
                                    error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Error dropping custom column store: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }
  return DB_SUCCESS;
}

dberr_t Custom_column::drop(const dict_table_t *table, trx_id_t trx_id) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }
  for (uint i = 0; i < table->n_def; i++) {
    const dict_col_t *col = table->get_col(i);
    dberr_t err = drop_column_storage(col, trx_id);

    if (err != DB_SUCCESS) {
      // Log error and continue dropping other columns.
      log_extended_error("Drop", table, nullptr, i);
      continue;
    }
  }
  return DB_SUCCESS;
}

static dberr_t insert_in_column_store(const dict_col_t *col, mtr_t *mtr,
                                      trx_id_t trx_id, const dfield_t *field,
                                      const dfield_t *pk_field,
                                      Custom_column::Ref &ref) {
  ulint col_data_len = 0;
  Custom_column::Data col_data{};
  col_data.data = field->get_extended_data(col_data_len);
  col_data.length = static_cast<uint32_t>(col_data_len);

  Custom_column::Data pk_data{
      static_cast<const unsigned char *>(pk_field->data),
      static_cast<uint32_t>(pk_field->len)};

  if (!col_data.data || col_data.length == 0) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Insert: Invalid custom column data";
    return DB_VILLAGESQL_ERROR;
  }

  auto &custom_column = col->custom_column;
  if (!custom_column->storage_ctx()) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Insert: Uninitialized custom column store";
    return DB_VILLAGESQL_ERROR;
  }

  const auto &intf = custom_column->storage_interface();
  char error_msg[Custom_column::ERROR_MSG_SIZE] = {};

  ut_ad(intf);
  bool failed =
      !intf || intf->insert(custom_column->storage_ctx(), mtr, trx_id, col_data,
                            pk_data, &ref, error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Error inserting data into custom column store: "
        << error_msg;
    return DB_VILLAGESQL_ERROR;
  }
  return DB_SUCCESS;
}

static dberr_t insert_impl(const dict_index_t *index, uint32_t index_field_num,
                           dfield_t *data_field, trx_id_t trx_id,
                           btr_pcur_t *pcur, ulint *offsets,
                           mem_heap_t **heap) {
  if (!data_field->is_extended() || dfield_is_null(data_field)) {
    return DB_SUCCESS;
  }
  mtr_t mtr;

  log_free_check();
  mtr_start(&mtr);

  ut_a(pcur->restore_position(BTR_MODIFY_LEAF, &mtr, UT_LOCATION_HERE));
  ut_a(pcur->m_rel_pos == BTR_PCUR_ON);
  rec_t *rec = pcur->get_rec();

  // Reset debug entries in offset in case record position has changed.
  rec_offs_make_valid(rec, index, offsets);
  ut_ad(rec_offs_validate(rec, index, offsets));

  dict_field_t *ind_field = index->get_field(index_field_num);
  dict_col_t *col = ind_field->col;
  ut_a(col->stored_by_extn());

  // Extract the primary key from field 0 of the current record.
  ulint pk_len = 0;
  ulint pk_off = rec_get_nth_field_offs(index, offsets, 0, &pk_len);
  dfield_t pk_field;
  dfield_set_data(&pk_field, rec + pk_off, pk_len);

  // Insert into column storage and get reference value
  Custom_column::Ref ref_val = Custom_column::EMPTY_REF;
  auto error =
      insert_in_column_store(col, &mtr, trx_id, data_field, &pk_field, ref_val);
  if (error != DB_SUCCESS) {
    mtr_commit(&mtr);
    return error;
  }

  // Update B-Tree record reference
  ulint ref_len = 0;
  ulint ref_off =
      rec_get_nth_field_offs(index, offsets, index_field_num, &ref_len);
  ut_a(ref_len == data_field->extended_ref_size());
  mlog_write_ull(rec + ref_off, ref_val, &mtr);

  // Update tuple reference for row logging
  data_field->set_extended_ref(ref_val);

  // We insert each extended column in a separate mtr. It is safe as the
  // column reference is also persisted in the inserted record.
  mtr_commit(&mtr);

  return DB_SUCCESS;
}

dberr_t Custom_column::insert(dict_table_t *table, trx_id_t trx_id,
                              const dtuple_t *index_entry, ulint *offsets,
                              mem_heap_t **heap) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }

  dict_index_t *index = table->first_index();
  btr_pcur_t pcur;

  mtr_t mtr;
  mtr_start(&mtr);
  // Position the cursor to B-tree record inserted.
  pcur.open(index, 0, index_entry, PAGE_CUR_LE, BTR_SEARCH_LEAF, &mtr,
            UT_LOCATION_HERE);

  pcur.store_position(&mtr);
  mtr_commit(&mtr);

  ut_a(index->is_clustered());

  for (uint32_t i = 0; i < index_entry->n_fields; i++) {
    dfield_t *field = dtuple_get_nth_field(index_entry, i);

    // Insert into column storage and get reference value
    dberr_t error = insert_impl(index, i, field, trx_id, &pcur, offsets, heap);
    if (error != DB_SUCCESS) {
      log_extended_error("Insert", nullptr, index, i);
      return error;
    }
  }

  mtr_start(&mtr);
  ut_a(pcur.restore_position(BTR_SEARCH_LEAF, &mtr, UT_LOCATION_HERE));

  if (dict_index_is_online_ddl(index)) {
    row_log_table_insert(pcur.get_rec(), index_entry, index, offsets);
  }
  mtr_commit(&mtr);

  return DB_SUCCESS;
}

dberr_t Custom_column::insert_direct(dict_table_t *table, trx_id_t trx_id,
                                     dtuple_t *tuple,
                                     Flush_observer *observer) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }

  // Durability of these no-redo pages depends on the observer flush; required.
  ut_a(observer != nullptr);

  dict_index_t *index = table->first_index();
  ut_a(index->is_clustered());

  const dfield_t *pk_field = dtuple_get_nth_field(tuple, 0);

  for (uint32_t i = 0; i < tuple->n_fields; i++) {
    dfield_t *field = dtuple_get_nth_field(tuple, i);
    if (!field->is_extended() || dfield_is_null(field)) {
      continue;
    }

    dict_col_t *col = index->get_field(i)->col;
    ut_a(col->stored_by_extn());

    // No-redo content writes, force-flushed by the observer before commit
    // (allocation is redo-logged in the storage ABI). See Page_load::init.
    mtr_t mtr;
    mtr_start(&mtr);
    mtr.set_log_mode(MTR_LOG_NO_REDO);
    mtr.set_flush_observer(observer);

    Custom_column::Ref ref_val = Custom_column::EMPTY_REF;
    auto err =
        insert_in_column_store(col, &mtr, trx_id, field, pk_field, ref_val);
    mtr_commit(&mtr);

    if (err != DB_SUCCESS) {
      log_extended_error("Insert", nullptr, index, i);
      return err;
    }
    field->set_extended_ref(ref_val);
  }
  return DB_SUCCESS;
}

static dberr_t mark_in_column_store(dict_col_t *col, mtr_t *mtr,
                                    trx_id_t trx_id, Custom_column::Ref ref,
                                    bool del_mark) {
  auto &custom_column = col->custom_column;
  if (!custom_column->storage_ctx()) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: " << (del_mark ? "Delete" : "Un-Delete")
        << ": Uninitialized custom column store.";
    return DB_VILLAGESQL_ERROR;
  }
  const auto &intf = custom_column->storage_interface();
  char error_msg[Custom_column::ERROR_MSG_SIZE] = {};

  ut_ad(intf);
  bool failed =
      !intf || intf->mark_delete(custom_column->storage_ctx(), mtr, trx_id, ref,
                                 del_mark, error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: " << (del_mark ? "Delete" : "Un-Delete")
        << ": Error marking data in custom column store: " << error_msg;
    return DB_VILLAGESQL_ERROR;
  }
  return DB_SUCCESS;
}

static dberr_t mark_impl(const dict_index_t *index, uint32_t index_field_num,
                         const dfield_t *data_field, trx_id_t trx_id,
                         bool del_mark) {
  if (!data_field->is_extended() || dfield_is_null(data_field)) {
    return DB_SUCCESS;
  }
  // We delete mark each extended column in a separate mtr.
  log_free_check();
  mtr_t mtr;
  mtr_start(&mtr);

  dict_field_t *ind_field = index->get_field(index_field_num);
  dict_col_t *col = ind_field->col;
  ut_a(col->stored_by_extn());

  // Get the column reference from the field
  Custom_column::Ref ref_val = data_field->get_extended_ref();

  if (ref_val == Custom_column::EMPTY_REF) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Delete: Invalid custom column reference.";
    return DB_VILLAGESQL_ERROR;
  }
  dberr_t error = mark_in_column_store(col, &mtr, trx_id, ref_val, del_mark);
  mtr_commit(&mtr);

  return error;
}

dberr_t Custom_column::mark_delete(const dict_table_t *table, trx_id_t trx_id,
                                   const upd_t *upd, const dtuple_t *row_entry,
                                   bool del_mark) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }

  const dict_index_t *index = table->first_index();
  ut_a(index->is_clustered());

  dberr_t error = DB_SUCCESS;
  uint32_t num_fields = (upd == nullptr) ? index->n_fields : upd->n_fields;

  for (uint32_t i = 0; i < num_fields; i++) {
    // Get the index in row tuple
    uint32_t index_i = (upd == nullptr) ? i : upd->fields[i].field_no;

    dict_field_t *ind_field = index->get_field(index_i);
    uint32_t row_i = ind_field->col->ind;

    const dfield_t *field = dtuple_get_nth_field(row_entry, row_i);

    // Mark the column as deleted in the extended column store
    error = mark_impl(index, index_i, field, trx_id, del_mark);

    if (error != DB_SUCCESS) {
      log_extended_error("Delete", nullptr, index, index_i);
      break;
    }
  }
  return error;
}

dberr_t Custom_column::update(const dict_table_t *table, trx_id_t trx_id,
                              btr_pcur_t *pcur, upd_t *update, ulint *offsets,
                              mem_heap_t **heap) {
  if (!table->has_extended_storage || !update->update_extended) {
    return DB_SUCCESS;
  }
  const dict_index_t *index = table->first_index();
  ut_a(index->is_clustered());

  dberr_t error = DB_SUCCESS;

  for (uint32_t i = 0; i < update->n_fields; i++) {
    upd_field_t *upd_field = upd_get_nth_field(update, i);
    uint32_t field_num = upd_field->field_no;

    // Step-1: Delete old columns.
    const dfield_t *old_field = &(upd_field->old_val);

    error = mark_impl(index, field_num, old_field, trx_id, true);

    if (error != DB_SUCCESS) {
      log_extended_error("Update[Delete]", nullptr, index, field_num);
      break;
    }

    // Step-2: Insert new columns and update row references.
    dfield_t *new_field = &(upd_field->new_val);

    error =
        insert_impl(index, field_num, new_field, trx_id, pcur, offsets, heap);
    if (error != DB_SUCCESS) {
      log_extended_error("Update[Insert]", nullptr, index, field_num);
      break;
    }
  }
  return error;
}

static dberr_t purge_in_column_store(const dict_col_t *col, mtr_t *mtr,
                                     trx_id_t trx_id, Custom_column::Ref ref) {
  ut_a(col->stored_by_extn());

  auto &custom_column = col->custom_column;
  if (!custom_column->storage_ctx()) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Purge: Uninitialized custom column store.";
    return DB_VILLAGESQL_ERROR;
  }
  const auto &intf = custom_column->storage_interface();
  char error_msg[Custom_column::ERROR_MSG_SIZE] = {};

  ut_ad(intf);
  bool failed = !intf || intf->purge(custom_column->storage_ctx(), mtr, trx_id,
                                     ref, error_msg, sizeof(error_msg));

  error_msg[sizeof(error_msg) - 1] = '\0';
  if (failed) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Purge: Error freeing data in custom column store: "
        << error_msg;
    return DB_VILLAGESQL_ERROR;
  }
  return DB_SUCCESS;
}

dberr_t Custom_column::purge_updated(const dict_table_t *table, trx_id_t trx_id,
                                     const upd_t *upd) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }
  const dict_index_t *index = table->first_index();
  ut_a(index->is_clustered());

  dberr_t error = DB_SUCCESS;

  for (uint32_t i = 0; i < upd->n_fields; i++) {
    // Get the index in row tuple.
    uint32_t index_i = upd->fields[i].field_no;

    dict_field_t *ind_field = index->get_field(index_i);
    dfield_t *data_field = &upd->fields[i].new_val;

    // Not an extended field or data value is NULL
    if (!data_field->is_extended() || dfield_is_null(data_field)) {
      continue;
    }

    Custom_column::Ref ref_val = data_field->get_extended_ref();
    // Extended reference is empty. The extended column data is not inserted
    // yet or is already freed. This is not expected in purge flow but we
    // ignore in release mode.
    ut_ad(ref_val != Custom_column::EMPTY_REF);
    if (ref_val == Custom_column::EMPTY_REF) {
      continue;
    }
    log_free_check();
    mtr_t mtr;
    mtr_start(&mtr);

    error = purge_in_column_store(ind_field->col, &mtr, trx_id, ref_val);

    mtr_commit(&mtr);

    if (error != DB_SUCCESS) {
      log_extended_error("Remove Updated", nullptr, index, index_i);
      break;
    }
  }
  return error;
}

static dberr_t purge_impl(const dict_table_t *table, const upd_t *upd,
                          const dtuple_t *old_row, const dtuple_t *new_row,
                          btr_pcur_t *pcur, mem_heap_t *&heap,
                          ulint *&offsets) {
  ut_ad(table->has_extended_storage);

  const dict_index_t *index = table->first_index();
  ut_a(index->is_clustered());

  dberr_t error = DB_SUCCESS;

  const auto *trx_col = table->get_sys_col(DATA_TRX_ID);
  auto *trx_id_field = dtuple_get_nth_field(new_row, trx_col->ind);

  // Field length must match with Transaction ID length
  ut_a(dfield_get_len(trx_id_field) == DATA_TRX_ID_LEN);
  trx_id_t trx_id =
      trx_read_trx_id(static_cast<const byte *>(dfield_get_data(trx_id_field)));

  // Valid Transaction ID
  ut_ad(trx_id != TRX_ID_MAX);
  uint32_t num_fields = (upd == nullptr) ? index->n_fields : upd->n_fields;

  for (uint32_t i = 0; i < num_fields; i++) {
    // Get the index in row tuple.
    uint32_t index_i = (upd == nullptr) ? i : upd->fields[i].field_no;

    dict_field_t *ind_field = index->get_field(index_i);
    uint32_t row_i = ind_field->col->ind;

    dfield_t *data_field = dtuple_get_nth_field(new_row, row_i);

    // Not an extended field or data value is NULL
    if (!data_field->is_extended() || dfield_is_null(data_field)) {
      continue;
    }

    Custom_column::Ref ref_val = data_field->get_extended_ref();
    // Extended reference is empty. The extended column data is not inserted
    // yet or is already freed. This a valid intermediate storage state when
    // insert or rollback is in progress.
    if (ref_val == Custom_column::EMPTY_REF) {
      continue;
    }

    // For update, check the old row to verify if the reference value is
    // updated yet. If the record still points to the old reference skip
    // purging.
    if (old_row != nullptr) {
      ut_ad(upd);
      dfield_t *old_field = dtuple_get_nth_field(old_row, row_i);
      if (old_field->is_extended() && !dfield_is_null(old_field)) {
        Custom_column::Ref old_ref = old_field->get_extended_ref();
        if (ref_val == old_ref) continue;
      }
    }

    log_free_check();
    mtr_t mtr;
    mtr_start(&mtr);

    ut_a(pcur->restore_position(BTR_MODIFY_LEAF, &mtr, UT_LOCATION_HERE));
    ut_a(pcur->m_rel_pos == BTR_PCUR_ON);

    // 1. Remove Extended column
    error = purge_in_column_store(ind_field->col, &mtr, trx_id, ref_val);

    if (error != DB_SUCCESS) {
      mtr_commit(&mtr);
      log_extended_error("Remove", nullptr, index, index_i);
      break;
    }

    // 2. Set B-Tree record reference empty
    rec_t *rec = pcur->get_rec();

    if (offsets == nullptr) {
      offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED,
                                UT_LOCATION_HERE, &heap);
    } else {
      rec_offs_make_valid(rec, index, offsets);
      ut_ad(rec_offs_validate(rec, index, offsets));
    }

    ref_val = Custom_column::EMPTY_REF;
    ulint ref_len = 0;
    ulint ref_off = rec_get_nth_field_offs(index, offsets, index_i, &ref_len);
    ut_a(ref_len == data_field->extended_ref_size());

    mlog_write_ull(rec + ref_off, ref_val, &mtr);
    mtr_commit(&mtr);
  }

  return error;
}

dberr_t Custom_column::purge_at_pcur(const dict_table_t *table,
                                     btr_pcur_t *pcur) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }

  const dict_index_t *index = table->first_index();
  ut_a(index->is_clustered());

  mtr_t mtr;
  ulint *offsets = nullptr;
  mem_heap_t *heap = nullptr;

  // Build row entry since it's not available during purge.
  mtr_start(&mtr);
  ut_a(pcur->restore_position(BTR_MODIFY_LEAF, &mtr, UT_LOCATION_HERE));
  ut_a(pcur->m_rel_pos == BTR_PCUR_ON);

  rec_t *rec = pcur->get_rec();
  offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &heap);

  // TODO(villagesql-performance): Optimize: We could exclude copying column
  // data other than extended columns.
  const dtuple_t *row_entry =
      row_build(ROW_COPY_DATA, index, rec, offsets, nullptr, nullptr, nullptr,
                nullptr, heap);
  mtr_commit(&mtr);
  dberr_t error =
      purge_impl(table, nullptr, nullptr, row_entry, pcur, heap, offsets);
  if (heap != nullptr) {
    mem_heap_free(heap);
  }
  return error;
}

dberr_t Custom_column::rollback_inserted(const dict_table_t *table,
                                         const dtuple_t *row_entry,
                                         btr_pcur_t *pcur) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }
  mem_heap_t *heap = nullptr;
  ulint *offsets = nullptr;
  dberr_t error =
      purge_impl(table, nullptr, nullptr, row_entry, pcur, heap, offsets);
  if (heap != nullptr) {
    mem_heap_free(heap);
  }
  return error;
}

dberr_t Custom_column::rollback_updated(const dict_table_t *table, ulint type,
                                        const upd_t *upd, trx_id_t undo_trx_id,
                                        const dtuple_t *undo_row,
                                        const dtuple_t *cur_row,
                                        btr_pcur_t *pcur) {
  if (!table->has_extended_storage) {
    return DB_SUCCESS;
  }
  dberr_t err = DB_SUCCESS;
  mem_heap_t *heap = nullptr;
  ulint *offsets = nullptr;

  if (type == TRX_UNDO_UPD_EXIST_REC) {
    // Free new version(post-update) of extended columns.
    err = purge_impl(table, upd, undo_row, cur_row, pcur, heap, offsets);

    if (err == DB_SUCCESS) {
      // Remove Delete mark from old version(pre-update) of updated columns.
      err = mark_delete(table, undo_trx_id, upd, undo_row, false);
    }
  } else if (type == TRX_UNDO_DEL_MARK_REC) {
    // Remove Delete mark from all columns.
    err = mark_delete(table, undo_trx_id, nullptr, cur_row, false);

  } else {
    ut_a(type == TRX_UNDO_UPD_DEL_REC);
    // Free new version(post-update) of extended columns.
    err = purge_impl(table, upd, nullptr, cur_row, pcur, heap, offsets);
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }
  return err;
}

dberr_t Custom_column::fetch(const dict_table_t *table, const dict_col_t *col,
                             byte *dest, ulint dest_len, const byte *src,
                             ulint src_len) {
  ut_a(table->has_extended_storage);
  ut_a(col->stored_by_extn());

  ut_a(src_len == dfield_t::extended_ref_size());
  Custom_column::Ref ref_val = mach_read_from_8(src);

  auto &custom_column = col->custom_column;
  if (!custom_column->storage_ctx()) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Fetch: Uninitialized custom column store.";
    memset(dest, 0, dest_len);
    return DB_VILLAGESQL_ERROR;
  }
  bool deleted = false;
  Custom_column::TrxRef trx_ref = 0;
  Custom_column::Data rowid_prefix{};
  Custom_column::Data col_data{};

  const auto &intf = custom_column->storage_interface();
  ut_ad(intf);

  mtr_t mtr;
  mtr_start(&mtr);

  char error_msg[Custom_column::ERROR_MSG_SIZE] = {};
  bool failed =
      !intf || intf->select(custom_column->storage_ctx_, &mtr, ref_val,
                            &col_data, &rowid_prefix, &trx_ref, &deleted,
                            error_msg, sizeof(error_msg));
  error_msg[sizeof(error_msg) - 1] = '\0';
  // Ignore transaction reference and delete mark here. Those outputs would be
  // useful for a column only scan e.g. HNSW index scan on a vector column.
  if (!failed && col_data.length != dest_len) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Fetch: Invalid custom data length: " << col_data.length
        << ", expected length: " << dest_len;
    failed = true;
  }

  if (failed) {
    mtr_commit(&mtr);
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Error fetching data from custom column store: "
        << error_msg;
    log_extended_error("Fetch", table, nullptr, col->ind);

    memset(dest, 0, dest_len);
    return DB_VILLAGESQL_ERROR;
  }
  memcpy(dest, col_data.data, col_data.length);

  mtr_commit(&mtr);
  return DB_SUCCESS;
}

dberr_t Custom_column::fetch_for_bulk_ddl(const dict_index_t *new_index,
                                          const dict_index_t *old_index,
                                          const ulint *col_map, size_t n_fields,
                                          dfield_t *fields, mem_heap_t *heap) {
  ut_a(old_index != nullptr);

  auto fetch_at = [&](uint32_t field_pos, const dict_col_t *col) -> dberr_t {
    auto *data = static_cast<const byte *>(dfield_get_data(&fields[field_pos]));
    ulint len = dfield_get_len(&fields[field_pos]);

    auto err = allocate_fetch(old_index->table, col, data, len, heap);
    if (err != DB_SUCCESS) {
      ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
          << "InnoDB: DDL rebuild: Custom column allocate and fetch failed";
      return err;
    }

    dfield_set_data(&fields[field_pos], data, len);
    return DB_SUCCESS;
  };

  // No col_map: old and new tables are the same. Position i is valid in both
  // old and new index; fetch each extended field at its current position.
  if (col_map == nullptr) {
    for (uint32_t i = 0; i < n_fields; i++) {
      if (!fields[i].is_extended()) continue;
      auto err = fetch_at(i, old_index->get_field(i)->col);
      if (err != DB_SUCCESS) return err;
    }
    return DB_SUCCESS;
  }

  // col_map is set: columns may have moved positions. Iterate old user columns,
  // find extended ones, map to new index position, and fetch.
  const dict_table_t *old_table = old_index->table;
  const ulint n_old_user_cols = old_table->n_cols - DATA_N_SYS_COLS;

  for (ulint old_col_ind = 0; old_col_ind < n_old_user_cols; old_col_ind++) {
    const ulint new_col_ind = col_map[old_col_ind];
    if (new_col_ind == ULINT_UNDEFINED) continue;

    const dict_col_t *old_col = old_table->get_col(old_col_ind);
    if (!old_col->stored_by_extn()) continue;

    // Locate the field position in the new index for this column.
    uint32_t new_field_pos = UINT32_MAX;
    for (uint32_t i = 0; i < n_fields; i++) {
      if (new_index->get_field(i)->col->ind == new_col_ind) {
        new_field_pos = i;
        break;
      }
    }
    ut_a(new_field_pos != UINT32_MAX);

    auto err = fetch_at(new_field_pos, old_col);
    if (err != DB_SUCCESS) return err;
  }

  return DB_SUCCESS;
}

dberr_t Custom_column::allocate_fetch(const dict_table_t *table,
                                      const dict_col_t *col, const byte *&data,
                                      ulint &data_len, mem_heap_t *heap) {
  // data_len > extended_ref_size() means the column data was already
  // materialized and no fetch is needed.
  if (!col->stored_by_extn() || data_len > dfield_t::extended_ref_size()) {
    return DB_SUCCESS;
  }
  ut_a(data_len == dfield_t::extended_ref_size());

  ulint new_len = col->len;
  auto *new_data = static_cast<byte *>(mem_heap_alloc(heap, new_len));

  if (new_data == nullptr) {
    ib::error(ER_VILLAGESQL_GENERIC_MESSAGE)
        << "InnoDB: Fetch: Failed to allocate " << new_len << " bytes";
    log_extended_error("Fetch", table, nullptr, col->ind);
    return DB_VILLAGESQL_ERROR;
  }

  // Copy the extended column data reference
  ut_a(new_len > data_len);
  memcpy(new_data, data, data_len);

  auto ext_data = new_data + data_len;
  auto ext_len = new_len - data_len;

  // Fetch extended column data using the input column reference
  dberr_t err = fetch(table, col, ext_data, ext_len, data, data_len);

  if (err != DB_SUCCESS) {
    return err;
  }
  data = new_data;
  data_len = new_len;

  return DB_SUCCESS;
}

}  // namespace innodb
}  // namespace villagesql
