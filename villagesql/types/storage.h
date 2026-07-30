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

// StorageInterface wraps the storage function pointers from
// vef_type_storage_intf_t. It lives in TypeDescriptor and is constructed from
// the ABI struct when an extension registers column storage for a custom type.

#ifndef VILLAGESQL_TYPES_STORAGE_H_
#define VILLAGESQL_TYPES_STORAGE_H_

#include <cstdint>

#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"

namespace villagesql {

// C++ wrapper around the storage functions from ABI interface
// vef_type_storage_intf_t.
class StorageInterface {
 public:
  using CreateFunc = vef_type_storage_create_func_t;
  using DropFunc = vef_type_storage_drop_func_t;
  using LoadFunc = vef_type_storage_load_func_t;
  using UnloadFunc = vef_type_storage_unload_func_t;
  using InsertFunc = vef_type_storage_insert_func_t;
  using SelectFunc = vef_type_storage_select_func_t;
  using MarkDeleteFunc = vef_type_storage_mark_delete_func_t;
  using PurgeFunc = vef_type_storage_purge_func_t;

  // Function pointers are from VEF_STORAGE_TYPE_INTF_VERSION_1. If new
  // function pointers are added in future versions, add a version check
  // before assigning them.
  explicit StorageInterface(const vef_type_storage_intf_t &intf)
      : create_(intf.create),
        drop_(intf.drop),
        load_(intf.load),
        unload_(intf.unload),
        insert_(intf.insert),
        select_(intf.select),
        mark_delete_(intf.mark_delete),
        purge_(intf.purge) {}

  bool create(vef_storage_space_ref_t space_ref, vef_storage_trx_ref_t trx_ref,
              uint32_t col_len, vef_storage_arena_t *arena_ctx,
              vef_storage_arena_func_t arena_alloc, vef_storage_ctx_t **storage,
              char *error_msg, uint32_t error_msg_len) const {
    return create_(space_ref, trx_ref, col_len, arena_ctx, arena_alloc, storage,
                   error_msg, error_msg_len);
  }

  bool drop(vef_storage_ctx_t *storage, vef_storage_trx_ref_t trx_ref,
            char *error_msg, uint32_t error_msg_len) const {
    return drop_(storage, trx_ref, error_msg, error_msg_len);
  }

  bool load(vef_storage_ref_t storage_ref, vef_storage_arena_t *arena_ctx,
            vef_storage_arena_func_t arena_alloc, vef_storage_ctx_t **storage,
            char *error_msg, uint32_t error_msg_len) const {
    return load_(storage_ref, arena_ctx, arena_alloc, storage, error_msg,
                 error_msg_len);
  }

  bool unload(vef_storage_ctx_t *storage, char *error_msg,
              uint32_t error_msg_len) const {
    if (unload_ == nullptr) return false;
    return unload_(storage, error_msg, error_msg_len);
  }

  bool insert(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
              vef_storage_trx_ref_t trx_ref, vef_storage_col_data_t col_data,
              vef_storage_col_data_t rowid_prefix,
              vef_storage_col_ref_t *col_ref, char *error_msg,
              uint32_t error_msg_len) const {
    return insert_(storage, mctx, trx_ref, col_data, rowid_prefix, col_ref,
                   error_msg, error_msg_len);
  }

  bool select(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
              vef_storage_col_ref_t col_ref, vef_storage_col_data_t *col_data,
              vef_storage_col_data_t *rowid_prefix,
              vef_storage_trx_ref_t *trx_ref, bool *delete_marked,
              char *error_msg, uint32_t error_msg_len) const {
    return select_(storage, mctx, col_ref, col_data, rowid_prefix, trx_ref,
                   delete_marked, error_msg, error_msg_len);
  }

  bool mark_delete(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                   vef_storage_trx_ref_t trx_ref, vef_storage_col_ref_t col_ref,
                   bool delete_mark, char *error_msg,
                   uint32_t error_msg_len) const {
    return mark_delete_(storage, mctx, trx_ref, col_ref, delete_mark, error_msg,
                        error_msg_len);
  }

  bool purge(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
             vef_storage_trx_ref_t trx_ref, vef_storage_col_ref_t col_ref,
             char *error_msg, uint32_t error_msg_len) const {
    return purge_(storage, mctx, trx_ref, col_ref, error_msg, error_msg_len);
  }

 private:
  CreateFunc create_;
  DropFunc drop_;
  LoadFunc load_;
  UnloadFunc unload_;
  InsertFunc insert_;
  SelectFunc select_;
  MarkDeleteFunc mark_delete_;
  PurgeFunc purge_;
};

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_STORAGE_H_
