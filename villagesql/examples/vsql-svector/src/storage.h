// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#ifndef VILLAGESQL_EXAMPLES_VSQL_SVECTOR_SRC_STORAGE_H
#define VILLAGESQL_EXAMPLES_VSQL_SVECTOR_SRC_STORAGE_H

#include <villagesql/abi/storage.h>

namespace svector {

// ColumnStorage provides external storage for VECTOR columns.
// All methods are static and correspond to the VEF storage interface.
class ColumnStorage {
 public:
  // Create storage for vector column data
  static bool create(vef_storage_space_ref_t space_ref,
                     vef_storage_trx_ref_t trx_ref, uint32_t col_len,
                     vef_storage_arena_t *arena_ctx,
                     vef_storage_arena_func_t arena_alloc,
                     vef_storage_ctx_t **storage, char *error_msg,
                     uint32_t error_msg_len);

  // Drop storage for vector column data
  static bool drop(vef_storage_ctx_t *storage, vef_storage_trx_ref_t trx_ref,
                   char *error_msg, uint32_t error_msg_len);

  // Load existing storage from a storage reference
  static bool load(vef_storage_ref_t storage_ref,
                   vef_storage_arena_t *arena_ctx,
                   vef_storage_arena_func_t arena_alloc,
                   vef_storage_ctx_t **storage, char *error_msg,
                   uint32_t error_msg_len);

  // Insert vector data into storage
  static bool insert(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_trx_ref_t trx_ref,
                     vef_storage_col_data_t col_data,
                     vef_storage_col_data_t rowid_prefix,
                     vef_storage_col_ref_t *col_ref, char *error_msg,
                     uint32_t error_msg_len);

  // Fetch vector data from storage
  static bool select(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                     vef_storage_col_ref_t col_ref,
                     vef_storage_col_data_t *col_data,
                     vef_storage_col_data_t *rowid_prefix,
                     vef_storage_trx_ref_t *trx_ref, bool *delete_marked,
                     char *error_msg, uint32_t error_msg_len);

  // Mark or unmark vector as deleted
  static bool mark_delete(vef_storage_ctx_t *storage,
                          vef_storage_mtr_ref_t mctx,
                          vef_storage_trx_ref_t trx_ref,
                          vef_storage_col_ref_t col_ref, bool delete_mark,
                          char *error_msg, uint32_t error_msg_len);

  // Purge deleted vector from storage
  static bool purge(vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
                    vef_storage_trx_ref_t trx_ref,
                    vef_storage_col_ref_t col_ref, char *error_msg,
                    uint32_t error_msg_len);
};

}  // namespace svector

#endif  // VILLAGESQL_EXAMPLES_VSQL_SVECTOR_SRC_STORAGE_H
