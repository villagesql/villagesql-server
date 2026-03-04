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

#ifndef VILLAGESQL_ABI_STORAGE_H_
#define VILLAGESQL_ABI_STORAGE_H_

#include <stdbool.h>
#include <stdint.h>

// Storage Types and functions for column data stored by the extension.

// InnoDB tablespace reference.
typedef uint32_t vef_storage_space_ref_t;

// InnoDB storage reference: space and root page reference.
typedef uint64_t vef_storage_ref_t;

// InnoDB column reference: page reference and offset. The column is always
// stored in the tablespace referred by the storage reference.
typedef uint64_t vef_storage_col_ref_t;

// InnoDB transaction reference.
typedef uint64_t vef_storage_trx_ref_t;

// Mini-transaction context that can be passed to InnoDB storage interface
// acquiring pages.
typedef void *vef_storage_mtr_ref_t;

// Column data buffer and length
typedef struct {
  const unsigned char *data;
  uint32_t length;
} vef_storage_col_data_t;

// Empty column reference.
#define VEF_STORAGE_EMPTY_COLUMN_REF 0
// Alignment requirement for arena allocators
#define VEF_STORAGE_ALLOCATOR_ALIGNMENT 8

// Opaque storage arena context managed by VillageSQL/InnoDB.
typedef struct vef_storage_arena vef_storage_arena_t;

// Arena allocator callback type
// Arena Allocator function is implemented by VillageSQL. It returns a pointer
// to size bytes aligned to VEF_STORAGE_ALLOCATOR_ALIGNMENT. Author can use
// the Allocator callback to allocate memory for context. The allocated memory
// would be tracked and freed by InnoDB after the storage is dropped.
typedef void *(*vef_storage_arena_func_t)(vef_storage_arena_t *arena_ctx,
                                          uint32_t size);

// Partially opaque storage context with visible storage reference. Author may
// allocate a context object (using Arena allocator) of larger size with this
// structure as prefix to store additional private state during creation. The
// same context is passed to subsequent operations (insert, delete, etc.).
typedef struct {
  // Column storage reference
  vef_storage_ref_t ref;
} vef_storage_ctx_t;

// Storage function pointer types

// Create storage for column data.
// Parameters:
//   space_ref   - Tablespace in which to create the storage
//   trx_ref     - Transaction under which the creation runs
//   col_len     - Column storage length
//   arena_ctx   - Opaque arena handle passed to arena_alloc
//   arena_alloc - Allocator for the storage context; memory is freed when
//                 storage is dropped
//   storage     - Output: newly created storage context
//   error_msg   - Output: error description on failure (VEF_MAX_ERROR_LEN)
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_create_func_t)(
    vef_storage_space_ref_t space_ref, vef_storage_trx_ref_t trx_ref,
    uint32_t col_len, vef_storage_arena_t *arena_ctx,
    vef_storage_arena_func_t arena_alloc, vef_storage_ctx_t **storage,
    char *error_msg);

// Drop storage for column data.
// Parameters:
//   storage   - Storage context to drop
//   trx_ref   - Transaction under which the drop runs
//   error_msg - Output: error description on failure (VEF_MAX_ERROR_LEN)
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_drop_func_t)(vef_storage_ctx_t *storage,
                                             vef_storage_trx_ref_t trx_ref,
                                             char *error_msg);

// Load existing storage from a storage reference.
// Parameters:
//   storage_ref - Reference to the existing storage to load
//   arena_ctx   - Opaque arena handle passed to arena_alloc
//   arena_alloc - Allocator for the storage context; memory is freed when
//                 storage is dropped
//   storage     - Output: loaded storage context
//   error_msg   - Output: error description on failure (VEF_MAX_ERROR_LEN)
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_load_func_t)(
    vef_storage_ref_t storage_ref, vef_storage_arena_t *arena_ctx,
    vef_storage_arena_func_t arena_alloc, vef_storage_ctx_t **storage,
    char *error_msg);

// Insert column data into column storage along with transaction reference.
// Parameters:
//   storage      - Storage context to insert into
//   mctx         - Mini-transaction context
//   trx_ref      - Transaction performing the insert
//   col_data     - Column data to store
//   rowid_prefix - Row identifier prefix
//   col_ref      - Output: reference to the newly inserted column entry
//   error_msg    - Output: error description on failure (VEF_MAX_ERROR_LEN)
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_insert_func_t)(
    vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
    vef_storage_trx_ref_t trx_ref, vef_storage_col_data_t col_data,
    vef_storage_col_data_t rowid_prefix, vef_storage_col_ref_t *col_ref,
    char *error_msg);

// Fetch column data reference pointer acquiring appropriate latch.
// Parameters:
//   storage        - Storage context to read from
//   mctx           - Mini-transaction context
//   col_ref        - Reference to the column entry to fetch
//   col_data       - Output: fetched column data
//   rowid_prefix   - Output: row identifier prefix
//   trx_ref        - Output: transaction that last wrote this entry
//   delete_marked  - Output: whether the entry is marked for deletion
//   error_msg      - Output: error description on failure (VEF_MAX_ERROR_LEN)
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_select_func_t)(
    vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
    vef_storage_col_ref_t col_ref, vef_storage_col_data_t *col_data,
    vef_storage_col_data_t *rowid_prefix, vef_storage_trx_ref_t *trx_ref,
    bool *delete_marked, char *error_msg);

// Mark or unmark column as deleted by the transaction.
// Parameters:
//   storage     - Storage context
//   mctx        - Mini-transaction context
//   trx_ref     - Transaction performing the operation
//   col_ref     - Reference to the column entry to mark
//   delete_mark - true to mark as deleted, false to unmark
//   error_msg   - Output: error description on failure (VEF_MAX_ERROR_LEN)
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_mark_delete_func_t)(
    vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
    vef_storage_trx_ref_t trx_ref, vef_storage_col_ref_t col_ref,
    bool delete_mark, char *error_msg);

// Remove column only if it has the matching transaction reference.
// Parameters:
//   storage   - Storage context
//   mctx      - Mini-transaction context
//   trx_ref   - Transaction to match with the record to purge
//   col_ref   - Reference to the column entry to purge
//   error_msg - Output: error description on failure (VEF_MAX_ERROR_LEN)
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_purge_func_t)(vef_storage_ctx_t *storage,
                                              vef_storage_mtr_ref_t mctx,
                                              vef_storage_trx_ref_t trx_ref,
                                              vef_storage_col_ref_t col_ref,
                                              char *error_msg);

// Storage interface version constants.
#define VEF_STORAGE_INTF_VERSION_1 1

// Storage interface provided by an extension for custom column storage.
// The extension sets version to the storage interface version it implements.
// The server must check version before accessing any field introduced in
// later versions. The server reads `version` before accessing any field to
// determine which fields are present. New fields must always be appended at
// the end so that older structs remain a valid prefix.
typedef struct {
  // Must be set to one of the VEF_STORAGE_INTF_VERSION_* constants above.
  uint32_t version;

  // version >= VEF_STORAGE_INTF_VERSION_1
  vef_type_storage_create_func_t create;
  vef_type_storage_drop_func_t drop;
  vef_type_storage_load_func_t load;
  vef_type_storage_insert_func_t insert;
  vef_type_storage_select_func_t select;
  vef_type_storage_mark_delete_func_t mark_delete;
  vef_type_storage_purge_func_t purge;
} vef_type_storage_intf_t;

#endif  // VILLAGESQL_ABI_STORAGE_H_
