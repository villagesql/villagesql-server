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

// TODO(villagesql-beta): Column storage ABI is not ready for external use.
// See storage_api.h for details.

#ifndef VILLAGESQL_ABI_PREVIEW_STORAGE_H_
#define VILLAGESQL_ABI_PREVIEW_STORAGE_H_

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ===========================================================================
// ABI Header Overview
// ===========================================================================
// This header contains two logical sections:
//
// Section 1: Custom Type Storage Interfaces
// -----------------------------------------
// Defines storage-related types and function signatures for column data of
// custom types implemented by an extension.

// If an extension chooses to manage the storage of columns for its custom
// types, it must implement these functions. They allow the extension to
// control how values of the custom type are stored, retrieved, and managed
// at the column level.
//
// Section 2: InnoDB Storage Engine Interfaces
// -------------------------------------------
// Defines the ABI interfaces implemented by InnoDB and exposed to extensions.
//
// These interfaces allow an extension to use InnoDB’s storage infrastructure
// to persist and access its data through the InnoDB buffer pool. Extensions
// are typically expected to call these functions through a language-specific
// wrapper layer (for example a C++ convenience API).
//
// InnoDB owns and manages the underlying storage structures such as segments
// and pages, including their layout and life cycle. The extension may define
// the format used to store its data within the page payload area (for example,
// for custom column storage or index structures).
// ===========================================================================

// Section 1: Custom Type Storage Interfaces
// -----------------------------------------
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
#define VEF_STORAGE_MIN_ALLOCATOR_ALIGNMENT 8

// Opaque storage arena context managed by VillageSQL/InnoDB.
typedef struct vef_storage_arena vef_storage_arena_t;

// Arena allocator callback type
// Arena Allocator function is implemented by VillageSQL. It returns a pointer
// to size bytes aligned to at least VEF_STORAGE_MIN_ALLOCATOR_ALIGNMENT.
// Author can use the Allocator callback to allocate memory for context. The
// allocated memory would be tracked and freed by InnoDB after the storage is
// dropped.
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
//   error_msg   - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_create_func_t)(
    vef_storage_space_ref_t space_ref, vef_storage_trx_ref_t trx_ref,
    uint32_t col_len, vef_storage_arena_t *arena_ctx,
    vef_storage_arena_func_t arena_alloc, vef_storage_ctx_t **storage,
    char *error_msg, uint32_t error_msg_len);

// Drop storage for column data.
// Parameters:
//   storage   - Storage context to drop
//   trx_ref   - Transaction under which the drop runs
//   error_msg - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_drop_func_t)(vef_storage_ctx_t *storage,
                                             vef_storage_trx_ref_t trx_ref,
                                             char *error_msg,
                                             uint32_t error_msg_len);

// Load existing storage from a storage reference.
// Parameters:
//   storage_ref - Reference to the existing storage to load
//   arena_ctx   - Opaque arena handle passed to arena_alloc
//   arena_alloc - Allocator for the storage context; memory is freed when
//                 storage is dropped
//   storage     - Output: loaded storage context
//   error_msg   - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_load_func_t)(
    vef_storage_ref_t storage_ref, vef_storage_arena_t *arena_ctx,
    vef_storage_arena_func_t arena_alloc, vef_storage_ctx_t **storage,
    char *error_msg, uint32_t error_msg_len);

// Insert column data into column storage along with transaction reference.
// Parameters:
//   storage      - Storage context to insert into
//   mctx         - Mini-transaction context
//   trx_ref      - Transaction performing the insert
//   col_data     - Column data to store
//   rowid_prefix - Row identifier prefix
//   col_ref      - Output: reference to the newly inserted column entry
//   error_msg    - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_insert_func_t)(
    vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
    vef_storage_trx_ref_t trx_ref, vef_storage_col_data_t col_data,
    vef_storage_col_data_t rowid_prefix, vef_storage_col_ref_t *col_ref,
    char *error_msg, uint32_t error_msg_len);

// Fetch column data reference pointer acquiring appropriate latch.
// Parameters:
//   storage        - Storage context to read from
//   mctx           - Mini-transaction context
//   col_ref        - Reference to the column entry to fetch
//   col_data       - Output: fetched column data
//   rowid_prefix   - Output: row identifier prefix
//   trx_ref        - Output: transaction that last wrote this entry
//   delete_marked  - Output: whether the entry is marked for deletion
//   error_msg      - Output: error description on failure
//   error_msg_len  - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_select_func_t)(
    vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
    vef_storage_col_ref_t col_ref, vef_storage_col_data_t *col_data,
    vef_storage_col_data_t *rowid_prefix, vef_storage_trx_ref_t *trx_ref,
    bool *delete_marked, char *error_msg, uint32_t error_msg_len);

// Mark or unmark column as deleted by the transaction.
// Parameters:
//   storage     - Storage context
//   mctx        - Mini-transaction context
//   trx_ref     - Transaction performing the operation
//   col_ref     - Reference to the column entry to mark
//   delete_mark - true to mark as deleted, false to unmark
//   error_msg   - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_mark_delete_func_t)(
    vef_storage_ctx_t *storage, vef_storage_mtr_ref_t mctx,
    vef_storage_trx_ref_t trx_ref, vef_storage_col_ref_t col_ref,
    bool delete_mark, char *error_msg, uint32_t error_msg_len);

// Remove column only if it has the matching transaction reference.
// Parameters:
//   storage   - Storage context
//   mctx      - Mini-transaction context
//   trx_ref   - Transaction to match with the record to purge
//   col_ref   - Reference to the column entry to purge
//   error_msg - Output: error description on failure
//   error_msg_len - length of error message buffer
// Returns false on success, true on error (writes to error_msg).
typedef bool (*vef_type_storage_purge_func_t)(vef_storage_ctx_t *storage,
                                              vef_storage_mtr_ref_t mctx,
                                              vef_storage_trx_ref_t trx_ref,
                                              vef_storage_col_ref_t col_ref,
                                              char *error_msg,
                                              uint32_t error_msg_len);

// Custom type storage interface version constants.
#define VEF_STORAGE_TYPE_INTF_VERSION_1 1
#define VEF_STORAGE_TYPE_INTF_VERSION VEF_STORAGE_TYPE_INTF_VERSION_1

// Storage interface provided by an extension for custom column storage.
// The extension sets version to the storage interface version it implements.
// The server must check version before accessing any field introduced in
// later versions. The server reads `version` before accessing any field to
// determine which fields are present. New fields must always be appended at
// the end so that older structs remain a valid prefix.
typedef struct {
  // Must be set to one of the VEF_STORAGE_TYPE_INTF_VERSION_* constants above.
  uint32_t version;

  // version >= VEF_STORAGE_TYPE_INTF_VERSION_1
  vef_type_storage_create_func_t create;
  vef_type_storage_drop_func_t drop;
  vef_type_storage_load_func_t load;
  vef_type_storage_insert_func_t insert;
  vef_type_storage_select_func_t select;
  vef_type_storage_mark_delete_func_t mark_delete;
  vef_type_storage_purge_func_t purge;
} vef_type_storage_intf_t;

// Section 2: InnoDB Storage Engine Interfaces
// -------------------------------------------
// TODO(villagesql-windows): Export symbols with __declspec(dllexport) on
// Windows so that extensions can link against these functions.

// Storage engine interface version constants.
#define VEF_STORAGE_SE_INTF_VERSION_1 1
#define VEF_STORAGE_SE_INTF_VERSION VEF_STORAGE_SE_INTF_VERSION_1
#define VEF_STORAGE_MINIMUM_SE_INTF_VERSION VEF_STORAGE_SE_INTF_VERSION_1

// ABI compatibility guarantee:
// 1. Extensions built against any ABI version in the range
//    [VEF_STORAGE_MINIMUM_SE_INTF_VERSION, VEF_STORAGE_SE_INTF_VERSION]
//    MUST continue to work with this server.
//
// 2. Extensions built against an ABI version greater than
//    VEF_STORAGE_SE_INTF_VERSION MUST fail to load.
//
// 3. Extensions built against an ABI version lower than
//    VEF_STORAGE_MINIMUM_SE_INTF_VERSION MUST fail to load.
//
// ABI change rules:
// 1. Functions:
//     A. Modify existing functions (signature, behavior, ownership): NEVER
//     B. Add new functions: Bump VEF_STORAGE_SE_INTF_VERSION
//
// 2. Structures/Enums:
//     A. Modify, remove, reorder existing fields: NEVER
//     B. Add new fields at the end only (with size/version guarding):
//        Bump VEF_STORAGE_SE_INTF_VERSION
//
// 3. Constants:
//     A. Constants appearing in this file affects ABI-visible memory layout,
//        buffer size, on-disk format, or extension-visible limit.
//
//     B. If a constant must be changed:
//        Bump VEF_STORAGE_MINIMUM_SE_INTF_VERSION and document the break.
//

// Constants defining the on-page storage format used by the storage engine.
// These values are part of the persistent page layout and must not change
// without an incompatible format revision.
#define VEF_STORAGE_SEGMENT_NUM_SEGMENTS_SIZE 1
#define VEF_STORAGE_SEGMENT_HEADER_SIZE 10
#define VEF_STORAGE_PAGE_HEADER_SIZE 38
#define VEF_STORAGE_PAGE_TRAILER_SIZE 8
// Page header offsets for linked list pointers
#define VEF_STORAGE_FIL_PAGE_PREV 8
#define VEF_STORAGE_FIL_PAGE_NEXT 12

// Sentinel values representing an invalid number, position.
#define VEF_STORAGE_PAGE_NUM_INVALID UINT32_MAX
#define VEF_STORAGE_PAGE_POS_INVALID UINT64_MAX

// Error codes returned by ABI functions
#define VEF_STORAGE_SUCCESS 0
#define VEF_STORAGE_ERROR_GENERAL 1
#define VEF_STORAGE_ERROR_OUT_OF_MEMORY 2
#define VEF_STORAGE_ERROR_OUT_OF_SPACE 3
#define VEF_STORAGE_ERROR_INVALID_ARGUMENT 4
#define VEF_STORAGE_ERROR_PAGE_LOAD 5

//=======================================
// VEF Storage Mini-Transaction Interface
//=======================================

// Start a mini-transaction (mtr)
// @param buffer - Pre-allocated buffer for mtr (can be stack or heap allocated)
// @param buffer_size - Size of the provided buffer in bytes
// @param required_size - Output: actual size needed if buffer is too small
// @param required_alignment - Output: required alignment for the buffer
// @param error_msg - Output: error description on failure
// @param error_msg_len - length of error message buffer
// @return Non-NULL mtr reference on success, NULL if buffer too small
//         (caller should allocate larger buffer and retry)
vef_storage_mtr_ref_t vef_storage_mtr_start(void *buffer, uint32_t buffer_size,
                                            uint32_t *required_size,
                                            uint32_t *required_alignment,
                                            char *error_msg,
                                            uint32_t error_msg_len);

// Commit a mini-transaction, making all changes durable
// @param ref - Mini-transaction reference from vef_storage_mtr_start
void vef_storage_mtr_commit(vef_storage_mtr_ref_t ref);

//==============================
// VEF Storage Segment Interface
//==============================
// Page number
typedef uint32_t vef_storage_page_num_t;

// Buffer pool block reference. The block reference remains valid only while
// the page is latched.
typedef void *vef_storage_block_ref_t;

// Create storage segments within a tablespace
// Creates num_segments segments and returns the root page number.
// The root page contains headers for all created segments.
// @param space_ref - Tablespace identifier
// @param num_segments - Number of segments to create (1-255)
// @param trx_ref - Transaction ID for DDL logging
// @param root_page_num_p - Output: root page number containing segment headers
// @param error_msg - Output: error description on failure
// @param error_msg_len - length of error message buffer
// @return VEF_STORAGE_SUCCESS on success, error code on failure
int vef_storage_segment_create(vef_storage_space_ref_t space_ref,
                               uint8_t num_segments,
                               vef_storage_trx_ref_t trx_ref,
                               vef_storage_page_num_t *root_page_num_p,
                               char *error_msg, uint32_t error_msg_len);

// Drop storage segments associated with a root page
// Frees all pages allocated to the segments on the root page.
// @param space_ref - Tablespace identifier
// @param trx_ref - Transaction ID for DDL logging
// @param root_page_num - Root page number from vef_storage_segment_create
// @param error_msg - Output: error description on failure
// @param error_msg_len - length of error message buffer
// @return VEF_STORAGE_SUCCESS on success, error code on failure
int vef_storage_segment_drop(vef_storage_space_ref_t space_ref,
                             vef_storage_trx_ref_t trx_ref,
                             vef_storage_page_num_t root_page_num,
                             char *error_msg, uint32_t error_msg_len);

//===========================
// VEF Storage Page Interface
//===========================

// Offset within a page.
typedef uint16_t vef_storage_page_offset_t;

// Page latch types.
typedef int32_t vef_storage_latch_t;
#define VEF_STORAGE_PAGE_LATCH_NONE 0
#define VEF_STORAGE_PAGE_LATCH_SHARED 1
#define VEF_STORAGE_PAGE_LATCH_SHARED_EXCLUSIVE 2
#define VEF_STORAGE_PAGE_LATCH_EXCLUSIVE 3

// Integer byte sizes for page read/write operations
typedef int32_t vef_storage_integer_bytes_t;
#define VEF_STORAGE_PAGE_INT_1BYTE 0
#define VEF_STORAGE_PAGE_INT_2BYTES 1
#define VEF_STORAGE_PAGE_INT_4BYTES 2
#define VEF_STORAGE_PAGE_INT_8BYTES 3

// Load an existing page into the buffer pool and latch it
// @param block_p - Output: opaque buffer pool block reference
// @param position_p - Output: position for later latch/release
// @param data_p - Output: pointer to page data (valid while latched)
// @param data_size_p - Output: size of page data in bytes
// @param space_ref - Tablespace identifier
// @param page_num - Page number to load
// @param latch_mode - Latch type (SHARED, EXCLUSIVE, etc.)
// @param mtr_ref - Mini-transaction reference
// @param error_msg - Output: error description on failure
// @param error_msg_len - length of error message buffer
// @return VEF_STORAGE_SUCCESS on success, error code on failure
int vef_storage_page_load(vef_storage_block_ref_t *block_p,
                          uint64_t *position_p, unsigned char **data_p,
                          uint32_t *data_size_p,
                          vef_storage_space_ref_t space_ref,
                          vef_storage_page_num_t page_num,
                          vef_storage_latch_t latch_mode,
                          vef_storage_mtr_ref_t mtr_ref, char *error_msg,
                          uint32_t error_msg_len);

// Allocate a new page from a segment, load it into the buffer pool, and set
// its page type
// @param block_p - Output: opaque buffer pool block reference
// @param page_num_p - Output: page number of the allocated page
// @param data_p - Output: pointer to page data (valid while latched)
// @param data_size_p - Output: size of page data in bytes
// @param segment_header - Pointer to segment header to allocate from
// @param mtr_ref - Mini-transaction reference
// @param error_msg - Output: error description on failure
// @param error_msg_len - length of error message buffer
// @return VEF_STORAGE_SUCCESS on success, error code on failure
int vef_storage_page_allocate_and_load(vef_storage_block_ref_t *block_p,
                                       vef_storage_page_num_t *page_num_p,
                                       unsigned char **data_p,
                                       uint32_t *data_size_p,
                                       unsigned char *segment_header,
                                       vef_storage_mtr_ref_t mtr_ref,
                                       char *error_msg, uint32_t error_msg_len);

// Latch a page that was loaded with VEF_STORAGE_PAGE_LATCH_NONE
// @param block - Buffer pool block reference from vef_storage_page_load
// @param position - Position handle from vef_storage_page_load
// @param latch - requested latch type, actual latch type acquired
// @param mtr_ref - Mini-transaction reference
// @param error_msg - Output: error description on failure
// @param error_msg_len - length of error message buffer
// @return VEF_STORAGE_SUCCESS on success, error code on failure
int vef_storage_page_latch(vef_storage_block_ref_t block, uint64_t position,
                           vef_storage_latch_t latch,
                           vef_storage_mtr_ref_t mtr_ref, char *error_msg,
                           uint32_t error_msg_len);

// Release a page latch acquired via vef_storage_page_load or
// vef_storage_page_latch
// @param block - Buffer pool block reference
// @param position - Position handle from vef_storage_page_load
// @param mtr_ref - Mini-transaction reference
// @param error_msg - Output: error description on failure
// @param error_msg_len - length of error message buffer
// @return VEF_STORAGE_SUCCESS on success, error code on failure
int vef_storage_page_release(vef_storage_block_ref_t block, uint64_t position,
                             vef_storage_mtr_ref_t mtr_ref, char *error_msg,
                             uint32_t error_msg_len);

// Get the logical page size for a tablespace
// @param space_ref - Tablespace identifier
// @return Page size in bytes (typically 4K, 8K, 16K, 32K, or 64K)
uint32_t vef_storage_page_get_size(vef_storage_space_ref_t space_ref);

// Write an integer to a page with WAL/redo logging
// @param block - Buffer pool block reference (page must be latched)
// @param offset - Byte offset within page (must be in valid range)
// @param value - Integer value to write
// @param bytes - Size: VEF_STORAGE_PAGE_INT_1BYTE/2BYTES/4BYTES/8BYTES
// @param mtr_ref - Mini-transaction reference
void vef_storage_page_write_integer(vef_storage_block_ref_t block,
                                    vef_storage_page_offset_t offset,
                                    uint64_t value,
                                    vef_storage_integer_bytes_t bytes,
                                    vef_storage_mtr_ref_t mtr_ref);

// Write a byte string to a page with WAL/redo logging
// @param block - Buffer pool block reference (page must be latched)
// @param offset - Byte offset within page
// @param str - Byte string to write
// @param len - Length of string in bytes
// @param mtr_ref - Mini-transaction reference
void vef_storage_page_write_string(vef_storage_block_ref_t block,
                                   vef_storage_page_offset_t offset,
                                   const unsigned char *str, uint32_t len,
                                   vef_storage_mtr_ref_t mtr_ref);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_STORAGE_H_
